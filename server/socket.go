// Copyright 2018 The Nakama Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/gofrs/uuid/v5"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

type (
	socketSYN struct {
		E1     int64
		E2     int64
		Format uint8
		Token  string
		Lang   string
		Status bool
	}

	socketHeader struct {
		Seq  uint64
		Size uint16
	}

	SocketServer struct {
		ctx         context.Context
		ctxCancelFn context.CancelFunc
		logger      *zap.Logger
		listener    *net.TCPListener
		connChan    chan *net.TCPConn
		counter     *atomic.Int64
		once        sync.Once

		config               Config
		sessionRegistry      SessionRegistry
		sessionCache         SessionCache
		statusRegistry       StatusRegistry
		matchmaker           Matchmaker
		tracker              Tracker
		metrics              Metrics
		runtime              *Runtime
		protojsonMarshaler   *protojson.MarshalOptions
		protojsonUnmarshaler *protojson.UnmarshalOptions
		pipeline             *Pipeline
	}
)

func StartSocketServer(logger *zap.Logger, config Config, sessionRegistry SessionRegistry, sessionCache SessionCache, statusRegistry StatusRegistry, matchmaker Matchmaker, tracker Tracker, metrics Metrics, runtime *Runtime, protojsonMarshaler *protojson.MarshalOptions, protojsonUnmarshaler *protojson.UnmarshalOptions, pipeline *Pipeline) *SocketServer {
	ctx, cancel := context.WithCancel(context.Background())

	s := &SocketServer{
		ctx:                  ctx,
		ctxCancelFn:          cancel,
		logger:               logger,
		connChan:             make(chan *net.TCPConn, 128),
		counter:              atomic.NewInt64(0),
		config:               config,
		sessionRegistry:      sessionRegistry,
		sessionCache:         sessionCache,
		statusRegistry:       statusRegistry,
		matchmaker:           matchmaker,
		tracker:              tracker,
		metrics:              metrics,
		runtime:              runtime,
		protojsonMarshaler:   protojsonMarshaler,
		protojsonUnmarshaler: protojsonUnmarshaler,
		pipeline:             pipeline,
	}

	resolveAddr, err := net.ResolveTCPAddr(config.GetSocket().Protocol, fmt.Sprintf("%v:%d", config.GetSocket().Address, config.GetSocket().Port-3))
	if err != nil {
		logger.Fatal("Socket server failed to resolve TCP address", zap.Error(err))
	}

	listener, err := net.ListenTCP(config.GetSocket().Protocol, resolveAddr)
	if err != nil {
		logger.Fatal("Socket server listener failed to start", zap.Error(err))
	}

	s.listener = listener
	go s.processConnection()

	logger.Info("Starting API server for TCP requests", zap.Int("port", config.GetSocket().Port-3))
	return s
}

func (s *SocketServer) processAccept() {
	for {
		conn, err := s.listener.AcceptTCP()
		if err != nil {
			if e, ok := err.(*net.OpError); !ok || e.Err.Error() != "use of closed network connection" {
				s.logger.Error("Socket server failed to accept connection", zap.Error(err))
			}
			return
		}

		select {
		case <-s.ctx.Done():
			return

		case s.connChan <- conn:

		default:
			s.logger.Warn("Could not accept conn, conn queue full")
			conn.Close()
		}
	}
}

func (s *SocketServer) processConnection() {
	go s.processAccept()

	sessionIdGen := uuid.NewGenWithHWAF(func() (net.HardwareAddr, error) {
		hash := NodeToHash(s.config.GetName())
		return hash[:], nil
	})

	for {
		select {
		case conn, ok := <-s.connChan:
			if !ok {
				return
			}

			s.counter.Inc()
			go func(conn *net.TCPConn) {
				defer func() {
					s.counter.Dec()
				}()

				conn.SetReadBuffer(s.config.GetSocket().ReadBufferSizeBytes)
				conn.SetWriteBuffer(s.config.GetSocket().WriteBufferSizeBytes)

				sessionID := uuid.Must(sessionIdGen.NewV1())
				// Mark the start of the session.
				s.metrics.CountWebsocketOpened(1)

				// Wrap the connection for application handling.
				session := NewSessionTcp(s.logger, s.config, sessionID, s.protojsonMarshaler, s.protojsonUnmarshaler, conn, s.sessionRegistry, s.sessionCache, s.statusRegistry, s.matchmaker, s.tracker, s.metrics, s.pipeline, s.runtime)

				// Add to the session registry.
				s.sessionRegistry.Add(session)

				// Allow the server to begin processing incoming messages from this session.
				session.Consume()

				// Mark the end of the session.
				s.metrics.CountWebsocketClosed(1)

			}(conn)

		case <-s.ctx.Done():
			return
		}
	}
}

func (s *SocketServer) Stop() {
	s.once.Do(func() {
		if s.ctxCancelFn == nil {
			return
		}

		s.ctxCancelFn()
		if err := s.listener.Close(); err != nil {
			s.logger.Warn("Failed to close listener", zap.Error(err))
		}

	})
}
