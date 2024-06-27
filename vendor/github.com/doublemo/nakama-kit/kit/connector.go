// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrQueueFull = errors.New("outgoing queue full")

const (
	ConnectorState_STOPED    = iota // stoped
	ConnectorState_WAITRETRY        // retry
	ConnectorState_ALIVE            // alive
)

type (
	Connector interface {
		ID() string
		Role() string
		Context() context.Context
		Consume(histroy []*pb.ResponseWriter) bool
		Write(msg *pb.ResponseWriter, opts ...ConnectorWriteOption) error
		Close()
		State() int
		RetryOk() []*pb.ResponseWriter
		CloseWithRetry()
	}

	LocalConnector struct {
		ctx               context.Context
		ctxCancelFn       context.CancelFunc
		id                string
		role              string
		conn              pb.Peer_StreamServer
		logger            *zap.Logger
		outgoingCh        chan *pb.ResponseWriter
		state             *atomic.Int32
		handler           ServerHandler
		signalCh          *atomic.Pointer[chan struct{}]
		waitMessages      []*pb.ResponseWriter
		waitCounter       *atomic.Int32
		outgoingQueueSize int
		allowRetry        bool
		retryTimeout      int
		sync.RWMutex
	}
)

func NewLocalConnector(ctx context.Context, logger *zap.Logger, id, role string, conn pb.Peer_StreamServer, outgoingQueueSize int, handler ServerHandler, allowRetry bool, retryTimeout int) Connector {
	connectorLogger := logger.With(zap.String("id", id))
	connectorLogger.Info("New connector connected", zap.String("role", role))
	ctx, ctxCancelFn := context.WithCancel(ctx)
	return &LocalConnector{
		ctx:               ctx,
		ctxCancelFn:       ctxCancelFn,
		logger:            connectorLogger,
		id:                id,
		role:              role,
		conn:              conn,
		outgoingCh:        make(chan *pb.ResponseWriter, outgoingQueueSize),
		state:             atomic.NewInt32(ConnectorState_ALIVE),
		handler:           handler,
		signalCh:          atomic.NewPointer[chan struct{}](nil),
		waitMessages:      make([]*pb.ResponseWriter, outgoingQueueSize),
		waitCounter:       atomic.NewInt32(0),
		outgoingQueueSize: outgoingQueueSize,
		allowRetry:        allowRetry,
		retryTimeout:      retryTimeout,
	}
}

func (s *LocalConnector) ID() string {
	return s.id
}

func (s *LocalConnector) Role() string {
	return s.role
}

func (s *LocalConnector) Context() context.Context {
	return s.ctx
}

func (s *LocalConnector) State() int {
	return int(s.state.Load())
}

func (s *LocalConnector) RetryOk() []*pb.ResponseWriter {
	state := s.state.Load()
	if state != ConnectorState_WAITRETRY {
		return nil
	}

	waitCounter := s.waitCounter.Load()
	waitMssages := make([]*pb.ResponseWriter, waitCounter)
	if waitCounter > 0 {
		s.Lock()
		copy(waitMssages[0:], s.waitMessages[0:waitCounter])
		s.Unlock()
	}

	signalCh := s.signalCh.Swap(nil)
	if signalCh != nil && *signalCh != nil {
		close(*signalCh)
	}
	return waitMssages
}

func (s *LocalConnector) Consume(histroy []*pb.ResponseWriter) bool {
	go s.processOutgoing()
	if histroy != nil {
		go func() {
			for _, msg := range histroy {
				if msg == nil {
					continue
				}

				if err := s.Write(msg); err != nil {
					s.logger.Warn("failed to send histroy messages", zap.Error(err))
				}
			}
		}()
	}

IncomingLoop:
	for {
		select {
		case <-s.ctx.Done():
			break IncomingLoop
		default:
		}

		payload, err := s.conn.Recv()
		if state := s.state.Load(); state == ConnectState_STOPED {
			break IncomingLoop

		}
		if err != nil {
			if !s.allowRetry {
				break IncomingLoop
			}

			if ok := s.waitRetry(); !ok {
				if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
					break IncomingLoop
				}
				s.logger.Warn("Error reading message from client", zap.Error(err))
				break IncomingLoop
			}
			return true
		}

		switch payload.Payload.(type) {
		case *pb.Request_Ping:
			if err := s.conn.Send(&pb.ResponseWriter{Payload: &pb.ResponseWriter_Pong{Pong: "PONG"}}); err != nil {
				s.logger.Warn("failed to send ping", zap.Error(err))
				break IncomingLoop
			}

		default:
			s.handler.NotifyMsg(s, payload)
		}
	}
	s.Close()
	return false
}

func (s *LocalConnector) waitRetry() bool {
	if state := s.state.Swap(ConnectorState_WAITRETRY); state != ConnectorState_ALIVE {
		return false
	}

	signalCh := make(chan struct{})
	if !s.signalCh.CompareAndSwap(nil, &signalCh) {
		return false
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second*time.Duration(s.retryTimeout))
	defer waitCancel()

	select {
	case <-s.ctx.Done():
		return false

	case <-waitCtx.Done():
		return false

	case <-signalCh:
		//
	}
	return true
}

func (s *LocalConnector) processOutgoing() {
OutgoingLoop:
	for {
		select {
		case <-s.ctx.Done():
			break OutgoingLoop

		case payload := <-s.outgoingCh:
			state := s.state.Load()
			switch state {
			case ConnectorState_STOPED:
				break OutgoingLoop

			case ConnectorState_WAITRETRY:
				if payload.Context["Cache-Control"] == "no-cache" {
					continue
				}

				idx := s.waitCounter.Load()
				s.Lock()
				s.waitMessages[idx] = payload
				s.Unlock()
				nsize := s.waitCounter.Inc()
				if nsize >= int32(s.outgoingQueueSize) {
					break OutgoingLoop
				}
				continue
			default:
			}

			if err := s.conn.Send(payload); err != nil {
				s.logger.Warn("Failed to set write deadline", zap.Error(err))
				break OutgoingLoop
			}
		}
	}
	s.Close()
}

func (s *LocalConnector) Close() {
	if state := s.state.Swap(ConnectState_STOPED); state == ConnectState_STOPED {
		return
	}

	s.logger.Info("Closed connector connection", zap.String("role", s.role))
	s.ctxCancelFn()
}

func (s *LocalConnector) CloseWithRetry() {
	if state := s.state.Swap(ConnectState_STOPED); state == ConnectState_STOPED {
		return
	}
	s.ctxCancelFn()
}

func (s *LocalConnector) Write(msg *pb.ResponseWriter, opts ...ConnectorWriteOption) error {
	for _, opt := range opts {
		opt(msg)
	}

	select {
	case s.outgoingCh <- msg:
		return nil
	default:
		// The outgoing queue is full, likely because the remote client can't keep up.
		// Terminate the connection immediately because the only alternative that doesn't block the server is
		// to start dropping messages, which might cause unexpected behaviour.
		s.logger.Warn("Could not write message, session outgoing queue full")
		// Close in a goroutine as the method can block
		go s.Close()
		return ErrQueueFull
	}
}
