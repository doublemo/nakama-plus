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
	"crypto/rc4"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-common/runtime"
	"github.com/doublemo/nakama-plus/v3/internal/bytes"
	"github.com/doublemo/nakama-plus/v3/internal/dh"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var ErrSessionInvalidToken = errors.New("missing or invalid token")

type sessionTcp struct {
	sync.Mutex
	logger     *atomic.Pointer[zap.Logger]
	config     Config
	id         uuid.UUID
	format     *atomic.Uint32
	userID     *atomic.String
	username   *atomic.String
	vars       *atomic.Value
	expiry     *atomic.Int64
	clientIP   *atomic.String
	clientPort *atomic.String
	lang       *atomic.String
	seq        *atomic.Uint64

	ctx         context.Context
	ctxCancelFn context.CancelFunc

	protojsonMarshaler   *protojson.MarshalOptions
	protojsonUnmarshaler *protojson.UnmarshalOptions
	pingPeriodDuration   time.Duration
	pongWaitDuration     time.Duration
	writeWaitDuration    time.Duration
	encoder              *atomic.Pointer[rc4.Cipher]
	decoder              *atomic.Pointer[rc4.Cipher]

	sessionRegistry SessionRegistry
	sessionCache    SessionCache
	statusRegistry  StatusRegistry
	matchmaker      Matchmaker
	tracker         Tracker
	metrics         Metrics
	pipeline        *Pipeline
	runtime         *Runtime

	stopped                bool
	conn                   *net.TCPConn
	receivedMessageCounter int
	pingTimer              *time.Timer
	pingTimerCAS           *atomic.Uint32
	outgoingCh             chan []byte
	closeMu                sync.Mutex
}

func NewSessionTcp(logger *zap.Logger, config Config, sessionID uuid.UUID, protojsonMarshaler *protojson.MarshalOptions, protojsonUnmarshaler *protojson.UnmarshalOptions, conn *net.TCPConn, sessionRegistry SessionRegistry, sessionCache SessionCache, statusRegistry StatusRegistry, matchmaker Matchmaker, tracker Tracker, metrics Metrics, pipeline *Pipeline, runtime *Runtime) Session {
	sessionLogger := logger.With(zap.String("sid", sessionID.String()))

	sessionLogger.Info("New Socket session connected", zap.String("remote_address", conn.RemoteAddr().String()))

	ctx, ctxCancelFn := context.WithCancel(context.Background())
	s := &sessionTcp{
		logger:     atomic.NewPointer(sessionLogger),
		config:     config,
		id:         sessionID,
		format:     atomic.NewUint32(uint32(SessionFormatProtobuf)),
		userID:     atomic.NewString(""),
		username:   atomic.NewString(""),
		vars:       &atomic.Value{},
		expiry:     atomic.NewInt64(0),
		clientIP:   atomic.NewString(""),
		clientPort: atomic.NewString(""),
		lang:       atomic.NewString(""),
		seq:        atomic.NewUint64(0),

		ctx:         ctx,
		ctxCancelFn: ctxCancelFn,

		protojsonMarshaler:   protojsonMarshaler,
		protojsonUnmarshaler: protojsonUnmarshaler,
		pingPeriodDuration:   time.Duration(config.GetSocket().PingPeriodMs) * time.Millisecond,
		pongWaitDuration:     time.Duration(config.GetSocket().PongWaitMs) * time.Millisecond,
		writeWaitDuration:    time.Duration(config.GetSocket().WriteWaitMs) * time.Millisecond,
		encoder:              atomic.NewPointer[rc4.Cipher](nil),
		decoder:              atomic.NewPointer[rc4.Cipher](nil),

		sessionRegistry: sessionRegistry,
		sessionCache:    sessionCache,
		statusRegistry:  statusRegistry,
		matchmaker:      matchmaker,
		tracker:         tracker,
		metrics:         metrics,
		pipeline:        pipeline,
		runtime:         runtime,

		stopped:                false,
		conn:                   conn,
		receivedMessageCounter: config.GetSocket().PingBackoffThreshold,
		pingTimer:              time.NewTimer(time.Duration(config.GetSocket().PingPeriodMs) * time.Millisecond),
		pingTimerCAS:           atomic.NewUint32(1),
		outgoingCh:             make(chan []byte, config.GetSocket().OutgoingQueueSize),
	}

	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		s.clientIP.Store(addr.IP.String())
		s.clientPort.Store(strconv.Itoa(addr.Port))
	}
	s.vars.Store(make(map[string]string))
	return s
}

func (s *sessionTcp) Logger() *zap.Logger {
	return s.logger.Load()
}

func (s *sessionTcp) ID() uuid.UUID {
	return s.id
}

func (s *sessionTcp) UserID() uuid.UUID {
	return uuid.FromStringOrNil(s.userID.Load())
}

func (s *sessionTcp) ClientIP() string {
	return s.clientIP.Load()
}

func (s *sessionTcp) ClientPort() string {
	return s.clientPort.Load()
}

func (s *sessionTcp) Lang() string {
	return s.lang.Load()
}

func (s *sessionTcp) Context() context.Context {
	return s.ctx
}

func (s *sessionTcp) Username() string {
	return s.username.Load()
}

func (s *sessionTcp) SetUsername(username string) {
	s.username.Store(username)
}

func (s *sessionTcp) Vars() map[string]string {
	vars := make(map[string]string)
	for k, v := range s.vars.Load().(map[string]string) {
		vars[k] = v
	}
	return vars
}

func (s *sessionTcp) Expiry() int64 {
	return s.expiry.Load()
}

func (s *sessionTcp) Consume() {
	// Fire an event for session start.
	if fn := s.runtime.EventSessionStart(); fn != nil {
		fn(s.userID.String(), s.username.Load(), s.Vars(), s.expiry.Load(), s.id.String(), s.clientIP.Load(), s.clientPort.Load(), s.lang.Load(), time.Now().UTC().Unix())
	}

	logger := s.logger.Load()
	if err := s.conn.SetReadDeadline(time.Now().Add(s.pongWaitDuration)); err != nil {
		logger.Warn("Failed to set initial read deadline", zap.Error(err))
		s.Close("failed to set initial read deadline", runtime.PresenceReasonDisconnect)
		return
	}

	// Start a routine to process outbound messages.
	go s.processOutgoing()

	var (
		reason string
		data   []byte
	)

	header := make([]byte, 10)
IncomingLoop:
	for {

		_, err := io.ReadFull(s.conn, header)
		if err != nil {
			// Ignore underlying connection being shut down while read is waiting for data.
			if e, ok := err.(*net.OpError); !ok || e.Err.Error() != "use of closed network connection" {
				logger.Debug("Error reading message from client", zap.Error(err))
				reason = err.Error()
			}
			break
		}

		var mHeader socketHeader
		if err := bytes.UnPack(bytes.NewBytesBuffer(header), &mHeader); err != nil || int64(mHeader.Size) > s.config.GetSocket().MaxMessageSizeBytes {
			logger.Debug("Error reading header from client", zap.Error(err), zap.Uint16("size", mHeader.Size))
			reason = err.Error()
			break
		}

		data = make([]byte, mHeader.Size)
		n, err := io.ReadFull(s.conn, data)
		if err != nil {
			// Ignore underlying connection being shut down while read is waiting for data.
			if e, ok := err.(*net.OpError); !ok || e.Err.Error() != "use of closed network connection" {
				logger.Debug("Error reading message from client", zap.Error(err))
				reason = err.Error()
			}
			break
		}

		seq := s.seq.Inc()
		if seq != mHeader.Seq {
			logger.Debug("Error reading seq from client", zap.Error(err), zap.Uint64("seq", mHeader.Seq))
			reason = fmt.Sprintf("Error reading seq from client: %d != %d", seq, mHeader.Seq)
			break
		}

		s.receivedMessageCounter--
		if s.receivedMessageCounter <= 0 {
			s.receivedMessageCounter = s.config.GetSocket().PingBackoffThreshold
			if !s.maybeResetPingTimer() {
				// Problems resetting the ping timer indicate an error so we need to close the loop.
				reason = "error updating ping timer"
				break
			}
		}

		if decoder := s.decoder.Load(); decoder != nil {
			decoder.XORKeyStream(data, data)
		}

		if seq == 1 {
			if err := s.syn(data); err != nil {
				logger.Debug("Error SYN from client", zap.Error(err))
				reason = "error SYN from client"
				break
			}
			continue
		}

		if s.UserID().IsNil() {
			reason = "error userID is nil from client"
			break
		}

		request := &rtapi.Envelope{}
		switch s.Format() {
		case SessionFormatProtobuf:
			err = proto.Unmarshal(data, request)
		case SessionFormatJson:
			fallthrough
		default:
			err = s.protojsonUnmarshaler.Unmarshal(data, request)
		}
		if err != nil {
			// If the payload is malformed the client is incompatible or misbehaving, either way disconnect it now.
			logger.Warn("Received malformed payload", zap.Binary("data", data))
			reason = "received malformed payload"
			break
		}

		if request.Message != nil {
			switch request.Message.(type) {
			case *rtapi.Envelope_Ping:
				if s.receivedMessageCounter != s.config.GetSocket().PingBackoffThreshold {
					if !s.maybeResetPingTimer() {
						// Problems resetting the ping timer indicate an error so we need to close the loop.
						reason = "error updating ping timer"
						break IncomingLoop
					}
				}
			default:
			}
		}

		switch request.Cid {
		case "":
			if !s.pipeline.ProcessRequest(logger, s, request) {
				reason = "error processing message"
				break IncomingLoop
			}
		default:
			requestLogger := logger.With(zap.String("cid", request.Cid), zap.Int("size", n))
			if !s.pipeline.ProcessRequest(requestLogger, s, request) {
				reason = "error processing message"
				break IncomingLoop
			}
		}

		// Update incoming message metrics.
		s.metrics.Message(int64(n), false)
	}

	if reason != "" {
		// Update incoming message metrics.
		s.metrics.Message(int64(len(data)), true)
	}

	s.Close(reason, runtime.PresenceReasonDisconnect)
}

func (s *sessionTcp) maybeResetPingTimer() bool {
	// If there's already a reset in progress there's no need to wait.
	if !s.pingTimerCAS.CompareAndSwap(1, 0) {
		return true
	}
	defer s.pingTimerCAS.CompareAndSwap(0, 1)

	logger := s.logger.Load()
	s.Lock()
	if s.stopped {
		s.Unlock()
		return false
	}
	// CAS ensures concurrency is not a problem here.
	if !s.pingTimer.Stop() {
		select {
		case <-s.pingTimer.C:
		default:
		}
	}
	s.pingTimer.Reset(s.pingPeriodDuration)
	err := s.conn.SetReadDeadline(time.Now().Add(s.pongWaitDuration))
	s.Unlock()
	if err != nil {
		logger.Warn("Failed to set read deadline", zap.Error(err))
		s.Close("failed to set read deadline", runtime.PresenceReasonDisconnect)
		return false
	}
	return true
}

func (s *sessionTcp) processOutgoing() {
	var reason string

	logger := s.logger.Load()
OutgoingLoop:
	for {
		select {
		case <-s.ctx.Done():
			// Session is closing, close the outgoing process routine.
			break OutgoingLoop
		case <-s.pingTimer.C:
			// Periodically send pings.
			if msg, ok := s.pingNow(); !ok {
				// If ping fails the session will be stopped, clean up the loop.
				reason = msg
				break OutgoingLoop
			}
		case payload := <-s.outgoingCh:
			s.Lock()
			if s.stopped {
				// The connection may have stopped between the payload being queued on the outgoing channel and reaching here.
				// If that's the case then abort outgoing processing at this point and exit.
				s.Unlock()
				break OutgoingLoop
			}

			if encoder := s.encoder.Load(); encoder != nil {
				encoder.XORKeyStream(payload, payload)
			}

			// Process the outgoing message queue.
			if err := s.conn.SetWriteDeadline(time.Now().Add(s.writeWaitDuration)); err != nil {
				s.Unlock()
				logger.Warn("Failed to set write deadline", zap.Error(err))
				reason = err.Error()
				break OutgoingLoop
			}
			if _, err := s.conn.Write(s.pack(payload)); err != nil {
				s.Unlock()
				logger.Warn("Could not write message", zap.Error(err))
				reason = err.Error()
				break OutgoingLoop
			}
			s.Unlock()

			// Update outgoing message metrics.
			s.metrics.MessageBytesSent(int64(len(payload)))
		}
	}

	s.Close(reason, runtime.PresenceReasonDisconnect)
}

func (s *sessionTcp) pingNow() (string, bool) {
	s.Lock()
	if s.stopped {
		s.Unlock()
		return "", false
	}

	logger := s.logger.Load()
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.writeWaitDuration)); err != nil {
		s.Unlock()
		logger.Warn("Could not set write deadline to ping", zap.Error(err))
		return err.Error(), false
	}
	_, err := s.conn.Write(s.pack([]byte{}))
	s.Unlock()
	if err != nil {
		logger.Warn("Could not send ping", zap.Error(err))
		return err.Error(), false
	}

	return "", true
}

func (s *sessionTcp) Format() SessionFormat {
	return SessionFormat(s.format.Load())
}

func (s *sessionTcp) Send(envelope *rtapi.Envelope, reliable bool) error {
	var payload []byte
	var err error
	switch s.Format() {
	case SessionFormatProtobuf:
		payload, err = proto.Marshal(envelope)
	case SessionFormatJson:
		fallthrough
	default:
		if buf, err := s.protojsonMarshaler.Marshal(envelope); err == nil {
			payload = buf
		}
	}

	logger := s.logger.Load()
	if err != nil {
		logger.Warn("Could not marshal envelope", zap.Error(err))
		return err
	}

	if logger.Core().Enabled(zap.DebugLevel) {
		switch envelope.Message.(type) {
		case *rtapi.Envelope_Error:
			logger.Debug("Sending error message", zap.Binary("payload", payload))
		default:
			logger.Debug(fmt.Sprintf("Sending %T message", envelope.Message), zap.Any("envelope", envelope))
		}
	}

	return s.SendBytes(payload, reliable)
}

func (s *sessionTcp) SendBytes(payload []byte, reliable bool) error {
	// Attempt to queue messages and observe failures.
	select {
	case s.outgoingCh <- payload:
		return nil
	default:
		// The outgoing queue is full, likely because the remote client can't keep up.
		// Terminate the connection immediately because the only alternative that doesn't block the server is
		// to start dropping messages, which might cause unexpected behaviour.
		s.logger.Load().Warn("Could not write message, session outgoing queue full")
		// Close in a goroutine as the method can block
		go s.Close(ErrSessionQueueFull.Error(), runtime.PresenceReasonDisconnect)
		return ErrSessionQueueFull
	}
}

func (s *sessionTcp) CloseLock() {
	s.closeMu.Lock()
}

func (s *sessionTcp) CloseUnlock() {
	s.closeMu.Unlock()
}

func (s *sessionTcp) Close(msg string, reason runtime.PresenceReason, envelopes ...*rtapi.Envelope) {
	s.CloseLock()
	// Cancel any ongoing operations tied to this session.
	s.ctxCancelFn()
	s.CloseUnlock()

	s.Lock()
	if s.stopped {
		s.Unlock()
		return
	}
	s.stopped = true
	s.Unlock()

	logger := s.logger.Load()
	if logger.Core().Enabled(zap.DebugLevel) {
		logger.Info("Cleaning up closed client connection")
	}

	// When connection close originates internally in the session, ensure cleanup of external resources and references.
	if err := s.matchmaker.RemoveSessionAll(s.id.String()); err != nil {
		logger.Warn("Failed to remove all matchmaking tickets", zap.Error(err))
	}
	if logger.Core().Enabled(zap.DebugLevel) {
		logger.Info("Cleaned up closed connection matchmaker")
	}
	s.tracker.UntrackAll(s.id, reason)
	if logger.Core().Enabled(zap.DebugLevel) {
		logger.Info("Cleaned up closed connection tracker")
	}
	s.statusRegistry.UnfollowAll(s.id)
	if logger.Core().Enabled(zap.DebugLevel) {
		logger.Info("Cleaned up closed connection status registry")
	}
	s.sessionRegistry.Remove(s.id)
	if logger.Core().Enabled(zap.DebugLevel) {
		logger.Info("Cleaned up closed connection session registry")
	}

	// Clean up internals.
	s.pingTimer.Stop()

	// Send final messages, if any are specified.
	for _, envelope := range envelopes {
		var payload []byte
		var err error
		switch s.Format() {
		case SessionFormatProtobuf:
			payload, err = proto.Marshal(envelope)
		case SessionFormatJson:
			fallthrough
		default:
			if buf, err := s.protojsonMarshaler.Marshal(envelope); err == nil {
				payload = buf
			}
		}
		if err != nil {
			logger.Warn("Could not marshal envelope", zap.Error(err))
			continue
		}

		if logger.Core().Enabled(zap.DebugLevel) {
			switch envelope.Message.(type) {
			case *rtapi.Envelope_Error:
				logger.Debug("Sending error message", zap.Binary("payload", payload))
			default:
				logger.Debug(fmt.Sprintf("Sending %T message", envelope.Message), zap.Any("envelope", envelope))
			}
		}

		if err := s.write(payload); err != nil {
			continue
		}
	}

	// Close WebSocket.
	if err := s.conn.Close(); err != nil {
		logger.Debug("Could not close", zap.Error(err))
	}

	logger.Info("Closed client connection")

	// Fire an event for session end.
	if fn := s.runtime.EventSessionEnd(); fn != nil {
		fn(s.userID.Load(), s.username.Load(), s.Vars(), s.expiry.Load(), s.id.String(), s.clientIP.Load(), s.clientPort.Load(), s.lang.Load(), time.Now().UTC().Unix(), msg)
	}
}

func (s *sessionTcp) syn(data []byte) error {
	var syn socketSYN
	logger := s.logger.Load()
	if err := bytes.UnPack(bytes.NewBytesBuffer(data), &syn); err != nil {
		return err
	}

	if syn.E1 == 0 || syn.E2 == 0 || syn.Token == "" {
		return ErrSessionInvalidToken
	}

	userID, username, vars, expiry, _, ok := parseToken([]byte(s.config.GetSession().EncryptionKey), syn.Token)
	if !ok || !s.sessionCache.IsValidSession(userID, expiry, syn.Token) {
		return ErrSessionInvalidToken
	}

	format := SessionFormatProtobuf
	if syn.Format == uint8(SessionFormatJson) {
		format = SessionFormatJson
	}

	x1, e1 := dh.DHExchange()
	x2, e2 := dh.DHExchange()
	key1 := dh.DHKey(x1, big.NewInt(syn.E1))
	key2 := dh.DHKey(x2, big.NewInt(syn.E2))

	encoder, err := rc4.NewCipher([]byte(fmt.Sprintf("%v%v", "NK", key2)))
	if err != nil {
		return err
	}

	decoder, err := rc4.NewCipher([]byte(fmt.Sprintf("%v%v", "NK", key1)))
	if err != nil {
		return err
	}

	ack := &socketSYN{
		E1:     e1.Int64(),
		E2:     e2.Int64(),
		Format: uint8(format),
		Status: syn.Status,
	}

	ackBytes, err := bytes.Pack(bytes.NewBytesBuffer(nil), ack)
	if err != nil {
		return err
	}

	if err := s.write(ackBytes); err != nil {
		return err
	}

	if vars == nil {
		vars = make(map[string]string)
	}

	s.encoder.Store(encoder)
	s.decoder.Store(decoder)
	s.format.Store(uint32(format))
	s.userID.Store(userID.String())
	s.username.Store(username)
	s.expiry.Store(expiry)
	s.vars.Store(vars)
	s.lang.Store(syn.Lang)
	s.logger.Store(logger.With(zap.String("uid", userID.String())))

	// Register initial status tracking and presence(s) for this session.
	s.statusRegistry.Follow(s.id, map[uuid.UUID]struct{}{userID: {}})
	if syn.Status {
		// Both notification and status presence.
		s.tracker.TrackMulti(s.Context(), s.id, []*TrackerOp{
			{
				Stream: PresenceStream{Mode: StreamModeNotifications, Subject: userID},
				Meta:   PresenceMeta{Format: format, Username: username, Hidden: true},
			},
			{
				Stream: PresenceStream{Mode: StreamModeStatus, Subject: userID},
				Meta:   PresenceMeta{Format: format, Username: username, Status: ""},
			},
		}, userID)
	} else {
		// Only notification presence.
		s.tracker.Track(s.Context(), s.id, PresenceStream{Mode: StreamModeNotifications, Subject: userID}, userID, PresenceMeta{Format: format, Username: username, Hidden: true})
	}

	if s.config.GetSession().SingleSocket {
		// Kick any other sockets for this user.
		go s.sessionRegistry.SingleSession(s.Context(), s.tracker, userID, s.id)
	}

	fmt.Println(syn)
	return nil
}

func (s *sessionTcp) pack(b []byte) []byte {
	buffer := bytes.NewBytesBuffer(nil)
	buffer.WriteUint16(uint16(len(b)))
	buffer.WriteBytes(b...)
	return buffer.Bytes()
}

func (s *sessionTcp) write(data []byte) error {
	logger := s.logger.Load()
	if encoder := s.encoder.Load(); encoder != nil {
		encoder.XORKeyStream(data, data)
	}

	s.Lock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.writeWaitDuration)); err != nil {
		s.Unlock()
		logger.Warn("Failed to set write deadline", zap.Error(err))
		return err
	}
	if _, err := s.conn.Write(s.pack(data)); err != nil {
		s.Unlock()
		logger.Warn("Could not write message", zap.Error(err))
		return err
	}
	s.Unlock()
	return nil
}
