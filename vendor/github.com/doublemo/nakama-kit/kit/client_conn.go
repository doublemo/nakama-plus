// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"context"
	"io"
	"math"
	"sync"
	"time"

	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	ConnectState_STOPED = iota
	ConnectState_CONNECTING
	ConnectState_OK
)

const (
	// BaseDelay is the amount of time to backoff after the first failure.
	Backoff_INITIAL = 1.0

	// MaxDelay is the upper bound of backoff delay.
	Backoff_MAX = 120.0

	// Multiplier is the factor with which to multiply backoffs after a
	// failed retry. Should ideally be greater than 1.
	Backoff_MULTIPLIER = 1.6

	//Jitter is the factor with which backoffs are randomized.
	Backoff_JITTER = 0.2
)

type ClientConn struct {
	ctx              context.Context
	ctxCancelFn      context.CancelFunc
	logger           *zap.Logger
	addr             string
	stream           pb.Peer_StreamClient
	cc               *grpc.ClientConn
	dialFn           func(address string) (*grpc.ClientConn, error)
	md               metadata.MD
	outgoingCh       chan *pb.ResponseWriter
	lastRecvPacketAt *atomic.Time
	state            *atomic.Int32
	heartbeatTimeout time.Duration
	backoffMaxDelay  float64
	once             sync.Once
	sync.Mutex
}

func NewClientConn(ctx context.Context, logger *zap.Logger, addr string, outgoingCh chan *pb.ResponseWriter, md metadata.MD, dialFn func(address string) (*grpc.ClientConn, error), opt *ClientOptions) (*ClientConn, error) {
	ctx, cancel := context.WithCancel(ctx)
	cc := &ClientConn{
		ctx:              ctx,
		ctxCancelFn:      cancel,
		logger:           logger,
		addr:             addr,
		dialFn:           dialFn,
		md:               md,
		outgoingCh:       outgoingCh,
		lastRecvPacketAt: atomic.NewTime(time.Now()),
		heartbeatTimeout: time.Duration(opt.HeartbeatTimeout) * time.Second,
		backoffMaxDelay:  Backoff_MAX,
	}

	if opt.BackoffMaxDelay > 0 {
		cc.backoffMaxDelay = float64(opt.BackoffMaxDelay)
	}

	conn, err := dialFn(addr)
	if err != nil {
		return nil, err
	}

	streamClient, err := pb.NewPeerClient(conn).Stream(metadata.NewOutgoingContext(ctx, md))
	if err != nil {
		return nil, err
	}

	cc.cc = conn
	cc.stream = streamClient
	cc.state = atomic.NewInt32(ConnectState_OK)

	// recv msg
	go cc.processRecvMsg()
	go cc.processHeartbeat()
	return cc, nil
}

func (cc *ClientConn) Close() {
	cc.once.Do(func() {
		if cc.ctxCancelFn == nil {
			return
		}

		cc.stream.CloseSend()
		cc.Close()
		cc.state.Store(ConnectState_STOPED)
		cc.ctxCancelFn()
	})
}

func (cc *ClientConn) Write(msg *pb.Request) error {
	state := cc.state.Load()
	if state == ConnectState_STOPED {
		return ErrNotConnected
	}

	if state == ConnectState_CONNECTING {
		timeout := time.Now()

	breakFor:
		for {
			select {
			case <-time.After(time.Millisecond * 100):
				state := cc.state.Load()
				switch state {
				case ConnectState_STOPED:
					return ErrNotConnected

				case ConnectState_CONNECTING:
					if time.Since(timeout).Seconds() > 10 {
						return ErrTimeout
					}
					continue

				default:
					break breakFor
				}

			case <-cc.ctx.Done():
				return ErrNotConnected
			}
		}
	}
	return cc.stream.Send(msg)
}

func (cc *ClientConn) processRecvMsg() {
	for {
		cc.Lock()
		stream := cc.stream
		cc.Unlock()

		out, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return
			}

			switch status.Code(err) {
			case codes.Canceled:
				return

			case codes.Unavailable:
				cc.backoff()
				continue

			default:
				cc.logger.Error("recv message error", zap.Error(err))
			}
			return
		}

		switch out.Payload.(type) {
		case *pb.ResponseWriter_Pong:
			select {
			case <-cc.ctx.Done():
				return
			default:
			}
		default:
			select {
			case <-cc.ctx.Done():
				return
			case cc.outgoingCh <- out:
			default:
				cc.logger.Error("outgoingCh is full")
				return
			}
		}
		cc.lastRecvPacketAt.Store(time.Now())
	}
}

func (cc *ClientConn) processHeartbeat() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cc.ctx.Done():
			return

		case <-ticker.C:
			if state := cc.state.Load(); state != ConnectState_OK {
				continue
			}

			at := cc.lastRecvPacketAt.Load()
			if at.Add(cc.heartbeatTimeout).After(time.Now()) {
				continue
			}

			cc.sendHeartbeat()
		}
	}
}

func (cc *ClientConn) sendHeartbeat() {
	msg := &pb.Request{
		Payload: &pb.Request_Ping{Ping: "PING"},
	}

	cc.Lock()
	stream := cc.stream
	cc.Unlock()
	stream.Send(msg)
}

func (cc *ClientConn) retry() bool {
	conn, err := cc.dialFn(cc.addr)
	if err != nil {
		cc.logger.Warn("retry failed", zap.Error(err), zap.String("addr", cc.addr))
		return false
	}

	streamClient, err := pb.NewPeerClient(conn).Stream(metadata.NewOutgoingContext(cc.ctx, cc.md))
	if err != nil {
		cc.logger.Warn("retry failed", zap.Error(err), zap.String("addr", cc.addr))
		return false
	}

	cc.Lock()
	cc.cc = conn
	cc.stream = streamClient
	cc.Unlock()
	return true
}

func (cc *ClientConn) backoff() {
	if state := cc.state.Swap(ConnectState_CONNECTING); state != ConnectState_OK {
		return
	}

	cc.Lock()
	conn := cc.cc
	stream := cc.stream
	cc.Unlock()

	stream.CloseSend()
	conn.Close()
	currentBackoff := Backoff_INITIAL
	currentDeadline := time.Now().Add(time.Duration(Backoff_INITIAL) * time.Second)

	for {
		select {
		case <-time.After(time.Until(currentDeadline)):
			if ok := cc.retry(); ok {
				cc.state.Store(ConnectState_OK)
				return
			}

			currentBackoff = math.Min(currentBackoff*Backoff_MULTIPLIER, cc.backoffMaxDelay)
			currentDeadline = time.Now().Add(time.Duration(currentBackoff+RandomBetweenFloat64(-Backoff_JITTER*currentBackoff, Backoff_JITTER*currentBackoff)) * time.Second)

		case <-cc.ctx.Done():
			return
		}
	}
}
