// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	grpcpool "github.com/doublemo/nakama-kit/kit/grpc-pool"
	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

var (
	ErrNotConnected = errors.New("not connected to remote server")
	ErrConnected    = errors.New("connected to remote server")
	ErrUnsupported  = errors.New("Unsupported")
	ErrTimeout      = errors.New("Timeout")
)

type (
	ClientRecvHandler func(client Client, msg *pb.Peer_ResponseWriter)

	ClientOptions struct {
		// DialTimeout the timeout of create connection
		DialTimeout time.Duration

		// BackoffMaxDelay provided maximum delay when backing off after failed connection attempts.
		BackoffMaxDelay int

		// KeepAliveTime is the duration of time after which if the client doesn't see
		// any activity it pings the server to see if the transport is still alive.
		KeepAliveTime time.Duration

		// KeepAliveTimeout is the duration of time for which the client waits after having
		// pinged for keepalive check and if no activity is seen even after that the connection
		// is closed.
		KeepAliveTimeout time.Duration

		// InitialWindowSize we set it 1GB is to provide system's throughput.
		InitialWindowSize int32

		// InitialConnWindowSize we set it 1GB is to provide system's throughput.
		InitialConnWindowSize int32

		// MaxSendMsgSize set max gRPC request message size sent to server.
		// If any request message size is larger than current value, an error will be reported from gRPC.
		MaxSendMsgSize int

		// MaxRecvMsgSize set max gRPC receive message size received from server.
		// If any message size is larger than current value, an error will be reported from gRPC.
		MaxRecvMsgSize int

		// Maximum number of idle connections in the pool.
		MaxIdle int

		// Maximum number of connections allocated by the pool at a given time.
		// When zero, there is no limit on the number of connections in the pool.
		MaxActive int

		// MaxConcurrentStreams limit on the number of concurrent streams to each single connection
		MaxConcurrentStreams int

		// If Reuse is true and the pool is at the MaxActive limit, then Get() reuse
		// the connection to return, If Reuse is false and the pool is at the MaxActive limit,
		// create a one-time connection to return.
		Reuse bool

		// Cacert constructs TLS credentials from the provided root certificate authority
		// certificate file(s) to validate server connections.
		Cacert string

		// ServerNameOverride is for testing only. If set to a non empty string,
		// it will override the virtual host name of authority (e.g. :authority header
		// field) in requests
		ServerNameOverride string

		AccessToken string

		AllowStream bool

		MessageQueueSize int

		HeartbeatTimeout int
	}

	Client interface {
		Name() string
		Addr() string
		Role() string
		Status() pb.NodeMeta_Status
		Weight() int32
		Balancer() pb.NodeMeta_Balancer
		AllowStream() bool
		Metadata() *pb.NodeMeta
		SetMetadata(meta *pb.NodeMeta)
		Connected() (dialConnected bool, streamConnected bool)
		Dial(md metadata.MD, clientOptions ...*ClientOptions) error
		Handler(fn func(client Client, msg *pb.Peer_ResponseWriter))
		Do(ctx context.Context, msg *pb.Peer_Request) (*pb.Peer_ResponseWriter, error)
		Send(msg *pb.Peer_Request) error
		Close()
	}

	LocalClient struct {
		ctx         context.Context
		ctxCancelFn context.CancelFunc
		logger      *zap.Logger
		md          *atomic.Pointer[pb.NodeMeta]   //atomic.Pointer[pb.Bombus_Peer_NodeMeta]
		conn        *atomic.Pointer[ClientConn]    //atomic.Pointer[pb.PeerApi_StreamClient]
		pool        *atomic.Pointer[grpcpool.Pool] //atomic.Pointer[grpcpool.Pool]
		recvFn      atomic.Value
		once        sync.Once
	}
)

func NewLocalClient(ctx context.Context, logger *zap.Logger, meta *pb.NodeMeta, handler ...ClientRecvHandler) Client {
	ctx, ctxCancelFn := context.WithCancel(ctx)
	client := &LocalClient{
		ctx:         ctx,
		ctxCancelFn: ctxCancelFn,
		logger:      logger.With(zap.String("peer", meta.Name)),
		md:          atomic.NewPointer(meta),
		conn:        atomic.NewPointer[ClientConn](nil),
		pool:        atomic.NewPointer[grpcpool.Pool](nil),
	}

	if len(handler) > 0 {
		client.recvFn.Store(handler[0])
	}
	return client
}

func (client *LocalClient) Name() string {
	md := client.md.Load()
	if md == nil {
		return ""
	}
	return md.GetName()
}

func (client *LocalClient) Addr() string {
	md := client.md.Load()
	if md == nil {
		return ""
	}
	return net.JoinHostPort(md.Ip, strconv.Itoa(int(md.Port)))
}

func (client *LocalClient) Role() string {
	md := client.md.Load()
	if md == nil {
		return ""
	}
	return md.GetRole()
}

func (client *LocalClient) Status() pb.NodeMeta_Status {
	md := client.md.Load()
	if md == nil {
		return pb.NodeMeta_STOPED
	}
	return md.GetStatus()
}

func (client *LocalClient) Weight() int32 {
	md := client.md.Load()
	if md == nil {
		return 0
	}
	return md.GetWeight()
}

func (client *LocalClient) Balancer() pb.NodeMeta_Balancer {
	md := client.md.Load()
	if md == nil {
		return pb.NodeMeta_RANDOM
	}
	return md.GetBalancer()
}

func (client *LocalClient) AllowStream() bool {
	md := client.md.Load()
	if md == nil {
		return false
	}
	return md.GetAllowStream()
}

func (client *LocalClient) SetMetadata(meta *pb.NodeMeta) {
	client.md.Store(meta)
}

func (client *LocalClient) Metadata() *pb.NodeMeta {
	md := &pb.NodeMeta{
		Vars: make(map[string]string),
	}

	m := client.md.Load()
	if m == nil {
		return md
	}

	md.Name = m.Name
	md.Ip = m.Ip
	md.Port = m.Port
	md.Role = m.Role
	md.Status = m.Status
	md.Weight = m.Weight
	md.Balancer = m.Balancer
	md.AllowStream = m.AllowStream

	for k, v := range m.Vars {
		md.Vars[k] = v
	}
	return md
}

func (client *LocalClient) Connected() (dialConnected bool, streamConnected bool) {
	_, dialConnected = client.getPool()
	_, streamConnected = client.getStreamClient()
	return
}

func (client *LocalClient) Dial(md metadata.MD, clientOptions ...*ClientOptions) error {
	var opts *ClientOptions
	if len(clientOptions) > 0 {
		opts = clientOptions[0]
	} else {
		opts = NewClientOptions()
	}

	p, err := grpcpool.New(client.Addr(), grpcpool.Options{
		Dial:                 client.newDial(opts),
		MaxIdle:              opts.MaxIdle,
		MaxActive:            opts.MaxActive,
		MaxConcurrentStreams: opts.MaxConcurrentStreams,
		Reuse:                opts.Reuse,
	})
	if err != nil {
		return err
	}

	if ok := client.pool.CompareAndSwap(nil, &p); !ok {
		return ErrConnected
	}

	if !opts.AllowStream {
		return nil
	}

	if !client.AllowStream() {
		return ErrUnsupported
	}

	ch := make(chan *pb.Peer_ResponseWriter, opts.MessageQueueSize)
	cc, err := NewClientConn(client.ctx, client.logger, client.Addr(), ch, md, client.newDial(opts), opts)
	if err != nil {
		return err
	}

	client.conn.Store(cc)
	go client.recv(ch)
	return nil
}

func (client *LocalClient) Handler(fn func(client Client, msg *pb.Peer_ResponseWriter)) {
	client.recvFn.Store(fn)
}

func (client *LocalClient) Do(ctx context.Context, msg *pb.Peer_Request) (*pb.Peer_ResponseWriter, error) {
	p, ok := client.getPool()
	if !ok {
		return nil, ErrNotConnected
	}

	conn, err := p.Get()
	if err != nil {
		return nil, err
	}

	defer conn.Close()
	resp, err := pb.NewPeerApiClient(conn.Value()).Call(ctx, msg)
	if err != nil {
		return nil, err
	}

	resp.Cid = msg.GetCid()
	resp.Name = msg.GetName()
	return resp, nil
}

func (client *LocalClient) Send(msg *pb.Peer_Request) error {
	conn, ok := client.getStreamClient()
	if !ok {
		return ErrNotConnected
	}
	return conn.Write(msg)
}

func (client *LocalClient) Close() {
	client.once.Do(func() {
		defer func() {
			client.logger.Info("shutdown completed")
		}()

		if conn, ok := client.getStreamClient(); ok {
			if err := conn.stream.CloseSend(); err != nil {
				client.logger.Error("stream.CloseSend", zap.Error(err))
			}

			if err := conn.cc.Close(); err != nil {
				client.logger.Error("cc.Close", zap.Error(err))
			}
		}

		if p, ok := client.getPool(); ok {
			if err := p.Close(); err != nil {
				client.logger.Error("pool.Close", zap.Error(err))
			}

		}

		client.pool.Store(nil)
		client.conn.Store(nil)
		client.ctxCancelFn()
	})
}

func (client *LocalClient) recv(ch chan *pb.Peer_ResponseWriter) {
	for {
		select {
		case <-client.ctx.Done():
			return

		case msg, ok := <-ch:
			if !ok {
				return
			}
			if handle, ok := client.recvFn.Load().(ClientRecvHandler); ok && handle != nil {
				handle(client, msg)
			}
		}
	}
}

func (client *LocalClient) newDial(clientOptions *ClientOptions) func(address string) (*grpc.ClientConn, error) {
	return func(address string) (*grpc.ClientConn, error) {
		boff := backoff.Config{
			BaseDelay:  Backoff_INITIAL * time.Second,
			Multiplier: Backoff_MULTIPLIER,
			Jitter:     Backoff_JITTER,
			MaxDelay:   time.Duration(clientOptions.BackoffMaxDelay) * time.Second,
		}

		options := []grpc.DialOption{
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: boff, MinConnectTimeout: clientOptions.DialTimeout}),
			grpc.WithInitialWindowSize(clientOptions.InitialWindowSize),
			grpc.WithInitialConnWindowSize(clientOptions.InitialConnWindowSize),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(clientOptions.MaxSendMsgSize)),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(clientOptions.MaxRecvMsgSize)),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                clientOptions.KeepAliveTime,
				Timeout:             clientOptions.KeepAliveTimeout,
				PermitWithoutStream: true,
			}),
		}

		if clientOptions.AccessToken != "" {
			options = append(options, grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: clientOptions.AccessToken,
			})}))
		}

		if clientOptions.Cacert != "" {
			// Create tls based credential.
			creds, err := credentials.NewClientTLSFromFile(clientOptions.Cacert, clientOptions.ServerNameOverride)
			if err != nil {
				return nil, fmt.Errorf("failed to create credentials: %v", err)
			}
			options = append(options, grpc.WithTransportCredentials(creds))
		} else {
			options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		return grpc.NewClient(address, options...)
	}
}

func (client *LocalClient) getPool() (grpcpool.Pool, bool) {
	pool := client.pool.Load()
	if pool == nil || *pool == nil {
		return nil, false
	}

	return *pool, true
}

func (client *LocalClient) getStreamClient() (*ClientConn, bool) {
	conn := client.conn.Load()
	if conn == nil {
		return nil, false
	}
	return conn, true
}

func NewClientOptions() *ClientOptions {
	return &ClientOptions{
		DialTimeout:           grpcpool.DialTimeout,
		BackoffMaxDelay:       3,
		KeepAliveTime:         grpcpool.KeepAliveTime,
		KeepAliveTimeout:      grpcpool.KeepAliveTimeout,
		InitialWindowSize:     grpcpool.InitialWindowSize,
		InitialConnWindowSize: grpcpool.InitialConnWindowSize,
		MaxSendMsgSize:        grpcpool.MaxSendMsgSize,
		MaxRecvMsgSize:        grpcpool.MaxRecvMsgSize,
		MaxIdle:               1,
		MaxActive:             65535,
		MaxConcurrentStreams:  65535,
		Reuse:                 true,
		AllowStream:           false,
		MessageQueueSize:      128,
	}
}
