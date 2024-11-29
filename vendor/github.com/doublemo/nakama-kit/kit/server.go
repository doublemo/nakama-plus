// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"context"
	"crypto/tls"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doublemo/nakama-kit/pb"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type Server struct {
	pb.PeerServer
	name                 string
	role                 string
	ctx                  context.Context
	ctxCancelFn          context.CancelFunc
	logger               *zap.Logger
	protojsonMarshaler   *protojson.MarshalOptions
	protojsonUnmarshaler *protojson.UnmarshalOptions
	handler              atomic.Value
	grpc                 *grpc.Server
	serviceRegistry      ServiceRegistry
	connectorRegistry    *ConnectorRegistry
	etcdClient           *EtcdClientV3
	config               Config
	once                 sync.Once
}

func NewServer(logger *zap.Logger, md map[string]string, handler ServerHandler, protojsonMarshaler *protojson.MarshalOptions, protojsonUnmarshaler *protojson.UnmarshalOptions, c Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	meta := pb.NodeMeta{
		Name:        c.GetName(),
		Role:        c.GetRole(),
		Vars:        md,
		Weight:      c.GetWeight(),
		Balancer:    pb.NodeMeta_Balancer(c.GetBalancer()),
		Status:      pb.NodeMeta_OK,
		AllowStream: c.GetAllowStream(),
	}

	s := &Server{
		name:                 c.GetName(),
		role:                 c.GetRole(),
		ctx:                  ctx,
		ctxCancelFn:          cancel,
		logger:               logger,
		protojsonMarshaler:   protojsonMarshaler,
		protojsonUnmarshaler: protojsonUnmarshaler,
		config:               c,
		connectorRegistry:    NewConnectorRegistry(),
		serviceRegistry:      NewLocalServiceRegistry(ctx, logger, c.GetName()),
	}
	s.handler.Store(handler)

	opts := []grpc.ServerOption{
		grpc.InitialWindowSize(c.GetGrpc().InitialWindowSize),
		grpc.InitialConnWindowSize(c.GetGrpc().InitialConnWindowSize),
		grpc.MaxSendMsgSize(c.GetGrpc().MaxSendMsgSize),
		grpc.MaxRecvMsgSize(c.GetGrpc().MaxRecvMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Duration(c.GetGrpc().KeepAliveTime) * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Second,
			MaxConnectionAge:      30 * time.Second,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  time.Duration(c.GetGrpc().KeepAliveTime) * time.Second,
			Timeout:               time.Duration(c.GetGrpc().KeepAliveTimeout) * time.Second,
		}),
	}

	if c.GetGrpc().AccessToken != "" {
		opts = append(opts,
			grpc.ChainStreamInterceptor(ensureStreamValidToken(c), grpc_prometheus.StreamServerInterceptor),
			grpc.ChainUnaryInterceptor(ensureValidToken(c), grpc_prometheus.UnaryServerInterceptor),
		)
	}

	if len(c.GetGrpc().ServerCertPem) > 0 && len(c.GetGrpc().ServerCertKey) > 0 {
		cert, err := tls.LoadX509KeyPair(c.GetGrpc().ServerCertPem, c.GetGrpc().ServerCertKey)
		if err != nil {
			logger.Fatal("Failed load x509", zap.Error(err))
		}

		opts = append(opts,
			grpc.Creds(credentials.NewServerTLSFromCert(&cert)),
		)
	}

	var (
		listen net.Listener
		err    error
	)

	port := c.GetGrpc().Port
	for i := 0; i < 10000; i++ {
		if c.GetGrpc().Port == 0 {
			port = RandomBetween(c.GetGrpc().MinPort, c.GetGrpc().MaxPort)
		}

		listen, err = net.Listen("tcp", net.JoinHostPort(c.GetGrpc().Addr, strconv.Itoa(port)))
		if err != nil {
			logger.Warn("Failed listen from addr", zap.Error(err), zap.String("addr", c.GetGrpc().Addr), zap.Int("port", port))
			continue
		}
		break
	}

	if listen == nil {
		logger.Fatal("Failed listen from addr", zap.Error(err), zap.String("addr", c.GetGrpc().Addr), zap.Int("port", c.GetGrpc().Port))
	}

	meta.Ip = c.GetGrpc().Addr
	meta.Port = uint32(port)
	if meta.Ip == "" || meta.Ip == "0.0.0.0" {
		meta.Ip = c.GetGrpc().Domain
		if meta.Ip == "" {
			if m, err := GetLocalIP(); err != nil {
				meta.Ip = m.String()
			}
		}
	}

	s.grpc = grpc.NewServer(opts...)
	pb.RegisterPeerServer(s.grpc, s)
	service.RegisterChannelzServiceToServer(s.grpc)
	grpc_prometheus.Register(s.grpc)
	// Set grpc logger
	grpcLogger := NewGrpcCustomLogger(logger)
	grpclog.SetLoggerV2(grpcLogger)

	go func() {
		logger.Info("Starting API server for gRPC requests", zap.Int("port", port))
		if err := s.grpc.Serve(listen); err != nil {
			logger.Fatal("API server listener failed", zap.Error(err))
		}
	}()

	// register service
	s.etcdClient = NewEtcdClientV3(context.Background(), logger, c.GetEtcd())
	metabytes, _ := protojsonMarshaler.Marshal(&meta)
	if err := s.etcdClient.Register(c.GetName(), string(metabytes)); err != nil {
		s.logger.Fatal("Failed to register service", zap.Error(err))
	}

	s.onServiceUpdate()
	go s.processWatch()
	return s
}

func (s *Server) Call(ctx context.Context, in *pb.Request) (*pb.ResponseWriter, error) {
	if h, ok := s.handler.Load().(ServerHandler); ok && h != nil {
		return h.Call(ctx, in)
	}
	return nil, status.Error(codes.Internal, "Missing handler")
}

func (s *Server) Stream(stream pb.Peer_StreamServer) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok || (len(md["node"]) == 0 || len(md["role"]) == 0) {
		return status.Error(codes.InvalidArgument, "Missing metadata")
	}

	node := md["node"][0]
	role := md["role"][0]
	handler, ok := s.handler.Load().(ServerHandler)
	if !ok || handler == nil {
		return status.Error(codes.Internal, "Missing handler")
	}

	conn := NewLocalConnector(s.ctx, s.logger, node, role, stream, s.config.GetGrpc().MessageQueueSize, handler, s.config.GetGrpc().AllowRetry, s.config.GetGrpc().RetryTimeout)
	var histroy []*pb.ResponseWriter
	oldConn, ok := s.connectorRegistry.Get(conn.ID())
	s.connectorRegistry.Add(conn)
	if ok {
		histroy = oldConn.RetryOk()
	}

	if retryOk := conn.Consume(histroy); retryOk {
		conn.CloseWithRetry()
		return nil
	}
	s.connectorRegistry.Remove(conn.ID())
	return nil
}

func (s *Server) processWatch() {
	defer func() {
		s.logger.Info("stoped to register service")
	}()
	s.logger.Info("started to register service")

	ch := make(chan struct{}, 1)
	go s.etcdClient.Watch(ch)
	for {
		select {
		case <-ch:
			s.onServiceUpdate()

		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) Stop() {
	s.once.Do(func() {
		if s.ctxCancelFn == nil {
			return
		}

		if err := s.etcdClient.Deregister(s.name); err != nil {
			s.logger.Warn("failed to shutdown Deregister", zap.Error(err))
		}

		s.serviceRegistry.Shutdown()
		// s.connectorRegistry.
		s.ctxCancelFn()
		s.grpc.GracefulStop()
	})
}

func (s *Server) ServiceRegistry() ServiceRegistry {
	return s.serviceRegistry
}

func (s *Server) ConnectorRegistry() *ConnectorRegistry {
	return s.connectorRegistry
}

func (s *Server) onServiceUpdate() {
	entries, err := s.etcdClient.GetEntries()
	if err != nil {
		s.logger.Error("failed to GetEntries", zap.Error(err))
		return
	}

	nodes := make(map[string]map[string]bool)
	nodesname := make([]string, 0, len(entries))
	nodesremoved := make([]string, 0, len(entries))
	for _, v := range entries {
		var md pb.NodeMeta
		if err := s.protojsonUnmarshaler.Unmarshal([]byte(v), &md); err != nil {
			s.logger.Warn("failed to Unmarshal node meta", zap.Error(err), zap.String("md", v))
			continue
		}

		nodesname = append(nodesname, md.Name)
		if m, ok := nodes[md.Role]; ok {
			m[md.Name] = true
		} else {
			nodes[md.Role] = make(map[string]bool)
			nodes[md.Role][md.Name] = true
		}

		client, err := s.serviceRegistry.Register(&md)
		if err != nil {
			s.logger.Error("failed to register service", zap.Error(err), zap.String("md", v))
			continue
		}

		if h, ok := s.handler.Load().(ServerHandler); ok && h != nil {
			h.OnServiceUpdate(s.serviceRegistry, client)
		}
	}

	s.serviceRegistry.Range(func(key string, value Service) bool {
		m, ok := nodes[key]
		if !ok {
			nodesremoved = append(nodesremoved, key)
			s.serviceRegistry.Delete(value)
			return true
		}

		removed := make([]string, 0)
		n := 0
		for _, client := range value.GetClients() {
			if !m[client.Name()] {
				nodesremoved = append(nodesremoved, client.Name())
				removed = append(removed, client.Name())
				n++
			}
		}

		if n > 0 {
			s.serviceRegistry.Delete(value, removed...)
		}
		return true
	})
	s.logger.Info("updated service", zap.Strings("nodes", nodesname), zap.Strings("removed", nodesremoved))
}

func ensureValidToken(config Config) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "Missing metadata")
		}
		// The keys within metadata.MD are normalized to lowercase.
		// See: https://godoc.org/google.golang.org/grpc/metadata#New
		authorization := md["authorization"]
		if len(authorization) < 1 {
			return nil, status.Error(codes.PermissionDenied, "Invalid token")
		}

		token := strings.TrimPrefix(authorization[0], "Bearer ")
		if token != config.GetGrpc().AccessToken {
			return nil, status.Error(codes.PermissionDenied, "Invalid token")
		}

		// Continue execution of handler after ensuring a valid token.
		return handler(ctx, req)
	}
}

// func(srv interface{}, ss ServerStream, info *StreamServerInfo, handler StreamHandler)
func ensureStreamValidToken(config Config) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Error(codes.PermissionDenied, "Invalid token")
		}

		authorization := md["authorization"]
		if len(authorization) < 1 {
			return status.Error(codes.PermissionDenied, "Invalid token")
		}

		token := strings.TrimPrefix(authorization[0], "Bearer ")
		if token != config.GetGrpc().AccessToken {
			return status.Error(codes.PermissionDenied, "Invalid token")
		}

		// Continue execution of handler after ensuring a valid token.
		return handler(srv, ss)
	}
}
