// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"errors"
	"time"

	grpcpool "github.com/doublemo/nakama-kit/kit/grpc-pool"
)

type (
	Config interface {
		GetName() string
		GetRole() string
		GetWeight() int32
		GetBalancer() int32
		GetGrpc() *GrpcConfig
		GetEtcd() *EtcdClientv3Config
		GetAllowStream() bool
		Clone() Config
	}

	GrpcConfig struct {
		Addr                  string `yaml:"addr" json:"addr" usage:"Service listening address"`
		Port                  int    `yaml:"port" json:"port" usage:"Service listening port, default is 0."`
		MinPort               int    `yaml:"min-port" json:"minPort" usage:"The MinPort is 0, the minimum port and maximum port will be used for randomization"`
		MaxPort               int    `yaml:"max-port" json:"maxPort" usage:"The MaxPort is 0, the minimum port and maximum port will be used for randomization"`
		Domain                string `yaml:"domain" json:"domain" usage:"domain name"`
		DialTimeout           int    `yaml:"dial_timeout" json:"dial_timeout" usage:"DialTimeout the timeout of create connection."`
		BackoffMaxDelay       int    `yaml:"backoff_max_delay" json:"backoff_max_delay" usage:"BackoffMaxDelay provided maximum delay when backing off after failed connection attempts."`
		KeepAliveTime         int    `yaml:"keep_alive_time" json:"keep_alive_time" usage:"KeepAliveTime is the duration of time after which if the client doesn't see any activity it pings the server to see if the transport is still alive."`
		KeepAliveTimeout      int    `yaml:"keep_alive_timeout" json:"keep_alive_timeout" usage:"KeepAliveTimeout is the duration of time for which the client waits after having pinged for keepalive check and if no activity is seen even after that the connection is closed."`
		InitialWindowSize     int32  `yaml:"initial_window_size" json:"initial_window_size" usage:"InitialWindowSize we set it 1GB is to provide system's throughput."`
		InitialConnWindowSize int32  `yaml:"initial_conn_window_size" json:"initial_conn_window_size" usage:"InitialConnWindowSize we set it 1GB is to provide system's throughput."`
		MaxSendMsgSize        int    `yaml:"max_send_msg_size" json:"max_send_msg_size" usage:"MaxSendMsgSize set max gRPC request message size sent to server. If any request message size is larger than current value, an error will be reported from gRPC."`
		MaxRecvMsgSize        int    `yaml:"max_recv_msg_size" json:"max_recv_msg_size" usage:"MaxRecvMsgSize set max gRPC receive message size received from server. If any message size is larger than current value, an error will be reported from gRPC."`
		MaxIdle               int    `yaml:"max_idle" json:"max_idle" usage:"Maximum number of idle connections in the pool."`
		MaxActive             int    `yaml:"max_active" json:"max_active" usage:"Maximum number of connections allocated by the pool at a given time. When zero, there is no limit on the number of connections in the pool."`
		MaxConcurrentStreams  int    `yaml:"max_concurrent_streams" json:"max_concurrent_streams" usage:"MaxConcurrentStreams limit on the number of concurrent streams to each single connection."`
		Reuse                 bool   `yaml:"reuse" json:"reuse" usage:"If Reuse is true and the pool is at the MaxActive limit, then Get() reuse the connection to return, If Reuse is false and the pool is at the MaxActive limit, create a one-time connection to return."`
		Cacert                string `yaml:"ca_cert" json:"ca_cert" usage:"Cacert constructs TLS credentials from the provided root certificate authority certificate file(s) to validate server connections."`
		ServerNameOverride    string `yaml:"server_name_override" json:"server_name_override" usage:"serverNameOverride is for testing only. If set to a non empty string,it will override the virtual host name of authority (e.g. :authority header field) in requests."`
		ServerCertPem         string `yaml:"server_cert_pem" json:"server_cert_pem" usage:"ssl pem"`
		ServerCertKey         string `yaml:"server_cert_key" json:"server_cert_key" usage:"ssl key"`
		AccessToken           string `yaml:"access_token" json:"access_token" usage:"AccessToken authorization key"`
		MessageQueueSize      int    `yaml:"message_queue_size" json:"message_queue_size" usage:"grpc message queue size"`
		AllowRetry            bool   `yaml:"allow_retry" json:"allow_retry" usage:"allow retry"`
		RetryTimeout          int    `yaml:"retry_timeout" json:"retry_timeout" usage:"retry timeout, default 30s"`
		HeartbeatTimeout      int    `yaml:"heartbeat_timeout" json:"heartbeat_timeout" usage:"heartbeat timeout, default 10s"`
	}

	ServerConfig struct {
		Name        string              `yaml:"name" json:"name" usage:"server name"`
		Role        string              `yaml:"role" json:"role" usage:"server role"`
		Weight      int32               `yaml:"weight" json:"weight" usage:"weight"`
		Balancer    int32               `yaml:"balancer" json:"balancer" usage:"balancer"`
		Etcd        *EtcdClientv3Config `yaml:"etcd" json:"etcd" usage:"Etcd config"`
		Grpc        *GrpcConfig         `yaml:"grpc" json:"grpc" usage:"grpc client setting"`
		AllowStream bool                `yaml:"-" json:"allow_stream" usage:"allow_stream"`
	}
)

func (c *ServerConfig) GetName() string {
	return c.Name
}

func (c *ServerConfig) GetRole() string {
	return c.Role
}

func (c *ServerConfig) GetWeight() int32 {
	return c.Weight
}

func (c *ServerConfig) GetBalancer() int32 {
	return c.Balancer
}

func (c *ServerConfig) GetAllowStream() bool {
	return c.AllowStream
}

func (c *ServerConfig) GetGrpc() *GrpcConfig {
	return c.Grpc
}

func (c *ServerConfig) GetEtcd() *EtcdClientv3Config {
	return c.Etcd
}

func (c *ServerConfig) Clone() Config {
	return &ServerConfig{
		Name:     c.Name,
		Role:     c.Role,
		Weight:   c.Weight,
		Balancer: c.Balancer,
		Grpc:     c.Grpc.Clone(),
		Etcd:     c.Etcd.Clone(),
	}
}

func (c *ServerConfig) Valid() error {
	if c.Etcd == nil {
		return errors.New("ETCD is a necessary component for network discovery, and the configuration cannot be nil.")
	}

	if err := c.Etcd.Valid(); err != nil {
		return err
	}

	if c.Grpc == nil {
		return errors.New("GRPC configuration is nil")
	}

	if err := c.Grpc.Valid(); err != nil {
		return err
	}
	return nil
}

func (c *GrpcConfig) Valid() error {
	return nil
}

func (c *GrpcConfig) Clone() *GrpcConfig {
	return &GrpcConfig{
		Addr:                  c.Addr,
		Port:                  c.Port,
		MinPort:               c.MinPort,
		MaxPort:               c.MaxPort,
		Domain:                c.Domain,
		DialTimeout:           c.DialTimeout,
		BackoffMaxDelay:       c.BackoffMaxDelay,
		KeepAliveTime:         c.KeepAliveTime,
		KeepAliveTimeout:      c.KeepAliveTimeout,
		InitialWindowSize:     c.InitialWindowSize,
		InitialConnWindowSize: c.InitialConnWindowSize,
		MaxSendMsgSize:        c.MaxSendMsgSize,
		MaxRecvMsgSize:        c.MaxRecvMsgSize,
		MaxIdle:               c.MaxIdle,
		MaxActive:             c.MaxActive,
		MaxConcurrentStreams:  c.MaxConcurrentStreams,
		Reuse:                 c.Reuse,
		Cacert:                c.Cacert,
		ServerCertPem:         c.ServerCertPem,
		ServerCertKey:         c.ServerCertKey,
		ServerNameOverride:    c.ServerNameOverride,
		AccessToken:           c.AccessToken,
		MessageQueueSize:      c.MessageQueueSize,
		AllowRetry:            c.AllowRetry,
		RetryTimeout:          c.RetryTimeout,
		HeartbeatTimeout:      c.HeartbeatTimeout,
	}
}

func ToClientOptions(c *GrpcConfig) *ClientOptions {
	opt := NewClientOptions()
	if c.DialTimeout > 0 {
		opt.DialTimeout = time.Second * time.Duration(c.DialTimeout)
	}

	if c.BackoffMaxDelay > 0 {
		opt.BackoffMaxDelay = c.BackoffMaxDelay
	}

	if c.KeepAliveTime > 0 {
		opt.KeepAliveTime = time.Second * time.Duration(c.KeepAliveTime)
	}

	if c.KeepAliveTimeout > 0 {
		opt.KeepAliveTimeout = time.Second * time.Duration(c.KeepAliveTimeout)
	}

	if c.InitialWindowSize > 0 {
		opt.InitialWindowSize = c.InitialWindowSize
	}

	if c.InitialConnWindowSize > 0 {
		opt.InitialConnWindowSize = c.InitialConnWindowSize
	}

	if c.MaxSendMsgSize > 0 {
		opt.MaxSendMsgSize = c.MaxSendMsgSize
	}

	if c.MaxRecvMsgSize > 0 {
		opt.MaxRecvMsgSize = c.MaxRecvMsgSize
	}

	if c.MaxIdle > 0 {
		opt.MaxIdle = c.MaxIdle
	}

	if c.MaxActive > 0 {
		opt.MaxActive = c.MaxActive
	}

	if c.MaxConcurrentStreams > 0 {
		opt.MaxConcurrentStreams = c.MaxConcurrentStreams
	}

	if c.MessageQueueSize > 0 {
		opt.MessageQueueSize = c.MessageQueueSize
	}

	if c.HeartbeatTimeout > 0 {
		opt.HeartbeatTimeout = c.HeartbeatTimeout
	}

	opt.Reuse = c.Reuse
	opt.Cacert = c.Cacert
	opt.ServerNameOverride = c.ServerNameOverride
	opt.AccessToken = c.AccessToken
	return opt
}

func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Name: "bombus-serive",
		Role: "bombus",
		Etcd: NewEtcdClientv3Config(),
		Grpc: NewGrpcConfig(),
	}
}

func NewGrpcConfig() *GrpcConfig {
	return &GrpcConfig{
		Addr:                  "0.0.0.0",
		Port:                  0,
		MinPort:               30000,
		MaxPort:               40000,
		Domain:                "",
		DialTimeout:           5,
		BackoffMaxDelay:       3,
		KeepAliveTime:         10,
		KeepAliveTimeout:      3,
		InitialWindowSize:     grpcpool.InitialWindowSize,
		InitialConnWindowSize: grpcpool.InitialConnWindowSize,
		MaxSendMsgSize:        grpcpool.MaxSendMsgSize,
		MaxRecvMsgSize:        grpcpool.MaxRecvMsgSize,
		MaxIdle:               100,
		MaxActive:             65535,
		MaxConcurrentStreams:  65535,
		Reuse:                 true,
		MessageQueueSize:      128,
		AllowRetry:            true,
		RetryTimeout:          30,
		HeartbeatTimeout:      10,
	}
}
