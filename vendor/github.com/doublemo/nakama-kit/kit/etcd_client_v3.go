// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
)

type (
	EtcdClientv3Config struct {
		Endpoints              []string `yaml:"endpoints" json:"endpoints" usage:"Endpoints is a list of URLs."`
		AutoSyncIntervalMs     int      `yaml:"auto-sync-interval-ms" json:"autoSyncIntervalMs" usage:"AutoSyncInterval is the interval to update endpoints with its latest members.0 disables auto-sync. By default auto-sync is disabled."`
		DialTimeoutMs          int      `yaml:"dial-timeout-ms" json:"dialTimeoutMs" usage:"DialTimeout is the timeout for failing to establish a connection."`
		DialKeepAliveTimeMs    int      `yaml:"dial-keep-alive-time-ms" json:"dialKeepAliveTimeMs" usage:"DialKeepAliveTime is the time after which client pings the server to see if transport is alive."`
		DialKeepAliveTimeoutMs int      `yaml:"dial-keep-alive-timeout-ms" json:"dialKeepAliveTimeoutMs" usage:"DialKeepAliveTimeout is the time that the client waits for a response for the keep-alive probe. If the response is not received in this time, the connection is closed."`
		MaxCallSendMsgSize     int      `yaml:"max-call-send-msg-size" json:"maxCallSendMsgSize" usage:"MaxCallSendMsgSize is the client-side request send limit in bytes.If 0, it defaults to 2.0 MiB (2 * 1024 * 1024)."`
		MaxCallRecvMsgSize     int      `yaml:"max-call-recv-msg-size" json:"maxCallRecvMsgSize" usage:"MaxCallRecvMsgSize is the client-side response receive limit.If 0, it defaults to math.MaxInt32, because range response can easily exceed request send limits."`
		Username               string   `yaml:"username" json:"username" usage:"Username is a user name for authentication."`
		Password               string   `yaml:"password" json:"password" usage:"Password is a password for authentication."`
		Cert                   string   `yaml:"cert" json:"cert" usage:"Cert"`
		Key                    string   `yaml:"key" json:"key" usage:"Key"`
		CACert                 string   `yaml:"ca-cert" json:"caCert" usage:"CACert"`
		ServicePrefix          string   `yaml:"service-prefix" json:"servicePrefix" usage:"Service prefix"`
	}

	EtcdClientV3 struct {
		logger *zap.Logger
		cli    *clientv3.Client
		ctx    context.Context

		kv clientv3.KV

		// Watcher interface instance, used to leverage Watcher.Close()
		watcher clientv3.Watcher
		// watcher context
		wctx context.Context
		// watcher cancel func
		wcf context.CancelFunc

		// leaseID will be 0 (clientv3.NoLease) if a lease was not created
		leaseID clientv3.LeaseID

		hbch <-chan *clientv3.LeaseKeepAliveResponse
		// Lease interface instance, used to leverage Lease.Close()
		leaser clientv3.Lease

		servicePrefix string
	}
)

func (c *EtcdClientv3Config) Valid() error {
	if size := len(c.Endpoints); size < 1 {
		return errors.New("endpoints is empty")
	}

	if c.ServicePrefix == "" {
		c.ServicePrefix = "/nakama-plus/services/"
	}

	return nil
}

func (c *EtcdClientv3Config) Clone() *EtcdClientv3Config {
	newConfig := &EtcdClientv3Config{
		Endpoints:              make([]string, len(c.Endpoints)),
		AutoSyncIntervalMs:     c.AutoSyncIntervalMs,
		DialTimeoutMs:          c.DialTimeoutMs,
		DialKeepAliveTimeMs:    c.DialKeepAliveTimeMs,
		DialKeepAliveTimeoutMs: c.DialKeepAliveTimeoutMs,
		MaxCallSendMsgSize:     c.MaxCallSendMsgSize,
		MaxCallRecvMsgSize:     c.MaxCallRecvMsgSize,
		Username:               c.Username,
		Password:               c.Password,
		Cert:                   c.Cert,
		Key:                    c.Key,
		CACert:                 c.CACert,
		ServicePrefix:          c.ServicePrefix,
	}

	copy(newConfig.Endpoints, c.Endpoints)
	return newConfig
}

func NewEtcdClientv3Config() *EtcdClientv3Config {
	return &EtcdClientv3Config{
		ServicePrefix: "/nakama-plus/services/",
	}
}

func NewEtcdClientV3(ctx context.Context, logger *zap.Logger, c *EtcdClientv3Config) *EtcdClientV3 {
	cfg, err := bindPeerEtcdV3Config2V3Config(c)
	if err != nil {
		logger.Fatal("Failed to build etcd config", zap.Error(err))
	}

	cfg.Context = ctx
	cli, err := clientv3.New(*cfg)
	if err != nil {
		logger.Fatal("Failed to connect etcd", zap.Error(err))
	}

	return &EtcdClientV3{
		logger:        logger,
		cli:           cli,
		ctx:           ctx,
		kv:            clientv3.NewKV(cli),
		servicePrefix: c.ServicePrefix,
	}
}

func (c *EtcdClientV3) LeaseID() int64 { return int64(c.leaseID) }

// GetEntries implements the etcd Client interface.
func (c *EtcdClientV3) GetEntries() ([]string, error) {
	resp, err := c.kv.Get(c.ctx, c.servicePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	entries := make([]string, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		entries[i] = string(kv.Value)
	}

	return entries, nil
}

func (c *EtcdClientV3) ServicePrefix() string {
	return c.servicePrefix
}

func (c *EtcdClientV3) Update(name, value string) error {
	if _, err := c.kv.Put(c.ctx, c.Key(name), value, clientv3.WithLease(c.leaseID)); err != nil {
		return err
	}
	return nil
}

// WatchPrefix implements the etcd Client interface.
func (c *EtcdClientV3) Watch(ch chan struct{}) {
	c.wctx, c.wcf = context.WithCancel(c.ctx)
	c.watcher = clientv3.NewWatcher(c.cli)

	wch := c.watcher.Watch(c.wctx, c.servicePrefix, clientv3.WithPrefix(), clientv3.WithRev(0))
	ch <- struct{}{}
	for wr := range wch {
		if wr.Canceled {
			return
		}
		ch <- struct{}{}
	}
}

func (c *EtcdClientV3) Register(name, value string) error {
	var err error

	if c.leaser != nil {
		c.leaser.Close()
	}
	c.leaser = clientv3.NewLease(c.cli)

	if c.watcher != nil {
		c.watcher.Close()
	}
	c.watcher = clientv3.NewWatcher(c.cli)
	if c.kv == nil {
		c.kv = clientv3.NewKV(c.cli)
	}

	//s.TTL = NewTTLOption(time.Second*3, time.Second*10)
	ttl := time.Second * 10
	grantResp, err := c.leaser.Grant(c.ctx, int64(ttl.Seconds()))
	if err != nil {
		return err
	}
	c.leaseID = grantResp.ID

	_, err = c.kv.Put(
		c.ctx,
		c.Key(name),
		value,
		clientv3.WithLease(c.leaseID),
	)
	if err != nil {
		return err
	}

	// this will keep the key alive 'forever' or until we revoke it or
	// the context is canceled
	c.hbch, err = c.leaser.KeepAlive(c.ctx, c.leaseID)
	if err != nil {
		return err
	}

	// discard the keepalive response, make etcd library not to complain
	// fix bug #799
	go func() {
		for {
			select {
			case r := <-c.hbch:
				// avoid dead loop when channel was closed
				if r == nil {
					return
				}
			case <-c.ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (c *EtcdClientV3) Deregister(name string) error {
	defer c.close()

	if _, err := c.cli.Delete(c.ctx, c.Key(name), clientv3.WithIgnoreLease()); err != nil {
		return err
	}

	return nil
}

// close will close any open clients and call
// the watcher cancel func
func (c *EtcdClientV3) close() {
	if c.leaser != nil {
		c.leaser.Close()
	}
	if c.watcher != nil {
		c.watcher.Close()
	}
	if c.wcf != nil {
		c.wcf()
	}
}

func (c *EtcdClientV3) Key(name string) string {
	key := c.servicePrefix
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	key += name
	return key
}

func bindPeerEtcdV3Config2V3Config(c *EtcdClientv3Config) (*clientv3.Config, error) {
	var (
		tlscfg *tls.Config
		err    error
	)
	if c.Cert != "" && c.Key != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      c.Cert,
			KeyFile:       c.Key,
			TrustedCAFile: c.CACert,
		}
		tlscfg, err = tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}
	}
	cfg := &clientv3.Config{
		Endpoints:         c.Endpoints,
		DialTimeout:       time.Second * 3,
		DialKeepAliveTime: time.Second * 3,
		TLS:               tlscfg,
		Username:          c.Username,
		Password:          c.Password,
	}

	if c.AutoSyncIntervalMs > 0 {
		cfg.AutoSyncInterval = time.Duration(c.AutoSyncIntervalMs) * time.Millisecond
	}

	if c.DialTimeoutMs > 0 {
		cfg.DialTimeout = time.Duration(c.DialTimeoutMs) * time.Millisecond
	}

	if c.DialKeepAliveTimeMs > 0 {
		cfg.DialKeepAliveTime = time.Duration(c.DialKeepAliveTimeMs) * time.Millisecond
	}

	if c.DialKeepAliveTimeoutMs > 0 {
		cfg.DialKeepAliveTimeout = time.Duration(c.DialKeepAliveTimeoutMs) * time.Millisecond
	}

	if c.MaxCallSendMsgSize > 0 {
		cfg.MaxCallSendMsgSize = c.MaxCallSendMsgSize
	}

	if c.MaxCallRecvMsgSize > 0 {
		cfg.MaxCallRecvMsgSize = c.MaxCallRecvMsgSize
	}
	return cfg, nil
}
