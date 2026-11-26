// Copyright 2026 The Sky Go Authors
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
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// DefaultLockTimeout is the default TTL for acquired locks
	DefaultLockTimeout = 10 * time.Second

	// LockKeyPrefix namespaces all lock keys
	LockKeyPrefix = "sky:lock:"

	EventChannel = "events"
)

var (
	ErrLockTimeout     = errors.New("failed to acquire lock within timeout")
	ErrLockNotObtained = redislock.ErrNotObtained
	ErrLockNotHeld     = errors.New("lock is not held by this client")
)

// Redis defines a unified interface for Redis operations with distributed locking support.
type Redis interface {
	Close()
	Context() context.Context
	DB() redis.UniversalClient
	Prefix() string
	Namer(name string, subname ...string) string
	EventChannel(name string, subname ...string) string

	IsCluster() bool

	// ObtainLock tries to acquire a distributed lock.
	// Returns the lock handle on success (caller must call Release()).
	ObtainLock(ctx context.Context, key string, opts ...LockOption) (*redislock.Lock, error)

	// ObtainLockWithRetry acquires lock with retry logic.
	ObtainLockWithRetry(ctx context.Context, key string, opts ...LockOption) (*redislock.Lock, error)
}

// LockOption customizes lock acquisition behavior
type LockOption func(*lockOptions)

type lockOptions struct {
	ttl           time.Duration
	retryStrategy redislock.RetryStrategy
}

// WithLockTTL sets the lock expiration duration
func WithLockTTL(d time.Duration) LockOption {
	return func(o *lockOptions) { o.ttl = d }
}

// WithRetryStrategy sets a custom retry policy for lock acquisition
func WithRetryStrategy(s redislock.RetryStrategy) LockOption {
	return func(o *lockOptions) { o.retryStrategy = s }
}

func WithRetryStrategyForExponentialBackoff(min time.Duration, max time.Duration) LockOption {
	return func(o *lockOptions) {
		o.retryStrategy = redislock.ExponentialBackoff(min, max)
	}
}

func WithRetryStrategyForLimitRetry(min time.Duration, max time.Duration, retry int) LockOption {
	return func(o *lockOptions) {
		o.retryStrategy = redislock.LimitRetry(
			redislock.ExponentialBackoff(min, max),
			retry)
	}
}

// LocalRedisClient is a Redis client wrapper with distributed locking via bsm/redislock
type LocalRedisClient struct {
	logger      *zap.Logger
	db          redis.UniversalClient
	prefix      string
	ctx         context.Context
	ctxCancelFn context.CancelFunc
	isCluster   bool

	stopOnce sync.Once
}

// NewLocalRedisClient creates a new Redis client from configuration
func NewLocalRedisClient(ctx context.Context, logger *zap.Logger, cfg *RedisConfig) (Redis, error) {
	if logger == nil {
		return nil, errors.New("logger is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	opt := &redis.UniversalOptions{
		Addrs:         cfg.Addresses,
		DB:            cfg.DB,
		Username:      cfg.Username,
		Password:      cfg.Password,
		ReadOnly:      cfg.ReadOnly,
		IsClusterMode: cfg.ClusterMode,
	}

	// Configure TLS if enabled
	if cfg.AllowSSL {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.TLSServerName != "" {
			tlsConfig.ServerName = cfg.TLSServerName
		}
		if cfg.TLSCert != "" && cfg.TLSKey != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
			if err != nil {
				return nil, fmt.Errorf("failed to load TLS cert/key: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		opt.TLSConfig = tlsConfig
	}

	// Retry & timeout settings
	opt.MaxRetries = cfg.MaxRetries
	opt.MinRetryBackoff = cfg.MinRetryBackoff
	opt.MaxRetryBackoff = cfg.MaxRetryBackoff
	opt.DialTimeout = cfg.DialTimeout
	opt.ReadTimeout = cfg.ReadTimeout
	opt.WriteTimeout = cfg.WriteTimeout
	opt.ContextTimeoutEnabled = cfg.ContextTimeoutEnabled

	// Pool configuration
	opt.PoolFIFO = cfg.PoolFIFO
	opt.PoolSize = cfg.PoolSize
	opt.PoolTimeout = cfg.PoolTimeout
	opt.MinIdleConns = cfg.MinIdleConns
	opt.MaxIdleConns = cfg.MaxIdleConns
	opt.ConnMaxIdleTime = cfg.ConnMaxIdleTime
	opt.ConnMaxLifetime = cfg.ConnMaxLifetime

	// Sentinel / Cluster specific
	if cfg.MasterName != "" {
		opt.MasterName = cfg.MasterName
		opt.SentinelUsername = cfg.SentinelUsername
		opt.SentinelPassword = cfg.SentinelPassword
	}
	opt.MaxRedirects = cfg.MaxRedirects
	opt.RouteByLatency = cfg.RouteByLatency
	opt.RouteRandomly = cfg.RouteRandomly

	client := redis.NewUniversalClient(opt)
	_, isCluster := client.(*redis.ClusterClient)
	if isCluster && cfg.DB != 0 {
		_ = client.Close()
		return nil, errors.New("redis cluster requires database 0")
	}

	// Ping health check
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if _, err := client.Ping(pingCtx).Result(); err != nil {
		logger.Warn("Redis unavailable during startup. The client will retry on use.", zap.Error(err))
	}

	appCtx, cancel := context.WithCancel(ctx)

	return &LocalRedisClient{
		logger:      logger,
		db:          client,
		prefix:      cfg.Prefix,
		ctx:         appCtx,
		ctxCancelFn: cancel,
		isCluster:   isCluster,
	}, nil
}

func (r *LocalRedisClient) Close() {
	r.stopOnce.Do(func() {
		if r.ctxCancelFn != nil {
			r.ctxCancelFn()
		}
		if err := r.db.Close(); err != nil {
			r.logger.Error("failed to close redis client", zap.Error(err))
		}
		r.logger.Info("Redis client closed")
	})
}

func (r *LocalRedisClient) Context() context.Context {
	return r.ctx
}

func (r *LocalRedisClient) DB() redis.UniversalClient {
	return r.db
}

func (r *LocalRedisClient) Prefix() string {
	return r.prefix
}

func (r *LocalRedisClient) Namer(name string, subname ...string) string {
	parts := []string{r.prefix, name}
	if len(subname) > 0 {
		parts = append(parts, subname...)
	}
	return strings.Join(parts, ":")
}

func (r *LocalRedisClient) EventChannel(name string, subname ...string) string {
	parts := []string{r.prefix, EventChannel, name}
	if len(subname) > 0 {
		parts = append(parts, subname...)
	}
	return strings.Join(parts, ":")
}

func (r *LocalRedisClient) IsCluster() bool {
	return r.isCluster
}

// ObtainLock acquires a distributed lock using redislock.
// Caller MUST call lock.Release() when done (preferably with defer).
func (r *LocalRedisClient) ObtainLock(ctx context.Context, key string, opts ...LockOption) (*redislock.Lock, error) {
	o := &lockOptions{ttl: DefaultLockTimeout}
	for _, opt := range opts {
		opt(o)
	}

	locker := redislock.New(r.db)
	lockKey := r.Namer(LockKeyPrefix, key)

	lock, err := locker.Obtain(ctx, lockKey, o.ttl, &redislock.Options{
		RetryStrategy: o.retryStrategy,
	})
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// ObtainLockWithRetry acquires lock with exponential backoff retry
func (r *LocalRedisClient) ObtainLockWithRetry(ctx context.Context, key string, opts ...LockOption) (*redislock.Lock, error) {
	o := &lockOptions{ttl: DefaultLockTimeout}
	for _, opt := range opts {
		opt(o)
	}

	// Default exponential backoff if not provided
	if o.retryStrategy == nil {
		o.retryStrategy = redislock.LimitRetry(
			redislock.ExponentialBackoff(8*time.Millisecond, 512*time.Millisecond),
			10, // 最多重试 10 次
		)
	}

	locker := redislock.New(r.db)
	lockKey := r.Namer(LockKeyPrefix, key)

	lock, err := locker.Obtain(ctx, lockKey, o.ttl, &redislock.Options{
		RetryStrategy: o.retryStrategy,
	})
	if err != nil {
		if err == redislock.ErrNotObtained {
			return nil, ErrLockTimeout
		}
		return nil, err
	}
	return lock, nil
}
