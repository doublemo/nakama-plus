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
	"errors"
	"net"
	"runtime"
	"strings"
	"time"
)

// RedisConfig holds configuration for connecting to Redis.
// It supports standalone, sentinel, and cluster modes via go-redis UniversalClient.
type RedisConfig struct {
	Enabled bool `yaml:"enabled" json:"enabled" usage:"Enable the shared Redis client. Default false."`
	// Addresses is a list of Redis server addresses in "host:port" format.
	// For cluster/sentinel, multiple addresses can be provided.
	// Default: ["127.0.0.1:6379"]
	Addresses []string `yaml:"address" json:"address" usage:"Redis server address(es) in host:port format (comma-separated or array)"`

	// DB is the database number to select (only for standalone and failover clients).
	// Default: 0
	DB int `yaml:"db" json:"db" usage:"Database number (standalone & sentinel mode only)"`

	// Username and Password for Redis ACL authentication.
	Username string `yaml:"username" json:"username" usage:"Redis ACL username"`
	Password string `yaml:"password" json:"password" usage:"Redis ACL password"`

	// SentinelUsername and SentinelPassword for Sentinel authentication.
	SentinelUsername string `yaml:"sentinel_username" json:"sentinel_username" usage:"Sentinel authentication username"`
	SentinelPassword string `yaml:"sentinel_password" json:"sentinel_password" usage:"Sentinel authentication password"`

	// AllowSSL enables TLS connection.
	AllowSSL bool `yaml:"allow_ssl" json:"allow_ssl" usage:"Enable TLS/SSL connection"`

	// TLSServerName is used for SNI (Server Name Indication) during TLS handshake.
	TLSServerName string `yaml:"tls_server_name" json:"tls_server_name" usage:"Server name for TLS SNI and certificate verification"`

	// TLSCert and TLSKey are paths to client certificate and key for mutual TLS.
	TLSCert string `yaml:"tls_cert" json:"tls_cert" usage:"Path to client certificate file (mTLS)"`
	TLSKey  string `yaml:"tls_key" json:"tls_key" usage:"Path to client private key file (mTLS)"`

	// Connection timeouts and retry settings
	MaxRetries            int           `yaml:"max_retries" json:"max_retries" usage:"Maximum number of command retries on failure (0 = no retry)"`
	MinRetryBackoff       time.Duration `yaml:"min_retry_backoff" json:"min_retry_backoff" usage:"Minimum backoff duration between retries (e.g. 8ms)"`
	MaxRetryBackoff       time.Duration `yaml:"max_retry_backoff" json:"max_retry_backoff" usage:"Maximum backoff duration between retries (e.g. 512ms)"`
	DialTimeout           time.Duration `yaml:"dial_timeout" json:"dial_timeout" usage:"Timeout for establishing new connection"`
	ReadTimeout           time.Duration `yaml:"read_timeout" json:"read_timeout" usage:"Socket read timeout (0 = no timeout)"`
	WriteTimeout          time.Duration `yaml:"write_timeout" json:"write_timeout" usage:"Socket write timeout (0 = no timeout)"`
	ContextTimeoutEnabled bool          `yaml:"context_timeout_enabled" json:"context_timeout_enabled" usage:"Respect context timeout for all operations"`

	// Connection pool settings
	PoolFIFO        bool          `yaml:"pool_fifo" json:"pool_fifo" usage:"Use FIFO (fair) connection pool instead of LIFO"`
	PoolSize        int           `yaml:"pool_size" json:"pool_size" usage:"Maximum number of socket connections (default ~10 × CPU)"`
	PoolTimeout     time.Duration `yaml:"pool_timeout" json:"pool_timeout" usage:"Maximum time to wait for a connection from pool"`
	MinIdleConns    int           `yaml:"min_idle_conns" json:"min_idle_conns" usage:"Minimum number of idle connections to maintain"`
	MaxIdleConns    int           `yaml:"max_idle_conns" json:"max_idle_conns" usage:"Maximum number of idle connections (0 = no limit)"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time" json:"conn_max_idle_time" usage:"Max idle time for a connection before closing"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_life_time" json:"conn_max_life_time" usage:"Max lifetime of a connection (force reconnect after)"`

	// Cluster-specific options
	ClusterMode    bool `yaml:"cluster_mode" json:"cluster_mode" usage:"Use Redis Cluster with a single seed address."`
	MaxRedirects   int  `yaml:"max_redirects" json:"max_redirects" usage:"Maximum number of MOVED/ASK redirections to follow"`
	ReadOnly       bool `yaml:"read_only" json:"read_only" usage:"Allow routing read commands to replicas"`
	RouteByLatency bool `yaml:"route_by_latency" json:"route_by_latency" usage:"Route commands to the nearest node by latency"`
	RouteRandomly  bool `yaml:"route_randomly" json:"route_randomly" usage:"Route commands randomly among all nodes"`

	// Sentinel-specific options
	MasterName string `yaml:"master_name" json:"master_name" usage:"Name of Redis master set in Sentinel mode (required for sentinel)"`

	// Key prefix added to all keys
	Prefix string `yaml:"prefix" json:"prefix" usage:"Global key prefix prepended to all keys"`
}

// Default values when creating a new config
func NewRedisConfig() *RedisConfig {
	return &RedisConfig{
		Addresses:             []string{"127.0.0.1:6379"},
		DB:                    0,
		MaxRetries:            3,
		MinRetryBackoff:       100 * time.Millisecond,
		MaxRetryBackoff:       2 * time.Second,
		DialTimeout:           5 * time.Second,
		ReadTimeout:           3 * time.Second,
		WriteTimeout:          3 * time.Second,
		ContextTimeoutEnabled: true,
		PoolSize:              20 + runtime.NumCPU()*2, // reasonable default: 20
		MinIdleConns:          5,
		MaxIdleConns:          30,
		PoolTimeout:           5 * time.Second,
		ConnMaxIdleTime:       4 * time.Minute,
		ConnMaxLifetime:       30 * time.Minute, // no limit by default
		MaxRedirects:          8,
		Prefix:                "nakama",
	}
}

// Validate checks if the configuration is reasonable
func (c *RedisConfig) Validate() error {
	if c == nil || len(c.Addresses) == 0 {
		return errors.New("redis addresses cannot be empty")
	}

	for _, addr := range c.Addresses {
		if host, port, err := net.SplitHostPort(addr); err != nil || host == "" || port == "" {
			return errors.New("redis addresses must use host:port format")
		}
	}

	if c.AllowSSL {
		if c.TLSCert != "" && c.TLSKey == "" || c.TLSCert == "" && c.TLSKey != "" {
			return errors.New("both TLSCert and TLSKey must be provided when using client certificate")
		}
	}

	if strings.TrimSpace(c.Prefix) == "" {
		return errors.New("redis prefix cannot be empty")
	}
	if c.DB < 0 {
		return errors.New("redis database number must be non-negative")
	}

	return nil
}

func (c *RedisConfig) Clone() *RedisConfig {
	if c == nil {
		return nil
	}
	clone := *c
	clone.Addresses = make([]string, len(c.Addresses))
	copy(clone.Addresses, c.Addresses)
	return &clone
}
