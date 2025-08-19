package server

import "time"

// LeaderboardSyncConfig 配置 leaderboard 集群同步行为
type LeaderboardSyncConfig struct {
	// Enabled 是否启用 leaderboard 集群同步
	Enabled bool `yaml:"enabled" json:"enabled"`

	// BatchSize 批量同步的大小
	BatchSize int `yaml:"batch_size" json:"batch_size"`

	// BatchTimeout 批量同步的超时时间
	BatchTimeout time.Duration `yaml:"batch_timeout" json:"batch_timeout"`

	// EnableCompression 是否启用消息压缩
	EnableCompression bool `yaml:"enable_compression" json:"enable_compression"`

	// SyncBlacklist 不进行同步的 leaderboard ID 列表
	SyncBlacklist []string `yaml:"sync_blacklist" json:"sync_blacklist"`

	// MaxRetries 同步失败时的最大重试次数
	MaxRetries int `yaml:"max_retries" json:"max_retries"`

	// RetryInterval 重试间隔
	RetryInterval time.Duration `yaml:"retry_interval" json:"retry_interval"`
}

// NewDefaultLeaderboardSyncConfig 创建默认的同步配置
func NewDefaultLeaderboardSyncConfig() *LeaderboardSyncConfig {
	return &LeaderboardSyncConfig{
		Enabled:           true,
		BatchSize:         100,
		BatchTimeout:      time.Second,
		EnableCompression: false,
		SyncBlacklist:     []string{},
		MaxRetries:        3,
		RetryInterval:     time.Second * 5,
	}
}

// IsBlacklisted 检查指定的 leaderboard ID 是否在黑名单中
func (c *LeaderboardSyncConfig) IsBlacklisted(leaderboardId string) bool {
	for _, blacklisted := range c.SyncBlacklist {
		if blacklisted == "*" || blacklisted == leaderboardId {
			return true
		}
		// 支持简单的通配符匹配
		if len(blacklisted) > 0 && blacklisted[len(blacklisted)-1] == '*' {
			prefix := blacklisted[:len(blacklisted)-1]
			if len(leaderboardId) >= len(prefix) && leaderboardId[:len(prefix)] == prefix {
				return true
			}
		}
	}
	return false
}
