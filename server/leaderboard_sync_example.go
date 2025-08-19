package server

import (
	"context"
	"database/sql"

	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
)

// 这个文件展示如何在 main.go 中集成 leaderboard 集群同步功能

// InitializeLeaderboardCacheWithSync 初始化支持集群同步的 leaderboard cache
func InitializeLeaderboardCacheWithSync(
	ctx context.Context,
	logger, startupLogger *zap.Logger,
	db *sql.DB,
	config *LeaderboardConfig,
	peer Peer,
) (LeaderboardCache, LeaderboardRankCache) {

	// 创建支持集群同步的 leaderboard cache
	leaderboardCache := NewLeaderboardCacheSync(ctx, logger, startupLogger, db, peer)

	// 创建支持集群同步的 leaderboard rank cache
	leaderboardRankCache := NewLeaderboardRankCacheSync(ctx, startupLogger, db, config, leaderboardCache, peer)

	return leaderboardCache, leaderboardRankCache
}

// 在 main.go 中的使用示例：
/*
func main() {
	// ... 其他初始化代码

	// 创建 peer（集群节点）
	peer := NewLocalPeer(db, logger, config.GetName(), metadata, runtime, metrics,
		sessionRegistry, tracker, messageRouter, matchRegistry, matchmaker,
		partyRegistry, protojsonMarshaler, protojsonUnmarshaler, config)

	// 使用支持集群同步的 leaderboard cache
	leaderboardCache, leaderboardRankCache := InitializeLeaderboardCacheWithSync(
		ctx, logger, startupLogger, db, config.GetLeaderboard(), peer)

	// 创建 leaderboard scheduler
	leaderboardScheduler := NewLocalLeaderboardScheduler(logger, db, config,
		leaderboardCache, leaderboardRankCache)

	// ... 其他初始化代码

	// 启动集群
	if _, err := peer.Join(config.GetCluster().Join...); err != nil {
		startupLogger.Fatal("Failed to join cluster", zap.Error(err))
	}

	// ... 启动服务器
}
*/

// 使用示例：创建一个 leaderboard 并自动同步到集群
func ExampleCreateLeaderboard(leaderboardCache LeaderboardCache) {
	ctx := context.Background()

	// 创建一个新的 leaderboard，会自动同步到集群中的其他节点
	leaderboard, created, err := leaderboardCache.Create(
		ctx,
		"global_ranking",               // ID
		true,                           // authoritative
		LeaderboardSortOrderDescending, // sort order
		LeaderboardOperatorBest,        // operator
		"0 0 * * 0",                    // reset schedule (每周日重置)
		`{"description": "Global player ranking"}`, // metadata
		true, // enable ranks
	)

	if err != nil {
		// 处理错误
		return
	}

	if created {
		// leaderboard 创建成功，已自动同步到集群
		_ = leaderboard
	}
}

// 使用示例：更新玩家排名并自动同步到集群
func ExampleUpdatePlayerRank(leaderboardRankCache LeaderboardRankCache) {
	// 更新玩家排名，会自动同步到集群中的其他节点
	rank := leaderboardRankCache.Insert(
		"global_ranking",               // leaderboard ID
		LeaderboardSortOrderDescending, // sort order
		1000,                           // score
		500,                            // subscore
		1,                              // generation
		0,                              // expiry (0 表示不过期)
		mustParseUUID("550e8400-e29b-41d4-a716-446655440000"), // owner ID
		true, // enable ranks
	)

	// rank 现在包含玩家的当前排名
	_ = rank
}

// 辅助函数
func mustParseUUID(s string) uuid.UUID {
	id, err := uuid.FromString(s)
	if err != nil {
		panic(err)
	}
	return id
}
