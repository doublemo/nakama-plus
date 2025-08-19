package server

import (
	"context"
	"database/sql"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-plus/v3/internal/skiplist"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
)

// LeaderboardRankCacheSync 扩展 LocalLeaderboardRankCache 以支持集群同步
type LeaderboardRankCacheSync struct {
	*LocalLeaderboardRankCache
	peer   Peer
	logger *zap.Logger
}

// NewLeaderboardRankCacheSync 创建支持集群同步的 LeaderboardRankCache
func NewLeaderboardRankCacheSync(ctx context.Context, startupLogger *zap.Logger, db *sql.DB, config *LeaderboardConfig, leaderboardCache LeaderboardCache, peer Peer) LeaderboardRankCache {
	baseCache := NewLocalLeaderboardRankCache(ctx, startupLogger, db, config, leaderboardCache).(*LocalLeaderboardRankCache)

	return &LeaderboardRankCacheSync{
		LocalLeaderboardRankCache: baseCache,
		peer:                      peer,
		logger:                    startupLogger,
	}
}

// Insert 重写 Insert 方法以支持集群同步
func (l *LeaderboardRankCacheSync) Insert(leaderboardId string, sortOrder int, score, subscore int64, generation int32, expiryUnix int64, ownerID uuid.UUID, enableRanks bool) int64 {
	rank := l.LocalLeaderboardRankCache.Insert(leaderboardId, sortOrder, score, subscore, generation, expiryUnix, ownerID, enableRanks)

	// 如果启用了排名且不在黑名单中，同步到集群
	if l.peer != nil && enableRanks && !l.isBlacklisted(leaderboardId) && rank > 0 {
		l.peer.LeaderboardRankUpdate(leaderboardId, expiryUnix, ownerID, score, subscore, generation, sortOrder)
		l.logger.Debug("Broadcasted rank update to cluster",
			zap.String("leaderboard_id", leaderboardId),
			zap.String("owner_id", ownerID.String()),
			zap.Int64("score", score),
			zap.Int64("rank", rank))
	}

	return rank
}

// Delete 重写 Delete 方法以支持集群同步
func (l *LeaderboardRankCacheSync) Delete(leaderboardId string, expiryUnix int64, ownerID uuid.UUID) bool {
	deleted := l.LocalLeaderboardRankCache.Delete(leaderboardId, expiryUnix, ownerID)

	// 如果删除成功且不在黑名单中，同步到集群
	if deleted && l.peer != nil && !l.isBlacklisted(leaderboardId) {
		l.peer.LeaderboardRankDelete(leaderboardId, expiryUnix, ownerID)
		l.logger.Debug("Broadcasted rank delete to cluster",
			zap.String("leaderboard_id", leaderboardId),
			zap.String("owner_id", ownerID.String()))
	}

	return deleted
}

// HandleRemoteRankUpdate 处理来自远程节点的排名更新事件
func (l *LeaderboardRankCacheSync) HandleRemoteRankUpdate(leaderboardId string, expiryUnix int64, ownerID uuid.UUID, score, subscore int64, generation int32, sortOrder int) {
	if l.isBlacklisted(leaderboardId) {
		l.logger.Debug("Skipping remote rank update for blacklisted leaderboard",
			zap.String("leaderboard_id", leaderboardId))
		return
	}

	key := LeaderboardWithExpiry{LeaderboardId: leaderboardId, Expiry: expiryUnix}

	// 获取或创建 rank cache
	l.RLock()
	rankCache, ok := l.cache[key]
	l.RUnlock()

	if !ok {
		newRankCache := &RankCache{
			owners: make(map[uuid.UUID]cachedRecord),
			cache:  skiplist.New(),
		}
		l.Lock()
		// 双重检查
		if rankCache, ok = l.cache[key]; !ok {
			rankCache = newRankCache
			l.cache[key] = rankCache
		}
		l.Unlock()
	}

	// 准备新的排名数据
	rankData := newRank(sortOrder, score, subscore, ownerID)

	rankCache.Lock()
	oldRankData, exists := rankCache.owners[ownerID]

	// 只有当 generation 更新时才更新（避免重复处理）
	if !exists || generation > oldRankData.generation {
		if exists {
			rankCache.cache.Delete(oldRankData.record)
		}

		rankCache.owners[ownerID] = cachedRecord{generation: generation, record: rankData}
		rankCache.cache.Insert(rankData)

		l.logger.Debug("Applied remote rank update",
			zap.String("leaderboard_id", leaderboardId),
			zap.String("owner_id", ownerID.String()),
			zap.Int64("score", score),
			zap.Int32("generation", generation),
			zap.Bool("was_existing", exists))
	} else {
		l.logger.Debug("Skipped remote rank update (stale generation)",
			zap.String("leaderboard_id", leaderboardId),
			zap.String("owner_id", ownerID.String()),
			zap.Int32("remote_generation", generation),
			zap.Int32("local_generation", oldRankData.generation))
	}
	rankCache.Unlock()
}

// HandleRemoteRankDelete 处理来自远程节点的排名删除事件
func (l *LeaderboardRankCacheSync) HandleRemoteRankDelete(leaderboardId string, expiryUnix int64, ownerID uuid.UUID) {
	if l.isBlacklisted(leaderboardId) {
		l.logger.Debug("Skipping remote rank delete for blacklisted leaderboard",
			zap.String("leaderboard_id", leaderboardId))
		return
	}

	key := LeaderboardWithExpiry{LeaderboardId: leaderboardId, Expiry: expiryUnix}

	l.RLock()
	rankCache, ok := l.cache[key]
	l.RUnlock()

	if !ok {
		l.logger.Debug("No rank cache found for remote delete",
			zap.String("leaderboard_id", leaderboardId),
			zap.Int64("expiry", expiryUnix))
		return
	}

	rankCache.Lock()
	if rankData, exists := rankCache.owners[ownerID]; exists {
		delete(rankCache.owners, ownerID)
		rankCache.cache.Delete(rankData.record)

		l.logger.Debug("Applied remote rank delete",
			zap.String("leaderboard_id", leaderboardId),
			zap.String("owner_id", ownerID.String()))
	} else {
		l.logger.Debug("Owner not found for remote rank delete",
			zap.String("leaderboard_id", leaderboardId),
			zap.String("owner_id", ownerID.String()))
	}
	rankCache.Unlock()
}

// isBlacklisted 检查 leaderboard 是否在黑名单中
func (l *LeaderboardRankCacheSync) isBlacklisted(leaderboardId string) bool {
	if l.blacklistAll {
		return true
	}
	_, blacklisted := l.blacklistIds[leaderboardId]
	return blacklisted
}

// BatchUpdate 批量更新排名（用于优化大量更新的场景）
func (l *LeaderboardRankCacheSync) BatchUpdate(updates []RankUpdate) {
	if l.peer == nil {
		return
	}

	// 按 leaderboard 分组
	groupedUpdates := make(map[string][]RankUpdate)
	for _, update := range updates {
		if !l.isBlacklisted(update.LeaderboardId) {
			groupedUpdates[update.LeaderboardId] = append(groupedUpdates[update.LeaderboardId], update)
		}
	}

	// 分组发送更新
	for leaderboardId, leaderboardUpdates := range groupedUpdates {
		for _, update := range leaderboardUpdates {
			l.peer.LeaderboardRankUpdate(
				update.LeaderboardId,
				update.ExpiryUnix,
				update.OwnerID,
				update.Score,
				update.Subscore,
				update.Generation,
				update.SortOrder,
			)
		}

		l.logger.Debug("Broadcasted batch rank updates to cluster",
			zap.String("leaderboard_id", leaderboardId),
			zap.Int("update_count", len(leaderboardUpdates)))
	}
}

// RankUpdate 表示一个排名更新
type RankUpdate struct {
	LeaderboardId string
	ExpiryUnix    int64
	OwnerID       uuid.UUID
	Score         int64
	Subscore      int64
	Generation    int32
	SortOrder     int
}
