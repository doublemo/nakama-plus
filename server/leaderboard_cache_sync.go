package server

import (
	"context"
	"sort"
	"sync"

	"github.com/doublemo/nakama-plus/v3/internal/cronexpr"
	"go.uber.org/zap"
)

// LeaderboardCacheSync 扩展 LocalLeaderboardCache 以支持集群同步
type LeaderboardCacheSync struct {
	*LocalLeaderboardCache
	peer   Peer
	logger *zap.Logger
}

// NewLeaderboardCacheSync 创建支持集群同步的 LeaderboardCache
func NewLeaderboardCacheSync(ctx context.Context, logger, startupLogger *zap.Logger, db *sql.DB, peer Peer) LeaderboardCache {
	baseCache := NewLocalLeaderboardCache(ctx, logger, startupLogger, db).(*LocalLeaderboardCache)

	return &LeaderboardCacheSync{
		LocalLeaderboardCache: baseCache,
		peer:                  peer,
		logger:                logger,
	}
}

// Create 重写 Create 方法以支持集群同步
func (l *LeaderboardCacheSync) Create(ctx context.Context, id string, authoritative bool, sortOrder, operator int, resetSchedule, metadata string, enableRanks bool) (*Leaderboard, bool, error) {
	leaderboard, created, err := l.LocalLeaderboardCache.Create(ctx, id, authoritative, sortOrder, operator, resetSchedule, metadata, enableRanks)

	// 如果创建成功且是新创建的，同步到集群
	if err == nil && created && l.peer != nil {
		l.peer.LeaderboardCreate(leaderboard)
		l.logger.Debug("Broadcasted leaderboard create to cluster",
			zap.String("leaderboard_id", id),
			zap.Bool("authoritative", authoritative))
	}

	return leaderboard, created, err
}

// CreateTournament 重写 CreateTournament 方法以支持集群同步
func (l *LeaderboardCacheSync) CreateTournament(ctx context.Context, id string, authoritative bool, sortOrder, operator int, resetSchedule, metadata, title, description string, category, startTime, endTime, duration, maxSize, maxNumScore int, joinRequired, enableRanks bool) (*Leaderboard, bool, error) {
	leaderboard, created, err := l.LocalLeaderboardCache.CreateTournament(ctx, id, authoritative, sortOrder, operator, resetSchedule, metadata, title, description, category, startTime, endTime, duration, maxSize, maxNumScore, joinRequired, enableRanks)

	// 如果创建成功且是新创建的，同步到集群
	if err == nil && created && l.peer != nil {
		l.peer.LeaderboardCreate(leaderboard)
		l.logger.Debug("Broadcasted tournament create to cluster",
			zap.String("tournament_id", id),
			zap.String("title", title))
	}

	return leaderboard, created, err
}

// Delete 重写 Delete 方法以支持集群同步
func (l *LeaderboardCacheSync) Delete(ctx context.Context, rankCache LeaderboardRankCache, scheduler LeaderboardScheduler, id string) (bool, error) {
	deleted, err := l.LocalLeaderboardCache.Delete(ctx, rankCache, scheduler, id)

	// 如果删除成功，同步到集群
	if err == nil && deleted && l.peer != nil {
		l.peer.LeaderboardDelete(id)
		l.logger.Debug("Broadcasted leaderboard delete to cluster",
			zap.String("leaderboard_id", id))
	}

	return deleted, err
}

// HandleRemoteCreate 处理来自远程节点的 leaderboard 创建事件
func (l *LeaderboardCacheSync) HandleRemoteCreate(leaderboard *Leaderboard) {
	l.Lock()
	defer l.Unlock()

	// 检查是否已存在
	if _, exists := l.leaderboards[leaderboard.Id]; exists {
		l.logger.Debug("Leaderboard already exists, skipping remote create",
			zap.String("leaderboard_id", leaderboard.Id))
		return
	}

	// 解析重置计划
	if leaderboard.ResetScheduleStr != "" {
		expr, err := cronexpr.Parse(leaderboard.ResetScheduleStr)
		if err != nil {
			l.logger.Error("Error parsing leaderboard reset schedule from remote",
				zap.String("leaderboard_id", leaderboard.Id),
				zap.String("reset_schedule", leaderboard.ResetScheduleStr),
				zap.Error(err))
			return
		}
		leaderboard.ResetSchedule = expr
	}

	// 添加到缓存
	l.leaderboards[leaderboard.Id] = leaderboard
	l.allList = append(l.allList, leaderboard)

	if leaderboard.IsTournament() {
		l.tournamentList = append(l.tournamentList, leaderboard)
		sort.Sort(OrderedTournaments(l.tournamentList))
	} else {
		l.leaderboardList = append(l.leaderboardList, leaderboard)
	}

	l.logger.Debug("Added remote leaderboard to cache",
		zap.String("leaderboard_id", leaderboard.Id),
		zap.Bool("is_tournament", leaderboard.IsTournament()))
}

// HandleRemoteDelete 处理来自远程节点的 leaderboard 删除事件
func (l *LeaderboardCacheSync) HandleRemoteDelete(id string) {
	l.Lock()
	defer l.Unlock()

	leaderboard, exists := l.leaderboards[id]
	if !exists {
		l.logger.Debug("Leaderboard not found for remote delete",
			zap.String("leaderboard_id", id))
		return
	}

	// 从缓存中删除
	delete(l.leaderboards, id)

	// 从列表中移除
	l.removeFromLists(id, leaderboard.IsTournament())

	l.logger.Debug("Removed remote leaderboard from cache",
		zap.String("leaderboard_id", id),
		zap.Bool("was_tournament", leaderboard.IsTournament()))
}

// removeFromLists 从相应的列表中移除 leaderboard
func (l *LeaderboardCacheSync) removeFromLists(id string, isTournament bool) {
	// 从 allList 中移除
	for i, lb := range l.allList {
		if lb.Id == id {
			copy(l.allList[i:], l.allList[i+1:])
			l.allList[len(l.allList)-1] = nil
			l.allList = l.allList[:len(l.allList)-1]
			break
		}
	}

	// 从相应的专门列表中移除
	if isTournament {
		for i, lb := range l.tournamentList {
			if lb.Id == id {
				copy(l.tournamentList[i:], l.tournamentList[i+1:])
				l.tournamentList[len(l.tournamentList)-1] = nil
				l.tournamentList = l.tournamentList[:len(l.tournamentList)-1]
				break
			}
		}
	} else {
		for i, lb := range l.leaderboardList {
			if lb.Id == id {
				copy(l.leaderboardList[i:], l.leaderboardList[i+1:])
				l.leaderboardList[len(l.leaderboardList)-1] = nil
				l.leaderboardList = l.leaderboardList[:len(l.leaderboardList)-1]
				break
			}
		}
	}
}
