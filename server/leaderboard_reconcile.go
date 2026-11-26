package server

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/doublemo/nakama-plus/v3/internal/skiplist"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
)

func (ls *LocalLeaderboardScheduler) reconcileLoop(interval time.Duration) {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ls.ctx.Done():
			return
		case <-timer.C:
			if cache, ok := ls.cache.(*LocalLeaderboardCache); ok {
				changed, err := cache.refreshAll(ls.ctx)
				if err != nil && ls.ctx.Err() == nil {
					ls.logger.Warn("Failed to reconcile leaderboard definitions", zap.Error(err))
				}
				if changed {
					ls.Update()
				}
			}
			if ranks, ok := ls.rankCache.(*LocalLeaderboardRankCache); ok {
				if err := ranks.reconcile(ls.ctx, ls.db, ls.config.GetLeaderboard().RankCacheWorkers, ls.cache, time.Now().UTC()); err != nil && ls.ctx.Err() == nil {
					ls.logger.Warn("Failed to reconcile leaderboard ranks", zap.Error(err))
				}
			}
			timer.Reset(interval)
		}
	}
}

func (l *LocalLeaderboardRankCache) reconcile(ctx context.Context, db *sql.DB, workers int, definitions LeaderboardCache, now time.Time) error {
	if l.blacklistAll {
		return nil
	}
	l.refreshMu.Lock()
	defer l.refreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	l.Lock()
	for key := range l.cache {
		definition := definitions.Get(key.LeaderboardId)
		if definition == nil || !definition.EnableRanks || (key.Expiry != 0 && key.Expiry <= now.Unix()) {
			delete(l.cache, key)
		}
	}
	l.Unlock()

	work := make(chan *Leaderboard)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for range max(workers, 1) {
		wg.Go(func() {
			for definition := range work {
				key := LeaderboardWithExpiry{LeaderboardId: definition.Id, Expiry: rankExpiry(definition, now)}
				if key.Expiry != 0 && key.Expiry <= now.Unix() {
					continue
				}
				err := l.refreshRanks(key, func() (*RankCache, error) {
					return loadLeaderboardRanks(ctx, db, definition.SortOrder, key)
				}, func() bool {
					return sameLeaderboardDefinition(definitions.Get(definition.Id), definition)
				})
				if err != nil {
					once.Do(func() { firstErr = err })
				}
			}
		})
	}
	var cursor *LeaderboardAllCursor
	seen := make(map[string]struct{})
feed:
	for {
		var list []*Leaderboard
		list, _, cursor = definitions.ListAll(1000, false, cursor)
		for _, definition := range list {
			if _, forbidden := l.blacklistIds[definition.Id]; forbidden || !definition.EnableRanks {
				continue
			}
			if _, ok := seen[definition.Id]; ok {
				continue
			}
			seen[definition.Id] = struct{}{}
			select {
			case work <- definition:
			case <-ctx.Done():
				break feed
			}
		}
		if cursor == nil {
			break
		}
	}
	close(work)
	wg.Wait()
	return firstErr
}

func rankExpiry(definition *Leaderboard, now time.Time) int64 {
	if definition.IsTournament() {
		_, _, expiry := calculateTournamentDeadlines(definition.StartTime, definition.EndTime, int64(definition.Duration), definition.ResetSchedule, now)
		return expiry
	}
	if definition.ResetSchedule != nil {
		return definition.ResetSchedule.Next(now).Unix()
	}
	return 0
}

func (l *LocalLeaderboardRankCache) refreshRanks(key LeaderboardWithExpiry, load func() (*RankCache, error), valid func() bool) error {
	l.Lock()
	live := l.cache[key]
	if live == nil {
		live = &RankCache{cache: skiplist.New(), owners: make(map[uuid.UUID]cachedRecord)}
		l.cache[key] = live
	}
	live.Lock()
	live.refreshOwners = make(map[uuid.UUID]struct{})
	live.Unlock()
	l.Unlock()

	fresh, err := load()
	l.RLock()
	defer l.RUnlock()
	live.Lock()
	defer live.Unlock()
	defer func() { live.refreshOwners = nil }()
	if err != nil || l.cache[key] != live || !valid() {
		return err
	}
	// Preserve the final cached value (or deletion) for every owner updated
	// while the database scan was running.
	for owner := range live.refreshOwners {
		if old, ok := fresh.owners[owner]; ok {
			fresh.cache.Delete(old.record)
			delete(fresh.owners, owner)
		}
		if current, ok := live.owners[owner]; ok {
			fresh.owners[owner] = current
			fresh.cache.Insert(current.record)
		}
	}
	live.cache, live.owners = fresh.cache, fresh.owners
	return nil
}

func loadLeaderboardRanks(ctx context.Context, db *sql.DB, sortOrder int, key LeaderboardWithExpiry) (*RankCache, error) {
	// A single streaming query gives all rows in a consistent statement
	// snapshot even if concurrent writes change their score ordering.
	rows, err := db.QueryContext(ctx, `SELECT owner_id, score, subscore, num_score FROM leaderboard_record
		WHERE leaderboard_id = $1 AND expiry_time = $2`, key.LeaderboardId, time.Unix(key.Expiry, 0).UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fresh := &RankCache{cache: skiplist.New(), owners: make(map[uuid.UUID]cachedRecord)}
	for rows.Next() {
		var owner uuid.UUID
		var score, subscore int64
		var generation int32
		if err := rows.Scan(&owner, &score, &subscore, &generation); err != nil {
			return nil, err
		}
		record := newRank(sortOrder, score, subscore, owner)
		fresh.cache.Insert(record)
		fresh.owners[owner] = cachedRecord{generation: generation, record: record}
	}
	return fresh, rows.Err()
}
