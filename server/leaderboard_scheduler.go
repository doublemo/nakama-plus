// Copyright 2018 The Nakama Authors
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
	"database/sql"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type LeaderboardSchedulerCallback struct {
	id          string
	leaderboard *Leaderboard
	ts          int64
	t           time.Time
	kind        leaderboardCallbackKind
}

type LeaderboardScheduler interface {
	Start(runtime *Runtime)
	Pause()
	Resume()
	Stop()
	Update()
}

type LocalLeaderboardScheduler struct {
	logger    *zap.Logger
	db        *sql.DB
	config    Config
	cache     LeaderboardCache
	rankCache LeaderboardRankCache
	callbacks leaderboardCallbackStore

	fnLeaderboardReset RuntimeLeaderboardResetFunction
	fnTournamentReset  RuntimeTournamentResetFunction
	fnTournamentEnd    RuntimeTournamentEndFunction

	started bool
	active  *atomic.Uint32
	queue   chan *LeaderboardSchedulerCallback

	updateCh chan struct{}

	ctx         context.Context
	ctxCancelFn context.CancelFunc
}

func NewLocalLeaderboardScheduler(logger *zap.Logger, db *sql.DB, config Config, cache LeaderboardCache, rankCache LeaderboardRankCache) LeaderboardScheduler {
	ctx, ctxCancelFn := context.WithCancel(context.Background())
	s := &LocalLeaderboardScheduler{
		logger:    logger,
		db:        db,
		config:    config,
		cache:     cache,
		rankCache: rankCache,
		callbacks: &dbLeaderboardCallbacks{db: db},

		queue:    make(chan *LeaderboardSchedulerCallback, config.GetLeaderboard().CallbackQueueSize),
		active:   atomic.NewUint32(1),
		updateCh: make(chan struct{}, 1),

		ctx:         ctx,
		ctxCancelFn: ctxCancelFn,
	}

	// Ensure trimming of expired scores that don't have resets or functions attached.
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				s.rankCache.TrimExpired(t.Unix())
			}
		}
	}()

	return s
}

func (ls *LocalLeaderboardScheduler) Start(runtime *Runtime) {
	ls.logger.Info("Leaderboard scheduler start")
	ls.started = true

	ls.fnLeaderboardReset = runtime.LeaderboardReset()
	ls.fnTournamentReset = runtime.TournamentReset()
	ls.fnTournamentEnd = runtime.TournamentEnd()

	for i := 0; i < ls.config.GetLeaderboard().CallbackQueueWorkers; i++ {
		go ls.invokeCallback()
	}

	go ls.scheduleLoop()
	if seconds := ls.config.GetLeaderboard().CacheRefreshIntervalSec; seconds > 0 {
		go ls.reconcileLoop(time.Duration(seconds) * time.Second)
	}

	ls.Update()
}

func (ls *LocalLeaderboardScheduler) Pause() {
	ls.logger.Info("Leaderboard scheduler pause")
	if !ls.active.CompareAndSwap(1, 0) {
		return
	}
	// Wake the scheduling loop so it sees the paused state immediately.
	select {
	case ls.updateCh <- struct{}{}:
	default:
	}
}

func (ls *LocalLeaderboardScheduler) Resume() {
	ls.logger.Info("Leaderboard scheduler resume")
	if !ls.active.CompareAndSwap(0, 1) {
		return
	}
	ls.Update()
}

func (ls *LocalLeaderboardScheduler) Stop() {
	ls.ctxCancelFn()
}

// Update signals the scheduling loop to recompute its next wake time.
func (ls *LocalLeaderboardScheduler) Update() {
	if !ls.started {
		return
	}
	select {
	case ls.updateCh <- struct{}{}:
	default:
	}
}

func (ls *LocalLeaderboardScheduler) scheduleLoop() {
	var lastFireUnix int64
	for {
		// While paused, block until a signal arrives.
		if ls.active.Load() == 0 {
			select {
			case <-ls.ctx.Done():
				return
			case <-ls.updateCh:
			}
			continue
		}

		now := time.Now().UTC()
		endActiveTs, expiryTs, endActiveIds, expiryIds := ls.computeNext(now)

		// Filter out any events already processed in a previous iteration to
		// guard against re-queuing hooks within the same second as the last fire.
		if endActiveTs > 0 && endActiveTs <= lastFireUnix {
			endActiveTs = -1
			endActiveIds = nil
		}
		if expiryTs > 0 && expiryTs <= lastFireUnix {
			expiryTs = -1
			expiryIds = nil
		}

		// Pick the earlier of the two deadlines as the next wake time.
		wakeTs := int64(-1)
		if endActiveTs > 0 {
			wakeTs = endActiveTs
		}
		if expiryTs > 0 && (wakeTs < 0 || expiryTs < wakeTs) {
			wakeTs = expiryTs
		}

		ls.logger.Info("Leaderboard scheduler update",
			zap.Int64("end_active_ts", endActiveTs),
			zap.Int("end_active_count", len(endActiveIds)),
			zap.Int64("expiry_ts", expiryTs),
			zap.Int("expiry_count", len(expiryIds)),
		)

		if wakeTs < 0 {
			// Nothing to schedule, block until Update() signals or context is cancelled.
			select {
			case <-ls.ctx.Done():
				return
			case <-ls.updateCh:
				continue
			}
		}

		delay := max(time.Unix(wakeTs, 0).UTC().Sub(now), 0)

		timer := time.NewTimer(delay)
		select {
		case <-ls.ctx.Done():
			timer.Stop()
			return

		case <-ls.updateCh:
			timer.Stop()
			if t := time.Now().UTC(); t.Unix() >= wakeTs {
				// An update and the timer can become ready at the same deadline.
				lastFireUnix = t.Unix()
				ls.processHooks(t, endActiveTs, expiryTs, endActiveIds, expiryIds)
			}
		case t := <-timer.C:
			lastFireUnix = t.Unix()
			ls.processHooks(t, endActiveTs, expiryTs, endActiveIds, expiryIds)
		}
	}
}

func (ls *LocalLeaderboardScheduler) processHooks(ts time.Time, endActiveTs, expiryTs int64, endActiveIds, expiryIds []string) {
	if ls.active.Load() != 1 {
		return
	}

	fireUnix := ts.Unix()
	if endActiveTs > 0 && endActiveTs <= fireUnix {
		ls.processEndActive(time.Unix(endActiveTs, 0).UTC(), endActiveIds)
	}
	if expiryTs > 0 && expiryTs <= fireUnix {
		ls.rankCache.TrimExpired(expiryTs)
		ls.processExpiry(time.Unix(expiryTs, 0).UTC(), expiryIds)
	}
}

func (ls *LocalLeaderboardScheduler) computeNext(now time.Time) (endActiveTs, expiryTs int64, endActiveIds []string, expiryIds []string) {
	endActiveTs = -1
	expiryTs = -1
	nowUnix := now.Unix()

	var cursor *LeaderboardAllCursor
	for {
		var leaderboards []*Leaderboard
		leaderboards, _, cursor = ls.cache.ListAll(1_000, false, cursor)

		for _, l := range leaderboards {
			if l.IsTournament() {
				_, endActive, expiry := calculateTournamentDeadlines(l.StartTime, l.EndTime, int64(l.Duration), l.ResetSchedule, now)

				if l.EndTime > 0 && l.EndTime < nowUnix {
					// Tournament has ended permanently.
					continue
				}

				if endActive > 0 && nowUnix < endActive {
					if endActiveTs < 0 || endActive < endActiveTs {
						endActiveTs = endActive
						endActiveIds = []string{l.Id}
					} else if endActive == endActiveTs {
						endActiveIds = append(endActiveIds, l.Id)
					}
				}

				if expiry > 0 && nowUnix < expiry {
					if expiryTs < 0 || expiry < expiryTs {
						expiryTs = expiry
						expiryIds = []string{l.Id}
					} else if expiry == expiryTs {
						expiryIds = append(expiryIds, l.Id)
					}
				}
			} else {
				// Leaderboards don't end, only check for expiry.
				if l.ResetSchedule != nil {
					expiry := l.ResetSchedule.Next(now).UTC().Unix()
					if expiryTs < 0 || expiry < expiryTs {
						expiryTs = expiry
						expiryIds = []string{l.Id}
					} else if expiry == expiryTs {
						expiryIds = append(expiryIds, l.Id)
					}
				}
			}
		}

		if cursor == nil {
			break
		}
	}
	return
}

func (ls *LocalLeaderboardScheduler) processEndActive(t time.Time, ids []string) {
	if ls.fnTournamentEnd == nil {
		return
	}

	ts := t.Unix()
	tMinusOne := time.Unix(ts-1, 0).UTC()

	ls.logger.Info("Leaderboard scheduler end active", zap.Int("count", len(ids)))

	go func() {
		for _, id := range ids {
			leaderboard := ls.cache.Get(id)
			if leaderboard == nil || !leaderboard.IsTournament() {
				continue
			}
			select {
			case ls.queue <- &LeaderboardSchedulerCallback{id: id, leaderboard: leaderboard, ts: ts, t: tMinusOne, kind: leaderboardCallbackTournamentEnd}:
			case <-ls.ctx.Done():
				return
			}
		}
	}()
}

func (ls *LocalLeaderboardScheduler) processExpiry(t time.Time, ids []string) {
	ts := t.Unix()
	tMinusOne := time.Unix(ts-1, 0).UTC()

	ls.logger.Info("Leaderboard scheduler expiry reset", zap.Int("count", len(ids)))

	go func() {
		// Queue the current set of leaderboard and tournament resets.
		// Executes inside a goroutine to ensure further invocation timings are not skewed.
		for _, id := range ids {
			leaderboard := ls.cache.Get(id)
			if leaderboard == nil {
				// Cached entry was deleted before it reached the scheduler here.
				continue
			}
			if !leaderboard.IsTournament() && ls.fnLeaderboardReset == nil {
				// Skip further processing if there is no leaderboard reset callback registered.
				// Tournaments have some processing to do even if no callback is registered.
				continue
			}
			kind := leaderboardCallbackReset
			if leaderboard.IsTournament() {
				kind = leaderboardCallbackTournamentReset
			}
			select {
			case ls.queue <- &LeaderboardSchedulerCallback{id: id, leaderboard: leaderboard, ts: ts, t: tMinusOne, kind: kind}:
			case <-ls.ctx.Done():
				return
			}
		}
	}()
}

func (ls *LocalLeaderboardScheduler) invokeCallback() {
	for {
		select {
		case <-ls.ctx.Done():
			return
		case callback := <-ls.queue:
			ls.runCallback(callback)
		}
	}
}

func (ls *LocalLeaderboardScheduler) runCallback(callback *LeaderboardSchedulerCallback) {
	invoke := true
	switch callback.kind {
	case leaderboardCallbackReset:
		if ls.fnLeaderboardReset == nil {
			return
		}
	case leaderboardCallbackTournamentEnd:
		if ls.fnTournamentEnd == nil {
			return
		}
	case leaderboardCallbackTournamentReset:
		invoke = ls.fnTournamentReset != nil
	}

	claimed, err := ls.callbacks.claim(ls.ctx, callback, invoke)
	if err != nil {
		ls.logger.Error("Failed to claim leaderboard callback", zap.String("id", callback.id), zap.Int64("deadline", callback.ts), zap.Error(err))
		return
	}
	if claimed == nil {
		return
	}

	switch callback.kind {
	case leaderboardCallbackReset:
		err = ls.fnLeaderboardReset(ls.ctx, claimed.leaderboard, callback.ts)
	case leaderboardCallbackTournamentEnd:
		err = ls.fnTournamentEnd(ls.ctx, claimed.tournament, int64(claimed.tournament.EndActive), int64(claimed.tournament.NextReset))
	case leaderboardCallbackTournamentReset:
		err = ls.fnTournamentReset(ls.ctx, claimed.tournament, int64(claimed.tournament.EndActive), int64(claimed.tournament.NextReset))
	}
	if err != nil {
		ls.logger.Warn("Leaderboard callback failed after claiming its deadline", zap.String("id", callback.id), zap.Int64("deadline", callback.ts), zap.Error(err))
	}
}
