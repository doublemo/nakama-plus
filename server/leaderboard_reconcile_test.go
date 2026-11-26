package server

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-plus/v3/internal/cronexpr"
	"github.com/doublemo/nakama-plus/v3/internal/skiplist"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
)

func TestLeaderboardRankReconciliationReplacesSnapshotAndKeepsLiveWrites(t *testing.T) {
	ranks := &LocalLeaderboardRankCache{cache: make(map[LeaderboardWithExpiry]*RankCache)}
	key := LeaderboardWithExpiry{LeaderboardId: "board"}
	const order = LeaderboardSortOrderDescending
	stale, recovered, updated, deleted := uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4())
	ranks.Insert(key.LeaderboardId, order, 90, 0, 1, 0, stale, true)
	ranks.Insert(key.LeaderboardId, order, 10, 0, 1, 0, updated, true)
	if err := ranks.refreshRanks(key, func() (*RankCache, error) {
		// Simulate local/peer changes after the database read starts. The
		// deleted owner was missing from the previous cache as well.
		ranks.Insert(key.LeaderboardId, order, 80, 0, 2, 0, updated, true)
		ranks.Delete(key.LeaderboardId, 0, deleted)
		snapshot := &RankCache{cache: skiplist.New(), owners: make(map[uuid.UUID]cachedRecord)}
		for owner, score := range map[uuid.UUID]int64{recovered: 50, updated: 10, deleted: 100} {
			record := newRank(order, score, 0, owner)
			snapshot.owners[owner] = cachedRecord{generation: 1, record: record}
			snapshot.cache.Insert(record)
		}
		return snapshot, nil
	}, func() bool { return true }); err != nil {
		t.Fatal(err)
	}
	if ranks.Get(key.LeaderboardId, 0, updated) != 1 || ranks.Get(key.LeaderboardId, 0, recovered) != 2 ||
		ranks.Get(key.LeaderboardId, 0, stale) != 0 || ranks.Get(key.LeaderboardId, 0, deleted) != 0 {
		t.Fatal("reconciliation did not recover missing records and correctly merge updates and removals")
	}
	if err := ranks.refreshRanks(key, func() (*RankCache, error) { return nil, errors.New("connection interrupted") }, func() bool { return true }); err == nil {
		t.Fatal("a failed scan did not report an error")
	}
	if ranks.Get(key.LeaderboardId, 0, recovered) != 2 {
		t.Fatal("a failed scan replaced the last good cache")
	}
	if err := ranks.refreshRanks(key, func() (*RankCache, error) {
		ranks.DeleteLeaderboard(key.LeaderboardId, 0)
		return &RankCache{cache: skiplist.New()}, nil
	}, func() bool { return true }); err != nil || ranks.cache[key] != nil {
		t.Fatalf("a completed scan restored a removed rank cache: %v", err)
	}
}

func TestLeaderboardSchedulerClusterInvokesEachClaimedEventOnce(t *testing.T) {
	store := &testLeaderboardCallbackStore{claims: make(map[testLeaderboardCallbackKey]bool)}
	var leaderboardResets, tournamentEnds, tournamentResets atomic.Int32
	var schedulers []*LocalLeaderboardScheduler
	for range 2 {
		schedulers = append(schedulers, &LocalLeaderboardScheduler{
			ctx: t.Context(), callbacks: store, logger: zap.NewNop(),
			fnLeaderboardReset: func(context.Context, *api.Leaderboard, int64) error {
				leaderboardResets.Add(1)
				return errors.New("example side effect completed before the hook reported failure")
			},
			fnTournamentEnd: func(context.Context, *api.Tournament, int64, int64) error {
				tournamentEnds.Add(1)
				return nil
			},
			fnTournamentReset: func(context.Context, *api.Tournament, int64, int64) error {
				tournamentResets.Add(1)
				return nil
			},
		})
	}
	var wg sync.WaitGroup
	for _, event := range []*LeaderboardSchedulerCallback{
		{id: "board", kind: leaderboardCallbackReset, ts: 10},
		{id: "board", kind: leaderboardCallbackReset, ts: 20},
		{id: "tournament", kind: leaderboardCallbackTournamentEnd, ts: 10},
		{id: "tournament", kind: leaderboardCallbackTournamentReset, ts: 10},
	} {
		for _, scheduler := range schedulers {
			wg.Go(func() { scheduler.runCallback(event) })
		}
	}
	wg.Wait()
	if leaderboardResets.Load() != 2 || tournamentEnds.Load() != 1 || tournamentResets.Load() != 1 {
		t.Fatal("callbacks did not run independently by deadline and event kind, or ran more than once")
	}
}

func TestLeaderboardCallbackValidatesCurrentSchedule(t *testing.T) {
	deadline := time.Date(2026, 11, 25, 13, 0, 0, 0, time.UTC)
	definition := &Leaderboard{CreateTime: deadline.Add(-2 * time.Hour).Unix(), ResetSchedule: cronexpr.MustParse("0 * * * *")}
	callback := &LeaderboardSchedulerCallback{leaderboard: definition, kind: leaderboardCallbackReset, ts: deadline.Unix(), t: deadline.Add(-time.Second)}
	if !callback.matchesDeadline(definition) {
		t.Fatal("valid deadline was rejected")
	}
	changed := *definition
	changed.ResetSchedule = cronexpr.MustParse("30 * * * *")
	if callback.matchesDeadline(&changed) {
		t.Fatal("stale deadline still matched the new schedule")
	}
	changed = *definition
	changed.CreateTime++
	if callback.matchesDeadline(&changed) {
		t.Fatal("queued callback matched a replacement leaderboard")
	}
}

func TestLeaderboardDatabaseReconciliationAndCallbackClaims(t *testing.T) {
	if os.Getenv("TEST_DB_URL") == "" {
		t.Skip("set TEST_DB_URL to a migrated database")
	}
	db := NewDB(t)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	id := uuid.Must(uuid.NewV4()).String()
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM leaderboard_callback_claim WHERE leaderboard_id = $1", id)
		_, _ = db.ExecContext(context.Background(), "DELETE FROM leaderboard WHERE id = $1", id)
	})
	deadline := time.Now().UTC().Truncate(time.Second)
	creation, start := deadline.Add(-2*time.Hour), deadline.Add(-time.Hour)
	if _, err := db.ExecContext(ctx, `INSERT INTO leaderboard (id, create_time, start_time, end_time, duration, max_size, size, enable_ranks)
		VALUES ($1, $2, $3, $4, 3600, 10, 7, true)`, id, creation, start, deadline); err != nil {
		t.Fatal(err)
	}
	cache := &LocalLeaderboardCache{db: db, logger: zap.NewNop()}
	if changed, err := cache.refreshAll(ctx); !changed || err != nil || cache.Get(id) == nil {
		t.Fatalf("database refresh did not recover the missing definition: %v", err)
	}
	owner := uuid.Must(uuid.NewV4())
	if _, err := db.ExecContext(ctx, "INSERT INTO leaderboard_record (leaderboard_id, owner_id, expiry_time, score) VALUES ($1, $2, $3, 42)", id, owner, deadline); err != nil {
		t.Fatal(err)
	}
	ranks, err := loadLeaderboardRanks(ctx, db, LeaderboardSortOrderDescending, LeaderboardWithExpiry{LeaderboardId: id, Expiry: deadline.Unix()})
	if err != nil || len(ranks.owners) != 1 || ranks.owners[owner].generation != 1 {
		t.Fatalf("database refresh did not recover the rank: %v", err)
	}
	store := &dbLeaderboardCallbacks{db: db}
	reset := &LeaderboardSchedulerCallback{id: id, leaderboard: cache.Get(id), kind: leaderboardCallbackTournamentReset, ts: deadline.Unix(), t: deadline.Add(-time.Second)}
	if claimed, err := store.claim(ctx, reset, false); claimed != nil || err != nil {
		t.Fatalf("node without a hook could not reset capacity: %v", err)
	}
	var size int
	if err := db.QueryRowContext(ctx, "SELECT size FROM leaderboard WHERE id = $1", id).Scan(&size); err != nil || size != 0 {
		t.Fatalf("capacity reset did not commit: size=%d, error=%v", size, err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE leaderboard SET size = 2 WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	end := *reset
	end.kind = leaderboardCallbackTournamentEnd
	for _, event := range []*LeaderboardSchedulerCallback{reset, &end} {
		var wins atomic.Int32
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				claimed, err := store.claim(ctx, event, true)
				if err != nil {
					t.Error(err)
				} else if claimed != nil {
					wins.Add(1)
					if claimed.tournament.Size != 7 {
						t.Errorf("callback did not retain the ended period's size: %d", claimed.tournament.Size)
					}
				}
			})
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("expected exactly one winner for event %d, got %d", event.kind, wins.Load())
		}
	}
	if err := db.QueryRowContext(ctx, "SELECT size FROM leaderboard WHERE id = $1", id).Scan(&size); err != nil || size != 2 {
		t.Fatalf("duplicate capacity reset changed the new size: %d, %v", size, err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM leaderboard WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if changed, err := cache.refreshAll(ctx); !changed || err != nil || cache.Get(id) != nil {
		t.Fatalf("database refresh did not remove the deleted definition: %v", err)
	}
}

type testLeaderboardCallbackKey struct {
	id       string
	deadline int64
	kind     leaderboardCallbackKind
}

type testLeaderboardCallbackStore struct {
	sync.Mutex
	claims map[testLeaderboardCallbackKey]bool
}

func (s *testLeaderboardCallbackStore) claim(_ context.Context, event *LeaderboardSchedulerCallback, invoke bool) (*claimedLeaderboardCallback, error) {
	s.Lock()
	defer s.Unlock()
	key := testLeaderboardCallbackKey{event.id, event.ts, event.kind}
	if !invoke || s.claims[key] {
		return nil, nil
	}
	s.claims[key] = true
	return &claimedLeaderboardCallback{leaderboard: &api.Leaderboard{}, tournament: &api.Tournament{}}, nil
}
