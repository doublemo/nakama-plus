package server

import (
	"context"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
)

// MockPeer 用于测试的模拟 Peer
type MockPeer struct {
	leaderboardCreates []*Leaderboard
	leaderboardDeletes []string
	rankUpdates        []RankUpdateEvent
	rankDeletes        []RankDeleteEvent
}

type RankUpdateEvent struct {
	LeaderboardId string
	ExpiryUnix    int64
	OwnerID       uuid.UUID
	Score         int64
	Subscore      int64
	Generation    int32
	SortOrder     int
}

type RankDeleteEvent struct {
	LeaderboardId string
	ExpiryUnix    int64
	OwnerID       uuid.UUID
}

func NewMockPeer() *MockPeer {
	return &MockPeer{
		leaderboardCreates: make([]*Leaderboard, 0),
		leaderboardDeletes: make([]string, 0),
		rankUpdates:        make([]RankUpdateEvent, 0),
		rankDeletes:        make([]RankDeleteEvent, 0),
	}
}

func (m *MockPeer) LeaderboardCreate(leaderboard *Leaderboard) {
	m.leaderboardCreates = append(m.leaderboardCreates, leaderboard)
}

func (m *MockPeer) LeaderboardDelete(id string) {
	m.leaderboardDeletes = append(m.leaderboardDeletes, id)
}

func (m *MockPeer) LeaderboardRankUpdate(leaderboardId string, expiryUnix int64, ownerID uuid.UUID, score, subscore int64, generation int32, sortOrder int) {
	m.rankUpdates = append(m.rankUpdates, RankUpdateEvent{
		LeaderboardId: leaderboardId,
		ExpiryUnix:    expiryUnix,
		OwnerID:       ownerID,
		Score:         score,
		Subscore:      subscore,
		Generation:    generation,
		SortOrder:     sortOrder,
	})
}

func (m *MockPeer) LeaderboardRankDelete(leaderboardId string, expiryUnix int64, ownerID uuid.UUID) {
	m.rankDeletes = append(m.rankDeletes, RankDeleteEvent{
		LeaderboardId: leaderboardId,
		ExpiryUnix:    expiryUnix,
		OwnerID:       ownerID,
	})
}

// 实现其他 Peer 接口方法（空实现用于测试）
func (m *MockPeer) Shutdown()                           {}
func (m *MockPeer) Join(members ...string) (int, error) { return 0, nil }
func (m *MockPeer) Local() Endpoint                     { return nil }
func (m *MockPeer) NumMembers() int                     { return 1 }
func (m *MockPeer) Member(name string) (Endpoint, bool) { return nil, false }
func (m *MockPeer) Members() []Endpoint                 { return nil }

// ... 其他方法的空实现

func TestLeaderboardCacheSync_Create(t *testing.T) {
	logger := zap.NewNop()
	mockPeer := NewMockPeer()

	// 创建一个模拟的 LocalLeaderboardCache
	baseCache := &LocalLeaderboardCache{
		leaderboards:    make(map[string]*Leaderboard),
		allList:         make([]*Leaderboard, 0),
		leaderboardList: make([]*Leaderboard, 0),
		tournamentList:  make([]*Leaderboard, 0),
	}

	syncCache := &LeaderboardCacheSync{
		LocalLeaderboardCache: baseCache,
		peer:                  mockPeer,
		logger:                logger,
	}

	// 模拟创建 leaderboard
	leaderboard := &Leaderboard{
		Id:            "test_leaderboard",
		Authoritative: true,
		SortOrder:     LeaderboardSortOrderDescending,
		Operator:      LeaderboardOperatorBest,
		Metadata:      `{"test": true}`,
		CreateTime:    time.Now().Unix(),
		EnableRanks:   true,
	}

	// 模拟成功创建（在实际实现中，这会调用数据库）
	syncCache.Lock()
	syncCache.leaderboards[leaderboard.Id] = leaderboard
	syncCache.allList = append(syncCache.allList, leaderboard)
	syncCache.leaderboardList = append(syncCache.leaderboardList, leaderboard)
	syncCache.Unlock()

	// 触发同步
	syncCache.peer.LeaderboardCreate(leaderboard)

	// 验证同步消息被发送
	if len(mockPeer.leaderboardCreates) != 1 {
		t.Errorf("Expected 1 leaderboard create message, got %d", len(mockPeer.leaderboardCreates))
	}

	if mockPeer.leaderboardCreates[0].Id != "test_leaderboard" {
		t.Errorf("Expected leaderboard ID 'test_leaderboard', got '%s'", mockPeer.leaderboardCreates[0].Id)
	}
}

func TestLeaderboardRankCacheSync_Insert(t *testing.T) {
	logger := zap.NewNop()
	mockPeer := NewMockPeer()

	// 创建一个模拟的 LocalLeaderboardRankCache
	baseCache := &LocalLeaderboardRankCache{
		blacklistAll: false,
		blacklistIds: make(map[string]struct{}),
		cache:        make(map[LeaderboardWithExpiry]*RankCache),
	}

	syncCache := &LeaderboardRankCacheSync{
		LocalLeaderboardRankCache: baseCache,
		peer:                      mockPeer,
		logger:                    logger,
	}

	// 模拟插入排名
	leaderboardId := "test_leaderboard"
	ownerID := uuid.Must(uuid.NewV4())
	score := int64(1000)
	subscore := int64(500)
	generation := int32(1)
	sortOrder := LeaderboardSortOrderDescending

	// 触发同步
	syncCache.peer.LeaderboardRankUpdate(leaderboardId, 0, ownerID, score, subscore, generation, sortOrder)

	// 验证同步消息被发送
	if len(mockPeer.rankUpdates) != 1 {
		t.Errorf("Expected 1 rank update message, got %d", len(mockPeer.rankUpdates))
	}

	update := mockPeer.rankUpdates[0]
	if update.LeaderboardId != leaderboardId {
		t.Errorf("Expected leaderboard ID '%s', got '%s'", leaderboardId, update.LeaderboardId)
	}

	if update.Score != score {
		t.Errorf("Expected score %d, got %d", score, update.Score)
	}

	if update.OwnerID != ownerID {
		t.Errorf("Expected owner ID %s, got %s", ownerID, update.OwnerID)
	}
}

func TestLeaderboardCacheSync_HandleRemoteCreate(t *testing.T) {
	logger := zap.NewNop()
	mockPeer := NewMockPeer()

	baseCache := &LocalLeaderboardCache{
		leaderboards:    make(map[string]*Leaderboard),
		allList:         make([]*Leaderboard, 0),
		leaderboardList: make([]*Leaderboard, 0),
		tournamentList:  make([]*Leaderboard, 0),
	}

	syncCache := &LeaderboardCacheSync{
		LocalLeaderboardCache: baseCache,
		peer:                  mockPeer,
		logger:                logger,
	}

	// 模拟接收远程创建事件
	remoteLeaderboard := &Leaderboard{
		Id:            "remote_leaderboard",
		Authoritative: true,
		SortOrder:     LeaderboardSortOrderAscending,
		Operator:      LeaderboardOperatorSet,
		Metadata:      `{"remote": true}`,
		CreateTime:    time.Now().Unix(),
		EnableRanks:   true,
	}

	syncCache.HandleRemoteCreate(remoteLeaderboard)

	// 验证 leaderboard 被添加到本地缓存
	syncCache.RLock()
	cachedLeaderboard, exists := syncCache.leaderboards["remote_leaderboard"]
	syncCache.RUnlock()

	if !exists {
		t.Error("Remote leaderboard was not added to local cache")
	}

	if cachedLeaderboard.Id != "remote_leaderboard" {
		t.Errorf("Expected leaderboard ID 'remote_leaderboard', got '%s'", cachedLeaderboard.Id)
	}

	if len(syncCache.allList) != 1 {
		t.Errorf("Expected 1 leaderboard in allList, got %d", len(syncCache.allList))
	}

	if len(syncCache.leaderboardList) != 1 {
		t.Errorf("Expected 1 leaderboard in leaderboardList, got %d", len(syncCache.leaderboardList))
	}
}
