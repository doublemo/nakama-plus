# Leaderboard Cache 集群同步设计方案

## 1. 设计目标

实现 leaderboard cache 和 leaderboard rank cache 在集群节点间的实时同步，确保：
- 数据一致性：所有节点的 cache 数据保持同步
- 高性能：最小化同步开销，支持增量更新
- 容错性：节点故障时能够自动恢复数据

## 2. 同步架构

### 2.1 BinaryLog 扩展

在现有的 BinaryLog 基础上，添加 leaderboard 相关的消息类型：

```protobuf
// 在 peer.proto 中添加
message BinaryLog {
  // ... 现有字段
  oneof payload {
    // ... 现有类型
    LeaderboardCreate leaderboardCreate = 16;
    LeaderboardDelete leaderboardDelete = 17;
    LeaderboardRankUpdate leaderboardRankUpdate = 18;
    LeaderboardRankDelete leaderboardRankDelete = 19;
    LeaderboardCacheSync leaderboardCacheSync = 20;
  }
}

message LeaderboardCreate {
  string id = 1;
  bool authoritative = 2;
  int32 sort_order = 3;
  int32 operator = 4;
  string reset_schedule = 5;
  string metadata = 6;
  int64 create_time = 7;
  bool enable_ranks = 8;
  // Tournament fields
  string title = 9;
  string description = 10;
  int32 category = 11;
  int32 duration = 12;
  int32 max_size = 13;
  int32 max_num_score = 14;
  bool join_required = 15;
  int64 start_time = 16;
  int64 end_time = 17;
}

message LeaderboardDelete {
  string id = 1;
}

message LeaderboardRankUpdate {
  string leaderboard_id = 1;
  int64 expiry_unix = 2;
  string owner_id = 3;
  int64 score = 4;
  int64 subscore = 5;
  int32 generation = 6;
  int32 sort_order = 7;
}

message LeaderboardRankDelete {
  string leaderboard_id = 1;
  int64 expiry_unix = 2;
  string owner_id = 3;
}

message LeaderboardCacheSync {
  string leaderboard_id = 1;
  int64 expiry_unix = 2;
  repeated LeaderboardRankRecord records = 3;
}

message LeaderboardRankRecord {
  string owner_id = 1;
  int64 score = 2;
  int64 subscore = 3;
  int32 generation = 4;
}
```

### 2.2 接口扩展

扩展 Peer 接口，添加 leaderboard 同步方法：

```go
type Peer interface {
  // ... 现有方法
  LeaderboardCreate(leaderboard *Leaderboard)
  LeaderboardDelete(id string)
  LeaderboardRankUpdate(leaderboardId string, expiryUnix int64, ownerID uuid.UUID, score, subscore int64, generation int32, sortOrder int)
  LeaderboardRankDelete(leaderboardId string, expiryUnix int64, ownerID uuid.UUID)
  LeaderboardCacheSync(leaderboardId string, expiryUnix int64, records []*LeaderboardRankRecord)
}
```

## 3. 实现方案

### 3.1 LeaderboardCache 同步

修改 `LocalLeaderboardCache` 实现：

```go
type LocalLeaderboardCache struct {
  // ... 现有字段
  peer Peer // 添加 peer 引用
}

func (l *LocalLeaderboardCache) Create(ctx context.Context, id string, authoritative bool, sortOrder, operator int, resetSchedule, metadata string, enableRanks bool) (*Leaderboard, bool, error) {
  // ... 现有逻辑
  
  // 同步到集群
  if created && l.peer != nil {
    l.peer.LeaderboardCreate(leaderboard)
  }
  
  return leaderboard, created, nil
}

func (l *LocalLeaderboardCache) Delete(ctx context.Context, rankCache LeaderboardRankCache, scheduler LeaderboardScheduler, id string) (bool, error) {
  // ... 现有逻辑
  
  // 同步到集群
  if deleted && l.peer != nil {
    l.peer.LeaderboardDelete(id)
  }
  
  return deleted, nil
}

// 处理远程同步事件
func (l *LocalLeaderboardCache) HandleRemoteCreate(leaderboard *Leaderboard) {
  l.Lock()
  defer l.Unlock()
  
  if _, exists := l.leaderboards[leaderboard.Id]; !exists {
    l.leaderboards[leaderboard.Id] = leaderboard
    l.allList = append(l.allList, leaderboard)
    
    if leaderboard.IsTournament() {
      l.tournamentList = append(l.tournamentList, leaderboard)
      sort.Sort(OrderedTournaments(l.tournamentList))
    } else {
      l.leaderboardList = append(l.leaderboardList, leaderboard)
    }
  }
}

func (l *LocalLeaderboardCache) HandleRemoteDelete(id string) {
  l.Lock()
  defer l.Unlock()
  
  if leaderboard, exists := l.leaderboards[id]; exists {
    delete(l.leaderboards, id)
    
    // 从列表中移除
    l.removeFromLists(id, leaderboard.IsTournament())
  }
}
```

### 3.2 LeaderboardRankCache 同步

修改 `LocalLeaderboardRankCache` 实现：

```go
type LocalLeaderboardRankCache struct {
  // ... 现有字段
  peer Peer // 添加 peer 引用
}

func (l *LocalLeaderboardRankCache) Insert(leaderboardId string, sortOrder int, score, subscore int64, generation int32, expiryUnix int64, ownerID uuid.UUID, enableRanks bool) int64 {
  // ... 现有逻辑
  
  // 同步到集群
  if l.peer != nil && enableRanks && !l.isBlacklisted(leaderboardId) {
    l.peer.LeaderboardRankUpdate(leaderboardId, expiryUnix, ownerID, score, subscore, generation, sortOrder)
  }
  
  return rank
}

func (l *LocalLeaderboardRankCache) Delete(leaderboardId string, expiryUnix int64, ownerID uuid.UUID) bool {
  // ... 现有逻辑
  
  // 同步到集群
  if deleted && l.peer != nil && !l.isBlacklisted(leaderboardId) {
    l.peer.LeaderboardRankDelete(leaderboardId, expiryUnix, ownerID)
  }
  
  return deleted
}

// 处理远程同步事件
func (l *LocalLeaderboardRankCache) HandleRemoteRankUpdate(leaderboardId string, expiryUnix int64, ownerID uuid.UUID, score, subscore int64, generation int32, sortOrder int) {
  if l.isBlacklisted(leaderboardId) {
    return
  }
  
  key := LeaderboardWithExpiry{LeaderboardId: leaderboardId, Expiry: expiryUnix}
  
  l.RLock()
  rankCache, ok := l.cache[key]
  l.RUnlock()
  
  if !ok {
    // 创建新的 rank cache
    newRankCache := &RankCache{
      owners: make(map[uuid.UUID]cachedRecord),
      cache:  skiplist.New(),
    }
    l.Lock()
    if rankCache, ok = l.cache[key]; !ok {
      rankCache = newRankCache
      l.cache[key] = rankCache
    }
    l.Unlock()
  }
  
  rankData := newRank(sortOrder, score, subscore, ownerID)
  
  rankCache.Lock()
  oldRankData, exists := rankCache.owners[ownerID]
  
  // 只有当 generation 更新时才更新
  if !exists || generation > oldRankData.generation {
    if exists {
      rankCache.cache.Delete(oldRankData.record)
    }
    
    rankCache.owners[ownerID] = cachedRecord{generation: generation, record: rankData}
    rankCache.cache.Insert(rankData)
  }
  rankCache.Unlock()
}

func (l *LocalLeaderboardRankCache) HandleRemoteRankDelete(leaderboardId string, expiryUnix int64, ownerID uuid.UUID) {
  if l.isBlacklisted(leaderboardId) {
    return
  }
  
  key := LeaderboardWithExpiry{LeaderboardId: leaderboardId, Expiry: expiryUnix}
  
  l.RLock()
  rankCache, ok := l.cache[key]
  l.RUnlock()
  
  if !ok {
    return
  }
  
  rankCache.Lock()
  if rankData, exists := rankCache.owners[ownerID]; exists {
    delete(rankCache.owners, ownerID)
    rankCache.cache.Delete(rankData.record)
  }
  rankCache.Unlock()
}

func (l *LocalLeaderboardRankCache) isBlacklisted(leaderboardId string) bool {
  if l.blacklistAll {
    return true
  }
  _, blacklisted := l.blacklistIds[leaderboardId]
  return blacklisted
}
```

### 3.3 Peer 实现扩展

在 `LocalPeer` 中添加 leaderboard 同步方法：

```go
func (s *LocalPeer) LeaderboardCreate(leaderboard *Leaderboard) {
  s.BinaryLogBroadcast(pb.BinaryLog{
    Node: s.endpoint.Name(),
    Payload: &pb.BinaryLog_LeaderboardCreate{
      LeaderboardCreate: leaderboard2pb(leaderboard),
    },
  }, true)
}

func (s *LocalPeer) LeaderboardDelete(id string) {
  s.BinaryLogBroadcast(pb.BinaryLog{
    Node: s.endpoint.Name(),
    Payload: &pb.BinaryLog_LeaderboardDelete{
      LeaderboardDelete: &pb.LeaderboardDelete{Id: id},
    },
  }, true)
}

func (s *LocalPeer) LeaderboardRankUpdate(leaderboardId string, expiryUnix int64, ownerID uuid.UUID, score, subscore int64, generation int32, sortOrder int) {
  s.BinaryLogBroadcast(pb.BinaryLog{
    Node: s.endpoint.Name(),
    Payload: &pb.BinaryLog_LeaderboardRankUpdate{
      LeaderboardRankUpdate: &pb.LeaderboardRankUpdate{
        LeaderboardId: leaderboardId,
        ExpiryUnix:    expiryUnix,
        OwnerId:       ownerID.String(),
        Score:         score,
        Subscore:      subscore,
        Generation:    generation,
        SortOrder:     int32(sortOrder),
      },
    },
  }, true)
}

func (s *LocalPeer) LeaderboardRankDelete(leaderboardId string, expiryUnix int64, ownerID uuid.UUID) {
  s.BinaryLogBroadcast(pb.BinaryLog{
    Node: s.endpoint.Name(),
    Payload: &pb.BinaryLog_LeaderboardRankDelete{
      LeaderboardRankDelete: &pb.LeaderboardRankDelete{
        LeaderboardId: leaderboardId,
        ExpiryUnix:    expiryUnix,
        OwnerId:       ownerID.String(),
      },
    },
  }, true)
}
```

### 3.4 BinaryLog 处理扩展

在 `peer_binary_log.go` 中添加处理逻辑：

```go
func (s *LocalPeer) handleBinaryLog(v *pb.BinaryLog) {
  // ... 现有逻辑
  
  switch payload := v.Payload.(type) {
  // ... 现有 case
  
  case *pb.BinaryLog_LeaderboardCreate:
    leaderboard := pb2leaderboard(payload.LeaderboardCreate)
    if cache, ok := s.leaderboardCache.(*LocalLeaderboardCache); ok {
      cache.HandleRemoteCreate(leaderboard)
    }
    s.logger.Debug("processed leaderboard create", append(logFields, zap.String("id", leaderboard.Id))...)
    
  case *pb.BinaryLog_LeaderboardDelete:
    if cache, ok := s.leaderboardCache.(*LocalLeaderboardCache); ok {
      cache.HandleRemoteDelete(payload.LeaderboardDelete.Id)
    }
    s.logger.Debug("processed leaderboard delete", append(logFields, zap.String("id", payload.LeaderboardDelete.Id))...)
    
  case *pb.BinaryLog_LeaderboardRankUpdate:
    update := payload.LeaderboardRankUpdate
    ownerID, err := uuid.FromString(update.OwnerId)
    if err != nil {
      s.logger.Error("invalid owner ID in rank update", append(logFields, zap.Error(err))...)
      return
    }
    
    if cache, ok := s.leaderboardRankCache.(*LocalLeaderboardRankCache); ok {
      cache.HandleRemoteRankUpdate(update.LeaderboardId, update.ExpiryUnix, ownerID, update.Score, update.Subscore, update.Generation, int(update.SortOrder))
    }
    s.logger.Debug("processed leaderboard rank update", append(logFields, zap.String("leaderboard_id", update.LeaderboardId))...)
    
  case *pb.BinaryLog_LeaderboardRankDelete:
    delete := payload.LeaderboardRankDelete
    ownerID, err := uuid.FromString(delete.OwnerId)
    if err != nil {
      s.logger.Error("invalid owner ID in rank delete", append(logFields, zap.Error(err))...)
      return
    }
    
    if cache, ok := s.leaderboardRankCache.(*LocalLeaderboardRankCache); ok {
      cache.HandleRemoteRankDelete(delete.LeaderboardId, delete.ExpiryUnix, ownerID)
    }
    s.logger.Debug("processed leaderboard rank delete", append(logFields, zap.String("leaderboard_id", delete.LeaderboardId))...)
  }
  
  s.binaryLog.SetVersionByNode(v.Node, v.Version)
}
```

## 4. 优化策略

### 4.1 批量同步

对于大量排名更新，可以实现批量同步：

```go
func (s *LocalPeer) LeaderboardRankBatchUpdate(updates []*LeaderboardRankUpdate) {
  // 批量发送，减少网络开销
  for i := 0; i < len(updates); i += 100 {
    end := i + 100
    if end > len(updates) {
      end = len(updates)
    }
    
    batch := updates[i:end]
    // 发送批量更新
  }
}
```

### 4.2 增量同步

实现增量同步机制，只同步变更的数据：

```go
func (l *LocalLeaderboardRankCache) GetIncrementalUpdates(since int64) []*LeaderboardRankUpdate {
  // 返回指定时间戳之后的所有更新
}
```

### 4.3 压缩和去重

对同一用户的多次更新进行合并，只发送最新状态。

## 5. 配置选项

添加配置选项控制同步行为：

```yaml
cluster:
  leaderboard_sync:
    enabled: true
    batch_size: 100
    batch_timeout: "1s"
    compression: true
    blacklist_cache: ["tournament_*"]
```

## 6. 监控和指标

添加同步相关的监控指标：

- 同步消息发送/接收数量
- 同步延迟
- 同步失败率
- Cache 命中率

这个设计方案充分利用了现有的 Peer 同步机制，实现了 leaderboard cache 的集群同步功能，确保数据一致性和高性能。