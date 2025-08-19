# Leaderboard 集群同步功能

## 概述

这个实现为 Nakama 项目添加了 leaderboard 和 leaderboard rank cache 的集群同步功能，确保在多节点集群环境中所有节点的 leaderboard 数据保持一致。

## 功能特性

- **实时同步**：leaderboard 的创建、删除和排名更新会实时同步到集群中的所有节点
- **增量更新**：只同步变更的数据，减少网络开销
- **版本控制**：使用 generation 机制避免重复处理和数据冲突
- **容错性**：节点故障时能够自动恢复数据
- **可配置**：支持黑名单、批量同步等配置选项

## 架构设计

### 核心组件

1. **LeaderboardCacheSync**：扩展了 `LocalLeaderboardCache`，添加集群同步功能
2. **LeaderboardRankCacheSync**：扩展了 `LocalLeaderboardRankCache`，添加排名同步功能
3. **BinaryLog 扩展**：通过现有的 BinaryLog 机制传播同步消息
4. **Peer 接口扩展**：添加 leaderboard 相关的同步方法

### 同步流程

```
节点A: 创建/更新 leaderboard
    ↓
生成 BinaryLog 消息
    ↓
通过 memberlist 广播到集群
    ↓
节点B,C,D: 接收并处理消息
    ↓
更新本地 cache
```

## 使用方法

### 1. 初始化

在 `main.go` 中替换原有的 leaderboard cache 初始化：

```go
// 原来的方式
// leaderboardCache := server.NewLocalLeaderboardCache(ctx, logger, startupLogger, db)
// leaderboardRankCache := server.NewLocalLeaderboardRankCache(ctx, startupLogger, db, config.GetLeaderboard(), leaderboardCache)

// 新的方式（支持集群同步）
leaderboardCache, leaderboardRankCache := server.InitializeLeaderboardCacheWithSync(
    ctx, logger, startupLogger, db, config.GetLeaderboard(), peer)
```

### 2. 配置

在配置文件中添加同步相关配置：

```yaml
cluster:
  leaderboard_sync:
    enabled: true
    batch_size: 100
    batch_timeout: "1s"
    enable_compression: false
    sync_blacklist: 
      - "temp_*"        # 临时 leaderboard 不同步
      - "test_ranking"  # 特定 leaderboard 不同步
    max_retries: 3
    retry_interval: "5s"
```

### 3. Protobuf 定义

需要在 `peer.proto` 文件中添加以下消息定义：

```protobuf
message BinaryLog {
  // ... 现有字段
  oneof payload {
    // ... 现有类型
    LeaderboardCreate leaderboardCreate = 16;
    LeaderboardDelete leaderboardDelete = 17;
    LeaderboardRankUpdate leaderboardRankUpdate = 18;
    LeaderboardRankDelete leaderboardRankDelete = 19;
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
```

## API 使用示例

### 创建 Leaderboard

```go
// 创建会自动同步到集群
leaderboard, created, err := leaderboardCache.Create(
    ctx,
    "global_ranking",
    true,  // authoritative
    server.LeaderboardSortOrderDescending,
    server.LeaderboardOperatorBest,
    "0 0 * * 0",  // 每周日重置
    `{"description": "Global player ranking"}`,
    true,  // enable ranks
)
```

### 更新玩家排名

```go
// 排名更新会自动同步到集群
rank := leaderboardRankCache.Insert(
    "global_ranking",
    server.LeaderboardSortOrderDescending,
    1000,  // score
    500,   // subscore
    1,     // generation
    0,     // expiry
    playerID,
    true,  // enable ranks
)
```

### 删除 Leaderboard

```go
// 删除会自动同步到集群
deleted, err := leaderboardCache.Delete(ctx, leaderboardRankCache, scheduler, "global_ranking")
```

## 性能优化

### 1. 批量更新

对于大量排名更新，可以使用批量接口：

```go
updates := []server.RankUpdate{
    {LeaderboardId: "lb1", OwnerID: player1, Score: 1000, ...},
    {LeaderboardId: "lb1", OwnerID: player2, Score: 900, ...},
    // ...
}

if syncCache, ok := leaderboardRankCache.(*server.LeaderboardRankCacheSync); ok {
    syncCache.BatchUpdate(updates)
}
```

### 2. 黑名单配置

对于不需要同步的 leaderboard，可以配置黑名单：

```yaml
sync_blacklist:
  - "temp_*"      # 所有以 temp_ 开头的
  - "local_test"  # 特定的 leaderboard
  - "*"           # 禁用所有同步
```

## 监控和调试

### 日志

同步过程会产生详细的调试日志：

```
DEBUG processed leaderboard create {"node": "node1", "version": 123, "id": "global_ranking"}
DEBUG processed leaderboard rank update {"node": "node2", "leaderboard_id": "global_ranking", "owner_id": "uuid"}
```

### 指标

可以添加以下监控指标：

- `leaderboard_sync_messages_sent`：发送的同步消息数量
- `leaderboard_sync_messages_received`：接收的同步消息数量
- `leaderboard_sync_errors`：同步错误数量
- `leaderboard_cache_size`：缓存大小

## 故障处理

### 节点故障

- 当节点离开集群时，其他节点会自动清理相关数据
- 节点重新加入时，会通过 LocalState/MergeRemoteState 机制同步数据

### 网络分区

- 使用版本号机制确保数据一致性
- 分区恢复后会自动同步最新状态

### 数据冲突

- 使用 generation 字段解决并发更新冲突
- 较新的 generation 会覆盖较旧的数据

## 测试

运行测试：

```bash
go test ./server -run TestLeaderboardSync
```

测试覆盖：
- 基本同步功能
- 远程事件处理
- 黑名单过滤
- 批量更新

## 注意事项

1. **Protobuf 更新**：需要重新生成 protobuf 代码
2. **向后兼容**：新版本需要与旧版本节点兼容
3. **性能影响**：大量同步可能影响网络性能，建议合理配置批量大小
4. **内存使用**：rank cache 会占用更多内存，注意监控

## 未来改进

1. **压缩支持**：对大消息进行压缩
2. **增量同步**：只同步变更部分
3. **优先级队列**：重要更新优先同步
4. **持久化队列**：确保消息不丢失