package server

import (
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
)

// Leaderboard 同步相关的 Peer 方法扩展
func (s *LocalPeer) LeaderboardCreate(leaderboard *Leaderboard) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node:    s.endpoint.Name(),
		Version: s.binaryLog.RefreshVersion(),
		Payload: &pb.BinaryLog_LeaderboardCreate{
			LeaderboardCreate: leaderboard2pb(leaderboard),
		},
	}, true)
}

func (s *LocalPeer) LeaderboardDelete(id string) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node:    s.endpoint.Name(),
		Version: s.binaryLog.RefreshVersion(),
		Payload: &pb.BinaryLog_LeaderboardDelete{
			LeaderboardDelete: &pb.LeaderboardDelete{Id: id},
		},
	}, true)
}

func (s *LocalPeer) LeaderboardRankUpdate(leaderboardId string, expiryUnix int64, ownerID uuid.UUID, score, subscore int64, generation int32, sortOrder int) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node:    s.endpoint.Name(),
		Version: s.binaryLog.RefreshVersion(),
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
		Node:    s.endpoint.Name(),
		Version: s.binaryLog.RefreshVersion(),
		Payload: &pb.BinaryLog_LeaderboardRankDelete{
			LeaderboardRankDelete: &pb.LeaderboardRankDelete{
				LeaderboardId: leaderboardId,
				ExpiryUnix:    expiryUnix,
				OwnerId:       ownerID.String(),
			},
		},
	}, true)
}

// 辅助函数：将 Leaderboard 转换为 protobuf 消息
func leaderboard2pb(leaderboard *Leaderboard) *pb.LeaderboardCreate {
	return &pb.LeaderboardCreate{
		Id:            leaderboard.Id,
		Authoritative: leaderboard.Authoritative,
		SortOrder:     int32(leaderboard.SortOrder),
		Operator:      int32(leaderboard.Operator),
		ResetSchedule: leaderboard.ResetScheduleStr,
		Metadata:      leaderboard.Metadata,
		CreateTime:    leaderboard.CreateTime,
		EnableRanks:   leaderboard.EnableRanks,
		Title:         leaderboard.Title,
		Description:   leaderboard.Description,
		Category:      int32(leaderboard.Category),
		Duration:      int32(leaderboard.Duration),
		MaxSize:       int32(leaderboard.MaxSize),
		MaxNumScore:   int32(leaderboard.MaxNumScore),
		JoinRequired:  leaderboard.JoinRequired,
		StartTime:     leaderboard.StartTime,
		EndTime:       leaderboard.EndTime,
	}
}

// 辅助函数：将 protobuf 消息转换为 Leaderboard
func pb2leaderboard(pb *pb.LeaderboardCreate) *Leaderboard {
	leaderboard := &Leaderboard{
		Id:            pb.Id,
		Authoritative: pb.Authoritative,
		SortOrder:     int(pb.SortOrder),
		Operator:      int(pb.Operator),
		Metadata:      pb.Metadata,
		CreateTime:    pb.CreateTime,
		EnableRanks:   pb.EnableRanks,
		Title:         pb.Title,
		Description:   pb.Description,
		Category:      int(pb.Category),
		Duration:      int(pb.Duration),
		MaxSize:       int(pb.MaxSize),
		MaxNumScore:   int(pb.MaxNumScore),
		JoinRequired:  pb.JoinRequired,
		StartTime:     pb.StartTime,
		EndTime:       pb.EndTime,
	}

	// 解析重置计划
	if pb.ResetSchedule != "" {
		leaderboard.ResetScheduleStr = pb.ResetSchedule
		// 这里需要解析 cron 表达式，但为了简化示例，我们跳过
		// 在实际实现中，需要调用 cronexpr.Parse(pb.ResetSchedule)
	}

	return leaderboard
}
