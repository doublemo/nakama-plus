package server

import (
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
)

func (s *LocalPeer) LeaderboardCreate(leaderboard *Leaderboard, created bool) {
	s.BinaryLogBroadcast(&pb.BinaryLog{
		Payload: &pb.BinaryLog_LeaderboardCreate{
			LeaderboardCreate: &pb.Leaderboard_Create{
				Id:            leaderboard.Id,
				Authoritative: leaderboard.Authoritative,
				SortOrder:     int32(leaderboard.SortOrder),
				Operator:      int32(leaderboard.Operator),
				ResetSchedule: leaderboard.ResetScheduleStr,
				Metadata:      leaderboard.Metadata,
				EnableRanks:   leaderboard.EnableRanks,
				CreateTime:    leaderboard.CreateTime,
				Created:       created,
			},
		},
	}, false)
}

func (s *LocalPeer) LeaderboardInsert(id string, authoritative bool, sortOrder, operator int, resetSchedule, metadata string, createTime int64, enableRanks bool) {
	s.BinaryLogBroadcast(&pb.BinaryLog{
		Payload: &pb.BinaryLog_LeaderboardInsert{
			LeaderboardInsert: &pb.Leaderboard_Create{
				Id:            id,
				Authoritative: authoritative,
				SortOrder:     int32(sortOrder),
				Operator:      int32(operator),
				ResetSchedule: resetSchedule,
				Metadata:      metadata,
				EnableRanks:   enableRanks,
				CreateTime:    createTime,
			},
		},
	}, false)
}

func (s *LocalPeer) LeaderboardRemove(id string) {
	s.BinaryLogBroadcast(&pb.BinaryLog{
		Payload: &pb.BinaryLog_LeaderboardRemove{
			LeaderboardRemove: id,
		},
	}, false)
}

func (s *LocalPeer) LeaderboardCreateTournament(leaderboard *Leaderboard, created bool) {
	if leaderboard == nil {
		return
	}

	s.BinaryLogBroadcast(&pb.BinaryLog{
		Payload: &pb.BinaryLog_LeaderboardCreateTournament{
			LeaderboardCreateTournament: &pb.Leaderboard_CreateTournament{
				Id:            leaderboard.Id,
				Authoritative: leaderboard.Authoritative,
				SortOrder:     int32(leaderboard.SortOrder),
				Operator:      int32(leaderboard.Operator),
				ResetSchedule: leaderboard.ResetScheduleStr,
				Metadata:      leaderboard.Metadata,
				Title:         leaderboard.Title,
				Description:   leaderboard.Description,
				Category:      int32(leaderboard.Category),
				StartTime:     leaderboard.StartTime,
				EndTime:       leaderboard.EndTime,
				Duration:      int32(leaderboard.Duration),
				MaxSize:       int32(leaderboard.MaxSize),
				MaxNumScore:   int32(leaderboard.MaxNumScore),
				JoinRequired:  leaderboard.JoinRequired,
				EnableRanks:   leaderboard.EnableRanks,
				CreateTime:    leaderboard.CreateTime,
				Created:       created,
			},
		},
	}, false)
}

func (s *LocalPeer) LeaderboardInsertTournament(leaderboard *Leaderboard) {
	if leaderboard == nil {
		return
	}

	s.BinaryLogBroadcast(&pb.BinaryLog{
		Payload: &pb.BinaryLog_LeaderboardInsertTournament{
			LeaderboardInsertTournament: &pb.Leaderboard_CreateTournament{
				Id:            leaderboard.Id,
				Authoritative: leaderboard.Authoritative,
				SortOrder:     int32(leaderboard.SortOrder),
				Operator:      int32(leaderboard.Operator),
				ResetSchedule: leaderboard.ResetScheduleStr,
				Metadata:      leaderboard.Metadata,
				Title:         leaderboard.Title,
				Description:   leaderboard.Description,
				Category:      int32(leaderboard.Category),
				StartTime:     leaderboard.StartTime,
				EndTime:       leaderboard.EndTime,
				Duration:      int32(leaderboard.Duration),
				MaxSize:       int32(leaderboard.MaxSize),
				MaxNumScore:   int32(leaderboard.MaxNumScore),
				JoinRequired:  leaderboard.JoinRequired,
				EnableRanks:   leaderboard.EnableRanks,
				CreateTime:    leaderboard.CreateTime,
			},
		},
	}, false)
}

func (s *LocalPeer) LeaderboardRankCreate(leaderboardId string, sortOrder int, score, subscore int64, generation int32, expiryUnix int64, ownerID uuid.UUID, enable bool) {
	s.BinaryLogBroadcast(&pb.BinaryLog{
		Payload: &pb.BinaryLog_LeaderboardRankCreate{
			LeaderboardRankCreate: &pb.Leaderboard_Rank_Insert{
				LeaderboardId: leaderboardId,
				SortOrder:     int32(sortOrder),
				Score:         score,
				Subscore:      subscore,
				Generation:    generation,
				ExpiryUnix:    expiryUnix,
				OwnerID:       ownerID.String(),
				Enable:        enable,
			},
		},
	}, false)
}

func (s *LocalPeer) LeaderboardRankDeleteLeaderboard(items ...*pb.Leaderboard_Rank_Item) {
	if len(items) < 1 {
		return
	}

	s.BinaryLogBroadcast(&pb.BinaryLog{
		Payload: &pb.BinaryLog_LeaderboardRankDeleteLeaderboard{
			LeaderboardRankDeleteLeaderboard: &pb.Leaderboard_Rank_DeleteLeaderboard{Data: items},
		},
	}, false)
}

func (s *LocalPeer) LeaderboardRankDelete(items ...*pb.Leaderboard_Rank_Item) {
	if len(items) < 1 {
		return
	}

	s.BinaryLogBroadcast(&pb.BinaryLog{
		Payload: &pb.BinaryLog_LeaderboardRankDelete{
			LeaderboardRankDelete: &pb.Leaderboard_Rank_Delete{Data: items},
		},
	}, false)
}
