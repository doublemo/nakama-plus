package server

import (
	"time"

	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type (
	BinaryLog interface {
		GetVersion() int64
		GetVersionByNode(node string) int64
		SetVersionByNode(node string, ver int64)
		RefreshVersion() int64
	}

	LocalBinaryLog struct {
		logger *zap.Logger
		node   string

		version      *atomic.Int64
		nodesVersion *MapOf[string, int64]
	}
)

func NewLocalBinaryLog(logger *zap.Logger, name string) *LocalBinaryLog {
	return &LocalBinaryLog{
		logger:       logger,
		node:         name,
		version:      atomic.NewInt64(0),
		nodesVersion: &MapOf[string, int64]{},
	}
}

func (b *LocalBinaryLog) GetVersion() int64 {
	return b.version.Load()
}

func (b *LocalBinaryLog) RefreshVersion() int64 {
	ver := time.Now().UTC().UnixNano()
	b.version.Store(ver)
	return ver
}

func (b *LocalBinaryLog) GetVersionByNode(node string) int64 {
	ver, ok := b.nodesVersion.Load(node)
	if !ok {
		return 0
	}
	return ver
}

func (b *LocalBinaryLog) SetVersionByNode(node string, ver int64) {
	b.nodesVersion.Store(node, ver)
}

func (s *LocalPeer) handleBinaryLog(v *pb.BinaryLog) {
	switch payload := v.Payload.(type) {
	case *pb.BinaryLog_Ban:
		s.onBan(v.Node, payload.Ban)
	case *pb.BinaryLog_Track:
		s.onTrack(v.Node, payload.Track)
	case *pb.BinaryLog_Untrack:
		s.onUntrack(v.Node, payload.Untrack)
	case *pb.BinaryLog_MatchmakerAdd:
		extract := pb2MatchmakerExtract(payload.MatchmakerAdd)
		_, _, err := s.matchmaker.Add(s.ctx, extract.Presences, extract.SessionID, extract.PartyId, extract.Query, extract.MinCount, extract.MaxCount, extract.CountMultiple, extract.StringProperties, extract.NumericProperties)
		if err != nil {
			s.logger.Error("BinaryLog_MatchmakerAdd", zap.Error(err), zap.Any("extract", extract))
		}

	case *pb.BinaryLog_MatchmakerRemoveSession:
		extract := payload.MatchmakerRemoveSession
		if err := s.matchmaker.RemoveSession(extract.SessionId, extract.Ticket); err != nil {
			s.logger.Error("BinaryLog_MatchmakerRemoveSession", zap.Error(err), zap.Any("extract", extract))
		}

	case *pb.BinaryLog_MatchmakerRemoveSessionAll:
		extract := payload.MatchmakerRemoveSessionAll
		if err := s.matchmaker.RemoveSessionAll(extract.SessionId); err != nil {
			s.logger.Error("BinaryLog_MatchmakerRemoveSessionAll", zap.Error(err), zap.Any("extract", extract))
		}

	case *pb.BinaryLog_MatchmakerRemoveParty:
		extract := payload.MatchmakerRemoveParty
		if err := s.matchmaker.RemoveParty(extract.PartyId, extract.Ticket); err != nil {
			s.logger.Error("BinaryLog_MatchmakerRemoveParty", zap.Error(err), zap.Any("extract", extract))
		}

	case *pb.BinaryLog_MatchmakerRemovePartyAll:
		extract := payload.MatchmakerRemovePartyAll
		if err := s.matchmaker.RemovePartyAll(extract.PartyId); err != nil {
			s.logger.Error("BinaryLog_MatchmakerRemovePartyAll", zap.Error(err), zap.Any("extract", extract))
		}

	case *pb.BinaryLog_MatchmakerRemoveAll:
		s.matchmaker.RemoveAll(map[string]bool{payload.MatchmakerRemoveAll.Node: true})

	case *pb.BinaryLog_MatchmakerRemove:
		s.matchmaker.Remove(payload.MatchmakerRemove.Ticket)
	}
	s.binaryLog.SetVersionByNode(v.Node, v.Version)
}
