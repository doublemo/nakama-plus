package server

import (
	"strings"
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
	b.version.Store(time.Now().UTC().UnixNano())
	return b.version.Load()
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
	logFields := []zap.Field{
		zap.String("node", v.Node),
		zap.Int64("version", v.Version),
	}

	if lastVersion := s.binaryLog.GetVersionByNode(v.Node); lastVersion >= v.Version {
		s.logger.Debug("Skip log", append(logFields, zap.Int64("lastVersion", lastVersion))...)
		return
	}

	switch payload := v.Payload.(type) {
	case *pb.BinaryLog_Ban:
		s.onBan(v.Node, payload.Ban)
		s.logger.Debug("processed ban log", append(logFields, zap.Any("payload", payload.Ban))...)
	case *pb.BinaryLog_Track:
		s.onTrack(v.Node, payload.Track)
		s.logger.Debug("processed track log", append(logFields, zap.Any("payload", payload.Track))...)
	case *pb.BinaryLog_Untrack:
		s.onUntrack(v.Node, payload.Untrack)
		s.logger.Debug("processed untrack log", append(logFields, zap.Any("payload", payload.Untrack))...)
	case *pb.BinaryLog_MatchmakerAdd:
		extract := pb2MatchmakerExtract(payload.MatchmakerAdd)
		_, _, err := s.matchmaker.Add(s.ctx, extract.Presences, extract.SessionID, extract.PartyId, extract.Query, extract.MinCount, extract.MaxCount, extract.CountMultiple, extract.StringProperties, extract.NumericProperties)
		s.handleMatchmakerResult("MatchmakerAdd", err, extract, logFields)

	case *pb.BinaryLog_MatchmakerRemoveSession:
		extract := payload.MatchmakerRemoveSession
		err := s.matchmaker.RemoveSession(extract.SessionId, extract.Ticket)
		s.handleMatchmakerResult("MatchmakerRemoveSession", err, extract, logFields)

	case *pb.BinaryLog_MatchmakerRemoveSessionAll:
		extract := payload.MatchmakerRemoveSessionAll
		err := s.matchmaker.RemoveSessionAll(extract.SessionId)
		s.handleMatchmakerResult("MatchmakerRemoveSessionAll", err, extract, logFields)

	case *pb.BinaryLog_MatchmakerRemoveParty:
		extract := payload.MatchmakerRemoveParty
		err := s.matchmaker.RemoveParty(extract.PartyId, extract.Ticket)
		s.handleMatchmakerResult("MatchmakerRemoveParty", err, extract, logFields)

	case *pb.BinaryLog_MatchmakerRemovePartyAll:
		extract := payload.MatchmakerRemovePartyAll
		err := s.matchmaker.RemovePartyAll(extract.PartyId)
		s.handleMatchmakerResult("MatchmakerRemovePartyAll", err, extract, logFields)

	case *pb.BinaryLog_MatchmakerRemoveAll:
		s.matchmaker.RemoveAll(map[string]bool{payload.MatchmakerRemoveAll.Node: true})
		s.logger.Debug("processed matchmaker remove all", append(logFields, zap.String("targetNode", payload.MatchmakerRemoveAll.Node))...)

	case *pb.BinaryLog_MatchmakerRemove:
		s.matchmaker.Remove(payload.MatchmakerRemove.Ticket)
		s.logger.Debug("processed matchmaker remove", append(logFields, zap.String("ticket", strings.Join(payload.MatchmakerRemove.Ticket, ",")))...)
	case *pb.BinaryLog_PartyCreate:
		s.partyRegistry.(*LocalPartyRegistry).handleFromRemotePartyCreate(pb2partyIndex(payload.PartyCreate))
		s.logger.Debug("processed party create", append(logFields, zap.Any("payload", payload.PartyCreate))...)

	case *pb.BinaryLog_PartyClose:
		s.partyRegistry.(*LocalPartyRegistry).handleFromRemotePartyClose(payload.PartyClose)
		s.logger.Debug("processed party close", append(logFields, zap.String("id", payload.PartyClose))...)
	}
	s.binaryLog.SetVersionByNode(v.Node, v.Version)
}

func (s *LocalPeer) handleMatchmakerResult(operation string, err error, payload interface{}, baseFields []zap.Field) {
	fields := append(baseFields,
		zap.String("operation", operation),
		zap.Any("payload", payload))

	if err != nil {
		s.logger.Error("matchmaker operation failed", append(fields, zap.Error(err))...)
	} else {
		s.logger.Debug("matchmaker operation succeeded", fields...)
	}
}
