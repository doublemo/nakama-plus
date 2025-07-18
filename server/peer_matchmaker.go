package server

import (
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
)

func (s *LocalPeer) MatchmakerAdd(extract *pb.MatchmakerExtract) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node: s.endpoint.Name(),
		Payload: &pb.BinaryLog_MatchmakerAdd{
			MatchmakerAdd: extract,
		},
	}, true)
}
func (s *LocalPeer) MatchmakerRemoveSession(sessionID, ticket string) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node: s.endpoint.Name(),
		Payload: &pb.BinaryLog_MatchmakerRemoveSession{
			MatchmakerRemoveSession: &pb.MatchmakerExtract{
				SessionId: sessionID,
				Ticket:    ticket,
			},
		},
	}, true)
}

func (s *LocalPeer) MatchmakerRemoveSessionAll(sessionID string) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node: s.endpoint.Name(),
		Payload: &pb.BinaryLog_MatchmakerRemoveSessionAll{
			MatchmakerRemoveSessionAll: &pb.MatchmakerExtract{SessionId: sessionID},
		},
	}, true)
}

func (s *LocalPeer) MatchmakerRemoveParty(partyID, ticket string) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node: s.endpoint.Name(),
		Payload: &pb.BinaryLog_MatchmakerRemoveParty{
			MatchmakerRemoveParty: &pb.MatchmakerExtract{
				PartyId: partyID,
				Ticket:  ticket,
			},
		},
	}, true)
}

func (s *LocalPeer) MatchmakerRemovePartyAll(partyID string) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node: s.endpoint.Name(),
		Payload: &pb.BinaryLog_MatchmakerRemovePartyAll{
			MatchmakerRemovePartyAll: &pb.MatchmakerExtract{
				PartyId: partyID,
			},
		},
	}, true)
}

func (s *LocalPeer) MatchmakerRemoveAll(node string) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node: s.endpoint.Name(),
		Payload: &pb.BinaryLog_MatchmakerRemoveAll{
			MatchmakerRemoveAll: &pb.MatchmakerExtract{
				Node: node,
			},
		},
	}, true)
}

func (s *LocalPeer) MatchmakerRemove(tickets []string) {
	s.BinaryLogBroadcast(pb.BinaryLog{
		Node: s.endpoint.Name(),
		Payload: &pb.BinaryLog_MatchmakerRemove{
			MatchmakerRemove: &pb.BinaryLog_PartyMatchmakerRemove{Ticket: tickets},
		},
	}, false)
}

func pb2MatchmakerExtract(extract *pb.MatchmakerExtract) *MatchmakerExtract {
	if extract == nil {
		return nil
	}

	m := &MatchmakerExtract{
		Presences:         make([]*MatchmakerPresence, len(extract.Presences)),
		SessionID:         extract.SessionId,
		PartyId:           extract.PartyId,
		Query:             extract.Query,
		MinCount:          int(extract.MinCount),
		MaxCount:          int(extract.MaxCount),
		CountMultiple:     int(extract.CountMultiple),
		StringProperties:  extract.StringProperties,
		NumericProperties: extract.NumericProperties,
		Ticket:            extract.Ticket,
		Count:             int(extract.Count),
		Intervals:         int(extract.Intervals),
		CreatedAt:         extract.CreatedAt,
		Node:              extract.Node,
	}

	for k, v := range extract.Presences {
		m.Presences[k] = &MatchmakerPresence{
			UserId:    v.UserId,
			SessionId: v.SessionId,
			Username:  v.Username,
			Node:      v.Node,
			SessionID: uuid.FromStringOrNil(v.SessionId),
		}
	}
	return m
}

func matchmakerExtract2pb(extract *MatchmakerExtract) *pb.MatchmakerExtract {
	if extract == nil {
		return nil
	}

	m := &pb.MatchmakerExtract{
		Presences:         make([]*pb.MatchmakerPresence, len(extract.Presences)),
		SessionId:         extract.SessionID,
		PartyId:           extract.PartyId,
		Query:             extract.Query,
		MinCount:          int32(extract.MinCount),
		MaxCount:          int32(extract.MaxCount),
		CountMultiple:     int32(extract.CountMultiple),
		StringProperties:  extract.StringProperties,
		NumericProperties: extract.NumericProperties,
		Ticket:            extract.Ticket,
		Count:             int32(extract.Count),
		Intervals:         int32(extract.Intervals),
		CreatedAt:         extract.CreatedAt,
		Node:              extract.Node,
	}

	for k, v := range extract.Presences {
		m.Presences[k] = &pb.MatchmakerPresence{
			UserId:    v.UserId,
			SessionId: v.SessionId,
			Username:  v.Username,
			Node:      v.Node,
		}
	}
	return m
}

func matchmakerPresence2pb(m *MatchmakerPresence) *pb.MatchmakerPresence {
	if m == nil {
		return nil
	}

	return &pb.MatchmakerPresence{
		UserId:    m.GetUserId(),
		SessionId: m.GetSessionId(),
		Username:  m.GetUsername(),
		Node:      m.GetNodeId(),
	}
}

func pb2matchmakerPresence(m *pb.MatchmakerPresence) *MatchmakerPresence {
	if m == nil {
		return nil
	}

	return &MatchmakerPresence{
		UserId:    m.GetUserId(),
		SessionId: m.GetSessionId(),
		Username:  m.GetUsername(),
		Node:      m.GetNode(),
	}
}
