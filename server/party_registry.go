// Copyright 2021 The Nakama Authors
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
	"errors"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var ErrPartyNotFound = errors.New("party not found")

type PartyRegistry interface {
	Create(open bool, maxSize int, leader *rtapi.UserPresence) *PartyHandler
	Delete(id uuid.UUID)

	Join(id uuid.UUID, presences []*Presence)
	Leave(id uuid.UUID, presences []*Presence)

	PartyJoinRequest(ctx context.Context, id uuid.UUID, node string, presence *Presence) (bool, error)
	PartyPromote(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string, presence *rtapi.UserPresence) error
	PartyAccept(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string, presence *rtapi.UserPresence) error
	PartyRemove(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string, presence *rtapi.UserPresence) error
	PartyClose(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string) error
	PartyJoinRequestList(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string) ([]*rtapi.UserPresence, error)
	PartyMatchmakerAdd(ctx context.Context, id uuid.UUID, node, sessionID, fromNode, query string, minCount, maxCount, countMultiple int, stringProperties map[string]string, numericProperties map[string]float64) (string, []*PresenceID, error)
	PartyMatchmakerRemove(ctx context.Context, id uuid.UUID, node, sessionID, fromNode, ticket string) error
	PartyDataSend(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string, opCode int64, data []byte) error

	SetPeer(peer Peer)
}

type LocalPartyRegistry struct {
	logger        *zap.Logger
	config        Config
	matchmaker    Matchmaker
	tracker       Tracker
	streamManager StreamManager
	router        MessageRouter
	node          string

	parties *MapOf[uuid.UUID, *PartyHandler]
	peer    *atomic.Pointer[Peer]
}

func NewLocalPartyRegistry(logger *zap.Logger, config Config, matchmaker Matchmaker, tracker Tracker, streamManager StreamManager, router MessageRouter, node string) PartyRegistry {
	return &LocalPartyRegistry{
		logger:        logger,
		config:        config,
		matchmaker:    matchmaker,
		tracker:       tracker,
		streamManager: streamManager,
		router:        router,
		node:          node,

		parties: &MapOf[uuid.UUID, *PartyHandler]{},
		peer:    &atomic.Pointer[Peer]{},
	}
}

func (p *LocalPartyRegistry) Create(open bool, maxSize int, presence *rtapi.UserPresence) *PartyHandler {
	id := uuid.Must(uuid.NewV4())
	peer, _ := p.getPeer()
	partyHandler := NewPartyHandler(p.logger, p, p.matchmaker, p.tracker, p.streamManager, p.router, id, p.node, open, maxSize, presence, peer)

	p.parties.Store(id, partyHandler)

	return partyHandler
}

func (p *LocalPartyRegistry) Delete(id uuid.UUID) {
	p.parties.Delete(id)
}

func (p *LocalPartyRegistry) Join(id uuid.UUID, presences []*Presence) {
	ph, found := p.parties.Load(id)
	if !found {
		return
	}
	ph.Join(presences)
}

func (p *LocalPartyRegistry) Leave(id uuid.UUID, presences []*Presence) {
	ph, found := p.parties.Load(id)
	if !found {
		return
	}
	ph.Leave(presences)
}

func (p *LocalPartyRegistry) PartyJoinRequest(ctx context.Context, id uuid.UUID, node string, presence *Presence) (bool, error) {
	if node != p.node {
		peer, endpoint, ok := p.getEndpoint(node)
		if !ok {
			return false, ErrPartyNotFound
		}

		//remote node
		ret, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{Payload: &pb.Peer_Envelope_PartyJoinRequest{
			PartyJoinRequest: &pb.Party_JoinRequest{
				Id: id.String(),
				Presence: &pb.Presence{
					SessionID: presence.GetSessionId(),
					UserID:    presence.GetUserId(),
					Stream:    []*pb.PresenceStream{presenceStream2PB(presence.Stream)},
					Meta:      []*pb.PresenceMeta{presenceMeta2PB(presence.Meta)},
					Node:      presence.GetNodeId(),
				},
			},
		}})

		if err != nil {
			return false, err
		}
		return ret.GetPartyJoinRequestReply(), nil
	}

	ph, found := p.parties.Load(id)
	if !found {
		return false, ErrPartyNotFound
	}

	return ph.JoinRequest(presence)
}

func (p *LocalPartyRegistry) PartyPromote(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string, presence *rtapi.UserPresence) error {
	if node != p.node {
		peer, endpoint, ok := p.getEndpoint(node)
		if !ok {
			return ErrPartyNotFound
		}

		//remote node
		_, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{
			Payload: &pb.Peer_Envelope_PartyPromote{
				PartyPromote: &pb.Party_Promote{
					Id:           id.String(),
					SessionID:    sessionID,
					FromNode:     fromNode,
					UserPresence: presence,
				},
			},
		})
		return err
	}

	ph, found := p.parties.Load(id)
	if !found {
		return ErrPartyNotFound
	}

	return ph.Promote(sessionID, fromNode, presence)
}

func (p *LocalPartyRegistry) PartyAccept(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string, presence *rtapi.UserPresence) error {
	if node != p.node {
		peer, endpoint, ok := p.getEndpoint(node)
		if !ok {
			return ErrPartyNotFound
		}

		//remote node
		_, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{
			Payload: &pb.Peer_Envelope_PartyAccept{
				PartyAccept: &pb.Party_Accept{
					Id:           id.String(),
					SessionID:    sessionID,
					FromNode:     fromNode,
					UserPresence: presence,
				},
			},
		})

		return err
	}

	ph, found := p.parties.Load(id)
	if !found {
		return ErrPartyNotFound
	}

	return ph.Accept(sessionID, fromNode, presence, p.config.GetSession().SingleParty)
}

func (p *LocalPartyRegistry) PartyRemove(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string, presence *rtapi.UserPresence) error {
	if node != p.node {
		peer, endpoint, ok := p.getEndpoint(node)
		if !ok {
			return ErrPartyNotFound
		}

		//remote node
		_, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{
			Payload: &pb.Peer_Envelope_PartyRemove{
				PartyRemove: &pb.Party_Remove{
					Id:           id.String(),
					SessionID:    sessionID,
					FromNode:     fromNode,
					UserPresence: presence,
				},
			},
		})

		return err
	}

	ph, found := p.parties.Load(id)
	if !found {
		return ErrPartyNotFound
	}

	return ph.Remove(sessionID, fromNode, presence)
}

func (p *LocalPartyRegistry) PartyClose(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string) error {
	if node != p.node {
		peer, endpoint, ok := p.getEndpoint(node)
		if !ok {
			return ErrPartyNotFound
		}

		//remote node
		_, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{
			Payload: &pb.Peer_Envelope_PartyClose{
				PartyClose: &pb.Party_Close{
					Id:        id.String(),
					SessionID: sessionID,
					FromNode:  fromNode,
				},
			},
		})
		return err
	}

	ph, found := p.parties.Load(id)
	if !found {
		return ErrPartyNotFound
	}

	return ph.Close(sessionID, fromNode)
}

func (p *LocalPartyRegistry) PartyJoinRequestList(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string) ([]*rtapi.UserPresence, error) {
	if node != p.node {
		peer, endpoint, ok := p.getEndpoint(node)
		if !ok {
			return nil, ErrPartyNotFound
		}

		//remote node
		ret, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{
			Payload: &pb.Peer_Envelope_PartyJoinRequestList{
				PartyJoinRequestList: &pb.Party_JoinRequestList{
					Id:        id.String(),
					SessionID: sessionID,
					FromNode:  fromNode,
				},
			},
		})

		if err != nil {
			return nil, err
		}

		list := ret.GetPartyJoinRequestListReply()
		if list == nil {
			return make([]*rtapi.UserPresence, 0), nil
		}
		return list.GetUserPresence(), nil
	}

	ph, found := p.parties.Load(id)
	if !found {
		return nil, ErrPartyNotFound
	}

	return ph.JoinRequestList(sessionID, fromNode)
}

func (p *LocalPartyRegistry) PartyMatchmakerAdd(ctx context.Context, id uuid.UUID, node, sessionID, fromNode, query string, minCount, maxCount, countMultiple int, stringProperties map[string]string, numericProperties map[string]float64) (string, []*PresenceID, error) {
	if node != p.node {
		peer, endpoint, ok := p.getEndpoint(node)
		if !ok {
			return "", nil, ErrPartyNotFound
		}

		//remote node
		ret, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{
			Payload: &pb.Peer_Envelope_PartyMatchmakerAdd{
				PartyMatchmakerAdd: &pb.Party_MatchmakerAdd{
					Id:                id.String(),
					SessionID:         sessionID,
					FromNode:          fromNode,
					MinCount:          int32(minCount),
					MaxCount:          int32(maxCount),
					CountMultiple:     int32(countMultiple),
					StringProperties:  stringProperties,
					NumericProperties: numericProperties,
				},
			},
		})

		if err != nil {
			return "", nil, err
		}

		matchmaker := ret.GetPartyMatchmakerAddReply()
		if matchmaker == nil {
			return "", nil, ErrPartyNotFound
		}

		presences := make([]*PresenceID, len(matchmaker.PresenceID))
		for k, v := range matchmaker.PresenceID {
			presences[k] = &PresenceID{
				Node:      v.GetNode(),
				SessionID: uuid.FromStringOrNil(v.GetSessionID()),
			}
		}
		return matchmaker.Ticket, presences, nil

	}

	ph, found := p.parties.Load(id)
	if !found {
		return "", nil, ErrPartyNotFound
	}

	return ph.MatchmakerAdd(sessionID, fromNode, query, minCount, maxCount, countMultiple, stringProperties, numericProperties)
}

func (p *LocalPartyRegistry) PartyMatchmakerRemove(ctx context.Context, id uuid.UUID, node, sessionID, fromNode, ticket string) error {
	if node != p.node {
		peer, endpoint, ok := p.getEndpoint(node)
		if !ok {
			return ErrPartyNotFound
		}

		//remote node
		_, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{
			Payload: &pb.Peer_Envelope_PartyMatchmakerRemove{
				PartyMatchmakerRemove: &pb.Party_MatchmakerRemove{
					Id:        id.String(),
					SessionID: sessionID,
					FromNode:  fromNode,
					Ticket:    ticket,
				},
			},
		})
		return err
	}

	ph, found := p.parties.Load(id)
	if !found {
		return ErrPartyNotFound
	}

	return ph.MatchmakerRemove(sessionID, fromNode, ticket)
}

func (p *LocalPartyRegistry) PartyDataSend(ctx context.Context, id uuid.UUID, node, sessionID, fromNode string, opCode int64, data []byte) error {
	if node != p.node {
		peer, endpoint, ok := p.getEndpoint(node)
		if !ok {
			return ErrPartyNotFound
		}

		//remote node
		_, err := peer.Request(ctx, endpoint, &pb.Peer_Envelope{
			Payload: &pb.Peer_Envelope_PartyDataSend{
				PartyDataSend: &pb.Party_DataSend{
					Id:        id.String(),
					SessionID: sessionID,
					FromNode:  fromNode,
					OpCode:    opCode,
					Data:      data,
				},
			},
		})
		return err
	}

	ph, found := p.parties.Load(id)
	if !found {
		return ErrPartyNotFound
	}

	return ph.DataSend(sessionID, fromNode, opCode, data)
}

func (p *LocalPartyRegistry) SetPeer(peer Peer) {
	p.peer.Store(&peer)
}

func (p *LocalPartyRegistry) getPeer() (Peer, bool) {
	if p.peer == nil {
		return nil, false
	}

	peer := p.peer.Load()
	if peer == nil || *peer == nil {
		return nil, false
	}
	return *peer, true
}

func (p *LocalPartyRegistry) getEndpoint(node string) (Peer, Endpoint, bool) {
	peer, ok := p.getPeer()
	if !ok {
		return nil, nil, false
	}

	endpoint, ok := peer.Member(node)
	return peer, endpoint, ok
}
