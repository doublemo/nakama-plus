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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-common/runtime"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
)

type PartyJoinRequest struct {
	Presence          *Presence
	UserPresence      *rtapi.UserPresence
	acceptUnconfirmed bool
}

type partyMemberChange struct {
	left bool
}

type PartyLeader struct {
	PresenceID   *PresenceID
	UserPresence *rtapi.UserPresence
}

type PartyHandler struct {
	sync.RWMutex
	logger        *zap.Logger
	partyRegistry PartyRegistry
	matchmaker    Matchmaker
	tracker       Tracker
	streamManager StreamManager
	router        MessageRouter

	ID         uuid.UUID
	Node       string
	IDStr      string
	Open       bool
	CreateTime time.Time
	MaxSize    int
	Stream     PresenceStream

	stopped           bool
	tick              *atomic.Int64
	lastIdleCheckTick *atomic.Int64
	ctx               context.Context
	ctxCancelFn       context.CancelFunc
	leader            *PartyLeader
	joinRequests      []*PartyJoinRequest
	memberChanges     map[uuid.UUID]*partyMemberChange
	confirmedMembers  map[uuid.UUID]struct{}

	members *PartyPresenceList
	peer    Peer
}

func NewPartyHandler(logger *zap.Logger, partyRegistry PartyRegistry, matchmaker Matchmaker, tracker Tracker, streamManager StreamManager, router MessageRouter, id uuid.UUID, node string, open bool, maxSize int, presence *rtapi.UserPresence, peer Peer) *PartyHandler {
	idStr := fmt.Sprintf("%v.%v", id.String(), node)
	ctx, ctxCancelFn := context.WithCancel(context.Background())

	stream := PresenceStream{Mode: StreamModeParty, Subject: id, Label: node}

	leaderPresence := &Presence{
		ID: PresenceID{
			Node:      node,
			SessionID: uuid.FromStringOrNil(presence.SessionId),
		},
		Stream: stream,
		UserID: uuid.FromStringOrNil(presence.UserId),
		Meta: PresenceMeta{
			Username: presence.Username,
		},
	}

	p := &PartyHandler{
		logger:        logger.With(zap.String("party_id", idStr)),
		partyRegistry: partyRegistry,
		matchmaker:    matchmaker,
		tracker:       tracker,
		streamManager: streamManager,
		router:        router,

		ID:         id,
		Node:       node,
		IDStr:      idStr,
		Open:       open,
		CreateTime: time.Now(),
		MaxSize:    maxSize,
		Stream:     stream,

		stopped:           false,
		tick:              &atomic.Int64{},
		lastIdleCheckTick: &atomic.Int64{},
		ctx:               ctx,
		ctxCancelFn:       ctxCancelFn,
		leader: &PartyLeader{
			PresenceID:   &leaderPresence.ID,
			UserPresence: presence,
		},
		joinRequests:     make([]*PartyJoinRequest, 0, maxSize),
		memberChanges:    make(map[uuid.UUID]*partyMemberChange),
		confirmedMembers: map[uuid.UUID]struct{}{leaderPresence.ID.SessionID: {}},

		members: NewPartyPresenceList(maxSize),
		peer:    peer,
	}

	p.tick.Add(1)

	// No errors expected here.
	_, _ = p.members.Join([]*Presence{leaderPresence})

	return p
}

func (p *PartyHandler) stop() {
	p.ctxCancelFn()
	p.partyRegistry.Delete(p.ID)
	p.tracker.UntrackByStream(p.Stream)
	_ = p.matchmaker.RemovePartyAll(p.IDStr)
	if p.peer != nil {
		p.peer.MatchmakerRemovePartyAll(p.IDStr)
	}
}

func (p *PartyHandler) JoinRequest(presence *Presence) (bool, error) {
	p.Lock()
	if p.stopped {
		p.Unlock()
		return false, runtime.ErrPartyClosed
	}

	p.tick.Add(1)

	// Check if party is full.
	if p.members.Size() >= p.MaxSize {
		p.Unlock()
		return false, runtime.ErrPartyFull
	}
	// Check if party is open, and therefore automatically accepts join requests.
	if p.Open {
		_, err := p.members.Join([]*Presence{presence})
		p.Unlock()
		if err != nil {
			return false, err
		}
		// The party membership has changed, stop any ongoing matchmaking processes.
		_ = p.matchmaker.RemovePartyAll(p.IDStr)
		if p.peer != nil {
			p.peer.MatchmakerRemovePartyAll(p.IDStr)
		}
		return true, nil
	}
	// Check if party has room for more join requests.
	if len(p.joinRequests) >= p.MaxSize {
		p.Unlock()
		return false, runtime.ErrPartyJoinRequestsFull
	}
	// Check if party has already received join request from this user.
	for _, joinRequest := range p.joinRequests {
		if joinRequest.Presence.UserID == presence.UserID {
			p.Unlock()
			return false, runtime.ErrPartyJoinRequestDuplicate
		}
	}
	// Check if party already has this user.
	for _, member := range p.members.List() {
		if member.Presence.UserID == presence.UserID {
			p.Unlock()
			return false, runtime.ErrPartyJoinRequestAlreadyMember
		}
	}

	joinRequest := &PartyJoinRequest{
		Presence: presence,
		UserPresence: &rtapi.UserPresence{
			UserId:    presence.GetUserId(),
			SessionId: presence.GetSessionId(),
			Username:  presence.GetUsername(),
		},
	}
	p.joinRequests = append(p.joinRequests, joinRequest)
	leader := p.leader
	p.Unlock()

	// Send message to party leader.
	if leader != nil {
		envelope := &rtapi.Envelope{
			Message: &rtapi.Envelope_PartyJoinRequest{
				PartyJoinRequest: &rtapi.PartyJoinRequest{
					PartyId:   p.IDStr,
					Presences: []*rtapi.UserPresence{joinRequest.UserPresence},
				},
			},
		}
		p.router.SendToPresenceIDs(p.logger, []*PresenceID{leader.PresenceID}, envelope, true)
	}

	return false, nil
}

func (p *PartyHandler) Join(presences []*Presence) {
	p.Lock()
	if p.stopped {
		p.Unlock()
		return
	}

	confirmed := make([]*Presence, 0, len(presences))
	for _, presence := range presences {
		sessionID := presence.ID.SessionID
		if _, changing := p.memberChanges[sessionID]; changing {
			continue
		}
		if _, alreadyConfirmed := p.confirmedMembers[sessionID]; alreadyConfirmed {
			continue
		}
		// The party host has already admitted members through JoinRequest or
		// Accept. Delayed tracker events should only confirm that admission.
		if !p.members.Contains(presence.ID) {
			continue
		}
		p.confirmedMembers[sessionID] = struct{}{}
		confirmed = append(confirmed, presence)
	}
	if len(confirmed) == 0 {
		p.Unlock()
		return
	}
	p.tick.Add(1)
	if p.leader == nil {
		id, user := p.members.Oldest()
		p.leader = &PartyLeader{PresenceID: id, UserPresence: user}
	}
	members, leader, open := p.members.List(), p.leader.UserPresence, p.Open
	p.Unlock()

	memberUserPresences := make([]*rtapi.UserPresence, 0, len(members))
	for _, member := range members {
		memberUserPresences = append(memberUserPresences, member.UserPresence)
	}
	for _, presence := range confirmed {
		envelope := &rtapi.Envelope{Message: &rtapi.Envelope_Party{
			Party: &rtapi.Party{
				PartyId: p.IDStr,
				Open:    open,
				MaxSize: int32(p.MaxSize),
				Self: &rtapi.UserPresence{
					UserId:    presence.GetUserId(),
					SessionId: presence.GetSessionId(),
					Username:  presence.GetUsername(),
				},
				Leader:    leader,
				Presences: memberUserPresences,
			},
		}}
		p.router.SendToPresenceIDs(p.logger, []*PresenceID{&presence.ID}, envelope, true)
	}
}

func (p *PartyHandler) Leave(presences []*Presence) {
	if len(presences) == 0 {
		return
	}

	p.Lock()
	if p.stopped {
		p.Unlock()
		return
	}

	p.tick.Add(1)

	for _, presence := range presences {
		if change := p.memberChanges[presence.ID.SessionID]; change != nil {
			change.left = true
		}
	}
	presences, _ = p.members.Leave(presences)
	if len(presences) == 0 {
		p.Unlock()
		return
	}

	for _, presence := range presences {
		delete(p.confirmedMembers, presence.ID.SessionID)
	}
	// Remove the leader if they've left.
	for _, presence := range presences {
		if p.leader != nil && p.leader.PresenceID.SessionID == presence.ID.SessionID && p.leader.PresenceID.Node == presence.ID.Node {
			// Check is only meaningful if a leader exists. Leader may temporarily be nil here until a new
			// one is assigned below, when multiple presences leave concurrently and one was just the leader.
			p.leader = nil

			oldestPresenceID, oldestUserPresence := p.members.Oldest()
			if oldestPresenceID == nil || oldestUserPresence == nil {
				// Party is now empty, close it.
				p.stopped = true
				p.Unlock()
				p.stop()
				return
			}

			// Leader has left, but there are other party members. Promote the oldest presence as the new party leader.
			p.leader = &PartyLeader{
				PresenceID:   oldestPresenceID,
				UserPresence: oldestUserPresence,
			}

			// Send any new leader promotion message to party members.
			p.router.SendToStream(p.logger, p.Stream, &rtapi.Envelope{
				Message: &rtapi.Envelope_PartyLeader{
					PartyLeader: &rtapi.PartyLeader{
						PartyId:  p.IDStr,
						Presence: p.leader.UserPresence,
					},
				},
			}, true)

			break
		}
	}
	p.Unlock()

	// The party membership has changed, stop any ongoing matchmaking processes.
	_ = p.matchmaker.RemovePartyAll(p.IDStr)
	if p.peer != nil {
		p.peer.MatchmakerRemovePartyAll(p.IDStr)
	}
}

func (p *PartyHandler) Promote(sessionID, node string, presence *rtapi.UserPresence) error {
	p.Lock()
	if p.stopped {
		p.Unlock()
		return runtime.ErrPartyClosed
	}

	// Only the party leader may promote.
	if p.leader == nil || p.leader.UserPresence.SessionId != sessionID || p.leader.PresenceID.Node != node {
		p.Unlock()
		return runtime.ErrPartyNotLeader
	}
	p.tick.Add(1)

	members := p.members.List()

	var envelope *rtapi.Envelope
	for _, member := range members {
		if member.UserPresence.SessionId == presence.SessionId && member.UserPresence.UserId == presence.UserId && member.UserPresence.Username == presence.Username {
			// Found the party member being promoted.
			p.leader = &PartyLeader{
				PresenceID:   member.PresenceID,
				UserPresence: member.UserPresence,
			}

			envelope = &rtapi.Envelope{
				Message: &rtapi.Envelope_PartyLeader{
					PartyLeader: &rtapi.PartyLeader{
						PartyId:  p.IDStr,
						Presence: p.leader.UserPresence,
					},
				},
			}

			break
		}
	}

	p.Unlock()

	// Attempted to promote a party member that did not exist.
	if envelope == nil {
		return runtime.ErrPartyNotMember
	}

	p.router.SendToStream(p.logger, p.Stream, envelope, true)

	return nil
}

func (p *PartyHandler) Accept(sessionID, node string, presence *rtapi.UserPresence, singleParty bool) error {
	return p.accept(p.ctx, sessionID, node, presence, singleParty)
}

func (p *PartyHandler) accept(ctx context.Context, sessionID, node string, presence *rtapi.UserPresence, singleParty bool) error {
	p.Lock()
	if p.stopped {
		p.Unlock()
		return runtime.ErrPartyClosed
	}
	if p.leader == nil || p.leader.UserPresence.SessionId != sessionID || p.leader.PresenceID.Node != node {
		p.Unlock()
		return runtime.ErrPartyNotLeader
	}
	p.tick.Add(1)

	var pending *PartyJoinRequest
	for _, request := range p.joinRequests {
		if samePartyUser(request.UserPresence, presence) {
			pending = request
			break
		}
	}
	if pending == nil {
		for _, member := range p.members.List() {
			if samePartyUser(member.UserPresence, presence) {
				p.Unlock()
				return nil
			}
		}
		p.Unlock()
		return runtime.ErrPartyNotRequest
	}

	target := pending.Presence
	id := target.ID.SessionID
	if p.memberChanges[id] != nil {
		p.Unlock()
		return runtime.ErrPartyAcceptRequest
	}
	if err := p.members.Reserve(target); err != nil {
		p.Unlock()
		return err
	}
	change := &partyMemberChange{}
	p.memberChanges[id] = change
	p.Unlock()

	err := p.joinPartyStream(ctx, target, singleParty)

	p.Lock()
	delete(p.memberChanges, id)
	if err != nil {
		p.members.Release(target)
		if target.ID.Node != p.Node && partyStreamChangeUncertain(err) {
			pending.acceptUnconfirmed = true
		}
		p.Unlock()
		p.logger.Debug("Failed to accept party member", zap.String("node", target.ID.Node), zap.Error(err))
		return runtime.ErrPartyAcceptRequest
	}
	if p.stopped || change.left {
		p.members.Release(target)
		stopped := p.stopped
		p.Unlock()
		// The stream changed while the owner closed the party or the member left.
		_ = p.leavePartyStream(context.WithoutCancel(ctx), target)
		if stopped {
			return runtime.ErrPartyClosed
		}
		return runtime.ErrPartyAcceptRequest
	}
	if _, err := p.members.Join([]*Presence{target}); err != nil {
		p.members.Release(target)
		p.Unlock()
		_ = p.leavePartyStream(context.WithoutCancel(ctx), target)
		return err
	}
	p.removeJoinRequestLocked(pending)
	p.Unlock()

	p.Join([]*Presence{target})
	_ = p.matchmaker.RemovePartyAll(p.IDStr)
	if p.peer != nil {
		p.peer.MatchmakerRemovePartyAll(p.IDStr)
	}
	return nil
}

func (p *PartyHandler) Remove(sessionID, node string, presence *rtapi.UserPresence) error {
	return p.remove(p.ctx, sessionID, node, presence)
}

func (p *PartyHandler) remove(ctx context.Context, sessionID, node string, presence *rtapi.UserPresence) error {
	p.Lock()
	if p.stopped {
		p.Unlock()
		return runtime.ErrPartyClosed
	}
	if p.leader == nil || p.leader.UserPresence.SessionId != sessionID || p.leader.PresenceID.Node != node {
		p.Unlock()
		return runtime.ErrPartyNotLeader
	}
	p.tick.Add(1)
	if samePartyUser(p.leader.UserPresence, presence) {
		p.Unlock()
		return runtime.ErrPartyRemoveSelf
	}

	var target *Presence
	for _, member := range p.members.List() {
		if samePartyUser(member.UserPresence, presence) {
			target = member.Presence
			break
		}
	}
	var pending *PartyJoinRequest
	if target == nil {
		for _, request := range p.joinRequests {
			if samePartyUser(request.UserPresence, presence) {
				pending, target = request, request.Presence
				break
			}
		}
	}
	if target == nil {
		p.Unlock()
		return runtime.ErrPartyNotMember
	}

	id := target.ID.SessionID
	if p.memberChanges[id] != nil {
		p.Unlock()
		return runtime.ErrPartyRemove
	}
	if pending != nil && !pending.acceptUnconfirmed {
		p.removeJoinRequestLocked(pending)
		p.Unlock()
		return nil
	}
	change := &partyMemberChange{}
	p.memberChanges[id] = change
	p.Unlock()

	err := p.leavePartyStream(ctx, target)
	p.Lock()
	if err != nil && !change.left {
		delete(p.memberChanges, id)
		p.Unlock()
		p.logger.Debug("Failed to remove party member", zap.String("node", target.ID.Node), zap.Error(err))
		return runtime.ErrPartyRemove
	}
	if pending != nil {
		p.removeJoinRequestLocked(pending)
	}
	p.Unlock()

	if target.ID.Node != p.Node {
		p.tracker.Untrack(id, p.Stream, target.UserID, target.ID.Node)
	}
	p.Leave([]*Presence{target})
	p.Lock()
	delete(p.memberChanges, id)
	p.Unlock()

	if pending == nil {
		p.router.SendToPresenceIDs(p.logger, []*PresenceID{&target.ID}, &rtapi.Envelope{Message: &rtapi.Envelope_PartyClose{PartyClose: &rtapi.PartyClose{
			PartyId: p.IDStr,
		}}}, true)
	}
	return nil
}

func (p *PartyHandler) joinPartyStream(ctx context.Context, target *Presence, singleParty bool) error {
	if target.ID.Node != p.Node {
		return requestPartyStreamChange(ctx, p.peer, p.ID, target, true, singleParty)
	}
	ok, _, err := p.streamManager.UserJoin(p.Stream, target.UserID, target.ID.SessionID, false, false, "")
	if err != nil {
		return err
	}
	if !ok {
		return runtime.ErrPartyAcceptRequest
	}
	if singleParty {
		p.tracker.UntrackByModes(target.ID.SessionID, partyStreamMode, p.Stream)
	}
	return nil
}

func (p *PartyHandler) leavePartyStream(ctx context.Context, target *Presence) error {
	if target.ID.Node != p.Node {
		return requestPartyStreamChange(ctx, p.peer, p.ID, target, false, false)
	}
	return p.streamManager.UserLeave(p.Stream, target.UserID, target.ID.SessionID)
}

func (p *PartyHandler) removeJoinRequestLocked(request *PartyJoinRequest) {
	for i, pending := range p.joinRequests {
		if pending == request {
			copy(p.joinRequests[i:], p.joinRequests[i+1:])
			p.joinRequests[len(p.joinRequests)-1] = nil
			p.joinRequests = p.joinRequests[:len(p.joinRequests)-1]
			return
		}
	}
}

func samePartyUser(a, b *rtapi.UserPresence) bool {
	return a.GetSessionId() == b.GetSessionId() && a.GetUserId() == b.GetUserId() && a.GetUsername() == b.GetUsername()
}

func (p *PartyHandler) Close(sessionID, node string) error {
	p.Lock()
	if p.stopped {
		p.Unlock()
		return runtime.ErrPartyClosed
	}

	// Only the party leader may close the party.
	if p.leader == nil || p.leader.UserPresence.SessionId != sessionID || p.leader.PresenceID.Node != node {
		p.Unlock()
		return runtime.ErrPartyNotLeader
	}

	p.stopped = true
	p.Unlock()

	p.router.SendToStream(p.logger, p.Stream, &rtapi.Envelope{Message: &rtapi.Envelope_PartyClose{PartyClose: &rtapi.PartyClose{
		PartyId: p.IDStr,
	}}}, true)

	p.stop()
	return nil
}

func (p *PartyHandler) JoinRequestList(sessionID, node string) ([]*rtapi.UserPresence, error) {
	p.RLock()
	if p.stopped {
		p.RUnlock()
		return nil, runtime.ErrPartyClosed
	}

	// Only the party leader may request a list of pending join requests.
	if p.leader == nil || p.leader.UserPresence.SessionId != sessionID || p.leader.PresenceID.Node != node {
		p.RUnlock()
		return nil, runtime.ErrPartyNotLeader
	}

	p.tick.Add(1)

	joinRequestUserPresences := make([]*rtapi.UserPresence, 0, len(p.joinRequests))
	for _, joinRequest := range p.joinRequests {
		joinRequestUserPresences = append(joinRequestUserPresences, joinRequest.UserPresence)
	}

	p.RUnlock()

	return joinRequestUserPresences, nil
}

func (p *PartyHandler) MatchmakerAdd(sessionID, node, query string, minCount, maxCount, countMultiple int, stringProperties map[string]string, numericProperties map[string]float64) (string, []*PresenceID, error) {
	p.RLock()
	if p.stopped {
		p.RUnlock()
		return "", nil, runtime.ErrPartyClosed
	}

	// Only the party leader may start a matchmaking process.
	if p.leader == nil || p.leader.UserPresence.SessionId != sessionID || p.leader.PresenceID.Node != node {
		p.RUnlock()
		return "", nil, runtime.ErrPartyNotLeader
	}

	p.tick.Add(1)

	members := p.members.List()

	// Prepare the list of presences that will go into the matchmaker as part of the party.
	presences := make([]*MatchmakerPresence, 0, len(members))
	memberPresenceIDs := make([]*PresenceID, 0, len(members)-1)
	for _, member := range members {
		memberUserPresence := member.UserPresence
		presences = append(presences, &MatchmakerPresence{
			UserId:    memberUserPresence.UserId,
			SessionId: memberUserPresence.SessionId,
			Username:  memberUserPresence.Username,
			Node:      member.PresenceID.Node,
			SessionID: member.PresenceID.SessionID,
		})
		if member.PresenceID.SessionID == p.leader.PresenceID.SessionID && member.PresenceID.Node == p.leader.PresenceID.Node {
			continue
		}
		memberPresenceIDs = append(memberPresenceIDs, member.PresenceID)
	}

	p.RUnlock()

	ticket, createdAt, err := p.matchmaker.Add(p.ctx, presences, "", p.IDStr, query, minCount, maxCount, countMultiple, stringProperties, numericProperties)
	if err != nil {
		return "", nil, err
	}

	if p.peer != nil {
		p.peer.MatchmakerAdd(matchmakerExtract2pb(&MatchmakerExtract{
			Presences:         presences,
			SessionID:         "",
			PartyId:           p.IDStr,
			Query:             query,
			MinCount:          minCount,
			MaxCount:          maxCount,
			CountMultiple:     countMultiple,
			StringProperties:  stringProperties,
			NumericProperties: numericProperties,
			Ticket:            ticket,
			CreatedAt:         createdAt,
		}))
	}
	return ticket, memberPresenceIDs, nil
}

func (p *PartyHandler) MatchmakerRemove(sessionID, node, ticket string) error {
	p.RLock()
	if p.stopped {
		p.RUnlock()
		return runtime.ErrPartyClosed
	}

	// Only the party leader may stop a matchmaking process.
	if p.leader == nil || p.leader.UserPresence.SessionId != sessionID || p.leader.PresenceID.Node != node {
		p.RUnlock()
		return runtime.ErrPartyNotLeader
	}

	p.tick.Add(1)

	p.RUnlock()

	if err := p.matchmaker.RemoveParty(p.IDStr, ticket); err != nil {
		return err
	}

	if p.peer != nil {
		p.peer.MatchmakerRemoveParty(p.IDStr, ticket)
	}
	return nil
}

func (p *PartyHandler) DataSend(sessionID, node string, opCode int64, data []byte) error {
	p.RLock()
	if p.stopped {
		p.RUnlock()
		return runtime.ErrPartyClosed
	}

	members := p.members.List()

	// Check if the sender is a party member.
	var sender *rtapi.UserPresence
	for _, member := range members {
		if member.UserPresence.SessionId == sessionID && member.PresenceID.Node == node {
			sender = member.UserPresence
			break
		}
	}
	var recipients []*PresenceID
	if sender != nil && len(members) > 0 {
		recipients = make([]*PresenceID, 0, len(members)-1)
		for _, member := range members {
			if member.UserPresence.SessionId == sessionID && member.PresenceID.Node == node {
				continue
			}
			recipients = append(recipients, member.PresenceID)
		}
	}

	p.RUnlock()

	if sender == nil {
		return runtime.ErrPartyNotMember
	}
	p.tick.Add(1)
	if len(recipients) == 0 {
		return nil
	}

	// Sender was a party member, construct and send the correct envelope.
	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_PartyData{
			PartyData: &rtapi.PartyData{
				PartyId:  p.IDStr,
				Presence: sender,
				OpCode:   opCode,
				Data:     data,
			},
		},
	}
	p.router.SendToPresenceIDs(p.logger, recipients, envelope, true)

	return nil
}

func (p *PartyHandler) Update(sessionID, node, label string, open, hidden bool) error {
	p.Lock()
	if p.stopped {
		p.Unlock()
		return runtime.ErrPartyClosed
	}

	// Only the party leader may update the label.
	if p.leader == nil || p.leader.UserPresence.SessionId != sessionID || p.leader.PresenceID.Node != node {
		p.Unlock()
		return runtime.ErrPartyNotLeader
	}
	p.tick.Add(1)

	if err := p.partyRegistry.LabelUpdate(p.ID, p.Node, label, open, hidden, p.MaxSize, p.CreateTime); err != nil {
		p.Unlock()
		return err
	}

	p.Open = open

	p.Unlock()

	envelope := &rtapi.Envelope{
		Message: &rtapi.Envelope_PartyUpdate{
			PartyUpdate: &rtapi.PartyUpdate{
				PartyId: p.IDStr,
				Open:    open,
				Hidden:  hidden,
				Label:   label,
			},
		},
	}

	// Send updated label and open status to all party members.
	p.router.SendToStream(p.logger, p.Stream, envelope, true)

	return nil
}

func (p *PartyHandler) CloseIfIdle() {
	p.Lock()
	if p.stopped {
		p.Unlock()
		return
	}
	p.Unlock()

	tick := p.tick.Load()
	lastIdleCheckTick := p.lastIdleCheckTick.Swap(tick)

	if lastIdleCheckTick != tick {
		// Party has activity, or was just created.
		return
	}

	if p.tracker.CountByStream(p.Stream) == 0 {
		// Party is idle and has no members.
		p.Lock()
		if p.stopped {
			p.Unlock()
			return
		}
		p.stopped = true
		p.Unlock()

		p.stop()
	}
}
