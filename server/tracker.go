// Copyright 2018 The Nakama Authors
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
	syncAtomic "sync/atomic"
	"time"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-common/runtime"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	StreamModeNotifications uint8 = iota
	StreamModeStatus
	StreamModeChannel
	StreamModeGroup
	StreamModeDM
	StreamModeMatchRelayed
	StreamModeMatchAuthoritative
	StreamModeParty
)

type PresenceID struct {
	Node      string
	SessionID uuid.UUID
}

type PresenceStream struct {
	Mode       uint8
	Subject    uuid.UUID
	Subcontext uuid.UUID
	Label      string
}

type PresenceMeta struct {
	Format      SessionFormat
	Hidden      bool
	Persistence bool
	Username    string
	Status      string
	Reason      uint32
}

func (pm *PresenceMeta) GetHidden() bool {
	return pm.Hidden
}
func (pm *PresenceMeta) GetPersistence() bool {
	return pm.Persistence
}
func (pm *PresenceMeta) GetUsername() string {
	return pm.Username
}
func (pm *PresenceMeta) GetStatus() string {
	return pm.Status
}
func (pm *PresenceMeta) GetReason() runtime.PresenceReason {
	return runtime.PresenceReason(syncAtomic.LoadUint32(&pm.Reason))
}

type Presence struct {
	ID     PresenceID
	Stream PresenceStream
	UserID uuid.UUID
	Meta   PresenceMeta
}

func (p *Presence) GetUserId() string {
	return p.UserID.String()
}
func (p *Presence) GetSessionId() string {
	return p.ID.SessionID.String()
}
func (p *Presence) GetNodeId() string {
	return p.ID.Node
}
func (p *Presence) GetHidden() bool {
	return p.Meta.Hidden
}
func (p *Presence) GetPersistence() bool {
	return p.Meta.Persistence
}
func (p *Presence) GetUsername() string {
	return p.Meta.Username
}
func (p *Presence) GetStatus() string {
	return p.Meta.Status
}
func (p *Presence) GetReason() runtime.PresenceReason {
	return runtime.PresenceReason(syncAtomic.LoadUint32(&p.Meta.Reason))
}

type PresenceEvent struct {
	Joins  []*Presence
	Leaves []*Presence

	QueueTime time.Time
}

type TrackerOp struct {
	Stream PresenceStream
	Meta   PresenceMeta
}

type Tracker interface {
	SetMatchJoinListener(func(id uuid.UUID, joins []*MatchPresence))
	SetMatchLeaveListener(func(id uuid.UUID, leaves []*MatchPresence))
	SetPartyJoinListener(func(id uuid.UUID, joins []*Presence))
	SetPartyLeaveListener(func(id uuid.UUID, leaves []*Presence))
	Stop()

	// Track returns success true/false, and new presence true/false.
	Track(ctx context.Context, sessionID uuid.UUID, stream PresenceStream, userID uuid.UUID, meta PresenceMeta) (bool, bool)
	TrackMulti(ctx context.Context, sessionID uuid.UUID, ops []*TrackerOp, userID uuid.UUID, otherNode ...string) bool
	Untrack(sessionID uuid.UUID, stream PresenceStream, userID uuid.UUID, otherNode ...string)
	UntrackMulti(sessionID uuid.UUID, streams []*PresenceStream, userID uuid.UUID)
	UntrackAll(sessionID uuid.UUID, reason runtime.PresenceReason, otherNode ...string)
	// Update returns success true/false - will only fail if the user has no presence, otherwise is an upsert.
	Update(ctx context.Context, sessionID uuid.UUID, stream PresenceStream, userID uuid.UUID, meta PresenceMeta, otherNode ...string) bool

	// Remove all presences on a stream, effectively closing it.
	UntrackByStream(stream PresenceStream)
	// Remove all presences on a stream from the local node.
	UntrackLocalByStream(stream PresenceStream)
	// Remove the given session from any streams matching the given mode, except the specified stream.
	UntrackLocalByModes(sessionID uuid.UUID, modes map[uint8]struct{}, skipStream PresenceStream)

	// List the nodes that have at least one presence for the given stream.
	ListNodesForStream(stream PresenceStream) map[string]struct{}

	// Check if a stream exists (has any presences) or not.
	StreamExists(stream PresenceStream) bool
	// Get current total number of presences.
	Count() int
	// Get the number of presences in the given stream.
	CountByStream(stream PresenceStream) int
	// Get a snapshot of current presence counts for streams with one of the given stream modes.
	CountByStreamModeFilter(modes map[uint8]*uint8) map[*PresenceStream]int32
	// Check if a single presence on the current node exists.
	GetLocalBySessionIDStreamUserID(sessionID uuid.UUID, stream PresenceStream, userID uuid.UUID) *PresenceMeta
	// List presences by stream, optionally include hidden ones and not hidden ones.
	ListByStream(stream PresenceStream, includeHidden bool, includeNotHidden bool) []*Presence

	// Fast lookup of local session IDs to use for message delivery.
	ListLocalSessionIDByStream(stream PresenceStream) []uuid.UUID
	// Fast lookup of node + session IDs to use for message delivery.
	ListPresenceIDByStream(stream PresenceStream) []*PresenceID
	// Fast lookup of presences for a set of user IDs + stream mode.
	ListPresenceIDByStreams(fill map[PresenceStream][]*PresenceID)

	// Loop stream
	Range(fn func(sessionID uuid.UUID, presences []*Presence) bool)
	MergeRemoteState(node string, presences []*pb.Presence) bool
	ClearTrackByNode(nodes map[string]bool)
	ClearRemoteTrack()
	SetPeer(peer Peer)
	UntrackByModes(sessionID uuid.UUID, modes map[uint8]struct{}, skipStream PresenceStream)
	// Fast lookup of local node + session IDs to use for message delivery.
	ListLocalPresenceIDByStream(stream PresenceStream) []*PresenceID
}

type presenceCompact struct {
	ID     PresenceID
	Stream PresenceStream
	UserID uuid.UUID
}

func presenceWithReason(p *Presence, reason runtime.PresenceReason) *Presence {
	snapshot := *p
	snapshot.Meta.Reason = uint32(reason)
	return &snapshot
}

type LocalTracker struct {
	sync.RWMutex
	logger             *zap.Logger
	matchJoinListener  func(id uuid.UUID, joins []*MatchPresence)
	matchLeaveListener func(id uuid.UUID, leaves []*MatchPresence)
	partyJoinListener  func(id uuid.UUID, joins []*Presence)
	partyLeaveListener func(id uuid.UUID, leaves []*Presence)
	sessionRegistry    SessionRegistry
	statusRegistry     StatusRegistry
	metrics            Metrics
	protojsonMarshaler *protojson.MarshalOptions
	name               string
	eventsCh           chan *PresenceEvent
	membershipEvents   []PresenceEvent
	membershipReady    chan struct{}
	presencesByStream  map[uint8]map[PresenceStream]map[presenceCompact]*Presence
	presencesBySession map[uuid.UUID]map[presenceCompact]*Presence
	count              *atomic.Int64

	ctx         context.Context
	ctxCancelFn context.CancelFunc

	peer Peer
}

// storePresenceLocked updates both indexes and returns the previous presence.
func (t *LocalTracker) storePresenceLocked(key presenceCompact, presence *Presence) *Presence {
	bySession := t.presencesBySession[key.ID.SessionID]
	if bySession == nil {
		bySession = make(map[presenceCompact]*Presence)
		t.presencesBySession[key.ID.SessionID] = bySession
	}
	previous := bySession[key]
	bySession[key] = presence
	if previous == nil {
		t.count.Inc()
	}
	byMode := t.presencesByStream[key.Stream.Mode]
	if byMode == nil {
		byMode = make(map[PresenceStream]map[presenceCompact]*Presence)
		t.presencesByStream[key.Stream.Mode] = byMode
	}
	if byMode[key.Stream] == nil {
		byMode[key.Stream] = make(map[presenceCompact]*Presence)
	}
	byMode[key.Stream][key] = presence
	return previous
}

func (t *LocalTracker) removePresenceLocked(key presenceCompact) *Presence {
	bySession := t.presencesBySession[key.ID.SessionID]
	presence := bySession[key]
	if presence == nil {
		return nil
	}
	delete(bySession, key)
	if len(bySession) == 0 {
		delete(t.presencesBySession, key.ID.SessionID)
	}
	byMode := t.presencesByStream[key.Stream.Mode]
	byStream := byMode[key.Stream]
	delete(byStream, key)
	if len(byStream) == 0 {
		delete(byMode, key.Stream)
	}
	if len(byMode) == 0 {
		delete(t.presencesByStream, key.Stream.Mode)
	}
	t.count.Dec()
	return presence
}

func StartLocalTracker(logger *zap.Logger, config Config, sessionRegistry SessionRegistry, statusRegistry StatusRegistry, metrics Metrics, protojsonMarshaler *protojson.MarshalOptions) Tracker {
	ctx, ctxCancelFn := context.WithCancel(context.Background())

	t := &LocalTracker{
		logger:             logger,
		sessionRegistry:    sessionRegistry,
		statusRegistry:     statusRegistry,
		metrics:            metrics,
		protojsonMarshaler: protojsonMarshaler,
		name:               config.GetName(),
		eventsCh:           make(chan *PresenceEvent, config.GetTracker().EventQueueSize),
		membershipReady:    make(chan struct{}, 1),
		presencesByStream:  make(map[uint8]map[PresenceStream]map[presenceCompact]*Presence),
		presencesBySession: make(map[uuid.UUID]map[presenceCompact]*Presence),
		count:              atomic.NewInt64(0),

		ctx:         ctx,
		ctxCancelFn: ctxCancelFn,
		peer:        nil,
	}

	go func() {
		// Asynchronously process and dispatch presence events.
		ticker := time.NewTicker(15 * time.Second)
		for {
			select {
			case <-t.ctx.Done():
				return
			case <-t.membershipReady:
				t.processMembershipEvents()
			case e := <-t.eventsCh:
				select {
				case <-t.membershipReady:
					t.processMembershipEvents()
				default:
				}
				t.processEvent(e)
			case <-ticker.C:
				t.metrics.GaugePresences(float64(t.count.Load()))
			}
		}
	}()

	return t
}

func (t *LocalTracker) SetMatchJoinListener(f func(id uuid.UUID, joins []*MatchPresence)) {
	t.Lock()
	t.matchJoinListener = f
	t.Unlock()
}

func (t *LocalTracker) SetMatchLeaveListener(f func(id uuid.UUID, leaves []*MatchPresence)) {
	t.Lock()
	t.matchLeaveListener = f
	t.Unlock()
}

func (t *LocalTracker) SetPartyJoinListener(f func(id uuid.UUID, joins []*Presence)) {
	t.Lock()
	t.partyJoinListener = f
	t.Unlock()
}

func (t *LocalTracker) SetPartyLeaveListener(f func(id uuid.UUID, leaves []*Presence)) {
	t.Lock()
	t.partyLeaveListener = f
	t.Unlock()
}

func (t *LocalTracker) Stop() {
	// No need to explicitly clean up the events channel, just let the application exit.
	t.ctxCancelFn()
}

func (t *LocalTracker) Track(ctx context.Context, sessionID uuid.UUID, stream PresenceStream, userID uuid.UUID, meta PresenceMeta) (bool, bool) {
	session := t.getSession(sessionID)
	if session == nil {
		return false, false
	}
	defer session.CloseUnlock()

	meta.Reason = uint32(runtime.PresenceReasonJoin)
	key := presenceCompact{ID: PresenceID{Node: t.name, SessionID: sessionID}, Stream: stream, UserID: userID}
	p := &Presence{ID: key.ID, Stream: stream, UserID: userID, Meta: meta}
	t.Lock()
	if ctx.Err() != nil {
		t.Unlock()
		return false, false
	}
	if t.presencesBySession[sessionID][key] != nil {
		t.Unlock()
		return true, false
	}

	t.storePresenceLocked(key, p)
	if !meta.Hidden {
		t.queueEventLocked([]*Presence{p}, nil, true)
	}
	t.Unlock()

	t.TrackPeer(sessionID, userID, []*TrackerOp{{Stream: stream, Meta: meta}})
	return true, true
}

func (t *LocalTracker) TrackMulti(ctx context.Context, sessionID uuid.UUID, ops []*TrackerOp, userID uuid.UUID, otherNode ...string) bool {
	node := t.name
	if len(otherNode) > 0 && otherNode[0] != "" {
		node = otherNode[0]
	}

	if session := t.getSession(sessionID); session == nil {
		if node == t.name {
			return false
		}
	} else {
		defer session.CloseUnlock()
	}

	id := PresenceID{Node: node, SessionID: sessionID}
	joins := make([]*Presence, 0, len(ops))
	t.Lock()
	if ctx.Err() != nil {
		t.Unlock()
		return false
	}
	for _, op := range ops {
		key := presenceCompact{ID: id, Stream: op.Stream, UserID: userID}
		if t.presencesBySession[sessionID][key] != nil {
			continue
		}
		meta := op.Meta
		meta.Reason = uint32(runtime.PresenceReasonJoin)
		p := &Presence{ID: id, Stream: op.Stream, UserID: userID, Meta: meta}
		t.storePresenceLocked(key, p)
		if !p.Meta.Hidden {
			joins = append(joins, p)
		}
	}
	t.queueEventLocked(joins, nil, true)
	t.Unlock()

	if node == t.name {
		t.TrackPeer(sessionID, userID, ops)
	}
	return true
}

func (t *LocalTracker) Untrack(sessionID uuid.UUID, stream PresenceStream, userID uuid.UUID, otherNode ...string) {
	node := t.name
	if len(otherNode) > 0 && otherNode[0] != "" {
		node = otherNode[0]
	}

	key := presenceCompact{ID: PresenceID{Node: node, SessionID: sessionID}, Stream: stream, UserID: userID}
	t.Lock()
	p := t.removePresenceLocked(key)
	if p != nil && !p.Meta.Hidden {
		t.queueEventLocked(nil, []*Presence{presenceWithReason(p, runtime.PresenceReasonLeave)}, true)
	}
	t.Unlock()

	if p != nil && node == t.name {
		t.UntrackPeer(sessionID, userID, []*PresenceStream{&stream}, nil, runtime.PresenceReasonLeave, nil)
	}
}

func (t *LocalTracker) UntrackMulti(sessionID uuid.UUID, streams []*PresenceStream, userID uuid.UUID) {
	leaves := make([]*Presence, 0, len(streams))
	t.Lock()
	if len(t.presencesBySession[sessionID]) == 0 {
		t.Unlock()
		return
	}
	id := PresenceID{Node: t.name, SessionID: sessionID}
	for _, stream := range streams {
		p := t.removePresenceLocked(presenceCompact{ID: id, Stream: *stream, UserID: userID})
		if p != nil && !p.Meta.Hidden {
			leaves = append(leaves, presenceWithReason(p, runtime.PresenceReasonLeave))
		}
	}
	t.queueEventLocked(nil, leaves, true)
	t.Unlock()

	t.UntrackPeer(sessionID, userID, streams, nil, runtime.PresenceReasonLeave, nil)
}

func (t *LocalTracker) UntrackAll(sessionID uuid.UUID, reason runtime.PresenceReason, otherNode ...string) {
	node := t.name
	if len(otherNode) > 0 && otherNode[0] != "" {
		node = otherNode[0]
	}

	t.Lock()
	bySession := t.presencesBySession[sessionID]
	if len(bySession) == 0 {
		t.Unlock()
		return
	}
	leaves := make([]*Presence, 0, len(bySession))
	for key, p := range bySession {
		if key.ID.Node != node {
			continue
		}
		t.removePresenceLocked(key)
		if !p.Meta.Hidden {
			leaves = append(leaves, presenceWithReason(p, reason))
		}
	}
	t.queueEventLocked(nil, leaves, true)
	t.Unlock()

	if node == t.name {
		t.UntrackPeer(sessionID, uuid.Nil, nil, nil, reason, nil)
	}
}

func (t *LocalTracker) Update(ctx context.Context, sessionID uuid.UUID, stream PresenceStream, userID uuid.UUID, meta PresenceMeta, otherNode ...string) bool {
	node := t.name
	if len(otherNode) > 0 && otherNode[0] != "" {
		node = otherNode[0]
	}
	if session := t.getSession(sessionID); session == nil {
		if node == t.name {
			return false
		}
	} else {
		defer session.CloseUnlock()
	}

	meta.Reason = uint32(runtime.PresenceReasonUpdate)
	key := presenceCompact{ID: PresenceID{Node: node, SessionID: sessionID}, Stream: stream, UserID: userID}
	p := &Presence{ID: key.ID, Stream: stream, UserID: userID, Meta: meta}
	t.Lock()
	if ctx.Err() != nil {
		t.Unlock()
		return false
	}
	previous := t.storePresenceLocked(key, p)
	var joins, leaves []*Presence
	if !meta.Hidden {
		joins = []*Presence{p}
	}
	if previous != nil && !previous.Meta.Hidden {
		leaves = []*Presence{presenceWithReason(previous, runtime.PresenceReasonUpdate)}
	}
	t.queueEventLocked(joins, leaves, previous == nil || previous.Meta.Hidden != meta.Hidden)
	t.Unlock()

	if node == t.name {
		t.UpdateTrackPeer(sessionID, userID, []*TrackerOp{{Stream: stream, Meta: meta}})
	}
	return true
}

func (t *LocalTracker) UntrackLocalByStream(stream PresenceStream) {
	// NOTE: Generates no presence notifications as everyone on the stream is going away all at once.
	t.Lock()

	byStream, anyTracked := t.presencesByStream[stream.Mode][stream]
	if !anyTracked {
		// Nothing tracked for the stream.
		t.Unlock()
		return
	}

	// Drop the presences from tracking for each session.
	for pc := range byStream {
		if bySession := t.presencesBySession[pc.ID.SessionID]; len(bySession) == 1 {
			// This is the only presence for that session, discard the whole list.
			delete(t.presencesBySession, pc.ID.SessionID)
		} else {
			// There were other presences for the session, drop just this one.
			delete(bySession, pc)
		}
		t.count.Dec()
	}

	// Discard the tracking for stream.
	if byStreamMode := t.presencesByStream[stream.Mode]; len(byStreamMode) == 1 {
		// This is the only stream for this stream mode.
		delete(t.presencesByStream, stream.Mode)
	} else {
		// There are other streams for this stream mode.
		delete(byStreamMode, stream)
	}

	t.Unlock()
}

func (t *LocalTracker) UntrackByStream(stream PresenceStream) {
	// NOTE: Generates no presence notifications as everyone on the stream is going away all at once.
	t.Lock()

	byStream, anyTracked := t.presencesByStream[stream.Mode][stream]
	if !anyTracked {
		// Nothing tracked for the stream.
		t.Unlock()
		return
	}

	// Drop the presences from tracking for each session.
	for pc := range byStream {
		if bySession := t.presencesBySession[pc.ID.SessionID]; len(bySession) == 1 {
			// This is the only presence for that session, discard the whole list.
			delete(t.presencesBySession, pc.ID.SessionID)
		} else {
			// There were other presences for the session, drop just this one.
			delete(bySession, pc)
		}
		t.count.Dec()
	}

	// Discard the tracking for stream.
	if byStreamMode := t.presencesByStream[stream.Mode]; len(byStreamMode) == 1 {
		// This is the only stream for this stream mode.
		delete(t.presencesByStream, stream.Mode)
	} else {
		// There are other streams for this stream mode.
		delete(byStreamMode, stream)
	}

	t.Unlock()
	t.UntrackPeer(uuid.Nil, uuid.Nil, []*PresenceStream{&stream}, nil, 0, nil)
}

func (t *LocalTracker) UntrackLocalByModes(sessionID uuid.UUID, modes map[uint8]struct{}, skipStream PresenceStream) {
	t.untrackByModes(sessionID, modes, skipStream)
}

func (t *LocalTracker) untrackByModes(sessionID uuid.UUID, modes map[uint8]struct{}, skipStream PresenceStream) bool {
	t.Lock()
	defer t.Unlock()
	bySession := t.presencesBySession[sessionID]
	if len(bySession) == 0 {
		return false
	}
	var leaves []*Presence
	for key, p := range bySession {
		if _, ok := modes[key.Stream.Mode]; !ok || key.Stream == skipStream {
			continue
		}
		t.removePresenceLocked(key)
		if !p.Meta.Hidden {
			leaves = append(leaves, presenceWithReason(p, runtime.PresenceReasonLeave))
		}
	}
	t.queueEventLocked(nil, leaves, true)
	return true
}

func (t *LocalTracker) ListNodesForStream(stream PresenceStream) map[string]struct{} {
	nodes := make(map[string]struct{})
	t.RLock()
	values, anyTracked := t.presencesByStream[stream.Mode][stream]
	t.RUnlock()
	if anyTracked {
		for k := range values {
			nodes[k.ID.Node] = struct{}{}
		}
	}
	return nodes
}

func (t *LocalTracker) StreamExists(stream PresenceStream) bool {
	var exists bool
	t.RLock()
	exists = t.presencesByStream[stream.Mode][stream] != nil
	t.RUnlock()
	return exists
}

func (t *LocalTracker) Count() int {
	return int(t.count.Load())
}

func (t *LocalTracker) CountByStream(stream PresenceStream) int {
	var count int
	t.RLock()
	// If the stream exists use its presence count, otherwise 0.
	if byStream, anyTracked := t.presencesByStream[stream.Mode][stream]; anyTracked {
		count = len(byStream)
	}
	t.RUnlock()
	return count
}

func (t *LocalTracker) CountByStreamModeFilter(modes map[uint8]*uint8) map[*PresenceStream]int32 {
	counts := make(map[*PresenceStream]int32)
	t.RLock()
	for mode, byStreamMode := range t.presencesByStream {
		if modes[mode] == nil {
			continue
		}
		for s, ps := range byStreamMode {
			cs := s
			counts[&cs] = int32(len(ps))
		}
	}
	t.RUnlock()
	return counts
}

func (t *LocalTracker) GetLocalBySessionIDStreamUserID(sessionID uuid.UUID, stream PresenceStream, userID uuid.UUID) *PresenceMeta {
	pc := presenceCompact{ID: PresenceID{Node: t.name, SessionID: sessionID}, Stream: stream, UserID: userID}
	t.RLock()
	bySession, anyTracked := t.presencesBySession[sessionID]
	if !anyTracked {
		// Nothing tracked for the session.
		t.RUnlock()
		return nil
	}
	p, found := bySession[pc]
	t.RUnlock()
	if !found {
		return nil
	}
	return &p.Meta
}

func (t *LocalTracker) ListByStream(stream PresenceStream, includeHidden bool, includeNotHidden bool) []*Presence {
	if !includeHidden && !includeNotHidden {
		return []*Presence{}
	}

	t.RLock()
	byStream, anyTracked := t.presencesByStream[stream.Mode][stream]
	if !anyTracked {
		t.RUnlock()
		return []*Presence{}
	}
	ps := make([]*Presence, 0, len(byStream))
	for _, p := range byStream {
		if (p.Meta.Hidden && includeHidden) || (!p.Meta.Hidden && includeNotHidden) {
			ps = append(ps, p)
		}
	}
	t.RUnlock()
	return ps
}

func (t *LocalTracker) ListLocalSessionIDByStream(stream PresenceStream) []uuid.UUID {
	t.RLock()
	byStream, anyTracked := t.presencesByStream[stream.Mode][stream]
	if !anyTracked {
		t.RUnlock()
		return []uuid.UUID{}
	}
	ps := make([]uuid.UUID, 0, len(byStream))
	for pc := range byStream {
		if pc.ID.Node != t.name {
			continue
		}
		ps = append(ps, pc.ID.SessionID)
	}
	t.RUnlock()
	return ps
}

func (t *LocalTracker) ListPresenceIDByStream(stream PresenceStream) []*PresenceID {
	t.RLock()
	byStream, anyTracked := t.presencesByStream[stream.Mode][stream]
	if !anyTracked {
		t.RUnlock()
		return []*PresenceID{}
	}
	ps := make([]*PresenceID, 0, len(byStream))
	for pc := range byStream {
		pid := pc.ID
		ps = append(ps, &pid)
	}
	t.RUnlock()
	return ps
}

func (t *LocalTracker) ListPresenceIDByStreams(fill map[PresenceStream][]*PresenceID) {
	if len(fill) == 0 {
		return
	}

	t.RLock()
	for stream, presences := range fill {
		byStream, anyTracked := t.presencesByStream[stream.Mode][stream]
		if !anyTracked {
			continue
		}
		for pc := range byStream {
			pid := pc.ID
			presences = append(presences, &pid)
		}
		fill[stream] = presences
	}
	t.RUnlock()
}

// queueEventLocked queues membership changes in tracker mutation order. Client
// notifications retain their bounded, best-effort queue.
func (t *LocalTracker) queueEventLocked(joins, leaves []*Presence, membershipChanged bool) {
	if len(joins) == 0 && len(leaves) == 0 {
		return
	}
	if membershipChanged {
		membership := PresenceEvent{Joins: t.hostedMembershipPresences(joins), Leaves: t.hostedMembershipPresences(leaves)}
		if len(membership.Joins) != 0 || len(membership.Leaves) != 0 {
			t.membershipEvents = append(t.membershipEvents, membership)
			select {
			case t.membershipReady <- struct{}{}:
			default:
			}
		}
	}
	select {
	case t.eventsCh <- &PresenceEvent{Joins: joins, Leaves: leaves, QueueTime: time.Now()}:
		// Event queued for asynchronous dispatch.
	default:
		// Event queue is full, log an error and completely drain the queue.
		t.logger.Error("Presence event dispatch queue is full, client notifications may be lost")
		for {
			select {
			case <-t.eventsCh:
				// Discard the event.
			default:
				// Queue is now empty.
				return
			}
		}
	}
}

func (t *LocalTracker) hostedMembershipPresences(presences []*Presence) []*Presence {
	var hosted []*Presence
	for _, p := range presences {
		if p.Stream.Label == t.name && (p.Stream.Mode == StreamModeMatchAuthoritative || p.Stream.Mode == StreamModeParty) {
			hosted = append(hosted, p)
		}
	}
	return hosted
}

func (t *LocalTracker) processMembershipEvents() {
	t.Lock()
	events := t.membershipEvents
	t.membershipEvents = nil
	partyJoin, partyLeave := t.partyJoinListener, t.partyLeaveListener
	matchJoin, matchLeave := t.matchJoinListener, t.matchLeaveListener
	t.Unlock()

	for _, event := range events {
		t.dispatchMembershipChanges(event.Leaves, partyLeave, matchLeave)
		t.dispatchMembershipChanges(event.Joins, partyJoin, matchJoin)
	}
}

func (t *LocalTracker) dispatchMembershipChanges(presences []*Presence, party func(uuid.UUID, []*Presence), match func(uuid.UUID, []*MatchPresence)) {
	parties := make(map[uuid.UUID][]*Presence)
	matches := make(map[uuid.UUID][]*MatchPresence)
	for _, p := range presences {
		id := p.Stream.Subject
		switch p.Stream.Mode {
		case StreamModeParty:
			if party != nil {
				parties[id] = append(parties[id], p)
			}
		case StreamModeMatchAuthoritative:
			if match != nil {
				matches[id] = append(matches[id], &MatchPresence{
					Node: p.ID.Node, UserID: p.UserID, SessionID: p.ID.SessionID,
					Username: p.Meta.Username, Reason: p.GetReason(),
				})
			}
		}
	}
	for id, changes := range matches {
		match(id, changes)
	}
	for id, changes := range parties {
		party(id, changes)
	}
}

func (t *LocalTracker) processEvent(e *PresenceEvent) {
	dequeueTime := time.Now()
	defer func() {
		t.metrics.PresenceEvent(dequeueTime.Sub(e.QueueTime), time.Since(dequeueTime))
	}()

	t.logger.Debug("Processing presence event", zap.Int("joins", len(e.Joins)), zap.Int("leaves", len(e.Leaves)))

	// Group joins/leaves by stream to allow batching.
	// Convert to wire representation at the same time.
	streamJoins := make(map[PresenceStream][]*rtapi.UserPresence, 0)
	streamLeaves := make(map[PresenceStream][]*rtapi.UserPresence, 0)

	for _, p := range e.Joins {
		pWire := &rtapi.UserPresence{
			UserId:      p.UserID.String(),
			SessionId:   p.ID.SessionID.String(),
			Username:    p.Meta.Username,
			Persistence: p.Meta.Persistence,
		}
		if p.Stream.Mode == StreamModeStatus {
			// Status field is only populated for status stream presences.
			pWire.Status = &wrapperspb.StringValue{Value: p.Meta.Status}
		}
		if j, ok := streamJoins[p.Stream]; ok {
			streamJoins[p.Stream] = append(j, pWire)
		} else {
			streamJoins[p.Stream] = []*rtapi.UserPresence{pWire}
		}
	}
	for _, p := range e.Leaves {
		pWire := &rtapi.UserPresence{
			UserId:      p.UserID.String(),
			SessionId:   p.ID.SessionID.String(),
			Username:    p.Meta.Username,
			Persistence: p.Meta.Persistence,
		}
		if p.Stream.Mode == StreamModeStatus {
			// Status field is only populated for status stream presences.
			pWire.Status = &wrapperspb.StringValue{Value: p.Meta.Status}
		}
		if l, ok := streamLeaves[p.Stream]; ok {
			streamLeaves[p.Stream] = append(l, pWire)
		} else {
			streamLeaves[p.Stream] = []*rtapi.UserPresence{pWire}
		}
	}

	// Send joins, together with any leaves for the same stream.
	for stream, joins := range streamJoins {
		leaves, ok := streamLeaves[stream]
		if ok {
			delete(streamLeaves, stream)
		}

		if stream.Mode == StreamModeStatus {
			t.statusRegistry.Queue(stream.Subject, joins, leaves)
			continue
		}

		// Construct the wire representation of the stream.
		streamWire := &rtapi.Stream{
			Mode:  int32(stream.Mode),
			Label: stream.Label,
		}
		if stream.Subject != uuid.Nil {
			streamWire.Subject = stream.Subject.String()
		}
		if stream.Subcontext != uuid.Nil {
			streamWire.Subcontext = stream.Subcontext.String()
		}

		// Find the list of event recipients first so we can skip event encoding work if it's not necessary.
		sessionIDs := t.ListLocalSessionIDByStream(stream)
		if len(sessionIDs) == 0 {
			continue
		}

		// Construct the wire representation of the event based on the stream mode.
		var envelope *rtapi.Envelope
		switch stream.Mode {
		case StreamModeChannel:
			channelID, err := StreamToChannelId(stream)
			if err != nil {
				// Should not happen thanks to previous validation, but guard just in case.
				t.logger.Error("Error converting stream to channel identifier in presence event", zap.Error(err), zap.Any("stream", stream))
				continue
			}
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_ChannelPresenceEvent{ChannelPresenceEvent: &rtapi.ChannelPresenceEvent{
				ChannelId: channelID,
				Joins:     joins,
				Leaves:    leaves,
				RoomName:  streamWire.Label,
			}}}
		case StreamModeGroup:
			channelID, err := StreamToChannelId(stream)
			if err != nil {
				// Should not happen thanks to previous validation, but guard just in case.
				t.logger.Error("Error converting stream to channel identifier in presence event", zap.Error(err), zap.Any("stream", stream))
				continue
			}
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_ChannelPresenceEvent{ChannelPresenceEvent: &rtapi.ChannelPresenceEvent{
				ChannelId: channelID,
				Joins:     joins,
				Leaves:    leaves,
				GroupId:   streamWire.Subject,
			}}}
		case StreamModeDM:
			channelID, err := StreamToChannelId(stream)
			if err != nil {
				// Should not happen thanks to previous validation, but guard just in case.
				t.logger.Error("Error converting stream to channel identifier in presence event", zap.Error(err), zap.Any("stream", stream))
				continue
			}
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_ChannelPresenceEvent{ChannelPresenceEvent: &rtapi.ChannelPresenceEvent{
				ChannelId: channelID,
				Joins:     joins,
				Leaves:    leaves,
				UserIdOne: streamWire.Subject,
				UserIdTwo: streamWire.Subcontext,
			}}}
		case StreamModeMatchRelayed:
			fallthrough
		case StreamModeMatchAuthoritative:
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_MatchPresenceEvent{MatchPresenceEvent: &rtapi.MatchPresenceEvent{
				MatchId: fmt.Sprintf("%v.%v", stream.Subject.String(), stream.Label),
				Joins:   joins,
				Leaves:  leaves,
			}}}
		case StreamModeParty:
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_PartyPresenceEvent{PartyPresenceEvent: &rtapi.PartyPresenceEvent{
				PartyId: fmt.Sprintf("%v.%v", stream.Subject.String(), stream.Label),
				Joins:   joins,
				Leaves:  leaves,
			}}}
		default:
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_StreamPresenceEvent{StreamPresenceEvent: &rtapi.StreamPresenceEvent{
				Stream: streamWire,
				Joins:  joins,
				Leaves: leaves,
			}}}
		}

		// Prepare payload variables but do not initialize until we hit a session that needs them to avoid unnecessary work.
		var payloadProtobuf []byte
		var payloadJSON []byte

		// Deliver event.
		for _, sessionID := range sessionIDs {
			session := t.sessionRegistry.Get(sessionID)
			if session == nil {
				t.logger.Debug("Could not deliver presence event, no session", zap.String("sid", sessionID.String()))
				continue
			}

			var err error
			switch session.Format() {
			case SessionFormatProtobuf:
				if payloadProtobuf == nil {
					// Marshal the payload now that we know this format is needed.
					payloadProtobuf, err = proto.Marshal(envelope)
					if err != nil {
						t.logger.Error("Could not marshal presence event", zap.Error(err))
						return
					}
				}
				err = session.SendBytes(payloadProtobuf, true)
			case SessionFormatJson:
				fallthrough
			default:
				if payloadJSON == nil {
					// Marshal the payload now that we know this format is needed.
					if buf, err := t.protojsonMarshaler.Marshal(envelope); err == nil {
						payloadJSON = buf
					} else {
						t.logger.Error("Could not marshal presence event", zap.Error(err))
						return
					}
				}
				err = session.SendBytes(payloadJSON, true)
			}
			if err != nil {
				t.logger.Error("Failed to deliver presence event", zap.String("sid", sessionID.String()), zap.Error(err))
			}
		}
	}

	// If there are leaves without corresponding joins.
	for stream, leaves := range streamLeaves {
		if stream.Mode == StreamModeStatus {
			t.statusRegistry.Queue(stream.Subject, nil, leaves)
			continue
		}

		// Construct the wire representation of the stream.
		streamWire := &rtapi.Stream{
			Mode:  int32(stream.Mode),
			Label: stream.Label,
		}
		if stream.Subject != uuid.Nil {
			streamWire.Subject = stream.Subject.String()
		}
		if stream.Subcontext != uuid.Nil {
			streamWire.Subcontext = stream.Subcontext.String()
		}

		// Find the list of event recipients first so we can skip event encoding work if it's not necessary.
		sessionIDs := t.ListLocalSessionIDByStream(stream)
		if len(sessionIDs) == 0 {
			continue
		}

		// Construct the wire representation of the event based on the stream mode.
		var envelope *rtapi.Envelope
		switch stream.Mode {
		case StreamModeChannel:
			channelID, err := StreamToChannelId(stream)
			if err != nil {
				// Should not happen thanks to previous validation, but guard just in case.
				t.logger.Error("Error converting stream to channel identifier in presence event", zap.Error(err), zap.Any("stream", stream))
				continue
			}
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_ChannelPresenceEvent{ChannelPresenceEvent: &rtapi.ChannelPresenceEvent{
				ChannelId: channelID,
				// No joins.
				Leaves:   leaves,
				RoomName: streamWire.Label,
			}}}
		case StreamModeGroup:
			channelID, err := StreamToChannelId(stream)
			if err != nil {
				// Should not happen thanks to previous validation, but guard just in case.
				t.logger.Error("Error converting stream to channel identifier in presence event", zap.Error(err), zap.Any("stream", stream))
				continue
			}
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_ChannelPresenceEvent{ChannelPresenceEvent: &rtapi.ChannelPresenceEvent{
				ChannelId: channelID,
				// No joins.
				Leaves:  leaves,
				GroupId: streamWire.Subject,
			}}}
		case StreamModeDM:
			channelID, err := StreamToChannelId(stream)
			if err != nil {
				// Should not happen thanks to previous validation, but guard just in case.
				t.logger.Error("Error converting stream to channel identifier in presence event", zap.Error(err), zap.Any("stream", stream))
				continue
			}
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_ChannelPresenceEvent{ChannelPresenceEvent: &rtapi.ChannelPresenceEvent{
				ChannelId: channelID,
				// No joins.
				Leaves:    leaves,
				UserIdOne: streamWire.Subject,
				UserIdTwo: streamWire.Subcontext,
			}}}
		case StreamModeMatchRelayed:
			fallthrough
		case StreamModeMatchAuthoritative:
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_MatchPresenceEvent{MatchPresenceEvent: &rtapi.MatchPresenceEvent{
				MatchId: fmt.Sprintf("%v.%v", stream.Subject.String(), stream.Label),
				// No joins.
				Leaves: leaves,
			}}}
		case StreamModeParty:
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_PartyPresenceEvent{PartyPresenceEvent: &rtapi.PartyPresenceEvent{
				PartyId: fmt.Sprintf("%v.%v", stream.Subject.String(), stream.Label),
				// No joins.
				Leaves: leaves,
			}}}
		default:
			envelope = &rtapi.Envelope{Message: &rtapi.Envelope_StreamPresenceEvent{StreamPresenceEvent: &rtapi.StreamPresenceEvent{
				Stream: streamWire,
				// No joins.
				Leaves: leaves,
			}}}
		}

		// Prepare payload variables but do not initialize until we hit a session that needs them to avoid unnecessary work.
		var payloadProtobuf []byte
		var payloadJSON []byte

		// Deliver event.
		for _, sessionID := range sessionIDs {
			session := t.sessionRegistry.Get(sessionID)
			if session == nil {
				t.logger.Debug("Could not deliver presence event, no session", zap.String("sid", sessionID.String()))
				continue
			}

			var err error
			switch session.Format() {
			case SessionFormatProtobuf:
				if payloadProtobuf == nil {
					// Marshal the payload now that we know this format is needed.
					payloadProtobuf, err = proto.Marshal(envelope)
					if err != nil {
						t.logger.Error("Could not marshal presence event", zap.Error(err))
						return
					}
				}
				err = session.SendBytes(payloadProtobuf, true)
			case SessionFormatJson:
				fallthrough
			default:
				if payloadJSON == nil {
					// Marshal the payload now that we know this format is needed.
					if buf, err := t.protojsonMarshaler.Marshal(envelope); err == nil {
						payloadJSON = buf
					} else {
						t.logger.Error("Could not marshal presence event", zap.Error(err))
						return
					}
				}
				err = session.SendBytes(payloadJSON, true)
			}
			if err != nil {
				t.logger.Error("Failed to deliver presence event", zap.String("sid", sessionID.String()), zap.Error(err))
			}
		}
	}
}

func (t *LocalTracker) getSession(id uuid.UUID) Session {
	session := t.sessionRegistry.Get(id)
	if session == nil {
		return nil
	}

	session.CloseLock()

	// Session is invalid
	if session.Context().Err() != nil {
		session.CloseUnlock()
		return nil
	}

	return session
}
