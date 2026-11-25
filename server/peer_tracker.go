package server

import (
	"github.com/doublemo/nakama-common/runtime"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
)

func (t *LocalTracker) SetPeer(peer Peer) {
	t.Lock()
	t.peer = peer
	t.Unlock()
}

func (t *LocalTracker) Range(fn func(sessionID uuid.UUID, presences []*Presence) bool) {
	t.RLock()
	defer t.RUnlock()
	for sessionID, bySession := range t.presencesBySession {
		presences := make([]*Presence, 0, len(bySession))
		for _, v := range bySession {
			presences = append(presences, v)
		}

		if !fn(sessionID, presences) {
			return
		}
	}
}

func (t *LocalTracker) MergeRemoteState(fromNode string, presences []*pb.Presence) bool {
	incoming := make(map[presenceCompact]*Presence)
	for _, presence := range presences {
		sessionID, userID, ops, ok := decodeTrackerPresence(presence)
		if !ok || presence.GetNode() != fromNode {
			return false
		}
		id := PresenceID{Node: fromNode, SessionID: sessionID}
		for _, op := range ops {
			key := presenceCompact{ID: id, Stream: op.Stream, UserID: userID}
			incoming[key] = &Presence{ID: id, Stream: op.Stream, UserID: userID, Meta: op.Meta}
		}
	}

	t.Lock()
	defer t.Unlock()
	var joins, leaves, updatedJoins, updatedLeaves []*Presence
	for _, bySession := range t.presencesBySession {
		for key, current := range bySession {
			if key.ID.Node != fromNode {
				continue
			}
			next := incoming[key]
			delete(incoming, key)
			if next == nil {
				t.removePresenceLocked(key)
				if !current.Meta.Hidden {
					leaves = append(leaves, presenceWithReason(current, runtime.PresenceReasonLeave))
				}
				continue
			}

			oldMeta, newMeta := current.Meta, next.Meta
			oldMeta.Reason, newMeta.Reason = 0, 0
			if oldMeta == newMeta {
				continue
			}
			t.storePresenceLocked(key, next)
			switch {
			case oldMeta.Hidden && !newMeta.Hidden:
				joins = append(joins, presenceWithReason(next, runtime.PresenceReasonJoin))
			case !oldMeta.Hidden && newMeta.Hidden:
				leaves = append(leaves, presenceWithReason(current, runtime.PresenceReasonLeave))
			case !oldMeta.Hidden && !newMeta.Hidden:
				updatedJoins = append(updatedJoins, presenceWithReason(next, runtime.PresenceReasonUpdate))
				updatedLeaves = append(updatedLeaves, presenceWithReason(current, runtime.PresenceReasonUpdate))
			}
		}
	}
	for key, presence := range incoming {
		t.storePresenceLocked(key, presence)
		if !presence.Meta.Hidden {
			joins = append(joins, presenceWithReason(presence, runtime.PresenceReasonJoin))
		}
	}
	t.queueEventLocked(joins, leaves, true)
	t.queueEventLocked(updatedJoins, updatedLeaves, false)
	return true
}

func decodeTrackerPresence(presence *pb.Presence) (sessionID, userID uuid.UUID, ops []*TrackerOp, ok bool) {
	sessionID = uuid.FromStringOrNil(presence.GetSessionID())
	userID = uuid.FromStringOrNil(presence.GetUserID())
	streams, metas := presence.GetStream(), presence.GetMeta()
	if sessionID.IsNil() || userID.IsNil() || len(streams) == 0 || len(streams) != len(metas) {
		return sessionID, userID, nil, false
	}
	ops = make([]*TrackerOp, len(streams))
	for i, stream := range streams {
		if stream == nil || metas[i] == nil || stream.GetMode() > 255 {
			return sessionID, userID, nil, false
		}
		ops[i] = &TrackerOp{Stream: pb2PresenceStream(stream), Meta: pb2PresenceMeta(metas[i])}
	}
	return sessionID, userID, ops, true
}

func (t *LocalTracker) ClearRemoteTrack() {
	t.clearTrackedNodes(func(node string) bool { return node != t.name })
}

func (t *LocalTracker) ClearTrackByNode(nodes map[string]bool) {
	t.clearTrackedNodes(func(node string) bool { return nodes[node] })
}

func (t *LocalTracker) clearTrackedNodes(shouldClear func(string) bool) {
	t.Lock()
	defer t.Unlock()
	var leaves []*Presence
	for _, bySession := range t.presencesBySession {
		for key, presence := range bySession {
			if shouldClear(key.ID.Node) {
				t.removePresenceLocked(key)
				if !presence.Meta.Hidden {
					leaves = append(leaves, presenceWithReason(presence, runtime.PresenceReasonDisconnect))
				}
			}
		}
	}
	t.queueEventLocked(nil, leaves, true)
}

func (t *LocalTracker) TrackPeer(sessionID uuid.UUID, userID uuid.UUID, ops []*TrackerOp) {
	size := len(ops)
	presence := &pb.Presence{
		SessionID: sessionID.String(),
		UserID:    userID.String(),
		Stream:    make([]*pb.PresenceStream, size),
		Meta:      make([]*pb.PresenceMeta, size),
		Node:      t.name,
	}

	for k, v := range ops {
		presence.Stream[k] = presenceStream2PB(v.Stream)
		presence.Meta[k] = presenceMeta2PB(v.Meta)
		presence.Meta[k].Reason = uint32(runtime.PresenceReasonJoin)
	}

	t.peer.BinaryLogBroadcast(&pb.BinaryLog{
		Node:    t.name,
		Payload: &pb.BinaryLog_Track{Track: presence},
	}, true)
}

func (t *LocalTracker) UntrackPeer(sessionID uuid.UUID, userID uuid.UUID, streams []*PresenceStream, modes []uint32, reason runtime.PresenceReason, skipStream *PresenceStream) {
	size := len(streams)
	untrack := &pb.UntrackValue{
		SessionID: sessionID.String(),
		UserID:    userID.String(),
		Stream:    make([]*pb.PresenceStream, size),
		Modes:     modes,
		Reason:    uint32(reason),
		Skip:      nil,
	}

	if skipStream != nil {
		untrack.Skip = presenceStream2PB(*skipStream)
	}

	for k, v := range streams {
		untrack.Stream[k] = presenceStream2PB(*v)
	}

	t.peer.BinaryLogBroadcast(&pb.BinaryLog{
		Node:    t.name,
		Payload: &pb.BinaryLog_Untrack{Untrack: untrack},
	}, true)
}

func (t *LocalTracker) UpdateTrackPeer(sessionID uuid.UUID, userID uuid.UUID, ops []*TrackerOp) {
	size := len(ops)
	presence := &pb.Presence{
		SessionID: sessionID.String(),
		UserID:    userID.String(),
		Stream:    make([]*pb.PresenceStream, size),
		Meta:      make([]*pb.PresenceMeta, size),
		Node:      t.name,
	}

	for k, v := range ops {
		presence.Stream[k] = presenceStream2PB(v.Stream)
		presence.Meta[k] = presenceMeta2PB(v.Meta)
	}

	t.peer.BinaryLogBroadcast(&pb.BinaryLog{
		Node:    t.name,
		Payload: &pb.BinaryLog_UpdateTrack{UpdateTrack: presence},
	}, true)
}

func (t *LocalTracker) UntrackByModes(sessionID uuid.UUID, modes map[uint8]struct{}, skipStream PresenceStream) {
	if !t.untrackByModes(sessionID, modes, skipStream) {
		return
	}
	wireModes := make([]uint32, 0, len(modes))
	for mode := range modes {
		wireModes = append(wireModes, uint32(mode))
	}
	t.UntrackPeer(sessionID, uuid.Nil, nil, wireModes, 0, &skipStream)
}

func (t *LocalTracker) ListLocalPresenceIDByStream(stream PresenceStream) []*PresenceID {
	t.RLock()
	byStream, anyTracked := t.presencesByStream[stream.Mode][stream]
	if !anyTracked {
		t.RUnlock()
		return []*PresenceID{}
	}
	ps := make([]*PresenceID, 0, len(byStream))
	for pc := range byStream {
		if pc.ID.Node != t.name {
			continue
		}

		pid := pc.ID
		ps = append(ps, &pid)
	}
	t.RUnlock()
	return ps
}

func presenceStream2PB(stream PresenceStream) *pb.PresenceStream {
	return &pb.PresenceStream{
		Mode:       uint32(stream.Mode),
		Subject:    stream.Subject.String(),
		Subcontext: stream.Subcontext.String(),
		Label:      stream.Label,
	}
}

func pb2PresenceStream(stream *pb.PresenceStream) PresenceStream {
	return PresenceStream{
		Mode:       uint8(stream.Mode),
		Subject:    uuid.FromStringOrNil(stream.Subject),
		Subcontext: uuid.FromStringOrNil(stream.Subcontext),
		Label:      stream.Label,
	}
}

func presenceMeta2PB(meta PresenceMeta) *pb.PresenceMeta {
	return &pb.PresenceMeta{
		SessionFormat: uint32(meta.Format),
		Hidden:        meta.Hidden,
		Persistence:   meta.Persistence,
		Username:      meta.Username,
		Status:        meta.Status,
		Reason:        meta.Reason,
	}
}

func pb2PresenceMeta(meta *pb.PresenceMeta) PresenceMeta {
	return PresenceMeta{
		Format:      SessionFormat(meta.SessionFormat),
		Hidden:      meta.Hidden,
		Persistence: meta.Persistence,
		Username:    meta.Username,
		Status:      meta.Status,
		Reason:      meta.Reason,
	}
}
