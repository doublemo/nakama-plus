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
	syncAtomic "sync/atomic"

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
	for sesionID, bySession := range t.presencesBySession {
		presences := make([]*Presence, 0, len(bySession))
		for _, v := range bySession {
			presences = append(presences, v)
		}

		t.RUnlock()
		if !fn(sesionID, presences) {
			return
		}
		t.RLock()
	}
	t.RUnlock()
}

func (t *LocalTracker) MergeRemoteState(fromNode string, presences []*pb.Presence, join bool) {
	if join {
		t.ClearRemoteTrack()
	} else {
		t.ClearTrackByNode(fromNode)
	}

	t.Lock()
	for _, presence := range presences {
		sessionID := uuid.FromStringOrNil(presence.GetSessionID())
		userID := uuid.FromStringOrNil(presence.GetUserID())
		for k, v := range presence.Stream {
			stream := pb2PresenceStream(v)
			meta := pb2PresenceMeta(presence.Meta[k])
			pc := presenceCompact{ID: PresenceID{Node: presence.Node, SessionID: sessionID}, Stream: stream, UserID: userID}
			p := &Presence{ID: PresenceID{Node: presence.Node, SessionID: sessionID}, Stream: stream, UserID: userID, Meta: meta}

			if bySession, anyTracked := t.presencesBySession[sessionID]; anyTracked {
				if _, alreadyTracked := bySession[pc]; !alreadyTracked {
					bySession[pc] = p
				} else {
					continue
				}
			} else {
				bySession = make(map[presenceCompact]*Presence)
				bySession[pc] = p
				t.presencesBySession[sessionID] = bySession
			}
			t.count.Inc()

			// Update tracking for stream.
			byStreamMode, ok := t.presencesByStream[stream.Mode]
			if !ok {
				byStreamMode = make(map[PresenceStream]map[presenceCompact]*Presence)
				t.presencesByStream[stream.Mode] = byStreamMode
			}

			if byStream, ok := byStreamMode[stream]; !ok {
				byStream = make(map[presenceCompact]*Presence)
				byStream[pc] = p
				byStreamMode[stream] = byStream
			} else {
				byStream[pc] = p
			}
		}
	}
	t.Unlock()
}

func (t *LocalTracker) ClearRemoteTrack() {
	t.Lock()
	for sessionID, bySession := range t.presencesBySession {
		deleted := false
		for pc := range bySession {
			if pc.ID.Node == t.name {
				break
			}

			// Update the tracking for stream.
			if byStreamMode := t.presencesByStream[pc.Stream.Mode]; len(byStreamMode) == 1 {
				// This is the only stream for this stream mode.
				if byStream := byStreamMode[pc.Stream]; len(byStream) == 1 {
					// This was the only presence in the only stream for this stream mode, discard the whole list.
					delete(t.presencesByStream, pc.Stream.Mode)
				} else {
					// There were other presences for the stream, drop just this one.
					delete(byStream, pc)
				}
			} else {
				// There are other streams for this stream mode.
				if byStream := byStreamMode[pc.Stream]; len(byStream) == 1 {
					// This was the only presence for the stream, discard the whole list.
					delete(byStreamMode, pc.Stream)
				} else {
					// There were other presences for the stream, drop just this one.
					delete(byStream, pc)
				}
			}
			t.count.Dec()
			deleted = true
		}

		if deleted {
			delete(t.presencesBySession, sessionID)
		}
	}
	t.Unlock()
}

func (t *LocalTracker) ClearTrackByNode(node string) {
	t.Lock()
	for sessionID, bySession := range t.presencesBySession {
		deleted := false
		for pc := range bySession {
			if pc.ID.Node != node {
				break
			}

			// Update the tracking for stream.
			if byStreamMode := t.presencesByStream[pc.Stream.Mode]; len(byStreamMode) == 1 {
				// This is the only stream for this stream mode.
				if byStream := byStreamMode[pc.Stream]; len(byStream) == 1 {
					// This was the only presence in the only stream for this stream mode, discard the whole list.
					delete(t.presencesByStream, pc.Stream.Mode)
				} else {
					// There were other presences for the stream, drop just this one.
					delete(byStream, pc)
				}
			} else {
				// There are other streams for this stream mode.
				if byStream := byStreamMode[pc.Stream]; len(byStream) == 1 {
					// This was the only presence for the stream, discard the whole list.
					delete(byStreamMode, pc.Stream)
				} else {
					// There were other presences for the stream, drop just this one.
					delete(byStream, pc)
				}
			}
			t.count.Dec()
			deleted = true
		}

		if deleted {
			delete(t.presencesBySession, sessionID)
		}
	}
	t.Unlock()
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
	}

	t.peer.BroadcastBinaryLog(&pb.BinaryLog{
		Node:    t.name,
		Payload: &pb.BinaryLog_Track{Track: presence},
	})
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

	t.peer.BroadcastBinaryLog(&pb.BinaryLog{
		Node:    t.name,
		Payload: &pb.BinaryLog_Untrack{Untrack: untrack},
	})
}

func (t *LocalTracker) UpdateTrackPeer(sessionID uuid.UUID, userID uuid.UUID, ops []*TrackerOp) {
	size := len(ops)
	presence := &pb.Presence{
		SessionID: sessionID.String(),
		UserID:    userID.String(),
		Stream:    make([]*pb.PresenceStream, size),
		Meta:      make([]*pb.PresenceMeta, size),
	}

	for k, v := range ops {
		presence.Stream[k] = presenceStream2PB(v.Stream)
		presence.Meta[k] = presenceMeta2PB(v.Meta)
	}

	t.peer.BroadcastBinaryLog(&pb.BinaryLog{
		Node:    t.name,
		Payload: &pb.BinaryLog_UpdateTrack{UpdateTrack: presence},
	})
}

func (t *LocalTracker) UntrackByModes(sessionID uuid.UUID, modes map[uint8]struct{}, skipStream PresenceStream) {
	leaves := make([]*Presence, 0, 1)

	t.Lock()
	bySession, anyTracked := t.presencesBySession[sessionID]
	if !anyTracked {
		t.Unlock()
		return
	}

	for pc, p := range bySession {
		if _, found := modes[pc.Stream.Mode]; !found {
			// Not a stream mode we need to check.
			continue
		}
		if pc.Stream == skipStream {
			// Skip this stream based on input.
			continue
		}

		// Update the tracking for session.
		if len(bySession) == 1 {
			// This was the only presence for the session, discard the whole list.
			delete(t.presencesBySession, sessionID)
		} else {
			// There were other presences for the session, drop just this one.
			delete(bySession, pc)
		}
		t.count.Dec()

		// Update the tracking for stream.
		if byStreamMode := t.presencesByStream[pc.Stream.Mode]; len(byStreamMode) == 1 {
			// This is the only stream for this stream mode.
			if byStream := byStreamMode[pc.Stream]; len(byStream) == 1 {
				// This was the only presence in the only stream for this stream mode, discard the whole list.
				delete(t.presencesByStream, pc.Stream.Mode)
			} else {
				// There were other presences for the stream, drop just this one.
				delete(byStream, pc)
			}
		} else {
			// There are other streams for this stream mode.
			if byStream := byStreamMode[pc.Stream]; len(byStream) == 1 {
				// This was the only presence for the stream, discard the whole list.
				delete(byStreamMode, pc.Stream)
			} else {
				// There were other presences for the stream, drop just this one.
				delete(byStream, pc)
			}
		}

		if !p.Meta.Hidden {
			syncAtomic.StoreUint32(&p.Meta.Reason, uint32(runtime.PresenceReasonLeave))
			leaves = append(leaves, p)
		}
	}
	t.Unlock()

	if len(leaves) > 0 {
		t.queueEvent(nil, leaves)
	}

	wmodes := make([]uint32, 0, len(modes))
	for k := range modes {
		wmodes = append(wmodes, uint32(k))
	}
	t.UntrackPeer(sessionID, uuid.Nil, nil, wmodes, 0, &skipStream)
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
