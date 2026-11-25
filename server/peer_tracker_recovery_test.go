package server

import (
	"context"
	"testing"
	"time"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-common/runtime"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"github.com/hashicorp/memberlist"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func TestPresenceSnapshotReconcilesHostedMembers(t *testing.T) {
	peer := newSnapshotTestPeer(t, "host")
	tracker := newPresenceRecoveryTracker("host")
	peer.tracker = tracker
	var partyConfirmations int
	router := &testMessageRouter{sendToPresence: func(_ []*PresenceID, envelope *rtapi.Envelope) {
		if envelope.GetParty() != nil {
			partyConfirmations++
		}
	}}
	peer.partyRegistry.Init(peer.matchmaker, tracker, nil, router, nil)
	tracker.SetPartyJoinListener(peer.partyRegistry.Join)
	tracker.SetPartyLeaveListener(peer.partyRegistry.Leave)
	leader := &rtapi.UserPresence{UserId: uuid.Must(uuid.NewV4()).String(), SessionId: uuid.Must(uuid.NewV4()).String(), Username: "leader"}
	party, err := peer.partyRegistry.Create(true, false, 4, leader, "{}")
	if err != nil {
		t.Fatal(err)
	}
	defer party.ctxCancelFn()
	matchID := uuid.Must(uuid.NewV4())
	matchStream := PresenceStream{Mode: StreamModeMatchAuthoritative, Subject: matchID, Label: "host"}
	sessionID, userID := uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4())
	member := &Presence{ID: PresenceID{Node: "source", SessionID: sessionID}, UserID: userID, Stream: party.Stream, Meta: PresenceMeta{Username: "member"}}
	if admitted, err := party.JoinRequest(member); !admitted || err != nil {
		t.Fatalf("failed to admit member to open party: %v", err)
	}
	matchMember := &MatchPresence{Node: "source", SessionID: sessionID, UserID: userID, Username: "member"}
	core := &presenceRecoveryMatchCore{}
	match := &MatchHandler{stopped: atomic.NewBool(false), PresenceList: NewMatchPresenceList(),
		JoinMarkerList: NewMatchJoinMarkerList(NewConfig(peer.logger), 1), Core: core, callCh: make(chan func(*MatchHandler), 4), state: true}
	match.PresenceList.Join([]*MatchPresence{matchMember})
	match.JoinMarkerList.Add(matchMember, 0)
	matches := &LocalMatchRegistry{matches: &MapOf[uuid.UUID, *MatchHandler]{}}
	matches.matches.Store(matchID, match)
	tracker.SetMatchJoinListener(matches.Join)
	tracker.SetMatchLeaveListener(matches.Leave)

	// Force the client queue to overflow while the snapshot is arriving.
	tracker.eventsCh <- &PresenceEvent{}
	presence := &pb.Presence{Node: "source", SessionID: sessionID.String(), UserID: userID.String(),
		Stream: []*pb.PresenceStream{presenceStream2PB(party.Stream), presenceStream2PB(matchStream)},
		Meta:   []*pb.PresenceMeta{{Username: "member"}, {Username: "member"}}}
	snapshot := &pb.State{Node: "source", Nodes: []*pb.StateNode{{Node: "source", Version: 1, Presences: []*pb.Presence{presence}}}}
	peer.onMergeRemoteState(marshalSnapshotTestState(t, snapshot), false)
	tracker.processMembershipEvents()
	select {
	case apply := <-match.callCh:
		apply(match)
	default:
		t.Fatal("snapshot did not confirm the match join")
	}
	if partyConfirmations != 1 || len(match.JoinMarkerList.ClearExpired(1000)) != 0 {
		t.Fatal("snapshot did not confirm the existing Party and Match admissions")
	}

	peer.onMergeRemoteState(marshalSnapshotTestState(t, snapshot), false)
	presence.Meta[1].Username = "renamed"
	peer.onMergeRemoteState(marshalSnapshotTestState(t, snapshot), false)
	tracker.processMembershipEvents()
	if partyConfirmations != 1 || len(match.callCh) != 0 || party.members.Size() != 2 || match.PresenceList.Size() != 1 {
		t.Fatal("repeated or metadata-only snapshot changed membership")
	}
	if got := tracker.ListByStream(matchStream, true, true); len(got) != 1 || got[0].Meta.Username != "renamed" {
		t.Fatalf("snapshot did not refresh the tracker metadata: %v", got)
	}

	snapshot.Nodes[0].Presences = nil
	peer.onMergeRemoteState(marshalSnapshotTestState(t, snapshot), false)
	tracker.processMembershipEvents()
	select {
	case apply := <-match.callCh:
		apply(match)
	default:
		t.Fatal("snapshot did not queue the match leave")
	}
	if party.members.Size() != 1 || match.PresenceList.Size() != 0 || len(core.leaves) != 1 || core.leaves[0].Reason != runtime.PresenceReasonLeave || tracker.Count() != 0 {
		t.Fatal("snapshot did not remove the missing Party and Match members")
	}
}

func TestPresenceNodeDepartureQueuesDisconnectAndIgnoresLateReplication(t *testing.T) {
	peer := newPartyNetworkTestPeer(t, "host", &testSessionRegistry{})
	tracker := newPresenceRecoveryTracker("host")
	peer.tracker = tracker
	partyStream := PresenceStream{Mode: StreamModeParty, Subject: uuid.Must(uuid.NewV4()), Label: "host"}
	matchStream := PresenceStream{Mode: StreamModeMatchAuthoritative, Subject: uuid.Must(uuid.NewV4()), Label: "host"}
	presence := newSnapshotTestPresence("departed")
	presence.Stream = []*pb.PresenceStream{presenceStream2PB(partyStream), presenceStream2PB(matchStream)}
	presence.Meta = []*pb.PresenceMeta{{}, {}}
	if !tracker.MergeRemoteState("departed", []*pb.Presence{presence}) {
		t.Fatal("could not seed departed-node presences")
	}
	other := newSnapshotTestPresence("remaining")
	other.SessionID = presence.SessionID
	if !tracker.MergeRemoteState("remaining", []*pb.Presence{other}) {
		t.Fatal("could not seed unaffected presence")
	}
	var partyReason, matchReason runtime.PresenceReason
	tracker.SetPartyLeaveListener(func(_ uuid.UUID, leaves []*Presence) { partyReason = leaves[0].GetReason() })
	tracker.SetMatchLeaveListener(func(_ uuid.UUID, leaves []*MatchPresence) { matchReason = leaves[0].Reason })
	node := &memberlist.Node{Name: "departed"}
	peer.members.Store(node.Name, NewPeerEndpont(node.Name, nil, 0, 0, 0, false, nil, node))
	peer.NotifyLeave(node)
	for deadline := time.Now().Add(2 * time.Second); tracker.Count() != 1 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	tracker.processMembershipEvents()
	if partyReason != runtime.PresenceReasonDisconnect || matchReason != runtime.PresenceReasonDisconnect || tracker.Count() != 1 {
		t.Fatal("departure did not send disconnect leaves and preserve the other owner")
	}
	peer.handleBinaryLog(&pb.BinaryLog{Node: node.Name, Version: 10, Payload: &pb.BinaryLog_Track{Track: presence}})
	peer.onMergeRemoteState(marshalSnapshotTestState(t, &pb.State{Node: node.Name, Nodes: []*pb.StateNode{{Node: node.Name, Version: 11, Presences: []*pb.Presence{presence}}}}), false)
	if tracker.Count() != 1 {
		t.Fatal("late replication restored a departed node's presence")
	}
}

func TestPresenceUpdateTrackLogAppliesMetadataBeforeAdvancingVersion(t *testing.T) {
	peer := newSnapshotTestPeer(t, "host")
	tracker := newPresenceRecoveryTracker("host")
	peer.tracker = tracker
	peer.binaryLogBroadcastChan = make(chan *binaryLogMsg, 1)
	tracker.SetPeer(peer)
	stream := PresenceStream{Mode: StreamModeParty, Subject: uuid.Must(uuid.NewV4()), Label: "host"}
	presence := newSnapshotTestPresence("source")
	presence.Stream = []*pb.PresenceStream{presenceStream2PB(stream)}
	presence.Meta = []*pb.PresenceMeta{{Username: "before"}}
	if !tracker.MergeRemoteState("source", []*pb.Presence{presence}) {
		t.Fatal("could not seed presence")
	}
	tracker.processMembershipEvents()
	<-tracker.eventsCh
	tracker.SetPartyJoinListener(func(uuid.UUID, []*Presence) { t.Error("metadata change called the host join listener") })
	tracker.SetPartyLeaveListener(func(uuid.UUID, []*Presence) { t.Error("metadata change called the host leave listener") })
	presence.Node = "" // Older senders omit this field for updates.
	presence.Meta[0].Username = "after"
	presence.Meta[0].Status = "ready"
	update := &pb.BinaryLog{Node: "source", Version: 2, Payload: &pb.BinaryLog_UpdateTrack{UpdateTrack: presence}}
	peer.handleBinaryLog(update)
	tracker.processMembershipEvents()
	if got := tracker.ListByStream(stream, true, true); len(got) != 1 || got[0].Meta.Username != "after" || got[0].Meta.Status != "ready" || got[0].ID.Node != "source" {
		t.Fatalf("UpdateTrack did not apply remote metadata: %v", got)
	}
	select {
	case e := <-tracker.eventsCh:
		if len(e.Joins) != 1 || len(e.Leaves) != 1 || e.Leaves[0].Meta.Username != "before" {
			t.Fatal("client metadata update did not retain the old and new presences")
		}
	default:
		t.Fatal("missing client metadata update")
	}

	update.Version++
	presence.Meta = nil
	peer.handleBinaryLog(update)
	if got := peer.binaryLog.GetVersionByNode("source", peerLogPresenceMatchmaker); got != 2 || len(peer.binaryLogBroadcastChan) != 0 {
		t.Fatalf("invalid update advanced the version or rebroadcast the log: version=%d", got)
	}
	var joins, leaves int
	tracker.SetPartyJoinListener(func(_ uuid.UUID, members []*Presence) { joins += len(members) })
	tracker.SetPartyLeaveListener(func(_ uuid.UUID, members []*Presence) { leaves += len(members) })
	presence.Meta = []*pb.PresenceMeta{{Hidden: true}}
	peer.handleBinaryLog(update) // The valid retry can reuse the rejected version.
	tracker.processMembershipEvents()
	if joins != 0 || leaves != 1 || tracker.Count() != 1 {
		t.Fatal("hiding a presence did not remove only its visible membership")
	}
	update.Version++
	presence.Meta[0].Hidden = false
	peer.handleBinaryLog(update)
	tracker.processMembershipEvents()
	if joins != 1 || leaves != 1 || tracker.Count() != 1 {
		t.Fatal("restoring visibility did not emit exactly one join")
	}
}

func newPresenceRecoveryTracker(node string) *LocalTracker {
	return &LocalTracker{
		name: node, logger: zap.NewNop(), sessionRegistry: &testSessionRegistry{}, ctx: context.Background(),
		count: atomic.NewInt64(0), eventsCh: make(chan *PresenceEvent, 1), membershipReady: make(chan struct{}, 1),
		presencesByStream:  make(map[uint8]map[PresenceStream]map[presenceCompact]*Presence),
		presencesBySession: make(map[uuid.UUID]map[presenceCompact]*Presence),
	}
}

type presenceRecoveryMatchCore struct {
	RuntimeMatchCore
	leaves []*MatchPresence
}

func (m *presenceRecoveryMatchCore) MatchLeave(_ int64, state any, leaves []*MatchPresence) (any, error) {
	m.leaves = append(m.leaves, leaves...)
	return state, nil
}
