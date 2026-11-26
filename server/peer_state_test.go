package server

import (
	"context"
	"testing"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestPeerSnapshotRepairsMissingDeltasAtSameVersion(t *testing.T) {
	owner := newSnapshotTestPeer(t, "owner")
	receiver := newSnapshotTestPeer(t, "receiver")
	version := owner.binaryLog.GetVersion()
	first, second := newSnapshotTestPresence("owner"), newSnapshotTestPresence("owner")
	owner.tracker.MergeRemoteState("owner", []*pb.Presence{first, second})
	ticket, _, err := owner.matchmaker.Add(owner.ctx, []*MatchmakerPresence{{
		SessionId: first.SessionID, UserId: first.UserID, Node: "owner",
	}}, first.SessionID, "", "*", 2, 2, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	receiver.handleBinaryLog(&pb.BinaryLog{
		Node: "owner", Version: version, Payload: &pb.BinaryLog_Track{Track: second},
	})
	receiver.handleBinaryLog(&pb.BinaryLog{
		Node: "owner", Version: version - 1, Payload: &pb.BinaryLog_Track{Track: first},
	})
	if count := receiver.tracker.Count(); count != 1 {
		t.Fatalf("before reconciliation: got %d presences, want 1", count)
	}
	receiver.onMergeRemoteState(owner.LocalState(false), false)
	if count := receiver.tracker.Count(); count != 2 {
		t.Fatalf("same-version snapshot: got %d presences, want 2", count)
	}

	stale := &pb.State{Node: "owner", Nodes: []*pb.StateNode{{Node: "owner", Version: version - 1}}}
	receiver.onMergeRemoteState(marshalSnapshotTestState(t, stale), false)
	if count := receiver.tracker.Count(); count != 2 {
		t.Fatalf("stale snapshot overwrote current state: got %d presences", count)
	}
	if err := receiver.matchmaker.RemoveSession(first.SessionID, ticket); err != nil {
		t.Fatalf("snapshot did not restore the owner's matchmaker ticket: %v", err)
	}
	receiver.handleBinaryLog(&pb.BinaryLog{
		Node: "owner", Version: version + 1,
		Payload: &pb.BinaryLog_MatchmakerAdd{MatchmakerAdd: matchmakerExtract2pb(owner.matchmaker.Extract()[0])},
	})
	if tickets := receiver.matchmaker.Extract(); len(tickets) != 0 {
		t.Fatalf("receiver treats a replicated ticket as its own: %v", tickets)
	}
	if err := receiver.matchmaker.RemoveSession(first.SessionID, ticket); err != nil {
		t.Fatalf("replicated delta did not preserve the ticket ID: %v", err)
	}

	// A snapshot does not acknowledge deltas for data it cannot reconstruct.
	receiver.handleBinaryLog(&pb.BinaryLog{
		Node: "owner", Version: version - 1,
		Payload: &pb.BinaryLog_MatchmakerRemoveAll{MatchmakerRemoveAll: &pb.MatchmakerExtract{Node: "other"}},
	})
	if got := receiver.binaryLog.GetVersionByNode("owner", peerLogOther); got != version-1 {
		t.Fatalf("snapshot suppressed a delta outside its scope: version %d", got)
	}
}

func TestPeerSnapshotIgnoresRelayedStateAndLegacyPartyPages(t *testing.T) {
	owner := newSnapshotTestPeer(t, "owner")
	relay := newSnapshotTestPeer(t, "relay")
	receiver := newSnapshotTestPeer(t, "receiver")
	presence := newSnapshotTestPresence("owner")
	owner.tracker.MergeRemoteState("owner", []*pb.Presence{presence})
	relay.tracker.MergeRemoteState("owner", []*pb.Presence{presence})
	ticket, _, err := owner.matchmaker.Add(owner.ctx, []*MatchmakerPresence{{
		SessionId: presence.SessionID, UserId: presence.UserID, Node: "owner",
	}}, presence.SessionID, "", "*", 2, 2, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	id := uuid.Must(uuid.NewV4())
	if err := owner.partyRegistry.LabelUpdate(id, "owner", `{"mode":"ranked"}`, true, false, 4, time.Now()); err != nil {
		t.Fatal(err)
	}
	foreignID := uuid.Must(uuid.NewV4()).String() + ".relay"
	owner.partyRegistry.(*LocalPartyRegistry).handleFromRemotePartyCreate(&PartyIndexEntry{Id: foreignID, Node: "relay"})

	receiver.onMergeRemoteState(owner.LocalState(true), true)
	version := owner.binaryLog.GetVersion() + 1
	legacy := &pb.State{Node: "relay", Nodes: []*pb.StateNode{
		{Node: "relay", Version: version},
		{Node: "owner", Version: version},
	}}
	receiver.onMergeRemoteState(marshalSnapshotTestState(t, legacy), false)
	if count := receiver.tracker.Count(); count != 1 {
		t.Fatalf("relayed snapshot cleared the owner's presence: got %d", count)
	}
	if err := receiver.matchmaker.RemoveSession(presence.SessionID, ticket); err != nil {
		t.Fatalf("relayed snapshot cleared the owner's ticket: %v", err)
	}

	legacy.Node = "owner"
	receiver.onMergeRemoteState(marshalSnapshotTestState(t, legacy), false)

	entries, err := receiver.partyRegistry.Extract(receiver.ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Id != id.String()+".owner" || !entries[0].Open || entries[0].LabelString != `{"mode":"ranked"}` {
		t.Fatalf("unexpected party index after legacy snapshots: %+v", entries)
	}
	var state pb.State
	if err := proto.Unmarshal(relay.LocalState(true), &state); err != nil {
		t.Fatal(err)
	}
	if len(state.GetNodes()) != 1 || len(state.GetNodes()[0].GetPresences()) != 0 || !peerStateIsComplete(&state) {
		t.Fatalf("relay advertised another node's state: %v", &state)
	}
}

func TestPeerSnapshotReplacesPartyIndexWithCompleteEmptyState(t *testing.T) {
	owner := newSnapshotTestPeer(t, "owner")
	receiver := newSnapshotTestPeer(t, "receiver")
	version := owner.binaryLog.GetVersion()
	id := uuid.Must(uuid.NewV4())
	if err := owner.partyRegistry.LabelUpdate(id, "owner", "{}", true, false, 4, time.Now()); err != nil {
		t.Fatal(err)
	}
	owner.partyRegistry.(*LocalPartyRegistry).processUpdates(bluge.NewBatch())
	receiver.onMergeRemoteState(owner.LocalState(false), false)
	entry := &PartyIndexEntry{Id: id.String() + ".owner", Node: "owner", Open: true, LabelString: "{}"}
	owner.partyRegistry.Delete(id)
	owner.PartyClose(entry.Id) // The test peer has no outbound queue, so this delta is dropped.
	if owner.binaryLog.GetVersion() <= version {
		t.Fatal("dropped delta did not advance the owner's snapshot version")
	}
	receiver.handleBinaryLog(&pb.BinaryLog{
		Node: "owner", Version: owner.binaryLog.GetVersion(), Payload: &pb.BinaryLog_PartyCreate{PartyCreate: partyIndex2pb(entry)},
	})

	receiver.onMergeRemoteState(owner.LocalState(false), false)
	receiver.partyRegistry.(*LocalPartyRegistry).processUpdates(bluge.NewBatch())
	entries, err := receiver.partyRegistry.Extract(receiver.ctx, 0, 10)
	if err != nil || len(entries) != 0 {
		t.Fatalf("empty snapshot did not clear the index and pending update: %v, %v", entries, err)
	}
}

func newSnapshotTestPeer(t *testing.T, node string) *LocalPeer {
	t.Helper()
	logger := zap.NewNop()
	ctx, cancel := context.WithCancel(context.Background())
	config := NewConfig(logger)
	config.Name = node
	config.Party.LabelUpdateIntervalMs = 600000
	tracker := StartLocalTracker(logger, config, &testSessionRegistry{}, nil, metrics, nil)
	matchmaker := NewLocalBenchMatchmaker(logger, logger, config, nil, metrics, &Runtime{}, false).(*LocalMatchmaker)
	parties := NewLocalPartyRegistry(ctx, logger, logger, config, node, metrics).(*LocalPartyRegistry)
	t.Cleanup(func() {
		cancel()
		tracker.Stop()
		matchmaker.Stop()
		_ = matchmaker.indexWriter.Close()
		_ = parties.indexWriter.Close()
	})
	return &LocalPeer{
		ctx: ctx, logger: logger, endpoint: NewPeerEndpont(node, nil, 0, 0, 0, false, nil),
		tracker: tracker, matchmaker: matchmaker, partyRegistry: parties, binaryLog: NewLocalBinaryLog(logger, node),
	}
}

func newSnapshotTestPresence(node string) *pb.Presence {
	return &pb.Presence{
		Node: node, SessionID: uuid.Must(uuid.NewV4()).String(), UserID: uuid.Must(uuid.NewV4()).String(),
		Stream: []*pb.PresenceStream{{Mode: uint32(StreamModeNotifications)}}, Meta: []*pb.PresenceMeta{{Hidden: true}},
	}
}

func marshalSnapshotTestState(t *testing.T, state *pb.State) []byte {
	t.Helper()
	data, err := proto.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
