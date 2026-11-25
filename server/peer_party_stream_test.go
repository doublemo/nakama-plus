package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-common/runtime"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/doublemo/nakama-plus/v3/internal/worker"
	"github.com/gofrs/uuid/v5"
	"github.com/hashicorp/memberlist"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestPartyCrossNodeAcceptAndRemove(t *testing.T) {
	host, targetPeer := newSnapshotTestPeer(t, "host"), newSnapshotTestPeer(t, "session-node")
	session := newPartyStreamTestSession("session-node")
	sessions := &partyStreamTestSessions{session: session}
	targetPeer.sessionRegistry = sessions
	targetPeer.tracker.(*LocalTracker).sessionRegistry = sessions
	config := NewConfig(targetPeer.logger)
	config.Name = "session-node"
	targetPeer.streamManager = NewLocalStreamManager(config, sessions, targetPeer.tracker)
	targetPeer.tracker.SetPeer(&partyStreamTestPeer{})

	remote := &partyStreamTestPeer{endpoint: targetPeer.endpoint}
	var joined, removed int
	router := &testMessageRouter{sendToPresence: func(_ []*PresenceID, envelope *rtapi.Envelope) {
		if envelope.GetParty() != nil {
			joined++
		}
		if envelope.GetPartyClose() != nil {
			removed++
		}
	}}
	leader := &rtapi.UserPresence{UserId: uuid.Must(uuid.NewV4()).String(), SessionId: uuid.Must(uuid.NewV4()).String(), Username: "leader"}
	party := NewPartyHandler(host.logger, host.partyRegistry, host.matchmaker, host.tracker, nil, router,
		uuid.Must(uuid.NewV4()), "host", false, 2, leader, remote)
	defer party.ctxCancelFn()
	member := &Presence{ID: PresenceID{Node: "session-node", SessionID: session.ID()}, UserID: session.UserID(), Stream: party.Stream, Meta: PresenceMeta{Username: session.Username()}}
	memberWire := &rtapi.UserPresence{UserId: member.GetUserId(), SessionId: member.GetSessionId(), Username: member.GetUsername()}
	if _, err := party.JoinRequest(member); err != nil {
		t.Fatal(err)
	}
	if err := party.Accept(leader.SessionId, "host", memberWire, true); err != runtime.ErrPartyAcceptRequest || remote.requests != 0 || len(party.joinRequests) != 1 || party.members.Size() != 1 {
		t.Fatalf("legacy target mutated the pending request or reserved a seat: %v", err)
	}
	meta, err := targetPeer.endpoint.MarshalProtoBuffer()
	if err != nil {
		t.Fatal(err)
	}
	var nodeMeta pb.NodeMeta
	if err := proto.Unmarshal(meta, &nodeMeta); err != nil {
		t.Fatal(err)
	}
	targetPeer.endpoint.ResetMetadata(nodeMeta.GetVars())
	oldStream := PresenceStream{Mode: StreamModeParty, Subject: uuid.Must(uuid.NewV4()), Label: "other-host"}
	if ok, _, err := targetPeer.streamManager.UserJoin(oldStream, session.UserID(), session.ID(), false, false, ""); !ok || err != nil {
		t.Fatalf("failed to set up previous party: %v", err)
	}
	loseJoinReply, failLeave := true, false
	remote.request = func(envelope *pb.Peer_Envelope) (*pb.Peer_Envelope, error) {
		fields := envelope.GetContext()
		operation := fields[partyStreamCapability]
		if operation == "leave" && failLeave {
			return nil, status.Error(codes.Unavailable, "session node unavailable")
		}
		if err := targetPeer.applyPartyStreamChange("host", fields); err != nil {
			return nil, err
		}
		if operation == "join" {
			party.Join([]*Presence{member}) // Presence gossip can arrive before the RPC reply.
			if loseJoinReply {
				return nil, status.Error(codes.DeadlineExceeded, "reply lost")
			}
		}
		return &pb.Peer_Envelope{Context: map[string]string{partyStreamCapability: "ok"}}, nil
	}
	if err := party.Accept(leader.SessionId, "host", memberWire, true); err != runtime.ErrPartyAcceptRequest || len(party.joinRequests) != 1 || party.members.Size() != 1 || joined != 0 {
		t.Fatalf("lost reply corrupted the approval state: %v", err)
	}
	loseJoinReply = false
	if err := party.Accept(leader.SessionId, "host", memberWire, true); err != nil {
		t.Fatal(err)
	}
	party.Join([]*Presence{member})
	if len(party.joinRequests) != 0 || !party.members.Contains(member.ID) || joined != 1 || targetPeer.tracker.CountByStream(oldStream) != 0 || targetPeer.tracker.CountByStream(party.Stream) != 1 {
		t.Fatal("accept did not confirm exactly one membership and clear the old party on the session node")
	}
	if !host.tracker.TrackMulti(host.ctx, member.ID.SessionID, []*TrackerOp{{Stream: party.Stream, Meta: PresenceMeta{Hidden: true}}}, member.UserID, member.ID.Node) {
		t.Fatal("failed to install replica on the party host")
	}
	failLeave = true
	if err := party.Remove(leader.SessionId, "host", memberWire); err != runtime.ErrPartyRemove || !party.members.Contains(member.ID) || targetPeer.tracker.CountByStream(party.Stream) != 1 || removed != 0 {
		t.Fatalf("failed kick changed party or stream membership: %v", err)
	}
	failLeave = false
	if err := party.Remove(leader.SessionId, "host", memberWire); err != nil {
		t.Fatal(err)
	}
	if party.members.Contains(member.ID) || targetPeer.tracker.CountByStream(party.Stream) != 0 || host.tracker.CountByStream(party.Stream) != 0 || removed != 1 {
		t.Fatal("kick did not remove the member, their stream presence and the host replica exactly once")
	}
	if _, err := party.JoinRequest(member); err != nil {
		t.Fatal(err)
	}
	loseJoinReply = true
	if err := party.Accept(leader.SessionId, "host", memberWire, true); err != runtime.ErrPartyAcceptRequest {
		t.Fatalf("accept with a lost reply returned %v", err)
	}
	if err := party.Remove(leader.SessionId, "host", memberWire); err != nil {
		t.Fatal(err)
	}
	if len(party.joinRequests) != 0 || targetPeer.tracker.CountByStream(party.Stream) != 0 || joined != 1 || removed != 1 {
		t.Fatal("reject did not undo the previously unacknowledged join")
	}
	if err := party.Update(leader.SessionId, "host", "{}", true, false); err != nil {
		t.Fatal(err)
	}
	party.Join([]*Presence{member})
	if party.members.Contains(member.ID) || joined != 1 {
		t.Fatal("delayed tracking event restored a removed member after opening the party")
	}
	if autoJoin, err := party.JoinRequest(member); !autoJoin || err != nil {
		t.Fatalf("open party did not admit a new request: %v", err)
	}
	party.Join([]*Presence{member})
	if !party.members.Contains(member.ID) || joined != 2 {
		t.Fatal("open party did not confirm an admitted member")
	}
}

func TestPartyLegacyHostAllowsRejectButProtectsRemoteMember(t *testing.T) {
	caller := newSnapshotTestPeer(t, "caller")
	legacy := &partyStreamTestPeer{endpoint: NewPeerEndpont("legacy", nil, 0, 0, 0, false, nil)}
	caller.partyRegistry.Init(caller.matchmaker, caller.tracker, nil, &testMessageRouter{}, legacy)
	member := newPartyStreamTestSession("session-node")
	presence := &rtapi.UserPresence{UserId: member.UserID().String(), SessionId: member.ID().String(), Username: member.Username()}
	partyID := uuid.Must(uuid.NewV4())
	if err := caller.partyRegistry.PartyAccept(caller.ctx, partyID, "legacy", "leader", "caller", presence); err != runtime.ErrPartyAcceptRequest || legacy.requests != 0 {
		t.Fatalf("sent a cross-node approval to an old host: %v", err)
	}
	pending := false
	legacy.request = func(envelope *pb.Peer_Envelope) (*pb.Peer_Envelope, error) {
		if envelope.GetPartyJoinRequestList() != nil {
			reply := &pb.Party_JoinRequestListReply{}
			if pending {
				reply.UserPresence = []*rtapi.UserPresence{presence}
			}
			return &pb.Peer_Envelope{Payload: &pb.Peer_Envelope_PartyJoinRequestListReply{PartyJoinRequestListReply: reply}}, nil
		}
		if envelope.GetPartyRemove() == nil {
			t.Fatalf("unexpected legacy request: %v", envelope)
		}
		return &pb.Peer_Envelope{}, nil
	}
	if err := caller.partyRegistry.PartyRemove(caller.ctx, partyID, "legacy", "leader", "caller", presence); err != runtime.ErrPartyRemove || legacy.requests != 1 {
		t.Fatalf("sent a kick to an old host: %v", err)
	}
	pending = true
	if err := caller.partyRegistry.PartyRemove(caller.ctx, partyID, "legacy", "leader", "caller", presence); err != nil || legacy.requests != 3 {
		t.Fatalf("failed to reject a pending request on an old host: %v", err)
	}
	localMember := newPartyStreamTestSession("legacy")
	presence.SessionId = localMember.ID().String()
	legacy.request = func(envelope *pb.Peer_Envelope) (*pb.Peer_Envelope, error) {
		if envelope.GetPartyAccept() == nil {
			t.Fatal("same-node approval must use the original request")
		}
		return &pb.Peer_Envelope{}, nil
	}
	if err := caller.partyRegistry.PartyAccept(caller.ctx, partyID, "legacy", "leader", "caller", presence); err != nil || legacy.requests != 4 {
		t.Fatalf("same-node approval was blocked on an old host: %v", err)
	}
}

func TestPartyNestedClusterRequests(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback listener unavailable: %v", err)
	}
	_ = listener.Close()
	member := newPartyStreamTestSession("target")
	host := newPartyNetworkTestPeer(t, "host", &testSessionRegistry{})
	target := newPartyNetworkTestPeer(t, "target", &partyStreamTestSessions{session: member})
	if _, err := target.Join(host.Local().MemberlistNode().Address()); err != nil {
		t.Fatal(err)
	}
	creator := &rtapi.UserPresence{UserId: uuid.Must(uuid.NewV4()).String(), SessionId: uuid.Must(uuid.NewV4()).String(), Username: "creator"}
	party, err := host.partyRegistry.Create(false, false, 3, creator, "{}")
	if err != nil {
		t.Fatal(err)
	}
	defer party.ctxCancelFn()
	leader := newPartyStreamTestSession("target")
	party.Lock()
	party.leader = &PartyLeader{
		PresenceID:   &PresenceID{Node: "target", SessionID: leader.ID()},
		UserPresence: &rtapi.UserPresence{UserId: leader.UserID().String(), SessionId: leader.ID().String(), Username: "leader"},
	}
	party.Unlock()
	if _, err := party.JoinRequest(&Presence{
		ID: PresenceID{Node: "target", SessionID: member.ID()}, UserID: member.UserID(), Stream: party.Stream, Meta: PresenceMeta{Username: member.Username()},
	}); err != nil {
		t.Fatal(err)
	}
	presence := &rtapi.UserPresence{UserId: member.UserID().String(), SessionId: member.ID().String(), Username: member.Username()}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := target.partyRegistry.PartyAccept(ctx, party.ID, "host", leader.ID().String(), "target", presence); err != nil {
		t.Fatal(err)
	}
	if target.tracker.CountByStream(party.Stream) != 1 {
		t.Fatal("nested cluster request did not track the session on its node")
	}
	if err := target.partyRegistry.PartyRemove(ctx, party.ID, "host", leader.ID().String(), "target", presence); err != nil {
		t.Fatal(err)
	}
	if target.tracker.CountByStream(party.Stream) != 0 {
		t.Fatal("nested cluster request did not remove the session on its node")
	}
}

func newPartyNetworkTestPeer(t *testing.T, name string, sessions SessionRegistry) *LocalPeer {
	t.Helper()
	peer := newSnapshotTestPeer(t, name)
	ctx, cancel := context.WithCancel(peer.ctx)
	peer.ctx = ctx
	t.Cleanup(cancel)
	config := NewConfig(peer.logger)
	config.Name = name
	config.GetCluster().Addr = "127.0.0.1"
	config.GetCluster().Port = 0
	peer.sessionRegistry = sessions
	peer.tracker.(*LocalTracker).sessionRegistry = sessions
	peer.streamManager = NewLocalStreamManager(config, sessions, peer.tracker)
	peer.members = &MapOf[string, Endpoint]{}
	peer.msgChan = make(chan *peerMsg, 32)
	peer.binaryLogBroadcastChan = make(chan *binaryLogMsg, 16)
	peer.inbox = NewPeerInbox()
	var err error
	for range 5 {
		cfg := toMemberlistConfig(peer, name, config.GetCluster())
		cfg.GossipInterval, cfg.ProbeInterval, cfg.PushPullInterval = 0, 0, 0
		peer.memberlist, err = memberlist.Create(cfg)
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("failed to bind loopback for %s: %v", name, err)
	}
	peer.endpoint.BindMemberlistNode(peer.memberlist.LocalNode())
	peer.transmitLimitedQueue = &memberlist.TransmitLimitedQueue{NumNodes: peer.memberlist.NumMembers, RetransmitMult: 2}
	peer.wk = worker.New(8)
	router := &testMessageRouter{}
	peer.partyRegistry.Init(peer.matchmaker, peer.tracker, peer.streamManager, router, peer)
	peer.tracker.SetPeer(peer)
	peer.tracker.SetPartyJoinListener(peer.partyRegistry.Join)
	peer.tracker.SetPartyLeaveListener(peer.partyRegistry.Leave)
	go peer.processIncoming()
	go peer.processBinaryLogBroadcast()
	t.Cleanup(func() {
		_ = peer.memberlist.Shutdown()
		cancel()
		peer.wk.StopWait()
	})
	return peer
}

func TestPartyLabelUpdatesAndCloseReplicateToQueries(t *testing.T) {
	owner, receiver := newSnapshotTestPeer(t, "owner"), newSnapshotTestPeer(t, "receiver")
	owner.binaryLogBroadcastChan = make(chan *binaryLogMsg, 8)
	owner.tracker.SetPeer(owner)
	owner.partyRegistry.Init(owner.matchmaker, owner.tracker, nil, &testMessageRouter{}, owner)
	receiver.partyRegistry.Init(receiver.matchmaker, receiver.tracker, nil, &testMessageRouter{}, nil)
	leader := &rtapi.UserPresence{UserId: uuid.Must(uuid.NewV4()).String(), SessionId: uuid.Must(uuid.NewV4()).String(), Username: "leader"}
	party, err := owner.partyRegistry.Create(true, false, 4, leader, "{}")
	if err != nil {
		t.Fatal(err)
	}
	transfer := func() {
		t.Helper()
		select {
		case event := <-owner.binaryLogBroadcastChan:
			receiver.handleBinaryLog(event.msg)
		default:
			t.Fatal("expected a party index delta")
		}
	}
	transfer()
	registry := receiver.partyRegistry.(*LocalPartyRegistry)
	registry.processUpdates(bluge.NewBatch())
	if err := party.Update(leader.SessionId, "owner", `{"mode":"ranked"}`, false, false); err != nil {
		t.Fatal(err)
	}
	transfer()
	registry.processUpdates(bluge.NewBatch())
	open := false
	parties, _, err := registry.PartyList(receiver.ctx, 10, &open, false, "label.mode:ranked", "")
	if err != nil || len(parties) != 1 || parties[0].PartyId != party.IDStr || parties[0].Open {
		t.Fatalf("remote query missed the label or open-state update: %v, %v", parties, err)
	}
	if err := party.Update(leader.SessionId, "owner", "{}", true, false); err != nil {
		t.Fatal(err)
	}
	transfer()
	if err := party.Close(leader.SessionId, "owner"); err != nil {
		t.Fatal(err)
	}
	transfer()
	registry.processUpdates(bluge.NewBatch())
	parties, _, err = registry.PartyList(receiver.ctx, 10, nil, true, "", "")
	if err != nil || len(parties) != 0 {
		t.Fatalf("close left an indexed or queued party: %v, %v", parties, err)
	}
}

type partyStreamTestPeer struct {
	Peer
	endpoint Endpoint
	request  func(*pb.Peer_Envelope) (*pb.Peer_Envelope, error)
	requests int
}

func (p *partyStreamTestPeer) Member(node string) (Endpoint, bool) {
	return p.endpoint, p.endpoint != nil && p.endpoint.Name() == node
}

func (p *partyStreamTestPeer) Request(_ context.Context, _ Endpoint, envelope *pb.Peer_Envelope) (*pb.Peer_Envelope, error) {
	p.requests++
	return p.request(envelope)
}

func (*partyStreamTestPeer) BinaryLogBroadcast(*pb.BinaryLog, bool) {}
func (*partyStreamTestPeer) MatchmakerRemovePartyAll(string)        {}

type partyStreamTestSession struct {
	Session
	id, userID uuid.UUID
}

func newPartyStreamTestSession(node string) *partyStreamTestSession {
	id := uuid.Must(uuid.NewV4())
	hash := NodeToHash(node)
	copy(id[10:], hash[:])
	return &partyStreamTestSession{id: id, userID: uuid.Must(uuid.NewV4())}
}

func (s *partyStreamTestSession) ID() uuid.UUID                  { return s.id }
func (s *partyStreamTestSession) UserID() uuid.UUID              { return s.userID }
func (*partyStreamTestSession) Username() string                 { return "member" }
func (*partyStreamTestSession) Context() context.Context         { return context.Background() }
func (*partyStreamTestSession) Format() SessionFormat            { return SessionFormatProtobuf }
func (*partyStreamTestSession) CloseLock()                       {}
func (*partyStreamTestSession) CloseUnlock()                     {}
func (*partyStreamTestSession) SendBytes(_ []byte, _ bool) error { return nil }

type partyStreamTestSessions struct {
	SessionRegistry
	session Session
}

func (*partyStreamTestSessions) Count() int { return 1 }

func (r *partyStreamTestSessions) Get(id uuid.UUID) Session {
	if r.session.ID() == id {
		return r.session
	}
	return nil
}
