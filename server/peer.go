package server

import (
	"context"
	"database/sql"
	"errors"
	"math"
	coreruntime "runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"maps"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-common/runtime"
	"github.com/doublemo/nakama-kit/kit"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/doublemo/nakama-plus/v3/internal/worker"
	"github.com/gofrs/uuid/v5"
	"github.com/hashicorp/memberlist"
	uberatomic "go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	PeerMsg_NOTIFY = iota
	PeerMsg_REMOTESTATE
	PeerMsg_REMOTESTATEJOIN
)

type (
	Peer interface {
		memberlist.Delegate
		memberlist.EventDelegate
		memberlist.AliveDelegate
		memberlist.ConflictDelegate
		memberlist.MergeDelegate
		memberlist.PingDelegate
		Shutdown()
		Join(members ...string) (int, error)
		Local() Endpoint
		NumMembers() int
		Member(name string) (Endpoint, bool)
		Members() []Endpoint
		Broadcast(msg *pb.Peer_Envelope, reliable bool)
		BinaryLogBroadcast(b *pb.BinaryLog, toQueue bool)
		RefreshVersion()
		Send(endpoint Endpoint, msg *pb.Peer_Envelope, reliable bool) error
		Request(ctx context.Context, endpoint Endpoint, msg *pb.Peer_Envelope) (*pb.Peer_Envelope, error)
		GetServiceRegistry() kit.ServiceRegistry
		MatchmakerAdd(extract *pb.MatchmakerExtract)
		MatchmakerRemoveSession(sessionID, ticket string)
		MatchmakerRemoveSessionAll(sessionID string)
		MatchmakerRemoveParty(partyID, ticket string)
		MatchmakerRemovePartyAll(partyID string)
		MatchmakerRemoveAll(node string)
		MatchmakerRemove(tickets []string)
		ToClient(envelope *rtapi.Envelope, recipients []*pb.Recipienter)
		InvokeMS(ctx context.Context, in *api.AnyRequest) (*api.AnyResponseWriter, error)
		SendMS(ctx context.Context, in *api.AnyRequest) error
		Event(ctx context.Context, in *api.AnyRequest, names ...string) error
		Leader() bool
		AllowLeader() bool
	}

	peerMsg struct {
		msgType int
		data    []byte
	}

	LocalPeer struct {
		ctx                  context.Context
		ctxCancelFn          context.CancelFunc
		logger               *zap.Logger
		config               *PeerConfig
		runtimeConfig        *RuntimeConfig
		memberlist           *memberlist.Memberlist
		transmitLimitedQueue *memberlist.TransmitLimitedQueue
		members              *MapOf[string, Endpoint]
		endpoint             Endpoint
		serviceRegistry      kit.ServiceRegistry
		etcdClient           *kit.EtcdClientV3
		leader               *PeerLeader
		runtime              *Runtime
		metrics              Metrics
		sessionRegistry      SessionRegistry
		messageRouter        MessageRouter
		tracker              Tracker
		matchRegistry        MatchRegistry
		matchmaker           Matchmaker
		partyRegistry        PartyRegistry
		binaryLog            BinaryLog
		inbox                *PeerInbox
		msgChan              chan *peerMsg
		wk                   *worker.WorkerPool
		protojsonMarshaler   *protojson.MarshalOptions
		protojsonUnmarshaler *protojson.UnmarshalOptions
		db                   *sql.DB
		partyIndexOffset     *uberatomic.Int64

		once sync.Once
		sync.Mutex
	}
)

func NewLocalPeer(db *sql.DB, logger *zap.Logger, name string, metadata map[string]string, runtime *Runtime, metrics Metrics, sessionRegistry SessionRegistry, tracker Tracker, messageRouter MessageRouter, matchRegistry MatchRegistry, matchmaker Matchmaker, partyRegistry PartyRegistry, protojsonMarshaler *protojson.MarshalOptions, protojsonUnmarshaler *protojson.UnmarshalOptions, config Config) Peer {
	ctx, ctxCancelFn := context.WithCancel(context.Background())
	if metadata == nil {
		metadata = make(map[string]string)
	}
	c := config.GetCluster()
	endpoint := NewPeerEndpont(name, metadata, int32(pb.NodeMeta_OK), c.Weight, c.Balancer, false, protojsonMarshaler)
	s := &LocalPeer{
		ctx:                  ctx,
		ctxCancelFn:          ctxCancelFn,
		config:               c,
		runtimeConfig:        config.GetRuntime(),
		logger:               logger,
		members:              &MapOf[string, Endpoint]{},
		endpoint:             endpoint,
		serviceRegistry:      kit.NewLocalServiceRegistry(ctx, logger, name),
		metrics:              metrics,
		sessionRegistry:      sessionRegistry,
		matchRegistry:        matchRegistry,
		matchmaker:           matchmaker,
		partyRegistry:        partyRegistry,
		tracker:              tracker,
		inbox:                NewPeerInbox(),
		msgChan:              make(chan *peerMsg, c.BroadcastQueueSize),
		wk:                   worker.New(128),
		messageRouter:        messageRouter,
		protojsonMarshaler:   protojsonMarshaler,
		protojsonUnmarshaler: protojsonUnmarshaler,
		db:                   db,
		runtime:              runtime,
		partyIndexOffset:     uberatomic.NewInt64(0),
	}

	cfg := toMemberlistConfig(s, name, c)
	m, err := memberlist.Create(cfg)
	if err != nil {
		logger.Fatal("Failed to create memberlist", zap.Error(err))
	}

	s.endpoint.BindMemberlistNode(m.LocalNode())
	s.memberlist = m
	s.binaryLog = NewLocalBinaryLog(logger, name)

	s.transmitLimitedQueue = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return m.NumMembers()
		},
		RetransmitMult: c.RetransmitMult,
	}

	if c.Etcd != nil && len(c.Etcd.Endpoints) > 0 {
		s.etcdClient = kit.NewEtcdClientV3(context.Background(), logger, c.Etcd)
	}

	// Process incoming
	go s.processIncoming()
	return s
}

// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (s *LocalPeer) NodeMeta(limit int) []byte {
	bytes, err := s.endpoint.MarshalProtoBuffer()
	if err != nil {
		s.logger.Fatal("failed to marshal NodeMeta ", zap.Error(err))
	}
	return bytes
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
func (s *LocalPeer) NotifyMsg(msg []byte) {
	//s.metrics.PeerRecv(int64(len(msg)))

	select {
	case s.msgChan <- &peerMsg{msgType: PeerMsg_NOTIFY, data: msg}:
	default:
		s.logger.Warn("msg incoming queue full")
	}
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
func (s *LocalPeer) GetBroadcasts(overhead, limit int) [][]byte {
	broadcasts := s.transmitLimitedQueue.GetBroadcasts(overhead, limit)
	size := len(broadcasts)
	if size < 1 {
		return nil
	}

	sorted := make([][]byte, size)
	for i := 0; i < size; i++ {
		sorted[i] = broadcasts[size-(i+1)]
	}
	return sorted
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. ALogger
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (s *LocalPeer) LocalState(join bool) []byte {
	state := &pb.State{
		Node:  s.endpoint.Name(),
		Nodes: make([]*pb.StateNode, 0),
	}

	nodesMap := make(map[string]int)
	state.Nodes = append(state.Nodes, &pb.StateNode{
		Node:            s.endpoint.Name(),
		Version:         s.binaryLog.GetVersion(),
		Presences:       make([]*pb.Presence, 0),
		Matchmaker:      make([]*pb.MatchmakerExtract, 0),
		PartyIndexEntry: make([]*pb.Party_IndexEntry, 0),
	})
	nodesMap[s.endpoint.Name()] = 0
	presencesMap := make(map[uuid.UUID]int)
	s.tracker.Range(func(sessionID uuid.UUID, presences []*Presence) bool {
		for _, presence := range presences {
			presenceStream := &pb.PresenceStream{
				Mode:       uint32(presence.Stream.Mode),
				Subject:    presence.Stream.Subject.String(),
				Subcontext: presence.Stream.Subcontext.String(),
				Label:      presence.Stream.Label,
			}

			presenceMeta := &pb.PresenceMeta{
				SessionFormat: uint32(presence.Meta.Format),
				Hidden:        presence.Meta.Hidden,
				Persistence:   presence.Meta.Persistence,
				Username:      presence.Meta.Username,
				Status:        presence.Meta.Status,
				Reason:        atomic.LoadUint32(&presence.Meta.Reason),
			}

			nodeIdx, ok := nodesMap[presence.GetNodeId()]
			if !ok {
				state.Nodes = append(state.Nodes, &pb.StateNode{
					Node:       presence.GetNodeId(),
					Version:    s.binaryLog.GetVersionByNode(presence.GetNodeId()),
					Presences:  make([]*pb.Presence, 0),
					Matchmaker: make([]*pb.MatchmakerExtract, 0),
				})
				nodeIdx = len(state.Nodes) - 1
				nodesMap[presence.GetNodeId()] = nodeIdx
			}

			idx, ok := presencesMap[presence.ID.SessionID]
			if ok {
				state.Nodes[nodeIdx].Presences[idx].Stream = append(state.Nodes[nodeIdx].Presences[idx].Stream, presenceStream)
				state.Nodes[nodeIdx].Presences[idx].Meta = append(state.Nodes[nodeIdx].Presences[idx].Meta, presenceMeta)
				continue
			}

			p := &pb.Presence{
				SessionID: presence.GetSessionId(),
				UserID:    presence.GetUserId(),
				Stream:    []*pb.PresenceStream{presenceStream},
				Meta:      []*pb.PresenceMeta{presenceMeta},
				Node:      presence.GetNodeId(),
			}
			state.Nodes[nodeIdx].Presences = append(state.Nodes[nodeIdx].Presences, p)
			presencesMap[presence.ID.SessionID] = len(state.Nodes[nodeIdx].Presences) - 1
		}
		return true
	})

	for _, extract := range s.matchmaker.Extract() {
		state.Nodes[0].Matchmaker = append(state.Nodes[0].Matchmaker, matchmakerExtract2pb(extract))
	}

	if partyIndexs, err := s.partyRegistry.Extract(s.ctx, int(s.partyIndexOffset.Load()), 10000); err != nil {
		s.logger.Warn("Failed to get party index", zap.Error(err))
		s.partyIndexOffset.Store(0)
	} else {
		for _, index := range partyIndexs {
			state.Nodes[0].PartyIndexEntry = append(state.Nodes[0].PartyIndexEntry, partyIndex2pb(index))
		}

		indexSize := len(partyIndexs)
		if indexSize == 0 {
			s.partyIndexOffset.Store(0)
		} else {
			s.partyIndexOffset.Store(int64(indexSize) + 10000)
		}
	}

	bytes, err := proto.Marshal(state)
	if err != nil {
		s.logger.Warn("Failed to marshal LocalState", zap.Error(err))
		return nil
	}
	return bytes
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (s *LocalPeer) MergeRemoteState(buf []byte, join bool) {
	//s.metrics.PeerRecv(int64(len(buf)))

	msgType := PeerMsg_REMOTESTATE
	if join {
		msgType = PeerMsg_REMOTESTATEJOIN
	}

	select {
	case s.msgChan <- &peerMsg{msgType: msgType, data: buf}:
	default:
		s.logger.Warn("msg incoming queue full")
	}
}

// AckPayload is invoked when an ack is being sent; the returned bytes will be appended to the ack
func (s *LocalPeer) AckPayload() []byte {
	status := &pb.Status{
		Name:           s.endpoint.Name(),
		Health:         0,
		SessionCount:   int32(s.sessionRegistry.Count()),
		PresenceCount:  int32(s.tracker.Count()),
		MatchCount:     0,
		GoroutineCount: int32(coreruntime.NumGoroutine()),
		AvgLatencyMs:   math.Floor(s.metrics.SnapshotLatencyMs()*100) / 100,
		AvgRateSec:     math.Floor(s.metrics.SnapshotRateSec()*100) / 100,
		AvgInputKbs:    math.Floor(s.metrics.SnapshotRecvKbSec()*100) / 100,
		AvgOutputKbs:   math.Floor(s.metrics.SnapshotSentKbSec()*100) / 100,
	}
	bytes, _ := proto.Marshal(status)
	return bytes
}

// NotifyPing is invoked when an ack for a ping is received
func (s *LocalPeer) NotifyPingComplete(other *memberlist.Node, rtt time.Duration, payload []byte) {
	endpoint, ok := s.members.Load(other.Name)
	if !ok || endpoint == nil || other.Name == s.endpoint.Name() {
		return
	}

	endpoint.PingRTT(rtt)
	if rtt.Milliseconds() > 500 {
		s.logger.Warn("ping too slow", zap.Duration("rrt", rtt), zap.String("name", other.Name), zap.String("address", other.Address()))
	}

	if size := len(payload); size < 1 {
		return
	}

	var status pb.Status
	if err := proto.Unmarshal(payload, &status); err != nil {
		s.logger.Error("Failed to unmarshal payload", zap.Error(err))
		return
	}
	endpoint.UpdateState(&status)
}

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (s *LocalPeer) NotifyJoin(node *memberlist.Node) {
	if node.Name == s.endpoint.Name() {
		return
	}

	var md pb.NodeMeta
	if err := proto.Unmarshal(node.Meta, &md); err != nil {
		s.logger.Warn("Failed to unmarshal meta", zap.Error(err), zap.String("name", node.Name))
		return
	}

	s.members.Store(node.Name, NewPeerEndpont(md.GetName(), md.GetVars(), int32(md.GetStatus()), md.GetWeight(), int32(md.GetBalancer()), md.GetLeader(), s.protojsonMarshaler, node))
	s.logger.Debug("NotifyJoin", zap.String("name", md.GetName()))
}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (s *LocalPeer) NotifyLeave(node *memberlist.Node) {
	if node.Name == s.endpoint.Name() {
		return
	}

	s.members.Delete(node.Name)
	if !s.wk.Stopped() {
		s.wk.Submit(func() {
			s.tracker.ClearTrackByNode(map[string]bool{node.Name: true})
			s.matchmaker.RemoveAll(map[string]bool{node.Name: true})
			s.partyRegistry.(*LocalPartyRegistry).deleteAllFromNodeOptimized(s.ctx, node.Name)
			s.logger.Debug("NotifyLeave", zap.String("name", node.Name))
			//s.metrics.GaugePeers(float64(s.NumMembers()))
		})
	}
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (s *LocalPeer) NotifyUpdate(node *memberlist.Node) {
	var md pb.NodeMeta
	if err := proto.Unmarshal(node.Meta, &md); err != nil {
		s.logger.Warn("Failed to unmarshal meta", zap.Error(err), zap.String("name", node.Name))
		return
	}

	s.members.Store(node.Name, NewPeerEndpont(md.GetName(), md.GetVars(), int32(md.GetStatus()), md.GetWeight(), int32(md.GetBalancer()), md.GetLeader(), s.protojsonMarshaler, node))
	s.logger.Debug("NotifyUpdate", zap.String("name", md.GetName()))
}

// NotifyAlive implements the memberlist.AliveDelegate interface.
func (s *LocalPeer) NotifyAlive(node *memberlist.Node) error {
	return nil
}

// NotifyConflict is invoked when a name conflict is detected
func (s *LocalPeer) NotifyConflict(existing, other *memberlist.Node) {
	if other.Name == s.endpoint.Name() {
		s.logger.Warn("NotifyConflict", zap.String("existing", existing.Name+"/"+existing.Address()), zap.String("other", other.Name+"/"+existing.Address()))
	}
}

// NotifyMerge is invoked when a merge could take place.
// Provides a list of the nodes known by the peer. If
// the return value is non-nil, the merge is canceled.
func (s *LocalPeer) NotifyMerge(peers []*memberlist.Node) error {
	return nil
}

func (s *LocalPeer) Shutdown() {
	s.once.Do(func() {
		if s.ctxCancelFn == nil {
			return
		}

		defer func() {
			s.logger.Info("Peer shutdown complete", zap.String("node", s.endpoint.Name()), zap.Int("numMembers", s.memberlist.NumMembers()))
		}()

		if s.leader != nil {
			s.leader.Stop()
		}

		if s.etcdClient != nil {
			if err := s.etcdClient.Deregister(s.endpoint.Name()); err != nil {
				s.logger.Warn("failed to shutdown Deregister", zap.Error(err))
			}
		}

		s.serviceRegistry.Shutdown()
		if err := s.memberlist.Leave(time.Second * 15); err != nil {
			s.logger.Warn("failed to leave cluster", zap.Error(err))
		}

		timeoutCtx, timeoutCancel := context.WithTimeout(s.ctx, time.Second*10)
		go func() {
			defer timeoutCancel()
			if err := s.memberlist.Shutdown(); err != nil {
				s.logger.Error("failed to shutdown cluster", zap.Error(err))
			}
		}()
		<-timeoutCtx.Done()
		if err := timeoutCtx.Err(); !errors.Is(err, context.Canceled) {
			s.logger.Warn("Failed to shutdown memberlist", zap.Error(err))
		}
		s.wk.StopWait()
		s.ctxCancelFn()
	})
}

func (s *LocalPeer) Join(members ...string) (int, error) {
	if s.etcdClient != nil {
		md, err := s.endpoint.MarshalJSON()
		if err != nil {
			s.logger.Fatal("Failed to marshal metadata", zap.Error(err))
		}

		if err := s.etcdClient.Register(s.endpoint.Name(), string(md)); err != nil {
			s.logger.Fatal("Failed to register service", zap.Error(err))
		}

		if s.config.LeaderElection {
			leader, err := NewPeerLeader(s.ctx, s.logger, s.etcdClient)
			if err != nil {
				s.logger.Fatal("Failed to create PeerLeader", zap.Error(err))
			}
			leader.Run(s.endpoint, s.memberlist)
			s.leader = leader
		}

		s.onServiceUpdate()
		m, ok := s.serviceRegistry.Get(kit.SERVICE_NAME)
		if ok {
			members = make([]string, 0)
			for _, v := range m.GetClients() {
				members = append(members, v.Addr())
			}
		}
		go s.processWatch()
	}

	n, err := s.memberlist.Join(members)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (s *LocalPeer) Leader() bool {
	return s.endpoint.Leader()
}

func (s *LocalPeer) AllowLeader() bool {
	return s.leader != nil
}

func (s *LocalPeer) Local() Endpoint {
	return s.endpoint
}

func (s *LocalPeer) NumMembers() int {
	return s.memberlist.NumMembers()
}

func (s *LocalPeer) Member(name string) (Endpoint, bool) {
	m, ok := s.members.Load(name)
	return m, ok
}

func (s *LocalPeer) Members() []Endpoint {
	endpoint := make([]Endpoint, 0)
	s.members.Range(func(key string, value Endpoint) bool {
		endpoint = append(endpoint, value)
		return true
	})
	return endpoint
}

func (s *LocalPeer) GetServiceRegistry() kit.ServiceRegistry {
	return s.serviceRegistry
}

func (s *LocalPeer) Broadcast(msg *pb.Peer_Envelope, reliable bool) {
	request := &pb.Frame{
		Id:        uuid.Must(uuid.NewV4()).String(),
		Node:      s.endpoint.Name(),
		Timestamp: timestamppb.New(time.Now().UTC()),
		Payload:   &pb.Frame_Envelope{Envelope: msg},
	}

	if msg.Cid == "" {
		msg.Cid = "REQ"
	}
	b, _ := proto.Marshal(request)
	//s.metrics.PeerSent(int64(len(b)))

	var err error
	s.members.Range(func(key string, value Endpoint) bool {
		if value.Name() == s.endpoint.Name() {
			return true
		}

		if !reliable {
			err = s.memberlist.SendBestEffort(value.MemberlistNode(), b)
		} else {
			err = s.memberlist.SendReliable(value.MemberlistNode(), b)
		}

		if err != nil {
			s.logger.Error("Failed to send broadcast", zap.String("name", key))
		}
		return true
	})
}

func (s *LocalPeer) Send(endpoint Endpoint, msg *pb.Peer_Envelope, reliable bool) error {
	request := &pb.Frame{
		Id:        uuid.Must(uuid.NewV4()).String(),
		Node:      s.endpoint.Name(),
		Timestamp: timestamppb.New(time.Now().UTC()),
		Payload:   &pb.Frame_Envelope{Envelope: msg},
	}

	b, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	//s.metrics.PeerSent(int64(len(b)))
	if !reliable {
		return s.memberlist.SendBestEffort(endpoint.MemberlistNode(), b)
	}

	return s.memberlist.SendReliable(endpoint.MemberlistNode(), b)
}

func (s *LocalPeer) Request(ctx context.Context, endpoint Endpoint, msg *pb.Peer_Envelope) (*pb.Peer_Envelope, error) {
	if endpoint == nil {
		return nil, status.Error(codes.NotFound, "endpoint is not found")
	}

	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
		defer func() { cancel() }()
	} else {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Second*30)
			defer func() { cancel() }()
		}
	}

	msg.Cid = "REQ"
	inbox := uuid.Must(uuid.NewV4())
	request := &pb.Frame{
		Id:        uuid.Must(uuid.NewV4()).String(),
		Inbox:     inbox.String(),
		Node:      s.endpoint.Name(),
		Timestamp: timestamppb.New(time.Now().UTC()),
		Payload:   &pb.Frame_Envelope{Envelope: msg},
	}

	b, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	replyChan := make(chan *pb.Frame, 1)
	s.inbox.Register(request.Inbox, replyChan)
	defer func() {
		defer s.inbox.Deregister(request.Inbox)
		close(replyChan)
	}()

	if err := s.memberlist.SendReliable(endpoint.MemberlistNode(), b); err != nil {
		return nil, err
	}

	select {
	case m, ok := <-replyChan:
		if !ok {
			return nil, status.Error(codes.DeadlineExceeded, "DeadlineExceeded")
		}

		writer := m.GetEnvelope()
		if writer == nil {
			return nil, status.Error(codes.InvalidArgument, "InvalidArgument")
		}

		switch v := writer.Payload.(type) {
		case *pb.Peer_Envelope_Error:
			if err := v.Error; err != nil {
				return nil, status.Error(codes.Code(err.Code), err.Message)
			}

		default:
		}
		return writer, nil

	case <-ctx.Done():
	}

	return nil, status.Error(codes.DeadlineExceeded, "DeadlineExceeded")
}

func (s *LocalPeer) BinaryLogBroadcast(b *pb.BinaryLog, toQueue bool) {
	if b == nil {
		return
	}

	b.Node = s.endpoint.Name()
	b.Version = s.binaryLog.RefreshVersion()
	frame := &pb.Frame{
		Id:        uuid.Must(uuid.NewV4()).String(),
		Inbox:     "",
		Node:      b.Node,
		Timestamp: timestamppb.New(time.Now().UTC()),
		Payload:   &pb.Frame_BinaryLog{BinaryLog: b},
	}
	bytes, _ := proto.Marshal(frame)

	// 如果不加入队列
	// 那么就直接实时发出去
	if !toQueue {
		s.members.Range(func(key string, value Endpoint) bool {
			if value.Name() == s.endpoint.Name() {
				return true
			}

			if err := s.memberlist.SendReliable(value.MemberlistNode(), bytes); err != nil {
				s.logger.Error("Failed to send broadcast", zap.String("name", key))
			}
			return true
		})
		return
	}

	s.transmitLimitedQueue.QueueBroadcast(&PeerBroadcast{
		name:     frame.Id,
		msg:      bytes,
		finished: nil,
	})
}

func (s *LocalPeer) RefreshVersion() {
	s.binaryLog.RefreshVersion()
}

func (s *LocalPeer) Event(ctx context.Context, in *api.AnyRequest, names ...string) error {
	request := &pb.Frame{
		Id:        uuid.Must(uuid.NewV4()).String(),
		Node:      s.endpoint.Name(),
		Timestamp: timestamppb.New(time.Now().UTC()),
		Payload:   &pb.Frame_Event{Event: in},
	}

	b, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	req := toPeerRequest(in)
	// 全局服务广播
	if len(names) < 1 {
		s.members.Range(func(key string, value Endpoint) bool {
			if err := s.memberlist.SendReliable(value.MemberlistNode(), b); err != nil {
				s.logger.Error("Failed to send broadcast", zap.String("name", key), zap.Error(err))
			}
			return true
		})

		s.GetServiceRegistry().Range(func(key string, value kit.Service) bool {
			if key == "nakama" {
				return true
			}

			for _, client := range value.GetClients() {
				if !client.AllowStream() {
					s.wk.Submit(func(evtCtx context.Context, logger *zap.Logger, c kit.Client, r *pb.Peer_Request) func() {
						return func() {
							if _, err := c.Do(ctx, r); err != nil {
								logger.Error("Failed to broadcast event within the cluster.", zap.Error(err), zap.String("role", c.Role()), zap.String("name", c.Name()))
							}
						}
					}(ctx, s.logger, client, req))
				} else {
					s.wk.Submit(func(logger *zap.Logger, c kit.Client, r *pb.Peer_Request) func() {
						return func() {
							if err := c.Send(r); err != nil {
								logger.Error("Failed to broadcast event within the cluster.", zap.Error(err), zap.String("role", c.Role()), zap.String("name", c.Name()))
							}
						}
					}(s.logger, client, req))
				}
			}
			return true
		})
		return nil
	}

	for _, name := range names {
		if name == "nakama" {
			s.members.Range(func(key string, value Endpoint) bool {
				if err := s.memberlist.SendReliable(value.MemberlistNode(), b); err != nil {
					s.logger.Error("Failed to send broadcast", zap.String("name", key), zap.Error(err))
				}
				return true
			})

			continue
		}

		clients, ok := s.GetServiceRegistry().Get(name)
		if !ok {
			continue
		}

		for _, client := range clients.GetClients() {
			if !client.AllowStream() {
				s.wk.Submit(func(evtCtx context.Context, logger *zap.Logger, c kit.Client, r *pb.Peer_Request) func() {
					return func() {
						if _, err := c.Do(ctx, r); err != nil {
							logger.Error("Failed to broadcast event within the cluster.", zap.Error(err), zap.String("role", c.Role()), zap.String("name", c.Name()))
						}
					}
				}(ctx, s.logger, client, req))
			} else {
				s.wk.Submit(func(logger *zap.Logger, c kit.Client, r *pb.Peer_Request) func() {
					return func() {
						if err := c.Send(r); err != nil {
							logger.Error("Failed to broadcast event within the cluster.", zap.Error(err), zap.String("role", c.Role()), zap.String("name", c.Name()))
						}
					}
				}(s.logger, client, req))
			}
		}
	}
	return nil
}

func (s *LocalPeer) InvokeMS(ctx context.Context, in *api.AnyRequest) (*api.AnyResponseWriter, error) {
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Invalid Argument")
	}

	request := toPeerRequest(in)
	maps.Copy(request.Context, s.runtimeConfig.Environment)
	maps.Copy(request.Context, in.GetContext())
	endpoint, ok := s.GetServiceRegistry().Get(in.Name)
	if !ok {
		return nil, status.Error(codes.Unavailable, "Service Unavailable")
	}

	resp, err := endpoint.Do(ctx, request)
	if err != nil {
		return nil, err
	}

	if resp == nil {
		return &api.AnyResponseWriter{}, nil
	}

	w, ns := toAnyResponseWriter(resp)
	if ns != nil {
		_ = sendAnyResponseWriter(context.Background(), s.logger, s.db, s.tracker, s.messageRouter, "", nil, ns, resp.GetRecipient())
	}
	return w, nil
}

func (s *LocalPeer) SendMS(ctx context.Context, in *api.AnyRequest) error {
	if in.Name == "" {
		return status.Error(codes.InvalidArgument, "Invalid Argument")
	}

	request := toPeerRequest(in)
	maps.Copy(request.Context, s.runtimeConfig.Environment)
	maps.Copy(request.Context, in.GetContext())
	endpoint, ok := s.GetServiceRegistry().Get(in.Name)
	if !ok {
		return status.Error(codes.Unavailable, "Service Unavailable")
	}
	return endpoint.Send(request)
}

func (s *LocalPeer) processIncoming() {
	for {
		select {
		case <-s.ctx.Done():
			return

		case msg, ok := <-s.msgChan:
			if !ok {
				return
			}

			switch msg.msgType {
			case PeerMsg_NOTIFY:
				s.onNotifyMsg(msg.data)

			case PeerMsg_REMOTESTATE:
				s.onMergeRemoteState(msg.data, false)

			case PeerMsg_REMOTESTATEJOIN:
				s.onMergeRemoteState(msg.data, true)

			default:
			}
		}
	}
}

func (s *LocalPeer) processWatch() {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("Recovered from panic in processWatch", zap.Any("error", r), zap.String("debug", string(debug.Stack())))
		}
		s.logger.Info("Stopped service watch for cluster")
	}()

	s.logger.Info("Starting service watch for cluster")
	ch := make(chan struct{}, 1)
	go s.etcdClient.Watch(ch)

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				s.logger.Warn("Watch channel closed unexpectedly")
				return
			}
			s.onServiceUpdate()

		case <-s.ctx.Done():
			return
		}
	}
}

func (s *LocalPeer) onServiceUpdate() {
	if s.etcdClient == nil {
		return
	}

	entries, err := s.etcdClient.GetEntries()
	if err != nil {
		s.logger.Warn("failed to GetEntries", zap.Error(err))
		return
	}

	nodes := make(map[string]map[string]bool)
	nodesname := make([]string, 0, len(entries))
	nodesremoved := make([]string, 0, len(entries))
	for _, v := range entries {
		var md pb.NodeMeta
		if err := s.protojsonUnmarshaler.Unmarshal([]byte(v), &md); err != nil {
			s.logger.Warn("failed to Unmarshal node meta", zap.Error(err), zap.String("md", v))
			continue
		}

		nodesname = append(nodesname, md.Name)
		if m, ok := nodes[md.Role]; ok {
			m[md.Name] = true
		} else {
			nodes[md.Role] = make(map[string]bool)
			nodes[md.Role][md.Name] = true
		}

		client, err := s.serviceRegistry.Register(&md, s.handlerByPeerResponseWriter)
		if err != nil {
			s.logger.Error("failed to register service", zap.Error(err), zap.String("md", v))
			continue
		}

		if md.Role == kit.SERVICE_NAME {
			// skip main service
			continue
		}

		if dialConnected, _ := client.Connected(); dialConnected {
			continue
		}

		m := make(metadata.MD)
		m.Append("node", s.endpoint.Name())
		m.Append("role", kit.SERVICE_NAME)

		opt := kit.NewClientOptions()
		if s.config.Grpc != nil {
			opt = kit.ToClientOptions(s.config.Grpc)
		}

		opt.AllowStream = true
		if err := client.Dial(m, opt); err != nil {
			s.logger.Error("failed connect to service", zap.Error(err), zap.String("md", v))
		}
	}

	s.serviceRegistry.Range(func(key string, value kit.Service) bool {
		m, ok := nodes[key]
		if !ok {
			nodesremoved = append(nodesremoved, key)
			s.serviceRegistry.Delete(value)
			return true
		}

		removed := make([]string, 0)
		n := 0
		for _, client := range value.GetClients() {
			if !m[client.Name()] {
				nodesremoved = append(nodesremoved, client.Name())
				removed = append(removed, client.Name())
				n++
			}
		}

		if n > 0 {
			s.serviceRegistry.Delete(value, removed...)
		}
		return true
	})
	s.logger.Info("Updated services for cluster", zap.Strings("nodes", nodesname), zap.Strings("removed", nodesremoved))
}

func (s *LocalPeer) onNotifyMsg(msg []byte) {
	var frame pb.Frame
	if err := proto.Unmarshal(msg, &frame); err != nil {
		s.logger.Error("Failed to unmarshal NotifyMsg", zap.Error(err))
		return
	}

	switch v := frame.Payload.(type) {
	case *pb.Frame_BinaryLog:
		s.handleBinaryLog(frame.GetBinaryLog())

	case *pb.Frame_Status:
	case *pb.Frame_Envelope:
		if v.Envelope == nil {
			return
		}

		cid := v.Envelope.GetCid()
		switch cid {
		case "REQ", "":
			s.onRequest(&frame)

		case "RESP":
			s.onResponseWriter(&frame)

		default:
			s.logger.Error("", zap.String("No corresponding CID found.", cid))
		}

	case *pb.Frame_Event:
		if fn := s.runtime.EventPeer(); fn != nil {
			evtCtx := NewRuntimeGoContext(s.ctx, s.Local().Name(), "", s.runtimeConfig.Environment, RuntimeExecutionModePeerEvent, nil, nil, 0, "", "", nil, "", "", "", "")
			fn(evtCtx, NewRuntimeGoLogger(s.logger), v.Event)
		}
	}
}

func (s *LocalPeer) onMergeRemoteState(buf []byte, join bool) {
	var state pb.State
	if err := proto.Unmarshal(buf, &state); err != nil {
		s.logger.Error("Failed to unmarshal MergeRemoteState", zap.Error(err))
		return
	}

	nodeNames := make(map[string]bool)
	nodeMap := make(map[string]*pb.StateNode)
	currentNode := s.endpoint.Name()
	for _, stateNode := range state.GetNodes() {
		if stateNode.Node == currentNode {
			continue
		}

		if stateNode.Version <= s.binaryLog.GetVersionByNode(stateNode.Node) {
			continue
		}
		nodeNames[stateNode.Node] = true
		nodeMap[stateNode.Node] = stateNode
	}

	s.tracker.ClearTrackByNode(nodeNames)
	s.matchmaker.RemoveAll(nodeNames)
	for node, stateNode := range nodeMap {
		s.binaryLog.SetVersionByNode(node, stateNode.Version)
		s.tracker.MergeRemoteState(node, stateNode.GetPresences())

		matchmakerExtracts := make([]*MatchmakerExtract, len(stateNode.GetMatchmaker()))
		for k, v := range stateNode.GetMatchmaker() {
			matchmakerExtracts[k] = pb2MatchmakerExtract(v)
		}
		s.matchmaker.Insert(matchmakerExtracts)
		s.partyRegistry.SyncData(s.ctx, node, stateNode.GetPartyIndexEntry())
	}
}

func (s *LocalPeer) onResponseWriter(frame *pb.Frame) {
	if len(frame.Inbox) != 1 {
		s.inbox.Send(frame)
		return
	}
}

func (s *LocalPeer) onBan(node string, presence *pb.BanValue) {}

func (s *LocalPeer) onTrack(node string, presence *pb.Presence) {
	if node == s.endpoint.Name() {
		return
	}

	ops := make([]*TrackerOp, len(presence.Stream))
	for k, v := range presence.Stream {
		ops[k] = &TrackerOp{
			Stream: pb2PresenceStream(v),
			Meta:   pb2PresenceMeta(presence.Meta[k]),
		}
	}

	sessionID := uuid.FromStringOrNil(presence.SessionID)
	userID := uuid.FromStringOrNil(presence.UserID)
	if sessionID.IsNil() || userID.IsNil() {
		s.logger.Warn("onTrack sessionID/userID is nil", zap.String("SessionID", presence.SessionID), zap.String("userID", presence.UserID), zap.String("node", node))
		return
	}

	ok := s.tracker.TrackMulti(s.ctx, sessionID, ops, userID, node)
	if !ok {
		s.logger.Warn("TrackMulti failed", zap.String("SessionID", presence.SessionID), zap.String("userID", presence.UserID), zap.String("node", node))
		return
	}
}

func (s *LocalPeer) onUntrack(node string, presence *pb.UntrackValue) {
	if node == s.endpoint.Name() {
		return
	}

	sessionID := uuid.FromStringOrNil(presence.GetSessionID())
	userID := uuid.FromStringOrNil(presence.GetUserID())
	modeSize := len(presence.Modes)
	streamSize := len(presence.GetStream())
	if modeSize > 0 {
		modes := make(map[uint8]struct{}, modeSize)
		for _, mode := range presence.GetModes() {
			modes[uint8(mode)] = struct{}{}
		}

		var skip PresenceStream
		if presence.Skip != nil {
			skip = pb2PresenceStream(presence.GetSkip())
		}

		s.tracker.UntrackLocalByModes(sessionID, modes, skip)
		return
	}

	if sessionID.IsNil() && userID.IsNil() && streamSize > 0 {
		for _, stream := range presence.GetStream() {
			s.tracker.UntrackLocalByStream(pb2PresenceStream(stream))
		}
		return
	}

	if !sessionID.IsNil() && streamSize == 0 {
		s.tracker.UntrackAll(sessionID, runtime.PresenceReason(presence.GetReason()), node)
		return
	}

	for _, stream := range presence.GetStream() {
		s.tracker.Untrack(sessionID, pb2PresenceStream(stream), userID, node)
	}
}
