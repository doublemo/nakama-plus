// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package server

import (
	"sync"
	"time"

	"github.com/doublemo/nakama-kit/kit"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/hashicorp/memberlist"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	Endpoint interface {
		Name() string
		Weight(v ...int32) int32
		Balancer(v ...int32) int32
		Status(v ...int32) int32
		PingRTT(v ...time.Duration) time.Duration
		Metadata() map[string]string
		GetMetadata(k string) string
		SetMetadata(k, v string)
		ResetMetadata(md map[string]string)
		DeleteMetadata(deleteKey string)
		PingCompleted(rtt time.Duration)
		BindMemberlistNode(node *memberlist.Node)
		MemberlistNode() *memberlist.Node
		SessionCount() int32
		PresenceCount() int32
		MatchCount() int32
		GoroutineCount() int32
		AvgLatencyMs() float64
		AvgRateSec() float64
		AvgInputKbs() float64
		AvgOutputKbs() float64
		MergeStateAt() int64
		UpdateState(status *pb.Status)
		MarshalJSON() ([]byte, error)
		MarshalProtoBuffer() ([]byte, error)
		Leader(v ...bool) bool
	}

	PeerEndpoint struct {
		name           string
		weight         *atomic.Int32
		balancer       *atomic.Int32
		metadata       atomic.Value
		pingrtt        *atomic.Duration
		sessionCount   *atomic.Int32
		presenceCount  *atomic.Int32
		matchCount     *atomic.Int32
		goroutineCount *atomic.Int32
		avgLatencyMs   *atomic.Float64
		avgRateSec     *atomic.Float64
		avgInputKbs    *atomic.Float64
		avgOutputKbs   *atomic.Float64
		mergeStateAt   *atomic.Int64
		memberlistNode atomic.Pointer[memberlist.Node]
		status         *atomic.Int32
		leader         *atomic.Bool

		protojsonMarshaler *protojson.MarshalOptions
		sync.Mutex
	}
)

func NewPeerEndpont(name string, md map[string]string, status, weight, balancer int32, leader bool, protojsonMarshaler *protojson.MarshalOptions, node ...*memberlist.Node) Endpoint {
	ed := &PeerEndpoint{
		name:           name,
		pingrtt:        atomic.NewDuration(0),
		sessionCount:   &atomic.Int32{},
		presenceCount:  &atomic.Int32{},
		matchCount:     &atomic.Int32{},
		goroutineCount: &atomic.Int32{},
		avgLatencyMs:   &atomic.Float64{},
		avgRateSec:     &atomic.Float64{},
		avgInputKbs:    &atomic.Float64{},
		avgOutputKbs:   &atomic.Float64{},
		mergeStateAt:   &atomic.Int64{},
		weight:         atomic.NewInt32(weight),
		balancer:       atomic.NewInt32(balancer),
		status:         atomic.NewInt32(status),
		leader:         atomic.NewBool(leader),

		protojsonMarshaler: protojsonMarshaler,
	}

	if size := len(node); size > 0 {
		ed.memberlistNode.Store(node[0])
	}

	if md == nil {
		md = make(map[string]string)
	}

	ed.metadata.Store(md)
	return ed
}

func (endpoint *PeerEndpoint) Name() string {
	return endpoint.name
}

func (endpoint *PeerEndpoint) Weight(v ...int32) int32 {
	if len(v) > 0 {
		endpoint.weight.Store(v[0])
	}

	return endpoint.weight.Load()
}

func (endpoint *PeerEndpoint) Balancer(v ...int32) int32 {
	if len(v) > 0 {
		endpoint.balancer.Store(v[0])
	}

	return endpoint.balancer.Load()
}

func (endpoint *PeerEndpoint) Status(v ...int32) int32 {
	if len(v) > 0 {
		endpoint.status.Store(v[0])
	}

	return endpoint.status.Load()
}

func (endpoint *PeerEndpoint) PingRTT(v ...time.Duration) time.Duration {
	if len(v) > 0 {
		endpoint.pingrtt.Store(v[0])
	}

	return endpoint.pingrtt.Load()
}

func (endpoint *PeerEndpoint) GetMetadata(k string) string {
	return endpoint.metadata.Load().(map[string]string)[k]
}

func (endpoint *PeerEndpoint) SetMetadata(k, v string) {
	endpoint.Lock()
	md := endpoint.metadata.Load().(map[string]string)
	newMd := make(map[string]string)
	for k, v := range md {
		newMd[k] = v
	}
	newMd[k] = v
	endpoint.metadata.Store(newMd)
	endpoint.Unlock()
}

func (endpoint *PeerEndpoint) Metadata() map[string]string {
	md := endpoint.metadata.Load().(map[string]string)
	newMd := make(map[string]string)
	for k, v := range md {
		newMd[k] = v
	}
	return newMd
}

func (endpoint *PeerEndpoint) ResetMetadata(md map[string]string) {
	endpoint.Lock()
	if md == nil {
		md = make(map[string]string)
	}
	newMd := make(map[string]string)
	for k, v := range md {
		newMd[k] = v
	}
	endpoint.metadata.Store(newMd)
	endpoint.Unlock()
}

func (endpoint *PeerEndpoint) DeleteMetadata(deleteKey string) {
	endpoint.Lock()
	md := endpoint.metadata.Load().(map[string]string)
	newMd := make(map[string]string)
	for k := range md {
		if k == deleteKey {
			continue
		}
	}
	endpoint.metadata.Store(newMd)
	endpoint.Unlock()
}

func (endpoint *PeerEndpoint) PingCompleted(rtt time.Duration) {
	endpoint.pingrtt.Store(rtt)
}

func (endpoint *PeerEndpoint) BindMemberlistNode(node *memberlist.Node) {
	endpoint.memberlistNode.Store(node)
}

func (endpoint *PeerEndpoint) MemberlistNode() *memberlist.Node {
	return endpoint.memberlistNode.Load()
}

func (endpoint *PeerEndpoint) SessionCount() int32 {
	return endpoint.sessionCount.Load()
}

func (endpoint *PeerEndpoint) PresenceCount() int32 {
	return endpoint.presenceCount.Load()
}

func (endpoint *PeerEndpoint) MatchCount() int32 {
	return endpoint.matchCount.Load()
}

func (endpoint *PeerEndpoint) GoroutineCount() int32 {
	return endpoint.goroutineCount.Load()
}

func (endpoint *PeerEndpoint) AvgLatencyMs() float64 {
	return endpoint.avgLatencyMs.Load()
}

func (endpoint *PeerEndpoint) AvgRateSec() float64 {
	return endpoint.avgRateSec.Load()
}

func (endpoint *PeerEndpoint) AvgInputKbs() float64 {
	return endpoint.avgInputKbs.Load()
}

func (endpoint *PeerEndpoint) AvgOutputKbs() float64 {
	return endpoint.avgOutputKbs.Load()
}

func (endpoint *PeerEndpoint) MergeStateAt() int64 {
	return endpoint.mergeStateAt.Load()
}

func (endpoint *PeerEndpoint) Leader(v ...bool) bool {
	if len(v) > 0 {
		endpoint.leader.Store(v[0])
		return v[0]
	}
	return endpoint.leader.Load()
}

func (endpoint *PeerEndpoint) UpdateState(status *pb.Status) {
	endpoint.sessionCount.Store(status.SessionCount)
	endpoint.presenceCount.Store(status.PresenceCount)
	endpoint.matchCount.Store(status.MatchCount)
	endpoint.goroutineCount.Store(status.GoroutineCount)
	endpoint.avgLatencyMs.Store(status.AvgLatencyMs)
	endpoint.avgRateSec.Store(status.AvgRateSec)
	endpoint.avgInputKbs.Store(status.AvgInputKbs)
	endpoint.avgOutputKbs.Store(status.AvgOutputKbs)
}

func (endpoint *PeerEndpoint) MarshalJSON() ([]byte, error) {
	md := &pb.NodeMeta{
		Name:        endpoint.Name(),
		Vars:        make(map[string]string),
		Role:        kit.SERVICE_NAME,
		Status:      pb.NodeMeta_Status(endpoint.Status()),
		Weight:      endpoint.Weight(),
		Balancer:    pb.NodeMeta_Balancer(endpoint.Balancer()),
		AllowStream: false,
		Leader:      endpoint.Leader(),
	}

	node := endpoint.MemberlistNode()
	if node != nil {
		md.Ip = node.Addr.String()
		md.Port = uint32(node.Port)
	}

	return endpoint.protojsonMarshaler.Marshal(md)
}

func (endpoint *PeerEndpoint) MarshalProtoBuffer() ([]byte, error) {
	md := &pb.NodeMeta{
		Name:        endpoint.Name(),
		Vars:        make(map[string]string),
		Role:        kit.SERVICE_NAME,
		Status:      pb.NodeMeta_Status(endpoint.Status()),
		Weight:      endpoint.Weight(),
		Balancer:    pb.NodeMeta_Balancer(endpoint.Balancer()),
		AllowStream: false,
		Leader:      endpoint.Leader(),
	}

	node := endpoint.MemberlistNode()
	if node != nil {
		md.Ip = node.Addr.String()
		md.Port = uint32(node.Port)
	}

	return proto.Marshal(md)
}
