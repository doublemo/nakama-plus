// Copyright 2019 The Nakama Authors
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
	"math"
	"net"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/doublemo/nakama-kit/kit"
	"github.com/doublemo/nakama-plus/v3/console"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StatusHandler interface {
	GetStatus(ctx context.Context) ([]*console.StatusList_Status, error)
	GetServices(ctx context.Context) []*console.StatusList_ServiceStatus
	SetPeer(peer Peer)
}

type LocalStatusHandler struct {
	logger          *zap.Logger
	sessionRegistry SessionRegistry
	matchRegistry   MatchRegistry
	partyRegistry   PartyRegistry
	tracker         Tracker
	metrics         Metrics
	node            string
	createTime      time.Time
	peer            Peer
}

func NewLocalStatusHandler(logger *zap.Logger, sessionRegistry SessionRegistry, matchRegistry MatchRegistry, partyRegistry PartyRegistry, tracker Tracker, metrics Metrics, node string, createTime time.Time) StatusHandler {
	return &LocalStatusHandler{
		logger:          logger,
		sessionRegistry: sessionRegistry,
		matchRegistry:   matchRegistry,
		partyRegistry:   partyRegistry,
		tracker:         tracker,
		metrics:         metrics,
		node:            node,
		createTime:      createTime,
		peer:            nil,
	}
}

func (s *LocalStatusHandler) GetStatus(ctx context.Context) ([]*console.StatusList_Status, error) {
	status := make([]*console.StatusList_Status, 0)
	status = append(status, &console.StatusList_Status{
		Name:           s.node,
		Health:         console.StatusHealth_STATUS_HEALTH_OK,
		SessionCount:   int32(s.sessionRegistry.Count()),
		PresenceCount:  int32(s.tracker.Count()),
		MatchCount:     int32(s.matchRegistry.Count()),
		GoroutineCount: int32(runtime.NumGoroutine()),
		AvgLatencyMs:   math.Floor(s.metrics.SnapshotLatencyMs()*100) / 100,
		AvgRateSec:     math.Floor(s.metrics.SnapshotRateSec()*100) / 100,
		AvgInputKbs:    math.Floor(s.metrics.SnapshotRecvKbSec()*100) / 100,
		AvgOutputKbs:   math.Floor(s.metrics.SnapshotSentKbSec()*100) / 100,
		Leader:         s.peer.Local().Leader(),
		PartyCount:     int32(s.partyRegistry.Count()),
		CreateTime:     timestamppb.New(s.createTime),
	})

	if s.peer == nil {
		return status, nil
	}

	for _, member := range s.peer.Members() {
		if member.Name() == s.node {
			continue
		}

		status = append(status, &console.StatusList_Status{
			Name:           member.Name(),
			Health:         console.StatusHealth_STATUS_HEALTH_OK,
			SessionCount:   member.SessionCount(),
			PresenceCount:  member.PresenceCount(),
			MatchCount:     member.MatchCount(),
			GoroutineCount: member.GoroutineCount(),
			AvgLatencyMs:   member.AvgLatencyMs(),
			AvgRateSec:     member.AvgRateSec(),
			AvgInputKbs:    member.AvgInputKbs(),
			AvgOutputKbs:   member.AvgOutputKbs(),
			Leader:         member.Leader(),
			PartyCount:     member.PartyCount(),
			CreateTime:     member.ServerCreateTime(),
		})
	}

	sort.Slice(status, func(i, j int) bool {
		return status[i].Name < status[j].Name
	})
	return status, nil
}

func (s *LocalStatusHandler) GetServices(ctx context.Context) []*console.StatusList_ServiceStatus {
	services := make([]*console.StatusList_ServiceStatus, 0)
	if s.peer == nil {
		return services
	}

	s.peer.GetServiceRegistry().Range(func(key string, value kit.Service) bool {
		if key == kit.SERVICE_NAME {
			return true
		}

		for _, v := range value.GetClients() {
			ip, port, _ := net.SplitHostPort(v.Addr())
			portValue, _ := strconv.Atoi(port)
			services = append(services, &console.StatusList_ServiceStatus{
				Name:        v.Name(),
				Vars:        v.Metadata().Vars,
				Ip:          ip,
				Port:        uint32(portValue),
				Role:        v.Role(),
				Status:      int32(v.Status()),
				Weight:      v.Weight(),
				Balancer:    int32(v.Balancer()),
				AllowStream: v.AllowStream(),
			})
		}
		return true
	})
	return services
}

func (s *LocalStatusHandler) SetPeer(peer Peer) {
	s.peer = peer
}
