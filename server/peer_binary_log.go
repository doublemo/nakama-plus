// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package server

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/doublemo/nakama-kit/pb"
	"github.com/doublemo/nakama-plus/v3/internal/skiplist"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type (
	BinaryLog interface {
		Push(log *pb.BinaryLog) bool
		NextID() uint64
		CurrentID() uint64
		GetBroadcasts(limit int) []*pb.BinaryLog
		GetCheckPoint() *pb.CheckPoint
		MergeCheckPoint(checkPoint *pb.CheckPoint, join bool)
		SetLocalCheckPoint(name string, version uint64)
		ClearBinaryLogByNode(node string)
		GetBinaryLogVersionByNode(node string) (min, max uint64)
		GetBinaryLogVersions() map[string][2]uint64
		Len() int
		Stop()
	}

	binaryLogValue struct {
		value *pb.BinaryLog
	}

	LocalBinaryLog struct {
		ctx         context.Context
		ctxCancelFn context.CancelFunc
		logger      *zap.Logger
		node        string
		data        *skiplist.SkipList
		checkPoint  map[string]map[string]uint64
		idGenValue  *atomic.Uint64
		numMembers  func() int
		once        sync.Once
		sync.RWMutex
	}
)

func (v *binaryLogValue) Less(other interface{}) bool {
	otherValue := other.(*binaryLogValue)
	aTimestamp := v.value.GetTimestamp()
	bTimestamp := otherValue.value.GetTimestamp()
	if aTimestamp == nil || bTimestamp == nil {
		if v.value.Node < otherValue.value.Node {
			return true
		}

		if v.value.Node == otherValue.value.Node && v.value.Id < otherValue.value.Id {
			return true
		}

		return false
	}

	atime := aTimestamp.GetSeconds()
	btime := bTimestamp.GetSeconds()
	if atime < btime {
		return true
	}

	if atime == btime && aTimestamp.GetNanos() < bTimestamp.GetNanos() {
		return true
	}

	if atime == btime && v.value.Node < otherValue.value.Node {
		return true
	}

	if atime == btime && v.value.Node == otherValue.value.Node && v.value.Id < otherValue.value.Id {
		return true
	}
	return false
}

func NewLocalBinaryLog(ctx context.Context, logger *zap.Logger, node string, numMembers func() int) BinaryLog {
	ctx, ctxCancelFn := context.WithCancel(ctx)
	b := &LocalBinaryLog{
		ctx:         ctx,
		ctxCancelFn: ctxCancelFn,
		logger:      logger,
		node:        node,
		data:        skiplist.New(),
		checkPoint:  make(map[string]map[string]uint64),
		idGenValue:  atomic.NewUint64(0),
		numMembers:  numMembers,
	}

	go b.processClear()
	return b
}

func (b *LocalBinaryLog) Len() int {
	b.RLock()
	if b.data == nil {
		b.RUnlock()
		return 0
	}

	size := b.data.Len()
	b.RUnlock()
	return size
}

func (b *LocalBinaryLog) Stop() {
	b.once.Do(func() {
		if b.ctxCancelFn == nil {
			return
		}

		b.ctxCancelFn()
	})
}

func (b *LocalBinaryLog) Push(log *pb.BinaryLog) bool {
	value := &binaryLogValue{value: log}
	b.Lock()
	point, ok := b.checkPoint[b.node]
	if ok {
		if version := point[log.GetNode()]; log.Id-version != 1 {
			b.Unlock()
			return false
		}
	} else {
		b.checkPoint[b.node] = make(map[string]uint64)
	}

	if v := b.data.Find(value); v != nil {
		b.Unlock()
		return false
	}
	b.checkPoint[b.node][log.GetNode()] = log.GetId()
	b.data.Insert(value)
	b.Unlock()
	return true
}

func (b *LocalBinaryLog) NextID() uint64 {
	return b.idGenValue.Add(1)
}

func (b *LocalBinaryLog) CurrentID() uint64 {
	return b.idGenValue.Load()
}

func (b *LocalBinaryLog) GetBroadcasts(limit int) []*pb.BinaryLog {
	data := make([]*pb.BinaryLog, 0, limit)
	b.RLock()
	if b.data == nil {
		b.RUnlock()
		return data
	}

	i := 0
	for e := b.data.Front(); e != nil; e = e.Next() {
		if i >= limit {
			break
		}

		value := e.Value.(*binaryLogValue).value
		data = append(data, value)
		i++
	}
	b.RUnlock()
	return data
}

func (b *LocalBinaryLog) GetCheckPoint() *pb.CheckPoint {
	checkPoint := &pb.CheckPoint{
		Value: make(map[string]*pb.Point),
	}

	b.RLock()
	for k, point := range b.checkPoint {
		value := &pb.Point{
			Point: make(map[string]uint64),
		}

		for i, v := range point {
			value.Point[i] = v
		}

		checkPoint.Value[k] = value
	}
	b.RUnlock()
	return checkPoint
}

func (b *LocalBinaryLog) MergeCheckPoint(checkPoint *pb.CheckPoint, join bool) {
	if checkPoint == nil {
		return
	}

	mPoint := make(map[string]uint64)
	b.Lock()
	for k, point := range checkPoint.GetValue() {
		if k == b.node || point == nil {
			continue
		}

		if _, ok := b.checkPoint[k]; !ok {
			b.checkPoint[k] = make(map[string]uint64)
		}

		for kk, vv := range point.GetPoint() {
			ver := b.checkPoint[k][kk]
			if vv > ver {
				b.checkPoint[k][kk] = vv
			}

			if mPoint[kk] < vv {
				mPoint[kk] = vv
			}
		}
	}

	if join {
		b.checkPoint[b.node] = mPoint
	}
	b.Unlock()
}

func (b *LocalBinaryLog) SetLocalCheckPoint(name string, version uint64) {
	b.Lock()
	if point, ok := b.checkPoint[b.node]; ok {
		if point[name] < version {
			point[name] = version
		}
	} else {
		b.checkPoint[b.node] = make(map[string]uint64)
		b.checkPoint[b.node][name] = version
	}
	b.Unlock()
}

func (b *LocalBinaryLog) ClearBinaryLogByNode(node string) {
	b.Lock()
	n := skiplist.New()
	for e := b.data.Front(); e != nil; e = e.Next() {
		if e.Value.(*binaryLogValue).value.Node == node {
			continue
		}
		n.Insert(e.Value)
	}

	delete(b.checkPoint, node)
	if m, ok := b.checkPoint[b.node]; ok {
		delete(m, node)
	}
	b.data = n
	b.Unlock()
}

func (b *LocalBinaryLog) GetBinaryLogVersionByNode(node string) (min, max uint64) {
	values := make([]uint64, 0)
	n := 0
	b.RLock()
	for _, point := range b.checkPoint {
		values = append(values, point[node])
		n++
	}
	b.RUnlock()

	if n < 1 {
		return
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})

	min = values[0]
	max = values[n-1]
	return
}

func (b *LocalBinaryLog) GetBinaryLogVersions() map[string][2]uint64 {
	values := make(map[string][]uint64)
	b.RLock()
	for _, point := range b.checkPoint {
		for k, v := range point {
			if _, ok := values[k]; ok {
				values[k] = append(values[k], v)
			} else {
				values[k] = make([]uint64, 0)
				values[k] = append(values[k], v)
			}
		}
	}
	b.RUnlock()

	data := make(map[string][2]uint64)
	for k, v := range values {
		sort.Slice(v, func(i, j int) bool {
			return v[i] < v[j]
		})

		n := len(v)
		if n < 1 {
			continue
		}
		data[k] = [2]uint64{v[0], v[n-1]}
	}

	return data
}

func (b *LocalBinaryLog) processClear() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return

		case <-ticker.C:
			ticker.Stop()
			versionMap := b.GetBinaryLogVersions()
			numMembers := b.numMembers()

			b.Lock()
			if len(b.checkPoint) < numMembers {
				b.Unlock()
				ticker.Reset(time.Minute)
				continue
			}

			list := skiplist.New()
			for e := b.data.Front(); e != nil; e = e.Next() {
				v := e.Value.(*binaryLogValue).value
				if b.isExpired(v, versionMap) {
					continue
				}

				list.Insert(e.Value)
			}
			b.data = list
			b.Unlock()
			ticker.Reset(time.Minute)
		}
	}
}

func (b *LocalBinaryLog) isExpired(v *pb.BinaryLog, versionMap map[string][2]uint64) bool {
	if m, ok := versionMap[v.Node]; ok && m[0] > 0 && m[0] >= v.Id {
		return true
	}

	ts := v.GetTimestamp()
	if ts == nil {
		return false
	}

	return ts.AsTime().Add(time.Minute * 10).Before(time.Now().UTC())
}

func (s *LocalPeer) onBinaryLog(log *pb.BinaryLog) {
	if !s.binaryLog.Push(log) {
		return
	}

	s.logger.Debug("onBinaryLog", zap.Any("log", log))
	switch log.Payload.(type) {
	case *pb.BinaryLog_Ban:
		s.onBan(log.GetNode(), log.GetBan())

	case *pb.BinaryLog_Track:
		s.onTrack(log.GetNode(), log.GetTrack())

	case *pb.BinaryLog_Untrack:
		s.onUntrack(log.GetNode(), log.GetUntrack())

	case *pb.BinaryLog_MatchmakerAdd:
		extract := pb2MatchmakerExtract(log.GetMatchmakerAdd())
		_, _, err := s.matchmaker.Add(s.ctx, extract.Presences, extract.SessionID, extract.PartyId, extract.Query, extract.MinCount, extract.MaxCount, extract.CountMultiple, extract.StringProperties, extract.NumericProperties)
		if err != nil {
			s.logger.Error("BinaryLog_MatchmakerAdd", zap.Error(err), zap.Any("extract", extract))
		}
	case *pb.BinaryLog_MatchmakerRemoveSession:
		extract := log.GetMatchmakerRemoveSession()
		if err := s.matchmaker.RemoveSession(extract.SessionId, extract.Ticket); err != nil {
			s.logger.Error("BinaryLog_MatchmakerRemoveSession", zap.Error(err), zap.Any("extract", extract))
		}
	case *pb.BinaryLog_MatchmakerRemoveSessionAll:
		extract := log.GetMatchmakerRemoveSessionAll()
		if err := s.matchmaker.RemoveSessionAll(extract.SessionId); err != nil {
			s.logger.Error("BinaryLog_MatchmakerRemoveSessionAll", zap.Error(err), zap.Any("extract", extract))
		}
	case *pb.BinaryLog_MatchmakerRemoveParty:
		extract := log.GetMatchmakerRemoveParty()
		if err := s.matchmaker.RemoveParty(extract.PartyId, extract.Ticket); err != nil {
			s.logger.Error("BinaryLog_MatchmakerRemoveParty", zap.Error(err), zap.Any("extract", extract))
		}
	case *pb.BinaryLog_MatchmakerRemovePartyAll:
		extract := log.GetMatchmakerRemovePartyAll()
		if err := s.matchmaker.RemovePartyAll(extract.PartyId); err != nil {
			s.logger.Error("BinaryLog_MatchmakerRemovePartyAll", zap.Error(err), zap.Any("extract", extract))
		}
	case *pb.BinaryLog_MatchmakerRemoveAll:
		extract := log.GetMatchmakerRemoveAll()
		s.matchmaker.RemoveAll(extract.Node)

	case *pb.BinaryLog_MatchmakerRemove:
		extract := log.GetMatchmakerRemove()
		s.matchmaker.Remove(extract.Ticket)
	}
}
