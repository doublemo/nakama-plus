// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"context"
	"math"
	"sync"

	"github.com/doublemo/nakama-kit/kit/hashring"
	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type (
	Service interface {
		Name() string
		Role() string
		Balancer() pb.NodeMeta_Balancer
		AddClient(client Client)
		RemoveClient(name string)
		ResetBalancer()
		GetClients() []Client
		GetClientByBalancer(key ...string) (Client, bool)
		GetClientByHashRing(key string) (Client, bool)
		GetClientByRoundRobin() (Client, bool)
		GetClientByRandom() (Client, bool)
		GetClientByName(name string) (Client, bool)
		Do(ctx context.Context, msg *pb.Peer_Request) (*pb.Peer_ResponseWriter, error)
		Send(msg *pb.Peer_Request) error
		Count() int64
		Exist(name string) (ok bool)
		Stop()
	}

	LocalService struct {
		ctx         context.Context
		ctxCancelFn context.CancelFunc
		name        string
		role        string
		logger      *zap.Logger
		clientsMap  map[string]int
		clients     []Client
		hashring    *hashring.HashRing
		balancer    *atomic.Int64
		count       *atomic.Int64
		weight      *atomic.Int32
		roundRobin  *atomic.Int32
		once        sync.Once
		sync.RWMutex
	}
)

func NewLocalService(ctx context.Context, logger *zap.Logger, name, role string) Service {
	ctx, ctxCancelFn := context.WithCancel(ctx)
	s := &LocalService{
		ctx:         ctx,
		ctxCancelFn: ctxCancelFn,
		name:        name,
		role:        role,
		logger:      logger,
		balancer:    atomic.NewInt64(0),
		count:       atomic.NewInt64(0),
		weight:      atomic.NewInt32(0),
		roundRobin:  atomic.NewInt32(0),
		clientsMap:  make(map[string]int),
		clients:     make([]Client, 0),
	}
	return s
}

func (s *LocalService) Name() string {
	return s.name
}

func (s *LocalService) Role() string {
	return s.role
}

func (s *LocalService) Balancer() pb.NodeMeta_Balancer {
	return pb.NodeMeta_Balancer(s.balancer.Load())
}

func (s *LocalService) Count() int64 {
	return s.count.Load()
}

func (s *LocalService) Exist(name string) (ok bool) {
	s.RLock()
	_, ok = s.clientsMap[name]
	s.RUnlock()
	return
}

func (s *LocalService) Stop() {
	s.once.Do(func() {
		if s.ctxCancelFn == nil {
			return
		}

		s.Lock()
		for _, client := range s.clients {
			client.Close()
		}

		s.clients = make([]Client, 0)
		s.clientsMap = make(map[string]int)
		s.hashring = nil
		s.Unlock()
		s.count.Store(0)
		s.weight.Store(0)
		s.roundRobin.Store(0)
		s.ctxCancelFn()
	})
}

func (s *LocalService) GetClientByBalancer(key ...string) (Client, bool) {
	palancer := s.Balancer()
	switch palancer {
	case pb.NodeMeta_HASHRING:
		if len(key) < 1 {
			return nil, false
		}

		return s.GetClientByHashRing(key[0])
	case pb.NodeMeta_ROUNDROBIN:
		return s.GetClientByRoundRobin()
	default:
	}
	return s.GetClientByRandom()
}

func (s *LocalService) GetClientByHashRing(key string) (Client, bool) {
	size := s.Count()
	if size < 1 {
		return nil, false
	}

	s.RLock()
	if s.hashring == nil {
		s.RUnlock()
		return nil, false
	}

	node, ok := s.hashring.GetNode(key)
	if !ok {
		s.RUnlock()
		return nil, false
	}

	client := s.clients[s.clientsMap[node]]
	s.RUnlock()
	return client, true
}

func (s *LocalService) GetClientByRoundRobin() (Client, bool) {
	size := s.Count()
	if size < 1 {
		return nil, false
	}

	s.roundRobin.CompareAndSwap(math.MaxInt32, 0)
	rb := s.roundRobin.Add(1)
	s.RLock()
	client := s.clients[int(rb)%len(s.clients)]
	s.RUnlock()
	return client, true
}

func (s *LocalService) GetClientByName(name string) (Client, bool) {
	s.RLock()
	idx, ok := s.clientsMap[name]
	if !ok {
		s.RUnlock()
		return nil, false
	}

	client := s.clients[idx]
	s.RUnlock()
	return client, true
}

func (s *LocalService) GetClients() []Client {
	clients := make([]Client, len(s.clients))
	s.RLock()
	copy(clients, s.clients)
	s.RUnlock()
	return clients
}

func (s *LocalService) GetClientByRandom() (Client, bool) {
	size := s.Count()
	if size < 1 {
		return nil, false
	}

	weight := s.weight.Load()
	if weight <= 0 || size == 1 {
		idx := 0
		if size > 1 {
			idx = Rand().Intn(int(size))
		}
		s.RLock()
		client := s.clients[idx]
		s.RUnlock()
		return client, true
	}

	var (
		begin        float64
		currentRatio float64
	)
	rnd := Rand().Float64()
	s.RLock()
	for _, v := range s.clients {
		currentRatio = float64(v.Weight()) / float64(weight)
		if rnd > begin && rnd <= begin+currentRatio {
			s.RUnlock()
			return v, true
		}
		begin += currentRatio
	}
	s.RUnlock()
	return nil, false
}

func (s *LocalService) Do(ctx context.Context, msg *pb.Peer_Request) (*pb.Peer_ResponseWriter, error) {
	client, ok := s.GetClientByBalancer()
	if !ok {
		return nil, ErrNotConnected
	}
	return client.Do(ctx, msg)
}

func (s *LocalService) Send(msg *pb.Peer_Request) error {
	client, ok := s.GetClientByBalancer()
	if !ok {
		return ErrNotConnected
	}

	if !client.AllowStream() {
		return ErrUnsupported
	}

	return client.Send(msg)
}

func (s *LocalService) AddClient(client Client) {
	s.Lock()
	i, ok := s.clientsMap[client.Name()]
	if ok {
		s.clients[i] = client
	} else {
		s.clients = append(s.clients, client)
		s.clientsMap[client.Name()] = len(s.clients) - 1
		s.count.Inc()
	}
	s.Unlock()
	s.ResetBalancer()
}

func (s *LocalService) RemoveClient(name string) {
	s.Lock()
	_, ok := s.clientsMap[name]
	if !ok {
		s.Unlock()
		return
	}

	size := len(s.clients)
	newClients := make([]Client, 0, size)
	newClientsMap := make(map[string]int)
	if size-1 < 1 {
		s.clients = newClients
		s.clientsMap = newClientsMap
		s.hashring = nil
		s.Unlock()
		s.balancer.Store(0)
		s.count.Store(0)
		s.weight.Store(0)
		s.roundRobin.Store(0)
		return
	}

	oldClients := make([]Client, 0)
	for _, v := range s.clients {
		if v.Name() == name {
			oldClients = append(oldClients, v)
			continue
		}
		newClients = append(newClients, v)
		newClientsMap[v.Name()] = len(newClients) - 1
	}

	s.clients = newClients
	s.clientsMap = newClientsMap
	s.Unlock()
	s.count.Dec()
	s.ResetBalancer()
	// close
	for _, v := range oldClients {
		v.Close()
	}
}

func (s *LocalService) ResetBalancer() {
	balancer := make(map[pb.NodeMeta_Balancer]int)
	weights := make(map[string]int)
	var weight int32
	s.Lock()
	for _, v := range s.clients {
		weights[v.Name()] = int(v.Weight())
		balancer[v.Balancer()]++
		weight += v.Weight()
	}

	s.hashring = hashring.NewWithWeights(weights)
	s.Unlock()
	s.weight.Store(weight)

	currentNumber := 0
	currentBalancer := s.Balancer()
	for k, v := range balancer {
		if currentNumber == 0 || currentNumber > v {
			currentNumber = v
			currentBalancer = k
		}
	}
	s.balancer.Store(int64(currentBalancer))
}
