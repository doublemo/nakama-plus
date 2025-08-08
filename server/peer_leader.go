package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/doublemo/nakama-kit/kit"
	"github.com/hashicorp/memberlist"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type PeerLeader struct {
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *zap.Logger
	session     *concurrency.Session
	election    *concurrency.Election
	etcdClient  *kit.EtcdClientV3
	electionKey string
	once        sync.Once

	resumeKey string
	resumeRev int64
}

func NewPeerLeader(ctx context.Context, logger *zap.Logger, etcdClient *kit.EtcdClientV3) (*PeerLeader, error) {
	session, err := concurrency.NewSession(etcdClient.GetClient(), concurrency.WithTTL(10))
	if err != nil {
		return nil, err
	}

	electionKey := "/nakama-plus/leader"
	servicePrefix := strings.Split(strings.Trim(etcdClient.ServicePrefix(), "/"), "/")
	if len(servicePrefix) >= 2 {
		electionKey = "/" + strings.Join(servicePrefix[0:len(servicePrefix)-1], "/") + "/leader"
	}

	election := concurrency.NewElection(session, electionKey)
	ctx, cancel := context.WithCancel(ctx)
	return &PeerLeader{
		ctx:         ctx,
		cancel:      cancel,
		logger:      logger,
		session:     session,
		election:    election,
		electionKey: electionKey,
		etcdClient:  etcdClient,
	}, nil
}

func (s *PeerLeader) Run(endpoint Endpoint, memberlist *memberlist.Memberlist) {
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-s.session.Done():
				s.reconnect()
				continue
			default:
			}

			if s.resumeKey != "" {
				s.election = concurrency.ResumeElection(s.session, s.electionKey, s.resumeKey, s.resumeRev)
				err := s.observeLeadership(endpoint, memberlist)
				if err != nil {
					s.logger.Warn("Failed to resume leadership", zap.Error(err))
					s.resumeKey = ""
					s.resumeRev = 0
				}
				continue
			}

			if err := s.election.Campaign(s.ctx, endpoint.Name()); err != nil {
				s.logger.Warn("Campaign failed", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			s.resumeKey = s.election.Key()
			s.resumeRev = s.election.Header().Revision
			endpoint.Leader(true)
			s.update(endpoint, memberlist)
			s.logger.Info("Became leader", zap.String("node", endpoint.Name()))

			err := s.observeLeadership(endpoint, memberlist)
			if err != nil {
				s.logger.Warn("Lost leadership", zap.Error(err))
				endpoint.Leader(false)
				s.update(endpoint, memberlist)
				s.resumeKey = ""
				s.resumeRev = 0
			}
		}
	}()

	go s.heartbeat()
}

func (s *PeerLeader) observeLeadership(endpoint Endpoint, memberlist *memberlist.Memberlist) error {
	leaderCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	ch := s.election.Observe(leaderCtx)

	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				endpoint.Leader(false)
				s.update(endpoint, memberlist)
				s.logger.Warn("Observe closed, resetting leader status")
				return nil
			}
			val := string(resp.Kvs[0].Value)
			isLeader := val == endpoint.Name()
			endpoint.Leader(isLeader)
			s.update(endpoint, memberlist)
			s.logger.Info("Leadership update", zap.Bool("isLeader", isLeader))
		case <-s.session.Done():
			endpoint.Leader(false)
			s.update(endpoint, memberlist)
			s.logger.Warn("Session expired during observe")
			return nil
		case <-s.ctx.Done():
			if endpoint.Leader() {
				_ = s.election.Resign(context.Background())
			}
			return nil
		}
	}
}

func (s *PeerLeader) reconnect() {
	s.resumeKey = ""
	backoff := time.Second
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		sess, err := concurrency.NewSession(s.etcdClient.GetClient(), concurrency.WithTTL(10))
		if err != nil {
			s.logger.Warn("Failed to recreate session", zap.Error(err))
			time.Sleep(backoff)
			backoff *= 2
			if backoff > time.Minute {
				backoff = time.Minute
			}
			continue
		}
		s.session = sess
		s.election = concurrency.NewElection(sess, s.electionKey)
		s.logger.Info("Session reconnected")
		return
	}
}

func (s *PeerLeader) heartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			resp, err := s.etcdClient.GetClient().Grant(context.Background(), 10)
			if err != nil {
				s.logger.Warn("Heartbeat grant failed", zap.Error(err))
				s.reconnect()
				continue
			}

			_, err = s.etcdClient.GetClient().KeepAliveOnce(context.Background(), clientv3.LeaseID(resp.ID))
			if err != nil {
				s.logger.Warn("Heartbeat keepalive failed", zap.Error(err))
				s.reconnect()
			}
		}
	}
}

func (s *PeerLeader) update(endpoint Endpoint, memberlist *memberlist.Memberlist) {
	md, err := endpoint.MarshalJSON()
	if err != nil {
		s.logger.Warn("Failed to marshal metadata", zap.Error(err))
	} else {
		if s.etcdClient == nil {
			s.logger.Error("etcdClient is nil, cannot Update metadata", zap.String("node", endpoint.Name()))
			return
		}

		if err := s.etcdClient.Update(endpoint.Name(), string(md)); err != nil {
			s.logger.Warn("Failed to update service", zap.Error(err))
		}
	}
	memberlist.UpdateNode(time.Second)
}

func (s *PeerLeader) Stop() {
	s.once.Do(func() {
		if s.cancel == nil {
			return
		}
		s.cancel()
		_ = s.session.Close()
	})
}
