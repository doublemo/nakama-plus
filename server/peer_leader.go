package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/doublemo/nakama-kit/kit"
	"github.com/hashicorp/memberlist"
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
				s.logger.Warn("Session expired, reconnecting")
				s.reconnect()
				continue
			default:
			}

			// 如果之前我们是 leader, 并且 lease 没过期，使用 ResumeElection 恢复之
			if s.resumeKey != "" {
				s.election = concurrency.ResumeElection(s.session, s.electionKey, s.resumeKey, s.resumeRev)
				// 跳过 Campaign，直接进入观察模式
				s.observeLeadership(endpoint, memberlist)
				// observe 返回的话说明 session 再次失效，重试循环
				continue
			}

			if err := s.election.Campaign(s.ctx, endpoint.Name()); err != nil {
				s.logger.Warn("Campaign failed", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			// 成为 leader
			s.resumeKey = s.election.Key()
			s.resumeRev = s.election.Header().Revision
			endpoint.Leader(true)
			s.update(endpoint, memberlist)
			s.logger.Info("Became leader", zap.String("node", endpoint.Name()))

			s.observeLeadership(endpoint, memberlist)
		}
	}()
}

// observeLeadership 维护 leader 状态，session 失效返回后重建重选
func (s *PeerLeader) observeLeadership(endpoint Endpoint, memberlist *memberlist.Memberlist) {
	leaderCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	ch := s.election.Observe(leaderCtx)

	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				// session error 或 reconnect 信号
				endpoint.Leader(false)
				s.update(endpoint, memberlist)
				s.logger.Warn("Observe closed, resetting leader status")
				return
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
			return
		case <-s.ctx.Done():
			if endpoint.Leader() {
				_ = s.election.Resign(context.Background())
			}
			return
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
