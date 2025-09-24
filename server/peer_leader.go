package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/doublemo/nakama-kit/kit"
	"github.com/hashicorp/memberlist"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	// session's TTL in seconds.
	PEERLEADER_SESSION_TTL = 15

	// Grant creates a new lease.
	// Note: This constant is no longer used as we rely on session's built-in keepalive.
	PEERLEADER_GRANT_TTL = 10
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
}

func NewPeerLeader(ctx context.Context, logger *zap.Logger, etcdClient *kit.EtcdClientV3) (*PeerLeader, error) {
	session, err := concurrency.NewSession(etcdClient.GetClient(), concurrency.WithTTL(PEERLEADER_SESSION_TTL))
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

			if err := s.election.Campaign(s.ctx, endpoint.Name()); err != nil {
				if !errors.Is(err, context.Canceled) {
					s.logger.Warn("Campaign failed", zap.Error(err))
				}
				select {
				case <-time.After(time.Second):
				case <-s.ctx.Done():
					return
				}
				continue
			}

			s.logger.Info("Became leader", zap.String("node", endpoint.Name()))
			endpoint.Leader(true)
			s.update(endpoint, memberlist)

			err := s.observeLeadership(endpoint, memberlist)
			if err != nil {
				s.logger.Warn("Lost leadership or observe failed", zap.Error(err))
			} else {
				s.logger.Info("Leadership observation ended normally.")
			}

			if endpoint.Leader() {
				endpoint.Leader(false)
				s.update(endpoint, memberlist)
			}
		}
	}()
}

func (s *PeerLeader) observeLeadership(endpoint Endpoint, memberlist *memberlist.Memberlist) error {
	leaderCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	ch := s.election.Observe(leaderCtx)

	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				s.logger.Warn("Observe closed, resetting leader status")
				return nil
			}

			if len(resp.Kvs) == 0 {
				s.logger.Warn("Received empty Kvs in observe response")
				continue
			}

			val := string(resp.Kvs[0].Value)
			isLeader := val == endpoint.Name()
			wasLeader := endpoint.Leader()
			if isLeader != wasLeader {
				endpoint.Leader(isLeader)
				s.update(endpoint, memberlist)
			}

			s.logger.Info("Leadership update", zap.Bool("isLeader", isLeader))
		case <-s.session.Done():
			s.logger.Warn("Session expired during observe")
			return nil
		case <-s.ctx.Done():
			if endpoint.Leader() {
				resignCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				if resignErr := s.election.Resign(resignCtx); resignErr != nil {
					s.logger.Warn("Failed to resign leadership gracefully", zap.Error(resignErr))
				} else {
					s.logger.Info("Resigned leadership gracefully")
				}
			}
			return s.ctx.Err()
		}
	}
}

func (s *PeerLeader) reconnect() {
	backoff := time.Second
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		sess, err := concurrency.NewSession(s.etcdClient.GetClient(), concurrency.WithTTL(PEERLEADER_SESSION_TTL))
		if err != nil {
			s.logger.Warn("Failed to recreate session", zap.Error(err))
			select {
			case <-time.After(backoff):
			case <-s.ctx.Done():
				return
			}

			backoff *= 2
			if backoff > time.Minute {
				backoff = time.Minute
			}
			continue
		}
		s.session = sess
		s.election = concurrency.NewElection(sess, s.electionKey)
		s.logger.Info("Session reconnected successfully")
		return
	}
}

func (s *PeerLeader) update(endpoint Endpoint, memberlist *memberlist.Memberlist) {
	md, err := endpoint.MarshalJSON()
	if err != nil {
		s.logger.Warn("Failed to marshal metadata", zap.Error(err))
		return
	}

	if s.etcdClient == nil {
		s.logger.Error("etcdClient is nil, cannot Update metadata", zap.String("node", endpoint.Name()))
		return
	}

	if err := s.etcdClient.Update(endpoint.Name(), string(md)); err != nil {
		s.logger.Warn("Failed to update service", zap.Error(err))
	}

	memberlist.UpdateNode(time.Second)
}

func (s *PeerLeader) Stop() {
	s.once.Do(func() {
		if s.cancel == nil {
			return
		}
		_ = s.session.Close()
		s.cancel()
	})
}
