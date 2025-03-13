package server

import (
	"context"
	"strings"
	"sync"

	"github.com/doublemo/nakama-kit/kit"
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
}

func NewPeerLeader(ctx context.Context, logger *zap.Logger, etcdClient *kit.EtcdClientV3) (*PeerLeader, error) {
	// 创建 Session（租约自动续期）
	session, err := concurrency.NewSession(etcdClient.GetClient(), concurrency.WithTTL(10))
	if err != nil {
		return nil, err
	}

	electionKey := "/nakama-plus/leader"
	servicePrefix := strings.Split(strings.Trim(etcdClient.ServicePrefix(), "/"), "/")
	if len(servicePrefix) >= 2 {
		electionKey = strings.Join(servicePrefix[0:len(servicePrefix)-1], "/") + "/leader"
	}

	// 创建选举对象
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

func (s *PeerLeader) Run(endpoint Endpoint) {
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				err := s.election.Campaign(s.ctx, endpoint.Name())
				if err != nil {
					s.logger.Info("Failed in then campaign for leader", zap.String("node", endpoint.Name()))
					continue
				}

				endpoint.Leader(true)
				s.update(endpoint)
				s.logger.Info("The current node has become the leader", zap.String("node", endpoint.Name()))

				// Leader 保活监控
				select {
				case <-s.session.Done():
					// 租约失效，退出 Leader 状态
					endpoint.Leader(false)
					s.update(endpoint)
					s.logger.Info("The current node has lost its Leader status", zap.String("node", endpoint.Name()))
				case <-s.ctx.Done():
					return
				}
			}
		}
	}()
}

func (s *PeerLeader) update(endpoint Endpoint) {
	md, err := endpoint.MarshalJSON()
	if err != nil {
		s.logger.Warn("Failed to marshal metadata", zap.Error(err))
	} else {
		if err := s.etcdClient.Update(endpoint.Name(), string(md)); err != nil {
			s.logger.Warn("Failed to upate service", zap.Error(err))
		}
	}
}

// Stop 停止选举并释放资源
func (s *PeerLeader) Stop() {
	s.once.Do(func() {
		if s.cancel == nil {
			return
		}

		s.cancel()
		_ = s.session.Close()
	})
}
