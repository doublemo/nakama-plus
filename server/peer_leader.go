package server

import (
	"context"
	"strings"
	"sync"

	"github.com/doublemo/nakama-kit/kit"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type PeerLeader struct {
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *zap.Logger
	session     *concurrency.Session
	election    *concurrency.Election
	isLeader    *atomic.Bool
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
		isLeader:    atomic.NewBool(false),
		electionKey: electionKey,
	}, nil
}

func (s *PeerLeader) Run(node string) {
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				// 尝试成为 Leader
				err := s.election.Campaign(s.ctx, node)
				if err != nil {
					s.logger.Info("Failed in then campaign for leader", zap.String("node", node))
					continue
				}

				// 成为 Leader 后的逻辑
				s.isLeader.Store(true)
				s.logger.Info("The current node has become the leader", zap.String("node", node))

				// Leader 保活监控
				select {
				case <-s.session.Done():
					// 租约失效，退出 Leader 状态
					s.isLeader.Store(false)
					s.logger.Info("The current node has lost its Leader status", zap.String("node", node))
				case <-s.ctx.Done():
					return
				}
			}
		}
	}()
}

// IsLeader 检查当前节点是否为 Leader
func (s *PeerLeader) IsLeader() bool {
	return s.isLeader.Load()
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
