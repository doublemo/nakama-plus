// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"context"
	"sync"

	"github.com/doublemo/nakama-kit/pb"
	"go.uber.org/zap"
)

const SERVICE_NAME = "bombus"

type (
	ServiceRegistry interface {
		Shutdown()
		Get(role string) (Service, bool)
		Register(md *pb.NodeMeta, handler ...ClientRecvHandler) (Client, error)
		Deregister(role string, names ...string)
		Delete(service Service, names ...string)
		Range(fn func(key string, value Service) bool)
	}

	LocalServiceRegistry struct {
		ctx         context.Context
		ctxCancelFn context.CancelFunc
		logger      *zap.Logger
		services    *MapOf[string, Service]
		node        string
		once        sync.Once
	}
)

func NewLocalServiceRegistry(ctx context.Context, logger *zap.Logger, node string) ServiceRegistry {
	ctx, cancel := context.WithCancel(ctx)
	s := &LocalServiceRegistry{
		ctx:         ctx,
		ctxCancelFn: cancel,
		logger:      logger,
		services:    &MapOf[string, Service]{},
		node:        node,
	}
	return s
}

func (s *LocalServiceRegistry) Shutdown() {
	s.once.Do(func() {
		if s.ctxCancelFn != nil {
			return
		}

		s.services.Range(func(key string, value Service) bool {
			value.Stop()
			s.logger.Debug("PeerService shutdown", zap.String("name", key))
			return true
		})

		s.ctxCancelFn()
	})
}

func (s *LocalServiceRegistry) Register(md *pb.NodeMeta, handler ...ClientRecvHandler) (Client, error) {
	services, readyed := s.services.LoadOrStore(md.Role, NewLocalService(s.ctx, s.logger, md.Name, md.Role))
	if readyed {
		client, ok := services.GetClientByName(md.Name)
		if ok {
			oldmd := client.Metadata()
			if oldmd.Ip == md.Ip && oldmd.Port == md.Port && oldmd.AllowStream == md.AllowStream {
				client.SetMetadata(md)
				services.ResetBalancer()
				return client, nil
			}
			services.RemoveClient(md.Name)
		}
	}

	client := NewLocalClient(s.ctx, s.logger, md, handler...)
	services.AddClient(client)
	return client, nil
}

func (s *LocalServiceRegistry) Deregister(role string, names ...string) {
	m, ok := s.services.Load(role)
	if !ok {
		return
	}

	if len(names) < 1 {
		m.Stop()
		s.services.Delete(role)
		return
	}

	for _, name := range names {
		m.RemoveClient(name)
	}

	if m.Count() < 1 {
		s.services.Delete(role)
	}
}

func (s *LocalServiceRegistry) Delete(service Service, names ...string) {
	if len(names) < 1 {
		service.Stop()
		s.services.Delete(service.Role())
		return
	}

	for _, name := range names {
		service.RemoveClient(name)
	}

	if service.Count() < 1 {
		s.services.Delete(service.Name())
	}
}

func (s *LocalServiceRegistry) Get(role string) (Service, bool) {
	return s.services.Load(role)
}

func (s *LocalServiceRegistry) Range(fn func(key string, value Service) bool) {
	s.services.Range(fn)
}
