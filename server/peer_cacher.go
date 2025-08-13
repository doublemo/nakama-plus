package server

import (
	"context"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/doublemo/nakama-common/runtime"
	store_etcd "github.com/doublemo/nakama-plus/v3/internal/gocache/store/etcd"
	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/marshaler"
	"github.com/eko/gocache/lib/v4/store"
	ristretto_store "github.com/eko/gocache/store/ristretto/v4"
	"github.com/vmihailenco/msgpack/v5"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type (
	PeerCacherConfig struct {
		// NumCounters determines the number of counters (keys) to keep that hold
		// access frequency information. It's generally a good idea to have more
		// counters than the max cache capacity, as this will improve eviction
		// accuracy and subsequent hit ratios.
		NumCounters int64 `yaml:"num_counters" json:"num_counters" usage:"NumCounters determines the number of counters (keys) to keep that hold access frequency information."`

		// MaxCost can be considered as the cache capacity, in whatever units you
		// choose to use.
		MaxCost int64 `yaml:"max_cost" json:"max_cost" usage:"MaxCost can be considered as the cache capacity, in whatever units you choose to use."`

		// BufferItems determines the size of Get buffers.
		//
		// Unless you have a rare use case, using `64` as the BufferItems value
		// results in good performance.
		BufferItems int64 `yaml:"buffer_items" json:"buffer_items" usage:"BufferItems determines the size of Get buffers."`

		// prefix
		Prefix string `yaml:"prefix" json:"prefix" usage:"prefix"`
	}

	PeerCacheValue struct {
		Node  string
		Value any
		Opts  *runtime.PeerCacheOptions
	}

	PeerCacher struct {
		ctx         context.Context
		ctxCancelFn context.CancelFunc

		node           string
		chain          *marshaler.Marshaler
		storeEtcd      *marshaler.Marshaler
		storeRistretto *marshaler.Marshaler
	}
)

func NewPeerCacher(logger *zap.Logger, etcdClient *clientv3.Client, node string, config *PeerCacherConfig) *PeerCacher {
	storeEtcd := store_etcd.NewEtcd(etcdClient, config.Prefix, store.WithExpiration(24*time.Hour*30))
	ristrettoClient, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: config.NumCounters,
		MaxCost:     config.MaxCost,
		BufferItems: config.BufferItems, // number of keys per Get buffer.
	})

	if err != nil {
		logger.Fatal("Failed to create ristretto client", zap.Error(err))
	}
	storeRistretto := ristretto_store.NewRistretto(ristrettoClient)
	ctx, cancel := context.WithCancel(context.Background())
	s := &PeerCacher{
		ctx:         ctx,
		ctxCancelFn: cancel,
		node:        node,
		chain: marshaler.New(cache.NewChain[any](
			cache.New[any](storeRistretto),
			cache.New[any](storeEtcd),
		)),
		storeEtcd:      marshaler.New(storeEtcd),
		storeRistretto: marshaler.New(storeRistretto),
	}

	storeEtcd.OnPut(func(evt *clientv3.Event) {
		var value PeerCacheValue
		if err := msgpack.Unmarshal(evt.Kv.Value, &value); err != nil {
			logger.Error("Failed to unmarshal PeerCacheValue", zap.Error(err))
			return
		}

		if value.Node == node {
			return
		}

		if value.Opts == nil {
			value.Opts = &runtime.PeerCacheOptions{}
		}

		value.Opts.MemoryOnly = true
		s.set(string(evt.Kv.Key), value)
	})

	storeEtcd.OnDelete(func(evt *clientv3.Event) {
		storeRistretto.Delete(context.TODO(), string(evt.Kv.Key))
	})
	return s
}

func (s *PeerCacher) Get(key string, options ...runtime.PeerCacheOption) (any, error) {
	var (
		value PeerCacheValue
		err   error
	)
	if o := s.applyOptions(options...); o.MemoryOnly {
		_, err = s.storeRistretto.Get(s.ctx, key, &value)
	} else {
		_, err = s.chain.Get(s.ctx, key, &value)
	}
	if err != nil {
		return nil, err
	}
	return value.Value, nil
}

func (s *PeerCacher) Set(key string, value any, options ...runtime.PeerCacheOption) error {
	return s.set(key, PeerCacheValue{
		Node:  s.node,
		Value: value,
		Opts:  s.applyOptions(options...),
	})
}

func (s *PeerCacher) set(key string, value PeerCacheValue) error {
	storeOptions := make([]store.Option, 0)
	if o := value.Opts; o != nil {
		if o.ClientSideCacheExpiration.Seconds() > 0 {
			storeOptions = append(storeOptions, store.WithClientSideCaching(o.ClientSideCacheExpiration))
		}

		if o.SynchronousSet {
			storeOptions = append(storeOptions, store.WithSynchronousSet())
		}

		if o.Cost > 0 {
			storeOptions = append(storeOptions, store.WithCost(o.Cost))
		}

		if o.Expiration.Seconds() > 0 {
			storeOptions = append(storeOptions, store.WithExpiration(o.Expiration))
		}

		if len(o.Tags) > 0 {
			storeOptions = append(storeOptions, store.WithTags(o.Tags))
		}

		if o.MemoryOnly {
			return s.storeRistretto.Set(s.ctx, key, value, storeOptions...)
		}
	}
	return s.chain.Set(s.ctx, key, value, storeOptions...)
}

func (s *PeerCacher) Delete(key string) error {
	return s.storeEtcd.Delete(s.ctx, key)
}

func (s *PeerCacher) Invalidate(tags ...string) error {
	return s.chain.Invalidate(s.ctx, store.WithInvalidateTags(tags))
}

func (s *PeerCacher) Clear() {
	s.chain.Clear(s.ctx)
}

func (s *PeerCacher) applyOptions(options ...runtime.PeerCacheOption) *runtime.PeerCacheOptions {
	peerCacheOptions := &runtime.PeerCacheOptions{}
	for _, fn := range options {
		fn(peerCacheOptions)
	}
	return peerCacheOptions
}
