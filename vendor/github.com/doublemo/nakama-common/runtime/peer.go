package runtime

import (
	"context"
	"time"

	"github.com/doublemo/nakama-common/api"
)

type (
	PeerCacheOption  func(o *PeerCacheOptions)
	PeerCacheOptions struct {
		SynchronousSet            bool
		Cost                      int64
		Expiration                time.Duration
		Tags                      []string
		ClientSideCacheExpiration time.Duration
		MemoryOnly                bool
	}
	PeerCacher interface {
		Get(key string, options ...PeerCacheOption) (any, error)
		Set(key string, value any, options ...PeerCacheOption) error
		Delete(key string) error
		Invalidate(tags ...string) error
		Clear()
	}

	Peer interface {
		// InvokeMS invoke microservice
		InvokeMS(ctx context.Context, in *api.AnyRequest) (*api.AnyResponseWriter, error)

		// Send
		SendMS(ctx context.Context, in *api.AnyRequest) error

		//Event
		Event(ctx context.Context, in *api.AnyRequest, names ...string) error

		//Cacher
		GetCacher() *PeerCacher
	}
)

func WithMemoryOnly(b bool) PeerCacheOption {
	return func(o *PeerCacheOptions) {
		o.MemoryOnly = b
	}
}

// WithCost allows setting the memory capacity used by the item when setting a value.
// Actually it seems to be used by Ristretto library only.
func WithCost(cost int64) PeerCacheOption {
	return func(o *PeerCacheOptions) {
		o.Cost = cost
	}
}

// WithSynchronousSet allows setting the behavior when setting a value, whether to wait until all buffered writes have been applied or not.
// Currently to be used by Ristretto library only.
func WithSynchronousSet() PeerCacheOption {
	return func(o *PeerCacheOptions) {
		o.SynchronousSet = true
	}
}

// WithExpiration allows to specify an expiration time when setting a value.
func WithExpiration(expiration time.Duration) PeerCacheOption {
	return func(o *PeerCacheOptions) {
		o.Expiration = expiration
	}
}

// WithTags allows to specify associated tags to the current value.
func WithTags(tags []string) PeerCacheOption {
	return func(o *PeerCacheOptions) {
		o.Tags = tags
	}
}

// WithClientSideCaching allows setting the client side caching, enabled by default
// Currently to be used by Rueidis(redis) library only.
func WithClientSideCaching(clientSideCacheExpiration time.Duration) PeerCacheOption {
	return func(o *PeerCacheOptions) {
		o.ClientSideCacheExpiration = clientSideCacheExpiration
	}
}
