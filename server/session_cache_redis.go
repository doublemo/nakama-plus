package server

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	sessionValidationTTL  = 3 * time.Second
	sessionRedisTimeout   = 100 * time.Millisecond
	sessionLocalMaxUsers  = 16384
	sessionCacheMaxTokens = 32
)

var (
	// The Redis bucket starts aging before the database read. The fence
	// prevents an in-flight read from recreating an invalidated bucket.
	sessionRedisLookup = redis.NewScript(`
local fence = redis.call("HGET", KEYS[1], "fence")
local ttl = redis.call("PTTL", KEYS[1])
if not fence or ttl <= 0 then
	redis.call("DEL", KEYS[1])
	redis.call("HSET", KEYS[1], "fence", ARGV[2])
	redis.call("PEXPIRE", KEYS[1], ARGV[3])
	return {"", ARGV[2], tonumber(ARGV[3])}
end
return {redis.call("HGET", KEYS[1], ARGV[1]) or "", fence, ttl}
`)
	sessionRedisFill = redis.NewScript(`
if redis.call("HGET", KEYS[1], "fence") == ARGV[1]
	and redis.call("PTTL", KEYS[1]) > 0
	and redis.call("HLEN", KEYS[1]) <= tonumber(ARGV[4]) then
	return redis.call("HSET", KEYS[1], ARGV[2], ARGV[3])
end
return 0
`)
	sessionRedisInvalidate = redis.NewScript(`
redis.call("DEL", KEYS[1])
return redis.call("PUBLISH", ARGV[1], ARGV[2])
`)
)

type sessionValidationEntry struct {
	valid    bool
	deadline time.Time
}

type sessionLocalBucket struct {
	deadline time.Time
	entries  map[string]sessionValidationEntry
}

type redisSessionCache struct {
	SessionCache
	logger  *zap.Logger
	client  redis.UniversalClient
	prefix  string
	channel string
	cancel  context.CancelFunc
	done    chan struct{}
	retryAt atomic.Int64

	mu    sync.Mutex
	local map[uuid.UUID]*sessionLocalBucket
}

func NewRedisSessionCache(client Redis, logger *zap.Logger, source SessionCache) SessionCache {
	ctx, cancel := context.WithCancel(client.Context())
	s := &redisSessionCache{
		SessionCache: source,
		logger:       logger,
		client:       client.DB(),
		prefix:       client.Namer("session-validation", "v1", "user") + ":",
		channel:      client.EventChannel("session-validation", "v1", "invalidate"),
		cancel:       cancel,
		done:         make(chan struct{}),
		local:        make(map[uuid.UUID]*sessionLocalBucket),
	}
	go s.run(ctx)
	return s
}

func (s *redisSessionCache) Stop() {
	s.cancel()
	<-s.done
	s.SessionCache.Stop()
}

func (s *redisSessionCache) validate(ctx context.Context, userID uuid.UUID, token sessionTokenRef, refresh bool) (bool, error) {
	field := strconv.FormatInt(token.version, 10) + ":" + strconv.FormatBool(refresh) + ":" + token.id
	now := time.Now()
	bucket, entry := s.localLookup(userID, field, now)
	if time.Now().Before(entry.deadline) {
		return entry.valid, nil
	}

	key := s.prefix + userID.String()
	var fence, cached string
	redisStarted := time.Now()
	var ttlMillis int64
	if redisStarted.UnixNano() >= s.retryAt.Load() {
		reply, err := s.eval(ctx, sessionRedisLookup, key, field, uuid.Must(uuid.NewV4()).String(), sessionValidationTTL.Milliseconds()).Slice()
		if err == nil && len(reply) == 3 {
			cached, _ = reply[0].(string)
			fence, _ = reply[1].(string)
			ttlMillis, _ = reply[2].(int64)
		} else if err != nil && ctx.Err() == nil {
			s.retryAt.Store(time.Now().Add(500 * time.Millisecond).UnixNano())
		}
	}
	// Include the Redis round trip in the age of a copied entry. Reading or
	// copying a cache hit never extends the original window.
	redisDeadline := redisStarted.Add(time.Duration(ttlMillis) * time.Millisecond)
	if (cached == "1" || cached == "0") && ttlMillis > 0 && ttlMillis <= sessionValidationTTL.Milliseconds() && time.Now().Before(redisDeadline) {
		valid := cached == "1"
		s.localFill(userID, bucket, field, sessionValidationEntry{valid, redisDeadline})
		return valid, nil
	}

	dbStarted := time.Now()
	valid, err := s.SessionCache.validate(ctx, userID, token, refresh)
	if err != nil {
		return false, err
	}
	if fence != "" && time.Now().Before(redisDeadline) {
		value := "0"
		if valid {
			value = "1"
		}
		_ = s.eval(ctx, sessionRedisFill, key, fence, field, value, sessionCacheMaxTokens).Err()
	}
	s.localFill(userID, bucket, field, sessionValidationEntry{valid, dbStarted.Add(sessionValidationTTL)})
	return valid, nil
}

func (s *redisSessionCache) issue(ctx context.Context, userID uuid.UUID, replace bool) (int64, error) {
	version, err := s.SessionCache.issue(ctx, userID, replace)
	if err == nil && replace {
		s.invalidate(ctx, []uuid.UUID{userID})
	}
	return version, err
}

func (s *redisSessionCache) revoke(ctx context.Context, userID uuid.UUID, access, refresh sessionTokenRef) error {
	if err := s.SessionCache.revoke(ctx, userID, access, refresh); err != nil {
		return err
	}
	s.invalidate(ctx, []uuid.UUID{userID})
	return nil
}

func invalidateSessionValidation(ctx context.Context, cache SessionCache, userIDs []uuid.UUID) {
	if cache, ok := cache.(*redisSessionCache); ok {
		cache.invalidate(ctx, userIDs)
	}
}

func (s *redisSessionCache) invalidate(ctx context.Context, userIDs []uuid.UUID) {
	if len(userIDs) == 0 {
		return
	}
	s.mu.Lock()
	for _, id := range userIDs {
		delete(s.local, id)
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionRedisTimeout)
	defer cancel()
	// Each script declares one key so Redis Cluster can route it to its slot.
	_, err := s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, id := range userIDs {
			id := id.String()
			sessionRedisInvalidate.Eval(ctx, pipe, []string{s.prefix + id}, s.channel, id)
		}
		return nil
	})
	if err != nil {
		s.logger.Warn("Session cache invalidation will rely on expiry.", zap.Error(err))
	}
}

func (s *redisSessionCache) eval(ctx context.Context, script *redis.Script, key string, args ...any) *redis.Cmd {
	ctx, cancel := context.WithTimeout(ctx, sessionRedisTimeout)
	defer cancel()
	return script.Run(ctx, s.client, []string{key}, args...)
}

func (s *redisSessionCache) localLookup(userID uuid.UUID, field string, now time.Time) (*sessionLocalBucket, sessionValidationEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if bucket := s.local[userID]; bucket != nil {
		if now.Before(bucket.deadline) {
			return bucket, bucket.entries[field]
		}
		delete(s.local, userID)
	}
	if len(s.local) >= sessionLocalMaxUsers {
		return nil, sessionValidationEntry{}
	}
	bucket := &sessionLocalBucket{deadline: now.Add(sessionValidationTTL)}
	s.local[userID] = bucket
	return bucket, sessionValidationEntry{}
}

func (s *redisSessionCache) localFill(userID uuid.UUID, bucket *sessionLocalBucket, field string, entry sessionValidationEntry) {
	if bucket == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.local[userID] != bucket || !time.Now().Before(bucket.deadline) {
		return
	}
	if bucket.entries == nil {
		bucket.entries = make(map[string]sessionValidationEntry)
	}
	if _, exists := bucket.entries[field]; exists || len(bucket.entries) < sessionCacheMaxTokens {
		bucket.entries[field] = entry
	}
}

func (s *redisSessionCache) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(sessionValidationTTL)
	defer ticker.Stop()
	subscribeCtx, cancel := context.WithTimeout(ctx, sessionRedisTimeout)
	sub := s.client.Subscribe(subscribeCtx, s.channel)
	cancel()
	defer sub.Close()
	updates := sub.ChannelWithSubscriptions()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-updates:
			if !ok {
				updates = nil
				continue
			}
			switch event := event.(type) {
			case *redis.Subscription:
				s.mu.Lock()
				clear(s.local)
				s.mu.Unlock()
			case *redis.Message:
				if id, err := uuid.FromString(event.Payload); err == nil {
					s.mu.Lock()
					delete(s.local, id)
					s.mu.Unlock()
				}
			}
		case now := <-ticker.C:
			s.mu.Lock()
			for id, bucket := range s.local {
				if !now.Before(bucket.deadline) {
					delete(s.local, id)
				}
			}
			s.mu.Unlock()
		}
	}
}
