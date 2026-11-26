package server

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
)

type sessionValidationTestSource struct {
	SessionCache
	queries atomic.Int64
	revoked atomic.Bool
	read    func(bool)
}

func (s *sessionValidationTestSource) Stop() {}

func (s *sessionValidationTestSource) validate(_ context.Context, _ uuid.UUID, _ sessionTokenRef, _ bool) (bool, error) {
	valid := !s.revoked.Load()
	s.queries.Add(1)
	if s.read != nil {
		s.read(valid)
	}
	return valid, nil
}

func (s *sessionValidationTestSource) revoke(_ context.Context, _ uuid.UUID, _, _ sessionTokenRef) error {
	s.revoked.Store(true)
	return nil
}

func newSessionTestRedis(t *testing.T, ctx context.Context, url, prefix string) Redis {
	t.Helper()
	opt, err := redis.ParseURL(url)
	require.NoError(t, err)
	cfg := NewRedisConfig()
	cfg.Addresses = []string{opt.Addr}
	cfg.Username, cfg.Password, cfg.DB = opt.Username, opt.Password, opt.DB
	cfg.AllowSSL = opt.TLSConfig != nil
	cfg.ClusterMode = os.Getenv("TEST_REDIS_CLUSTER") == "1"
	cfg.Prefix = prefix
	client, err := NewLocalRedisClient(ctx, zap.NewNop(), cfg)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func TestRedisDatabaseConfig(t *testing.T) {
	logger := zap.NewNop()
	require.False(t, NewConfig(logger).GetDatabase().Redis.Enabled)
	path := filepath.Join(t.TempDir(), "redis.yml")
	require.NoError(t, os.WriteFile(path, []byte(`
database:
  redis:
    enabled: true
    address: ["redis:6380"]
    password: redis-test-secret
    sentinel_password: sentinel-test-secret
    prefix: demo
    dial_timeout: 250ms
`), 0600))
	config := ParseArgs(logger, []string{"nakama", "--config", path, "--database.redis.db", "2"})
	cfg := config.GetDatabase().Redis
	require.NoError(t, cfg.Validate())
	require.True(t, cfg.Enabled)
	require.True(t, cfg.ContextTimeoutEnabled)
	require.Equal(t, "demo", cfg.Prefix)
	require.Equal(t, 2, cfg.DB)
	require.Equal(t, 250*time.Millisecond, cfg.DialTimeout)
	clone, err := config.Clone()
	require.NoError(t, err)
	clone.GetDatabase().Redis.Addresses[0] = "elsewhere:6379"
	require.Equal(t, []string{"redis:6380"}, cfg.Addresses)

	console := &ConsoleServer{logger: logger, config: config}
	shown, err := console.GetConfig(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotContains(t, shown.Config, "redis-test-secret")
	require.NotContains(t, shown.Config, "sentinel-test-secret")
	require.Equal(t, "redis-test-secret", cfg.Password)
}

func TestRedisSessionCacheFallsBackOnConnectionError(t *testing.T) {
	source := &sessionValidationTestSource{}
	s := &redisSessionCache{
		SessionCache: source,
		logger:       zap.NewNop(),
		client: redis.NewClient(&redis.Options{MaxRetries: -1, Dialer: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("redis unavailable")
		}}),
		local: make(map[uuid.UUID]*sessionLocalBucket),
	}
	defer s.client.Close()
	ctx := context.Background()
	userID := uuid.Must(uuid.NewV4())
	token := sessionTokenRef{id: "token"}
	valid, err := s.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.True(t, valid)
	_, err = s.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, source.queries.Load())

	require.NoError(t, s.revoke(ctx, userID, token, sessionTokenRef{}))
	valid, err = s.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.False(t, valid)
	require.EqualValues(t, 2, source.queries.Load())
}

func TestRedisSessionCacheAcrossNodes(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("set TEST_REDIS_URL to a Redis endpoint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	logger := zap.NewNop()
	userID := uuid.Must(uuid.NewV4())
	token := sessionTokenRef{id: "token", version: 1}
	namespace := "test-" + uuid.Must(uuid.NewV4()).String()
	source := &sessionValidationTestSource{}
	nodeA := NewRedisSessionCache(newSessionTestRedis(t, ctx, url, namespace), logger, source).(*redisSessionCache)
	defer nodeA.Stop()
	nodeB := NewRedisSessionCache(newSessionTestRedis(t, ctx, url, namespace), logger, source).(*redisSessionCache)
	defer nodeB.Stop()
	require.Eventually(t, func() bool {
		counts, err := nodeA.client.PubSubNumSub(ctx, nodeA.channel).Result()
		return err == nil && counts[nodeA.channel] == 2
	}, 2*time.Second, 10*time.Millisecond)

	valid, err := nodeA.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.True(t, valid)
	valid, err = nodeB.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.True(t, valid)
	require.EqualValues(t, 1, source.queries.Load(), "Redis should serve the second node")
	require.NoError(t, nodeA.revoke(ctx, userID, token, sessionTokenRef{}))
	require.Eventually(t, func() bool {
		valid, err := nodeB.validate(ctx, userID, token, false)
		return err == nil && !valid
	}, time.Second, 10*time.Millisecond)

	// Emulate a durable mutation whose invalidation failed. A local cache
	// populated near Redis expiry may only inherit the remaining lifetime.
	require.NoError(t, nodeA.revoke(ctx, userID, token, sessionTokenRef{}))
	source.revoked.Store(false)
	valid, err = nodeA.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.True(t, valid)
	err = nodeA.client.PExpire(ctx, nodeA.prefix+userID.String(), 350*time.Millisecond).Err()
	require.NoError(t, err)
	source.revoked.Store(true)

	nodeC := NewRedisSessionCache(newSessionTestRedis(t, ctx, url, namespace), logger, source).(*redisSessionCache)
	defer nodeC.Stop()
	valid, err = nodeC.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.True(t, valid)
	require.Eventually(t, func() bool {
		valid, err := nodeC.validate(ctx, userID, token, false)
		return err == nil && !valid
	}, 2*time.Second, 20*time.Millisecond)
}

func TestRedisSessionCacheDoesNotRestoreInvalidatedBucket(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("set TEST_REDIS_URL to a Redis endpoint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	var started atomic.Bool
	source := &sessionValidationTestSource{read: func(valid bool) {
		if valid && started.CompareAndSwap(false, true) {
			close(readStarted)
			select {
			case <-releaseRead:
			case <-ctx.Done():
			}
		}
	}}
	namespace := uuid.Must(uuid.NewV4()).String()
	cache := NewRedisSessionCache(newSessionTestRedis(t, ctx, url, namespace), zap.NewNop(), source).(*redisSessionCache)
	defer cache.Stop()
	userID := uuid.Must(uuid.NewV4())
	token := sessionTokenRef{id: "token"}
	oldReadDone := make(chan struct{})
	go func() {
		defer close(oldReadDone)
		_, _ = cache.validate(ctx, userID, token, false)
	}()
	select {
	case <-readStarted:
	case <-ctx.Done():
		t.Fatal("validation never reached the source")
	}
	require.NoError(t, cache.revoke(ctx, userID, token, sessionTokenRef{}))
	reader := NewRedisSessionCache(newSessionTestRedis(t, ctx, url, namespace), zap.NewNop(), source)
	defer reader.Stop()
	valid, err := reader.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.False(t, valid)
	close(releaseRead)
	<-oldReadDone

	// Inspect Redis directly so a correct local result cannot mask a stale fill.
	field := "0:false:token"
	reply, err := cache.eval(ctx, sessionRedisLookup, cache.prefix+userID.String(), field, "unused", sessionValidationTTL.Milliseconds()).Slice()
	require.NoError(t, err)
	require.Equal(t, "0", reply[0])
}

func TestRedisSessionCacheWithDatabase(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" || os.Getenv("TEST_DB_URL") == "" {
		t.Skip("set TEST_REDIS_URL and TEST_DB_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dbA, dbB := NewDB(t), NewDB(t)
	defer dbA.Close()
	defer dbB.Close()
	logger := zap.NewNop()
	config := NewConfig(logger)
	userID := uuid.Must(uuid.NewV4())
	InsertUser(t, dbA, userID)
	defer func() { _, _ = dbA.Exec("DELETE FROM users WHERE id = $1", userID) }()
	namespace := "db-test-" + userID.String()
	shared := newSessionTestRedis(t, ctx, url, namespace)
	cacheA := NewRedisSessionCache(shared, logger, NewDatabaseSessionCache(dbA))
	defer cacheA.Stop()
	cacheB := NewRedisSessionCache(newSessionTestRedis(t, ctx, url, namespace), logger, NewDatabaseSessionCache(dbB))
	defer cacheB.Stop()
	issuedAt := time.Now().Unix()
	signed, exp := generateToken(config, "selective-token", issuedAt, 0, userID.String(), "user", nil)
	token := sessionTokenRef{id: "selective-token", expiry: exp}
	valid, err := cacheB.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.True(t, valid)
	require.NoError(t, SessionLogout(ctx, config, cacheA, userID, signed, ""))
	require.Eventually(t, func() bool {
		valid, err := cacheB.validate(ctx, userID, token, false)
		return err == nil && !valid
	}, 4*time.Second, 10*time.Millisecond)

	token.id = "another-session"
	valid, err = cacheB.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.True(t, valid)
	require.NoError(t, BanUsers(ctx, logger, dbA, config, cacheA, &testSessionRegistry{}, &testTracker{}, []uuid.UUID{userID}))
	require.NoError(t, UnbanUsers(ctx, logger, dbB, cacheB, []uuid.UUID{userID}))
	require.Eventually(t, func() bool {
		valid, err := cacheB.validate(ctx, userID, token, false)
		return err == nil && !valid
	}, 4*time.Second, 10*time.Millisecond)
	version, err := cacheB.issue(ctx, userID, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, version)
	token.version = version
	valid, err = cacheA.validate(ctx, userID, token, false)
	require.NoError(t, err)
	require.True(t, valid)
	cacheA.Stop()
	require.NoError(t, shared.DB().Ping(ctx).Err())
}
