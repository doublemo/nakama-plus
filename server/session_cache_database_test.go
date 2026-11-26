package server

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/doublemo/nakama-common/runtime"
	"github.com/doublemo/nakama-kit/pb"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestSharedSessionAuthenticationFailsClosed(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://localhost/unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cache := NewDatabaseSessionCache(db)
	logger := zap.NewNop()
	config := NewConfig(logger)
	token, _ := generateToken(config, "session-id", time.Now().Unix(), 0, uuid.Must(uuid.NewV4()).String(), "user", nil)

	requestContext := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	_, err = securityInterceptorFunc(logger, config, cache, requestContext, nil, &grpc.UnaryServerInfo{FullMethod: "/nakama.api.Nakama/GetAccount"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable from API authentication, got %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/ws", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	NewSocketWsAcceptor(logger, config, nil, cache, nil, nil, nil, nil, nil, nil, nil, nil)(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected websocket handshake to fail closed, got %d", response.Code)
	}
}

func TestSharedSessionRevocationsAcrossNodes(t *testing.T) {
	if os.Getenv("TEST_DB_URL") == "" {
		t.Skip("set TEST_DB_URL to a migrated database")
	}
	dbA, dbB := NewDB(t), NewDB(t)
	t.Cleanup(func() { _ = dbA.Close(); _ = dbB.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	logger := zap.NewNop()
	config := NewConfig(logger)
	cacheA, cacheB := NewDatabaseSessionCache(dbA), NewDatabaseSessionCache(dbB)
	userID, otherUserID := uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4())
	InsertUser(t, dbA, userID)
	InsertUser(t, dbA, otherUserID)
	t.Cleanup(func() {
		if _, err := dbA.Exec("DELETE FROM users WHERE id = ANY($1::UUID[])", []uuid.UUID{userID, otherUserID}); err != nil {
			t.Error(err)
		}
	})

	issuedAt := time.Now().Unix()
	legacyID, otherID := GenerateString(), GenerateString()
	legacy, _ := generateToken(config, legacyID, issuedAt, 0, userID.String(), "user", nil)
	refresh, _ := generateRefreshToken(config, legacyID, issuedAt, 0, userID.String(), "user", nil)
	otherToken, _ := generateToken(config, otherID, issuedAt, 0, otherUserID.String(), "other", nil)

	check := func(token string, want codes.Code) {
		t.Helper()
		requestContext := metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
		_, err := securityInterceptorFunc(logger, config, cacheB, requestContext, nil, &grpc.UnaryServerInfo{FullMethod: "/nakama.api.Nakama/GetAccount"})
		if status.Code(err) != want {
			t.Fatalf("remote authentication returned %v, want %v", err, want)
		}
	}
	check(legacy, codes.OK)
	if err := SessionLogout(ctx, config, cacheA, userID, legacy, ""); err != nil {
		t.Fatal(err)
	}
	check(legacy, codes.Unauthenticated)
	check(otherToken, codes.OK)

	// An access-only logout preserves refresh but cannot resurrect the revoked ID.
	_, username, vars, tokenID, tokenIssuedAt, version, err := SessionRefresh(ctx, logger, dbB, config, cacheB, refresh)
	if err != nil || tokenID != legacyID || version != 0 {
		t.Fatalf("refresh lost the legacy session identity: %v, version %d", err, version)
	}
	renewed, _ := generateToken(config, tokenID, tokenIssuedAt, version, userID.String(), username, vars)
	check(renewed, codes.Unauthenticated)
	if err := SessionLogout(ctx, config, cacheA, userID, "", refresh); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, _, err := SessionRefresh(ctx, logger, dbB, config, cacheB, refresh); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("remote refresh accepted a revoked token: %v", err)
	}

	siblingID := GenerateString()
	sibling, _ := generateToken(config, siblingID, issuedAt, 0, userID.String(), "user", nil)
	check(sibling, codes.OK)
	if err := SessionLogout(ctx, config, cacheA, userID, "", ""); err != nil {
		t.Fatal(err)
	}
	check(sibling, codes.Unauthenticated)
	version, err = cacheB.issue(ctx, userID, false)
	if err != nil || version != 1 {
		t.Fatalf("new login after logout: version %d, error %v", version, err)
	}
	freshID := GenerateString()
	fresh, _ := generateToken(config, freshID, issuedAt, version, userID.String(), "user", nil)
	check(fresh, codes.OK)

	live := &revocationTestSession{user: userID, token: sessionTokenRef{id: freshID, version: version}, closed: make(chan bool, 1)}
	registry := &revocationTestRegistry{sessions: []Session{live}}
	if err := BanUsers(ctx, logger, dbA, config, cacheA, &testSessionRegistry{}, &testTracker{}, []uuid.UUID{userID}); err != nil {
		t.Fatal(err)
	}
	check(fresh, codes.Unauthenticated)
	if _, err := cacheB.issue(ctx, userID, false); err != errSessionAccountUnavailable {
		t.Fatalf("expected banned account to reject issuance, got %v", err)
	}

	// The receiver has no Presence for this socket, and can still act on a ban.
	peer := &LocalPeer{ctx: ctx, db: dbB, logger: logger, sessionRegistry: registry}
	ban := &pb.BanValue{Values: []string{userID.String(), "bad-uuid"}}
	peer.onBan("node-a", ban)
	select {
	case banned := <-live.closed:
		if !banned {
			t.Fatal("expected a ban notification when closing the socket")
		}
	case <-ctx.Done():
		t.Fatal("remote socket was not disconnected after ban")
	}

	if err := UnbanUsers(ctx, logger, dbA, cacheA, []uuid.UUID{userID}); err != nil {
		t.Fatal(err)
	}
	check(fresh, codes.Unauthenticated)
	version, err = cacheB.issue(ctx, userID, false)
	if err != nil || version != 2 {
		t.Fatalf("new login after unban: version %d, error %v", version, err)
	}
	freshID = GenerateString()
	fresh, _ = generateToken(config, freshID, issuedAt, version, userID.String(), "user", nil)
	check(fresh, codes.OK)
	live = &revocationTestSession{user: userID, token: sessionTokenRef{id: freshID, version: version}, closed: make(chan bool, 1)}
	registry.sessions = []Session{live}
	peer.onBan("node-a", ban) // Delayed ban after unban.
	if err := SessionLogout(ctx, config, cacheA, userID, fresh, ""); err != nil {
		t.Fatal(err)
	}
	if err := reconcileSessionRevocations(ctx, dbB, registry, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case banned := <-live.closed:
		if banned {
			t.Fatal("delayed ban disconnected a newer session as banned")
		}
	case <-ctx.Done():
		t.Fatal("remote socket was not disconnected after logout")
	}

	version, err = cacheA.issue(ctx, userID, true)
	if err != nil || version != 3 {
		t.Fatalf("single-session login: version %d, error %v", version, err)
	}
	replacement, _ := generateToken(config, GenerateString(), issuedAt, version, userID.String(), "user", nil)
	check(replacement, codes.OK)
	check(fresh, codes.Unauthenticated)
}

type revocationTestRegistry struct {
	testSessionRegistry
	sessions []Session
}

func (r *revocationTestRegistry) Range(fn func(Session) bool) {
	for _, s := range r.sessions {
		if !fn(s) {
			return
		}
	}
}

type revocationTestSession struct {
	Session
	user   uuid.UUID
	token  sessionTokenRef
	closed chan bool
}

func (s *revocationTestSession) UserID() uuid.UUID             { return s.user }
func (s *revocationTestSession) sessionToken() sessionTokenRef { return s.token }
func (s *revocationTestSession) Close(_ string, _ runtime.PresenceReason, envelopes ...*rtapi.Envelope) {
	s.closed <- len(envelopes) != 0
}
