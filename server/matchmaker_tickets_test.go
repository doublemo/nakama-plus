package server

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doublemo/nakama-common/rtapi"
	"github.com/gofrs/uuid/v5"
)

func TestMatchmakerClusterClaimsTicketsOnce(t *testing.T) {
	store := &testMatchmakerTicketStore{rows: make(map[string]testMatchmakerTicket)}
	left, right := newSnapshotTestPeer(t, "left"), newSnapshotTestPeer(t, "right")
	var hooks atomic.Int32
	notifications := make(chan string, 4)
	for _, peer := range []*LocalPeer{left, right} {
		m := peer.matchmaker.(*LocalMatchmaker)
		m.tickets = store
		m.router = &testMessageRouter{sendToPresence: func(_ []*PresenceID, envelope *rtapi.Envelope) {
			notifications <- envelope.GetMatchmakerMatched().GetTicket()
		}}
		m.runtime.matchmakerMatchedFunction = func(context.Context, []*MatchmakerEntry) (string, bool, error) {
			hooks.Add(1)
			return "", false, nil
		}
	}
	_, leftTicket := addClusterTestTicket(t, left)
	_, rightTicket := addClusterTestTicket(t, right)
	leftSnapshot, rightSnapshot := left.LocalState(false), right.LocalState(false)
	left.onMergeRemoteState(rightSnapshot, false)
	right.onMergeRemoteState(leftSnapshot, false)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, peer := range []*LocalPeer{left, right} {
		wg.Go(func() {
			<-start
			peer.matchmaker.(*LocalMatchmaker).Process()
		})
	}
	close(start)
	wg.Wait()
	if got := hooks.Load(); got != 1 || len(notifications) != 2 {
		t.Fatalf("got %d match callbacks and %d notifications, want one callback and two notifications", got, len(notifications))
	}
	gotTickets := []string{<-notifications, <-notifications}
	if !slices.Contains(gotTickets, leftTicket) || !slices.Contains(gotTickets, rightTicket) {
		t.Fatalf("notifications did not contain the original tickets: %v", gotTickets)
	}

	// Previously captured snapshots cannot revive consumed tickets, even if
	// neither peer received the winner's removal message.
	left.onMergeRemoteState(rightSnapshot, false)
	right.onMergeRemoteState(leftSnapshot, false)
	for _, peer := range []*LocalPeer{left, right} {
		m := peer.matchmaker.(*LocalMatchmaker)
		if len(m.indexes) != 0 {
			t.Fatalf("stale snapshot restored consumed tickets on %s", peer.Local().Name())
		}
		m.Process()
	}
	if got := hooks.Load(); got != 1 {
		t.Fatalf("expected no second match, got %d callbacks", got)
	}
}

func TestMatchmakerClusterCancellationRejectsStaleGroup(t *testing.T) {
	store := &testMatchmakerTicketStore{rows: make(map[string]testMatchmakerTicket)}
	owner, replica := newSnapshotTestPeer(t, "owner"), newSnapshotTestPeer(t, "replica")
	owner.matchmaker.(*LocalMatchmaker).tickets = store
	m := replica.matchmaker.(*LocalMatchmaker)
	m.tickets = store
	var hooks atomic.Int32
	m.router = &testMessageRouter{}
	m.runtime.matchmakerMatchedFunction = func(context.Context, []*MatchmakerEntry) (string, bool, error) {
		hooks.Add(1)
		return "", false, nil
	}
	session, cancelled := addClusterTestTicket(t, owner)
	_, remaining := addClusterTestTicket(t, replica)
	stale := owner.LocalState(false)
	replica.onMergeRemoteState(stale, false)
	if err := owner.matchmaker.RemoveSession(session, cancelled); err != nil {
		t.Fatal(err)
	}

	// Simulate a dropped cancellation notification. The stale replica still
	// proposes the group, but cannot consume any of its tickets.
	m.Process()
	if hooks.Load() != 0 || m.indexes[cancelled] != nil || m.indexes[remaining] == nil {
		t.Fatal("cancelled ticket matched, or the valid ticket was lost")
	}
	active, err := store.active(context.Background(), []string{cancelled, remaining})
	if err != nil || len(active) != 1 {
		t.Fatalf("a failed group claim modified the active ticket set: %v, %v", active, err)
	}
	replica.onMergeRemoteState(stale, false)
	if m.indexes[cancelled] != nil {
		t.Fatal("old snapshot restored an explicitly cancelled ticket")
	}
}

func TestMatchmakerClusterRetriesFailedClaim(t *testing.T) {
	store := &testMatchmakerTicketStore{
		rows: make(map[string]testMatchmakerTicket), claimErr: errors.New("temporary database error"),
	}
	peer := newSnapshotTestPeer(t, "owner")
	m := peer.matchmaker.(*LocalMatchmaker)
	m.tickets = store
	m.config.GetMatchmaker().MaxIntervals = 1
	var hooks atomic.Int32
	m.router = &testMessageRouter{}
	m.runtime.matchmakerMatchedFunction = func(context.Context, []*MatchmakerEntry) (string, bool, error) {
		hooks.Add(1)
		return "", false, nil
	}
	addClusterTestTicket(t, peer)
	addClusterTestTicket(t, peer)

	m.Process()
	if hooks.Load() != 0 || len(m.indexes) != 2 {
		t.Fatal("database failure emitted a result or discarded retryable tickets")
	}
	m.Process()
	if hooks.Load() != 1 || len(m.indexes) != 0 {
		t.Fatal("matching did not recover after the database became available")
	}
}

func TestMatchmakerClusterCancelsUnreplicatedPartyTicketOnDisconnect(t *testing.T) {
	store := &testMatchmakerTicketStore{rows: make(map[string]testMatchmakerTicket)}
	host, memberNode := newSnapshotTestPeer(t, "host"), newSnapshotTestPeer(t, "member-node")
	host.matchmaker.(*LocalMatchmaker).tickets = store
	replica := memberNode.matchmaker.(*LocalMatchmaker)
	replica.tickets = store
	member := uuid.Must(uuid.NewV4())
	ticket, _, err := host.matchmaker.Add(host.ctx, []*MatchmakerPresence{
		{SessionId: uuid.Must(uuid.NewV4()).String(), Node: "host"},
		{SessionId: member.String(), SessionID: member, Node: "member-node"},
	}, "", "party.host", "*", 3, 3, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldSnapshot := host.LocalState(false)
	if err := replica.RemoveSessionAll(member.String()); err != nil {
		t.Fatal(err)
	}
	memberNode.onMergeRemoteState(oldSnapshot, false)
	if replica.indexes[ticket] != nil {
		t.Fatal("snapshot restored a party ticket for a disconnected session")
	}
	if tickets := host.matchmaker.Extract(); len(tickets) != 0 {
		t.Fatal("the party host kept the cancelled ticket after rebuilding its snapshot")
	}
}

func TestMatchmakerTicketStoreAtomicClaims(t *testing.T) {
	if os.Getenv("TEST_DB_URL") == "" {
		t.Skip("set TEST_DB_URL to a migrated database")
	}
	db := NewDB(t)
	t.Cleanup(func() { _ = db.Close() })
	store := &dbMatchmakerTickets{db: db}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	node := uuid.Must(uuid.NewV4()).String()
	t.Cleanup(func() {
		if err := store.clearNode(context.Background(), node); err != nil {
			t.Error(err)
		}
	})
	common, onlyLeft, onlyRight := node+"-common", node+"-left", node+"-right"
	for _, ticket := range []string{common, onlyLeft, onlyRight} {
		if err := store.register(ctx, node, ticket, []string{node}); err != nil {
			t.Fatal(err)
		}
	}

	type result struct {
		missing []string
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, tickets := range [][]string{{common, onlyLeft}, {onlyRight, common}} {
		go func() {
			<-start
			missing, err := store.claim(ctx, tickets)
			results <- result{missing, err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("claims failed: %v, %v", first.err, second.err)
	}
	if len(first.missing) == 0 {
		first, second = second, first
	}
	if !slices.Equal(first.missing, []string{common}) || len(second.missing) != 0 {
		t.Fatalf("expected exactly one claim, missing tickets: %v, %v", first.missing, second.missing)
	}
	removed, err := store.cancelSession(ctx, node)
	if err != nil || len(removed) != 1 {
		t.Fatalf("the losing group's other ticket must still be active: %v, %v", removed, err)
	}
}

func addClusterTestTicket(t *testing.T, peer *LocalPeer) (string, string) {
	t.Helper()
	session := uuid.Must(uuid.NewV4())
	presence := &MatchmakerPresence{SessionID: session, SessionId: session.String(), Node: peer.Local().Name()}
	ticket, _, err := peer.matchmaker.Add(peer.ctx, []*MatchmakerPresence{presence}, session.String(), "", "*", 2, 2, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return session.String(), ticket
}

type testMatchmakerTicket struct {
	node     string
	sessions []string
}

type testMatchmakerTicketStore struct {
	sync.Mutex
	rows     map[string]testMatchmakerTicket
	claimErr error
}

func (s *testMatchmakerTicketStore) register(_ context.Context, node, ticket string, sessions []string) error {
	s.Lock()
	defer s.Unlock()
	s.rows[ticket] = testMatchmakerTicket{node, sessions}
	return nil
}

func (s *testMatchmakerTicketStore) active(_ context.Context, tickets []string) (map[string]struct{}, error) {
	s.Lock()
	defer s.Unlock()
	active := make(map[string]struct{})
	for _, ticket := range tickets {
		if _, ok := s.rows[ticket]; ok {
			active[ticket] = struct{}{}
		}
	}
	return active, nil
}

func (s *testMatchmakerTicketStore) claim(_ context.Context, tickets []string) ([]string, error) {
	s.Lock()
	defer s.Unlock()
	if err := s.claimErr; err != nil {
		s.claimErr = nil
		return nil, err
	}
	var missing []string
	for _, ticket := range tickets {
		if _, ok := s.rows[ticket]; !ok {
			missing = append(missing, ticket)
		}
	}
	if len(missing) == 0 {
		for _, ticket := range tickets {
			delete(s.rows, ticket)
		}
	}
	return missing, nil
}

func (s *testMatchmakerTicketStore) cancel(_ context.Context, tickets []string) (map[string]struct{}, error) {
	s.Lock()
	defer s.Unlock()
	removed := make(map[string]struct{})
	for _, ticket := range tickets {
		if _, ok := s.rows[ticket]; ok {
			removed[ticket] = struct{}{}
			delete(s.rows, ticket)
		}
	}
	return removed, nil
}

func (s *testMatchmakerTicketStore) cancelSession(_ context.Context, session string) (map[string]struct{}, error) {
	s.Lock()
	defer s.Unlock()
	removed := make(map[string]struct{})
	for ticket, row := range s.rows {
		if slices.Contains(row.sessions, session) {
			removed[ticket] = struct{}{}
			delete(s.rows, ticket)
		}
	}
	return removed, nil
}

func (s *testMatchmakerTicketStore) clearNode(_ context.Context, node string) error {
	s.Lock()
	defer s.Unlock()
	for ticket, row := range s.rows {
		if row.node == node {
			delete(s.rows, ticket)
		}
	}
	return nil
}
