package server

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"time"
)

// Only the source node registers a ticket. Removing its database row is the
// commit point for a match or cancellation. Replicas never register tickets.
type matchmakerTicketStore interface {
	register(ctx context.Context, node, ticket string, sessions []string) error
	active(ctx context.Context, tickets []string) (map[string]struct{}, error)
	claim(ctx context.Context, tickets []string) (missing []string, err error)
	cancel(ctx context.Context, tickets []string) (removed map[string]struct{}, err error)
	cancelSession(ctx context.Context, session string) (removed map[string]struct{}, err error)
	clearNode(ctx context.Context, node string) error
}

type dbMatchmakerTickets struct {
	db *sql.DB
}

const matchmakerTicketTimeout = 5 * time.Second

const deleteMatchmakerTickets = `DELETE FROM matchmaker_ticket WHERE ticket_id = ANY($1::TEXT[]) RETURNING ticket_id`

var errMatchmakerClaimConflict = errors.New("one or more matchmaking tickets are no longer active")

func (s *dbMatchmakerTickets) register(ctx context.Context, node, ticket string, sessions []string) error {
	ctx, cancel := context.WithTimeout(ctx, matchmakerTicketTimeout)
	defer cancel()
	return ExecuteInTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO matchmaker_ticket (ticket_id, node) VALUES ($1, $2)`, ticket, node); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO matchmaker_ticket_session (session_id, ticket_id)
			SELECT session_id, $1 FROM unnest($2::TEXT[]) AS sessions(session_id)`, ticket, sessions)
		return err
	})
}

func (s *dbMatchmakerTickets) active(ctx context.Context, tickets []string) (map[string]struct{}, error) {
	ctx, cancel := context.WithTimeout(ctx, matchmakerTicketTimeout)
	defer cancel()
	return queryMatchmakerTickets(ctx, s.db, `SELECT ticket_id FROM matchmaker_ticket WHERE ticket_id = ANY($1::TEXT[])`, tickets)
}

func (s *dbMatchmakerTickets) claim(ctx context.Context, tickets []string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, matchmakerTicketTimeout)
	defer cancel()
	var missing []string
	err := ExecuteInTx(ctx, s.db, func(tx *sql.Tx) error {
		// Lock tickets in a common order before attempting to consume the group.
		active, err := queryMatchmakerTickets(ctx, tx, `SELECT ticket_id FROM matchmaker_ticket
			WHERE ticket_id = ANY($1::TEXT[]) ORDER BY ticket_id FOR UPDATE`, tickets)
		if err != nil {
			return err
		}
		if len(active) != len(tickets) {
			missing = slices.DeleteFunc(slices.Clone(tickets), func(ticket string) bool {
				_, ok := active[ticket]
				return ok
			})
			return errMatchmakerClaimConflict
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM matchmaker_ticket WHERE ticket_id = ANY($1::TEXT[])`, tickets)
		return err
	})
	if errors.Is(err, errMatchmakerClaimConflict) {
		return missing, nil
	}
	return nil, err
}

func (s *dbMatchmakerTickets) cancel(ctx context.Context, tickets []string) (map[string]struct{}, error) {
	return s.remove(ctx, deleteMatchmakerTickets, tickets)
}

func (s *dbMatchmakerTickets) cancelSession(ctx context.Context, session string) (map[string]struct{}, error) {
	return s.remove(ctx, `DELETE FROM matchmaker_ticket WHERE ticket_id IN
		(SELECT ticket_id FROM matchmaker_ticket_session WHERE session_id = $1) RETURNING ticket_id`, session)
}

func (s *dbMatchmakerTickets) remove(ctx context.Context, query string, arg any) (map[string]struct{}, error) {
	ctx, cancel := context.WithTimeout(ctx, matchmakerTicketTimeout)
	defer cancel()
	var removed map[string]struct{}
	err := ExecuteInTx(ctx, s.db, func(tx *sql.Tx) error {
		var err error
		removed, err = queryMatchmakerTickets(ctx, tx, query, arg)
		return err
	})
	return removed, err
}

func (s *dbMatchmakerTickets) clearNode(ctx context.Context, node string) error {
	ctx, cancel := context.WithTimeout(ctx, matchmakerTicketTimeout)
	defer cancel()
	return ExecuteInTx(ctx, s.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM matchmaker_ticket WHERE node = $1`, node)
		return err
	})
}

func queryMatchmakerTickets(ctx context.Context, conn interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, arg any) (map[string]struct{}, error) {
	rows, err := conn.QueryContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tickets := make(map[string]struct{})
	for rows.Next() {
		var ticket string
		if err := rows.Scan(&ticket); err != nil {
			return nil, err
		}
		tickets[ticket] = struct{}{}
	}
	return tickets, rows.Err()
}
