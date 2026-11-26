package server

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/gofrs/uuid/v5"
)

const (
	enabledUserTime     = "1970-01-01 00:00:00 UTC"
	sessionStateTimeout = 5 * time.Second
)

var errSessionAccountUnavailable = errors.New("account does not exist or is banned")

type databaseSessionCache struct {
	db *sql.DB
}

func NewDatabaseSessionCache(db *sql.DB) SessionCache {
	return &databaseSessionCache{db: db}
}

func (s *databaseSessionCache) Stop() {}

func (s *databaseSessionCache) validate(ctx context.Context, userID uuid.UUID, token sessionTokenRef, refresh bool) (bool, error) {
	var valid bool
	err := s.db.QueryRowContext(ctx, `SELECT disable_time = $2::TIMESTAMPTZ
		AND session_version = $3
		AND NOT EXISTS (SELECT 1 FROM session_revocation r
			WHERE r.user_id = users.id AND r.session_version = $3 AND r.token_id = $4 AND r.refresh = $5)
		FROM users WHERE id = $1`, userID, enabledUserTime, token.version, token.id, refresh).Scan(&valid)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return valid, err
}

func (s *databaseSessionCache) issue(ctx context.Context, userID uuid.UUID, replace bool) (int64, error) {
	var version int64
	if !replace {
		err := s.db.QueryRowContext(ctx, `SELECT session_version FROM users
			WHERE id = $1 AND disable_time = $2::TIMESTAMPTZ`, userID, enabledUserTime).Scan(&version)
		if errors.Is(err, sql.ErrNoRows) {
			err = errSessionAccountUnavailable
		}
		return version, err
	}

	err := ExecuteInTx(ctx, s.db, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `UPDATE users SET session_version = session_version + 1
			WHERE id = $1 AND disable_time = $2::TIMESTAMPTZ RETURNING session_version`, userID, enabledUserTime).Scan(&version)
		if errors.Is(err, sql.ErrNoRows) {
			return errSessionAccountUnavailable
		}
		if err != nil {
			return err
		}
		return clearSessionRevocations(ctx, tx, []uuid.UUID{userID})
	})
	return version, err
}

func (s *databaseSessionCache) revoke(ctx context.Context, userID uuid.UUID, access, refresh sessionTokenRef) error {
	if access.id == "" && refresh.id == "" {
		return ExecuteInTx(ctx, s.db, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `UPDATE users SET session_version = session_version + 1 WHERE id = $1`, userID); err != nil {
				return err
			}
			return clearSessionRevocations(ctx, tx, []uuid.UUID{userID})
		})
	}

	// Refresh preserves the token ID while extending expiry. Keep tombstones
	// for the whole generation so a concurrently refreshed token stays revoked.
	return ExecuteInTx(ctx, s.db, func(tx *sql.Tx) error {
		var version int64
		err := tx.QueryRowContext(ctx, `SELECT session_version FROM users WHERE id = $1 FOR UPDATE`, userID).Scan(&version)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO session_revocation (user_id, session_version, token_id, refresh)
			SELECT $1::UUID, token_version, token_id, refresh
			FROM (VALUES ($2::BIGINT, $3::TEXT, false), ($4::BIGINT, $5::TEXT, true)) AS tokens(token_version, token_id, refresh)
			WHERE token_version = $6 AND token_id <> '' ON CONFLICT DO NOTHING`,
			userID, access.version, access.id, refresh.version, refresh.id, version)
		return err
	})
}

// Callers hold the user row locks while advancing versions and deleting the old records.
func clearSessionRevocations(ctx context.Context, tx *sql.Tx, userIDs []uuid.UUID) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM session_revocation WHERE user_id = ANY($1::UUID[])`, userIDs)
	return err
}
