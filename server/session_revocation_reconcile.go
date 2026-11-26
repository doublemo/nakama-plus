package server

import (
	"context"
	"database/sql"
	"slices"
	"time"

	"github.com/doublemo/nakama-common/runtime"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
)

const (
	sessionRevocationInterval = 15 * time.Second
	sessionRevocationBatch    = 256
)

type liveSessionToken struct {
	session Session
	userID  uuid.UUID
	token   sessionTokenRef
}

func StartSessionRevocationReconciler(ctx context.Context, logger *zap.Logger, db *sql.DB, registry SessionRegistry) {
	go func() {
		ticker := time.NewTicker(sessionRevocationInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				checkCtx, cancel := context.WithTimeout(ctx, sessionRevocationInterval)
				err := reconcileSessionRevocations(checkCtx, db, registry, nil)
				cancel()
				if err != nil && ctx.Err() == nil {
					logger.Warn("Error reconciling live sessions.", zap.Error(err))
				}
			}
		}
	}()
}

func reconcileSessionRevocations(ctx context.Context, db *sql.DB, registry SessionRegistry, users map[uuid.UUID]struct{}) error {
	var tokens []liveSessionToken
	registry.Range(func(session Session) bool {
		if authenticated, ok := session.(interface{ sessionToken() sessionTokenRef }); ok {
			if token := authenticated.sessionToken(); token.id != "" {
				userID := session.UserID()
				if _, selected := users[userID]; selected || users == nil {
					tokens = append(tokens, liveSessionToken{session, userID, token})
				}
			}
		}
		return true
	})

	for batch := range slices.Chunk(tokens, sessionRevocationBatch) {
		if err := reconcileSessionRevocationBatch(ctx, db, batch); err != nil {
			return err
		}
	}
	return nil
}

func reconcileSessionRevocationBatch(ctx context.Context, db *sql.DB, batch []liveSessionToken) error {
	userIDs := make([]uuid.UUID, len(batch))
	tokenIDs := make([]string, len(batch))
	versions := make([]int64, len(batch))
	for i, entry := range batch {
		userIDs[i], tokenIDs[i], versions[i] = entry.userID, entry.token.id, entry.token.version
	}

	// The JWT expiry controls new authentication. Established sockets retain
	// their existing lifetime and are closed here when their credential is revoked.
	rows, err := db.QueryContext(ctx, `SELECT t.position, COALESCE(u.disable_time <> $4::TIMESTAMPTZ, false)
		FROM ROWS FROM (unnest($1::UUID[]), unnest($2::TEXT[]), unnest($3::BIGINT[]))
			WITH ORDINALITY AS t(user_id, token_id, version, position)
		LEFT JOIN users u ON u.id = t.user_id
		WHERE u.id IS NULL OR u.disable_time <> $4::TIMESTAMPTZ OR u.session_version <> t.version
			OR EXISTS (SELECT 1 FROM session_revocation r WHERE r.user_id = t.user_id
				AND r.session_version = t.version AND r.token_id = t.token_id AND NOT r.refresh)`,
		userIDs, tokenIDs, versions, enabledUserTime)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var position int
		var banned bool
		if err := rows.Scan(&position, &banned); err != nil {
			return err
		}
		// Socket close can wait for an acknowledgement.
		go closeDisconnectedSession(batch[position-1].session, banned, runtime.PresenceReasonDisconnect)
	}
	return rows.Err()
}
