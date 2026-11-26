package server

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-plus/v3/internal/cronexpr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type leaderboardCallbackKind int16

const (
	leaderboardCallbackReset leaderboardCallbackKind = iota + 1
	leaderboardCallbackTournamentEnd
	leaderboardCallbackTournamentReset
	leaderboardCallbackTournamentSizeReset
)

type claimedLeaderboardCallback struct {
	leaderboard *api.Leaderboard
	tournament  *api.Tournament
}

type leaderboardCallbackStore interface {
	claim(ctx context.Context, callback *LeaderboardSchedulerCallback, invoke bool) (*claimedLeaderboardCallback, error)
}

type dbLeaderboardCallbacks struct {
	db *sql.DB
}

func (s *dbLeaderboardCallbacks) claim(ctx context.Context, callback *LeaderboardSchedulerCallback, invoke bool) (*claimedLeaderboardCallback, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var claimed *claimedLeaderboardCallback
	err := ExecuteInTx(ctx, s.db, func(tx *sql.Tx) error {
		claimed = nil
		var definition Leaderboard
		var createTime, startTime time.Time
		var endTime sql.NullTime
		var reset sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT create_time, start_time, end_time, duration, reset_schedule, authoritative, sort_order, operator, metadata
			FROM leaderboard WHERE id = $1 FOR UPDATE`, callback.id).Scan(&createTime, &startTime, &endTime, &definition.Duration, &reset,
			&definition.Authoritative, &definition.SortOrder, &definition.Operator, &definition.Metadata)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		definition.Id = callback.id
		definition.CreateTime = createTime.Unix()
		definition.StartTime = startTime.Unix()
		if endTime.Valid {
			definition.EndTime = endTime.Time.Unix()
		}
		if reset.Valid {
			definition.ResetSchedule, err = cronexpr.Parse(reset.String)
			if err != nil {
				return err
			}
		}
		if !callback.matchesDeadline(&definition) {
			return nil
		}

		result := &claimedLeaderboardCallback{}
		if definition.IsTournament() {
			row := tx.QueryRowContext(ctx, `SELECT id, sort_order, operator, reset_schedule, metadata, create_time,
				category, description, duration, end_time, max_size, max_num_score, title, size, start_time
				FROM leaderboard WHERE id = $1`, callback.id)
			result.tournament, err = parseTournament(row, callback.t)
			if err != nil {
				return err
			}
		} else {
			result.leaderboard = &api.Leaderboard{
				Id: definition.Id, SortOrder: uint32(definition.SortOrder), Operator: OperatorIntToEnum[definition.Operator],
				PrevReset: uint32(calculatePrevReset(callback.t, definition.StartTime, definition.ResetSchedule)),
				NextReset: uint32(callback.ts), Metadata: definition.Metadata,
				CreateTime: &timestamppb.Timestamp{Seconds: definition.CreateTime}, Authoritative: definition.Authoritative,
			}
		}

		var sizeReset bool
		if callback.kind == leaderboardCallbackTournamentReset {
			// A node without the runtime hook can still reset capacity. Its
			// transaction leaves the hook claim available to another node.
			sizeReset, err = insertLeaderboardClaim(ctx, tx, callback, createTime, leaderboardCallbackTournamentSizeReset, int64(result.tournament.Size))
			if err != nil {
				return err
			}
			if sizeReset {
				if _, err := tx.ExecContext(ctx, "UPDATE leaderboard SET size = 0 WHERE id = $1", callback.id); err != nil {
					return err
				}
			}
		}
		if !invoke {
			return nil
		}
		winner, err := insertLeaderboardClaim(ctx, tx, callback, createTime, callback.kind, nil)
		if err != nil || !winner {
			return err
		}
		if !sizeReset && result.tournament != nil && int64(result.tournament.NextReset) == callback.ts {
			// End and reset can be queued together on different workers. Use the
			// ended period's size even if the reset transaction committed first.
			var size int64
			err := tx.QueryRowContext(ctx, `SELECT tournament_size FROM leaderboard_callback_claim
				WHERE leaderboard_id = $1 AND leaderboard_create_time = $2 AND event_type = $3 AND deadline = $4`,
				callback.id, createTime, int16(leaderboardCallbackTournamentSizeReset), callback.ts).Scan(&size)
			if err == nil {
				result.tournament.Size = uint32(size)
				result.tournament.CanEnter = result.tournament.Size < result.tournament.MaxSize
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		claimed = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

func insertLeaderboardClaim(ctx context.Context, tx *sql.Tx, callback *LeaderboardSchedulerCallback, createTime time.Time, kind leaderboardCallbackKind, size any) (bool, error) {
	claim, err := tx.ExecContext(ctx, `INSERT INTO leaderboard_callback_claim
		(leaderboard_id, leaderboard_create_time, event_type, deadline, tournament_size) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (leaderboard_id, leaderboard_create_time, event_type, deadline) DO NOTHING`,
		callback.id, createTime, int16(kind), callback.ts, size)
	if err != nil {
		return false, err
	}
	affected, err := claim.RowsAffected()
	return affected != 0, err
}

func (callback *LeaderboardSchedulerCallback) matchesDeadline(definition *Leaderboard) bool {
	if callback.leaderboard.CreateTime != definition.CreateTime || callback.ts <= definition.CreateTime {
		return false
	}
	if callback.kind == leaderboardCallbackReset {
		return !definition.IsTournament() && definition.ResetSchedule != nil && definition.ResetSchedule.Next(callback.t).Unix() == callback.ts
	}
	if !definition.IsTournament() {
		return false
	}
	_, end, expiry := calculateTournamentDeadlines(definition.StartTime, definition.EndTime, int64(definition.Duration), definition.ResetSchedule, callback.t)
	return (callback.kind == leaderboardCallbackTournamentEnd && end == callback.ts) ||
		(callback.kind == leaderboardCallbackTournamentReset && expiry == callback.ts)
}
