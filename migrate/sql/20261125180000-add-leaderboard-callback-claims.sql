-- +migrate Up
CREATE TABLE IF NOT EXISTS leaderboard_callback_claim (
    leaderboard_id TEXT NOT NULL,
    leaderboard_create_time TIMESTAMPTZ NOT NULL,
    event_type SMALLINT NOT NULL,
    deadline BIGINT NOT NULL,
    tournament_size INTEGER,
    PRIMARY KEY (leaderboard_id, leaderboard_create_time, event_type, deadline)
);

-- +migrate Down
DROP TABLE IF EXISTS leaderboard_callback_claim;
