-- +migrate Up
ALTER TABLE users ADD COLUMN session_version BIGINT NOT NULL DEFAULT 0;

CREATE TABLE session_revocation (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_version BIGINT NOT NULL,
    token_id TEXT NOT NULL,
    refresh BOOLEAN NOT NULL,
    PRIMARY KEY (user_id, session_version, token_id, refresh)
);

-- +migrate Down
DROP TABLE session_revocation;
ALTER TABLE users DROP COLUMN session_version;
