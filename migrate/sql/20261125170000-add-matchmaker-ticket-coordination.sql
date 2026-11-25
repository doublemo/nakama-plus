-- +migrate Up
CREATE TABLE IF NOT EXISTS matchmaker_ticket (
    ticket_id TEXT PRIMARY KEY,
    node TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS matchmaker_ticket_node_idx ON matchmaker_ticket (node);

CREATE TABLE IF NOT EXISTS matchmaker_ticket_session (
    session_id TEXT NOT NULL,
    ticket_id TEXT NOT NULL REFERENCES matchmaker_ticket (ticket_id) ON DELETE CASCADE,
    PRIMARY KEY (session_id, ticket_id)
);

CREATE INDEX IF NOT EXISTS matchmaker_ticket_session_ticket_idx ON matchmaker_ticket_session (ticket_id);

-- +migrate Down
DROP TABLE IF EXISTS matchmaker_ticket_session;
DROP TABLE IF EXISTS matchmaker_ticket;
