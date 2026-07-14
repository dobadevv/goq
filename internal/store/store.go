// Package store provides SQLite-backed persistence for goq: declared topics,
// published messages, and per-consumer delivery/ack tracking.
package store

import (
	"database/sql"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS topics (
    name       TEXT PRIMARY KEY,
    mode       TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS messages (
    id         TEXT PRIMARY KEY,
    topic      TEXT NOT NULL REFERENCES topics(name),
    payload    BLOB NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS deliveries (
    message_id   TEXT NOT NULL REFERENCES messages(id),
    consumer_id  TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'queued',
    delivered_at DATETIME,
    acked_at     DATETIME,
    PRIMARY KEY (message_id, consumer_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic);
CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);
`

// Store owns a single serialized SQLite connection.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the SQLite database at path and applies the
// schema. Connections are limited to one so all writes are serialized.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// DeclareTopic inserts a new topic row. Callers guarantee the topic does not
// already exist (the broker is the concurrency gatekeeper).
func (s *Store) DeclareTopic(name, mode string) error {
	_, err := s.db.Exec("INSERT INTO topics(name, mode) VALUES(?, ?)", name, mode)
	return err
}

// LoadTopics returns every declared topic as a name→mode map.
func (s *Store) LoadTopics() (map[string]string, error) {
	rows, err := s.db.Query("SELECT name, mode FROM topics")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	topics := make(map[string]string)
	for rows.Next() {
		var name, mode string
		if err := rows.Scan(&name, &mode); err != nil {
			return nil, err
		}
		topics[name] = mode
	}
	return topics, rows.Err()
}
