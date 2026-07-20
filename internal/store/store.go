// Package store provides SQLite-backed persistence for goq: declared topics,
// published messages, and per-consumer delivery/ack tracking.
package store

import (
	"database/sql"
	"errors"

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
CREATE TABLE IF NOT EXISTS users (
    username       TEXT PRIMARY KEY,
    password_hash  TEXT NOT NULL,
    is_super_admin BOOLEAN NOT NULL DEFAULT 0,
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
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
	defer func() { _ = rows.Close() }()
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

// InsertMessage persists a published message; this is goq's durability point.
func (s *Store) InsertMessage(id, topic string, payload []byte) error {
	_, err := s.db.Exec(
		"INSERT INTO messages(id, topic, payload) VALUES(?, ?, ?)",
		id, topic, payload)
	return err
}

// InsertDelivery records intent to deliver a message to a consumer. It is
// idempotent and never overwrites an existing (possibly further-along) row.
func (s *Store) InsertDelivery(messageID, consumerID string) error {
	_, err := s.db.Exec(
		"INSERT OR IGNORE INTO deliveries(message_id, consumer_id, status) VALUES(?, ?, 'queued')",
		messageID, consumerID)
	return err
}

// MarkDelivered advances a delivery to 'delivered' only if it is still
// 'queued', so it cannot regress a row that was acked first.
func (s *Store) MarkDelivered(messageID, consumerID string) error {
	_, err := s.db.Exec(
		"UPDATE deliveries SET status='delivered', delivered_at=CURRENT_TIMESTAMP "+
			"WHERE message_id=? AND consumer_id=? AND status='queued'",
		messageID, consumerID)
	return err
}

// MarkAcked records a consumer's acknowledgement of a message.
func (s *Store) MarkAcked(messageID, consumerID string) error {
	_, err := s.db.Exec(
		"UPDATE deliveries SET status='acked', acked_at=CURRENT_TIMESTAMP "+
			"WHERE message_id=? AND consumer_id=?",
		messageID, consumerID)
	return err
}

// DeliveryStatus returns the current status for a (message, consumer) pair.
func (s *Store) DeliveryStatus(messageID, consumerID string) (string, error) {
	var status string
	err := s.db.QueryRow(
		"SELECT status FROM deliveries WHERE message_id=? AND consumer_id=?",
		messageID, consumerID).Scan(&status)
	return status, err
}

// ErrUserNotFound is returned by GetUser when no row matches the username.
var ErrUserNotFound = errors.New("store: user not found")

// User is an authenticatable goqd account.
type User struct {
	Username     string
	PasswordHash string
	IsSuperAdmin bool
}

// UpsertUser inserts a new user, or overwrites the password hash and admin
// flag of an existing one with the same username.
func (s *Store) UpsertUser(username, passwordHash string, isSuperAdmin bool) error {
	_, err := s.db.Exec(
		`INSERT INTO users(username, password_hash, is_super_admin) VALUES(?, ?, ?)
		 ON CONFLICT(username) DO UPDATE SET
		   password_hash=excluded.password_hash,
		   is_super_admin=excluded.is_super_admin`,
		username, passwordHash, isSuperAdmin)
	return err
}

// GetUser looks up a user by username, returning ErrUserNotFound if none
// exists.
func (s *Store) GetUser(username string) (User, error) {
	var u User
	err := s.db.QueryRow(
		"SELECT username, password_hash, is_super_admin FROM users WHERE username=?",
		username).Scan(&u.Username, &u.PasswordHash, &u.IsSuperAdmin)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrUserNotFound
	}
	if err != nil {
		return User{}, err
	}
	return u, nil
}
