package sqlite

import (
	"context"
	"database/sql"
	"time"

	"collaboration/internal/store"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStore struct {
    db *sql.DB
}

// NewSQLiteStore opens (or creates) the sqlite database at path and
// ensures required tables exist.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
    db, err := sql.Open("sqlite3", path+"?_busy_timeout=5000")
    if err != nil {
        return nil, err
    }
    s := &SQLiteStore{db: db}
    if err := s.migrate(); err != nil {
        db.Close()
        return nil, err
    }
    return s, nil
}

func (s *SQLiteStore) migrate() error {
    stmts := []string{
        `CREATE TABLE IF NOT EXISTS rooms (name TEXT PRIMARY KEY, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)` ,
        `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, room TEXT, type TEXT, payload BLOB, version INTEGER, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)` ,
    }
    for _, q := range stmts {
        if _, err := s.db.Exec(q); err != nil {
            return err
        }
    }
    return nil
}

// CreateRoom implements RoomRepository.
func (s *SQLiteStore) CreateRoom(ctx context.Context, name string) error {
    _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO rooms(name) VALUES(?)`, name)
    return err
}

func (s *SQLiteStore) GetRoom(ctx context.Context, name string) (*store.RoomMeta, error) {
    row := s.db.QueryRowContext(ctx, `SELECT name, created_at FROM rooms WHERE name = ?`, name)
    var rm store.RoomMeta
    var created string
    if err := row.Scan(&rm.Name, &created); err != nil {
        return nil, err
    }
    t, _ := time.Parse("2006-01-02 15:04:05", created)
    rm.CreatedAt = t
    return &rm, nil
}

func (s *SQLiteStore) DeleteRoom(ctx context.Context, name string) error {
    _, err := s.db.ExecContext(ctx, `DELETE FROM rooms WHERE name = ?`, name)
    return err
}

func (s *SQLiteStore) ListRooms(ctx context.Context) ([]store.RoomMeta, error) {
    rows, err := s.db.QueryContext(ctx, `SELECT name, created_at FROM rooms`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    var out []store.RoomMeta
    for rows.Next() {
        var rm store.RoomMeta
        var created string
        if err := rows.Scan(&rm.Name, &created); err != nil {
            return nil, err
        }
        t, _ := time.Parse("2006-01-02 15:04:05", created)
        rm.CreatedAt = t
        out = append(out, rm)
    }
    return out, nil
}

// AppendEvent implements EventRepository.
func (s *SQLiteStore) AppendEvent(ctx context.Context, e *store.Event) (int64, error) {
    res, err := s.db.ExecContext(ctx, `INSERT INTO events(room, type, payload, version) VALUES(?,?,?,?)`, e.Room, e.Type, e.Payload, e.Version)
    if err != nil {
        return 0, err
    }
    id, err := res.LastInsertId()
    if err != nil {
        return 0, err
    }
    return id, nil
}

func (s *SQLiteStore) ListEventsByRoom(ctx context.Context, room string, limit int) ([]store.Event, error) {
    q := `SELECT id, type, payload, version, created_at FROM events WHERE room = ? ORDER BY id DESC LIMIT ?`
    rows, err := s.db.QueryContext(ctx, q, room, limit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    var out []store.Event
    for rows.Next() {
        var ev store.Event
        var created string
        if err := rows.Scan(&ev.ID, &ev.Type, &ev.Payload, &ev.Version, &created); err != nil {
            return nil, err
        }
        t, _ := time.Parse("2006-01-02 15:04:05", created)
        ev.CreatedAt = t
        ev.Room = room
        out = append(out, ev)
    }
    return out, nil
}

func (s *SQLiteStore) Close() error {
    return s.db.Close()
}
