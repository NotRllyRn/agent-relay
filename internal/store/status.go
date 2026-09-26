package store

import (
	"context"
	"database/sql"
	"time"
)

type PeerPresence struct {
	PeerID     string     `json:"peer_id"`
	LastSeenAt *time.Time `json:"last_seen_at,omitempty"`
	LastSyncAt *time.Time `json:"last_sync_at,omitempty"`
	LastError  string     `json:"last_error,omitempty"`
}

func (s *Store) PeerPresence(ctx context.Context) ([]PeerPresence, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT peer_id,last_seen_at,last_sync_at,COALESCE(last_error,'') FROM peer_state ORDER BY peer_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PeerPresence
	for rows.Next() {
		var p PeerPresence
		var seen, sync sql.NullString
		if err = rows.Scan(&p.PeerID, &seen, &sync, &p.LastError); err != nil {
			return nil, err
		}
		p.LastSeenAt = parseNull(seen)
		p.LastSyncAt = parseNull(sync)
		out = append(out, p)
	}
	return out, rows.Err()
}
