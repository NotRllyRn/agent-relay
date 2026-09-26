package store

import (
	"context"
	"encoding/json"
	"time"
)

func (s *Store) ResetCursor(ctx context.Context, peer, origin string, n int64) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO peer_cursors VALUES(?,?,?,?) ON CONFLICT(peer_id,origin_id) DO UPDATE SET confirmed_seq=excluded.confirmed_seq,updated_at=excluded.updated_at", peer, origin, n, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
func (s *Store) RecordPeerSuccess(ctx context.Context, peer string, status any) error {
	raw, err := json.Marshal(status)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.db.ExecContext(ctx, "INSERT INTO peer_state(peer_id,last_seen_at,last_sync_at,last_status_json,last_error) VALUES(?,?,?,?,NULL) ON CONFLICT(peer_id) DO UPDATE SET last_seen_at=excluded.last_seen_at,last_sync_at=excluded.last_sync_at,last_status_json=excluded.last_status_json,last_error=NULL", peer, now, now, raw)
	return err
}
func (s *Store) RecordPeerError(ctx context.Context, peer string, cause error) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO peer_state(peer_id,last_error) VALUES(?,?) ON CONFLICT(peer_id) DO UPDATE SET last_error=excluded.last_error", peer, cause.Error())
	return err
}
