package store

import (
	"agent-relay/internal/domain"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"
)

func (s *Store) RebuildProjections(ctx context.Context, backupPath string) error {
	if backupPath != "" {
		if _, e := os.Stat(backupPath); e == nil {
			return fmt.Errorf("backup already exists")
		}
		if e := s.Backup(ctx, backupPath); e != nil {
			return e
		}
	}
	rows, e := s.db.QueryContext(ctx, "SELECT DISTINCT origin_id FROM events")
	if e != nil {
		return e
	}
	var origins []string
	for rows.Next() {
		var o string
		if e = rows.Scan(&o); e != nil {
			rows.Close()
			return e
		}
		origins = append(origins, o)
	}
	if e = rows.Err(); e != nil {
		rows.Close()
		return e
	}
	rows.Close()
	var events []domain.Event
	for _, o := range origins {
		es, er := s.EventsAfter(ctx, o, 0, 1<<30)
		if er != nil {
			return er
		}
		events = append(events, es...)
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].CreatedAt.Equal(events[j].CreatedAt) {
			if events[i].OriginID == events[j].OriginID {
				return events[i].OriginSeq < events[j].OriginSeq
			}
			return events[i].OriginID < events[j].OriginID
		}
		return events[i].CreatedAt.Before(events[j].CreatedAt)
	})
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, q := range []string{"DELETE FROM messages", "DELETE FROM threads", "DELETE FROM tasks", "DELETE FROM task_progress", "DELETE FROM delivery_jobs", "DELETE FROM task_delivery_jobs", "DELETE FROM notification_jobs", "DELETE FROM pending_projections"} {
		if _, e = tx.ExecContext(ctx, q); e != nil {
			return e
		}
	}
	remaining := append([]domain.Event(nil), events...)
	for len(remaining) > 0 {
		next := remaining[:0]
		progress := false
		for _, ev := range remaining {
			e = apply(ctx, tx, ev)
			if errors.Is(e, errProjectionDependency) {
				next = append(next, ev)
				continue
			}
			if e != nil {
				return fmt.Errorf("replay %s: %w", ev.EventID, e)
			}
			progress = true
		}
		if !progress {
			ids := make([]string, len(next))
			for i, ev := range next {
				ids[i] = ev.EventID
			}
			return fmt.Errorf("unresolved projection dependencies: %v", ids)
		}
		remaining = append([]domain.Event(nil), next...)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, e = tx.ExecContext(ctx, "INSERT INTO delivery_jobs(message_id,state,attempts,next_attempt_at) SELECT message_id,'pending',0,? FROM messages WHERE recipient_id=? AND delivered_at IS NULL", now, s.localID); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO task_delivery_jobs(task_id,state,attempts,next_attempt_at) SELECT task_id,'pending',0,? FROM tasks WHERE assigned_to=?", now, s.localID); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) Counts(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for name, q := range map[string]string{"messages": "SELECT COUNT(*) FROM messages", "tasks": "SELECT COUNT(*) FROM tasks", "pending_deliveries": "SELECT COUNT(*) FROM delivery_jobs WHERE state='pending'", "pending_notifications": "SELECT COUNT(*) FROM notification_jobs WHERE state='pending'", "pending_projections": "SELECT COUNT(*) FROM pending_projections"} {
		var n int64
		if e := s.db.QueryRowContext(ctx, q).Scan(&n); e != nil {
			return nil, e
		}
		out[name] = n
	}
	return out, nil
}
func BackupName(db string) string {
	return fmt.Sprintf("%s.rebuild-%s.bak", db, time.Now().UTC().Format("20060102T150405Z"))
}
