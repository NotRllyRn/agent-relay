package store

import (
	"agent-relay/internal/domain"
	"context"
	"fmt"
	"os"
	"time"
)

type eventWrap struct {
	priority int
	event    domain.Event
}

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
	rows.Close()
	var events []eventWrap
	for _, o := range origins {
		es, er := s.EventsAfter(ctx, o, 0, 1<<30)
		if er != nil {
			return er
		}
		for _, ev := range es {
			p := 1
			if ev.EventType == "message.created" || ev.EventType == "task.created" {
				p = 0
			}
			events = append(events, eventWrap{p, ev})
		}
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, q := range []string{"DELETE FROM messages", "DELETE FROM threads", "DELETE FROM tasks", "DELETE FROM task_progress", "DELETE FROM delivery_jobs", "DELETE FROM notification_jobs"} {
		if _, e = tx.ExecContext(ctx, q); e != nil {
			return e
		}
	}
	for pass := 0; pass < 2; pass++ {
		for _, w := range events {
			if w.priority == pass {
				if e = apply(ctx, tx, w.event); e != nil {
					return fmt.Errorf("replay %s: %w", w.event.EventID, e)
				}
			}
		}
	}
	return tx.Commit()
}
func (s *Store) Counts(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for name, q := range map[string]string{"messages": "SELECT COUNT(*) FROM messages", "tasks": "SELECT COUNT(*) FROM tasks", "pending_deliveries": "SELECT COUNT(*) FROM delivery_jobs WHERE state='pending'", "pending_notifications": "SELECT COUNT(*) FROM notification_jobs WHERE state='pending'"} {
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
