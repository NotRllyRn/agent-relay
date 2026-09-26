package store

import (
	"context"
	"fmt"
	"time"
)

type NotificationJob struct {
	ID, Body string
	Attempts int
}

func (s *Store) EnqueueNotification(ctx context.Context, id, taskID, body string) error {
	_, e := s.db.ExecContext(ctx, "INSERT INTO notification_jobs VALUES(?,?,?,'pending',0,?,NULL,?) ON CONFLICT(notification_id) DO NOTHING", id, null(taskID), body, time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	return e
}
func (s *Store) DueNotifications(ctx context.Context, limit int) ([]NotificationJob, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT notification_id,body,attempts FROM notification_jobs WHERE state='pending' AND next_attempt_at<=? ORDER BY next_attempt_at LIMIT ?", time.Now().UTC().Format(time.RFC3339Nano), limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []NotificationJob
	for rows.Next() {
		var x NotificationJob
		if e = rows.Scan(&x.ID, &x.Body, &x.Attempts); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) FinishNotification(ctx context.Context, id string) error {
	_, e := s.db.ExecContext(ctx, "UPDATE notification_jobs SET state='done',last_error=NULL WHERE notification_id=?", id)
	return e
}
func (s *Store) RetryNotification(ctx context.Context, id string, n int, cause error, next time.Time) error {
	_, e := s.db.ExecContext(ctx, "UPDATE notification_jobs SET attempts=?,last_error=?,next_attempt_at=? WHERE notification_id=?", n, fmt.Sprint(cause), next.UTC().Format(time.RFC3339Nano), id)
	return e
}
