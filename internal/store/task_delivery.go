package store

import (
	"context"
	"fmt"
	"time"
)

type TaskDeliveryJob struct {
	TaskID   string
	Attempts int
}

func (s *Store) QueueTaskDelivery(ctx context.Context, taskID string) error {
	_, e := s.db.ExecContext(ctx, "INSERT INTO task_delivery_jobs VALUES(?,'pending',0,?,NULL) ON CONFLICT(task_id) DO NOTHING", taskID, time.Now().UTC().Format(time.RFC3339Nano))
	return e
}
func (s *Store) DueTaskDeliveries(ctx context.Context, limit int) ([]TaskDeliveryJob, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT task_id,attempts FROM task_delivery_jobs WHERE state='pending' AND next_attempt_at<=? ORDER BY next_attempt_at LIMIT ?", time.Now().UTC().Format(time.RFC3339Nano), limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []TaskDeliveryJob
	for rows.Next() {
		var x TaskDeliveryJob
		if e = rows.Scan(&x.TaskID, &x.Attempts); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) FinishTaskDelivery(ctx context.Context, id string) error {
	_, e := s.db.ExecContext(ctx, "UPDATE task_delivery_jobs SET state='done',last_error=NULL WHERE task_id=?", id)
	return e
}
func (s *Store) RetryTaskDelivery(ctx context.Context, id string, n int, cause error, next time.Time) error {
	_, e := s.db.ExecContext(ctx, "UPDATE task_delivery_jobs SET attempts=?,last_error=?,next_attempt_at=? WHERE task_id=?", n, fmt.Sprint(cause), next.UTC().Format(time.RFC3339Nano), id)
	return e
}
