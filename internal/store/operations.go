package store

import (
	"agent-relay/internal/domain"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (s *Store) GetMessage(ctx context.Context, id string) (MessageView, error) {
	xs, e := s.Messages(ctx, "", false, 100)
	if e != nil {
		return MessageView{}, e
	}
	for _, x := range xs {
		if x.MessageID == id {
			return x, nil
		}
	}
	return MessageView{}, sql.ErrNoRows
}
func (s *Store) HasEvent(ctx context.Context, eventType, aggregate string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE event_type=? AND aggregate_id=?", eventType, aggregate).Scan(&n)
	return n > 0, err
}
func (s *Store) CountRecentMessages(ctx context.Context, sender, thread string, since time.Time) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM messages WHERE sender_id=? AND thread_id=? AND created_at>=?", sender, thread, since.UTC().Format(time.RFC3339Nano)).Scan(&n)
	return n, err
}

type TaskView struct {
	domain.Task
	UpdatedAt      time.Time
	LastProgressAt *time.Time
}

func (s *Store) GetTask(ctx context.Context, id string) (TaskView, error) {
	var v TaskView
	var raw []byte
	var created, updated string
	var progress sql.NullString
	err := s.db.QueryRowContext(ctx, "SELECT task_id,thread_id,created_by,assigned_to,objective,context_json,COALESCE(expected_deliverable,''),priority,status,COALESCE(blocker,''),COALESCE(final_result,''),update_interval_seconds,created_at,updated_at,last_progress_at FROM tasks WHERE task_id=?", id).Scan(&v.TaskID, &v.ThreadID, &v.CreatedBy, &v.AssignedTo, &v.Objective, &raw, &v.ExpectedDeliverable, &v.Priority, &v.Status, &v.Blocker, &v.FinalResult, &v.UpdateIntervalSeconds, &created, &updated, &progress)
	if err != nil {
		return v, err
	}
	v.Context = json.RawMessage(raw)
	v.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	v.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	v.LastProgressAt = parseNull(progress)
	return v, nil
}
func (s *Store) ListTasks(ctx context.Context, status, assigned string, limit int) ([]TaskView, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	q := "SELECT task_id FROM tasks WHERE 1=1"
	var a []any
	if status != "" {
		q += " AND status=?"
		a = append(a, status)
	}
	if assigned != "" {
		q += " AND assigned_to=?"
		a = append(a, assigned)
	}
	q += " ORDER BY updated_at DESC LIMIT ?"
	a = append(a, limit)
	rows, e := s.db.QueryContext(ctx, q, a...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		ids = append(ids, id)
	}
	var out []TaskView
	for _, id := range ids {
		v, e := s.GetTask(ctx, id)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

type SearchResult struct{ Kind, ID, ThreadID, Title, Snippet string }

func (s *Store) Search(ctx context.Context, q string, limit int) ([]SearchResult, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	like := "%" + q + "%"
	rows, e := s.db.QueryContext(ctx, `SELECT 'message',message_id,thread_id,COALESCE(subject,''),body_markdown FROM messages WHERE subject LIKE ? OR body_markdown LIKE ? UNION ALL SELECT 'task',task_id,thread_id,objective,COALESCE(final_result,'') FROM tasks WHERE objective LIKE ? OR final_result LIKE ? UNION ALL SELECT 'task_progress',p.event_id,t.thread_id,t.objective,p.summary FROM task_progress p JOIN tasks t ON t.task_id=p.task_id WHERE p.summary LIKE ? OR COALESCE(p.next_step,'') LIKE ? LIMIT ?`, like, like, like, like, like, like, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []SearchResult
	for rows.Next() {
		var x SearchResult
		if e = rows.Scan(&x.Kind, &x.ID, &x.ThreadID, &x.Title, &x.Snippet); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) EnsureHermesSession(ctx context.Context, thread string, create func() (string, error)) (string, error) {
	var id string
	e := s.db.QueryRowContext(ctx, "SELECT session_id FROM hermes_threads WHERE thread_id=?", thread).Scan(&id)
	if e == nil {
		return id, nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return "", e
	}
	id, e = create()
	if e != nil {
		return "", e
	}
	_, e = s.db.ExecContext(ctx, "INSERT INTO hermes_threads VALUES(?,?,?) ON CONFLICT(thread_id) DO NOTHING", thread, id, time.Now().UTC().Format(time.RFC3339Nano))
	if e != nil {
		return "", e
	}
	e = s.db.QueryRowContext(ctx, "SELECT session_id FROM hermes_threads WHERE thread_id=?", thread).Scan(&id)
	return id, e
}
func (s *Store) QueueDelivery(ctx context.Context, messageID string) error {
	_, e := s.db.ExecContext(ctx, "INSERT INTO delivery_jobs VALUES(?,'pending',0,?,NULL) ON CONFLICT(message_id) DO NOTHING", messageID, time.Now().UTC().Format(time.RFC3339Nano))
	return e
}

type DeliveryJob struct {
	MessageID string
	Attempts  int
}

func (s *Store) DueDeliveries(ctx context.Context, limit int) ([]DeliveryJob, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT message_id,attempts FROM delivery_jobs WHERE state='pending' AND next_attempt_at<=? ORDER BY next_attempt_at LIMIT ?", time.Now().UTC().Format(time.RFC3339Nano), limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []DeliveryJob
	for rows.Next() {
		var x DeliveryJob
		rows.Scan(&x.MessageID, &x.Attempts)
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) FinishDelivery(ctx context.Context, id string) error {
	_, e := s.db.ExecContext(ctx, "UPDATE delivery_jobs SET state='done',last_error=NULL WHERE message_id=?", id)
	return e
}
func (s *Store) RetryDelivery(ctx context.Context, id string, attempt int, err error, next time.Time) error {
	_, e := s.db.ExecContext(ctx, "UPDATE delivery_jobs SET attempts=?,last_error=?,next_attempt_at=? WHERE message_id=?", attempt, fmt.Sprint(err), next.UTC().Format(time.RFC3339Nano), id)
	return e
}
func (s *Store) HasProgressRequest(ctx context.Context, key string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM progress_requests WHERE request_key=?", key).Scan(&n)
	return n > 0, err
}
func (s *Store) MarkProgressRequest(ctx context.Context, key, taskID string) error {
	_, err := s.db.ExecContext(ctx, "INSERT OR IGNORE INTO progress_requests VALUES(?,?,?)", key, taskID, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
