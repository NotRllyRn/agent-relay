package store

import (
	"agent-relay/internal/domain"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Callback is a stable logical batch. Complete only after gateway admission.
// Repeated polling/retries return the same ID and events; consumers dedupe ID.
type Callback struct {
	CallbackID string                   `json:"callback_id"`
	Route      domain.ConversationRoute `json:"route"`
	Events     []domain.Event           `json:"events"`
	Attempts   int                      `json:"attempts"`
}

func callbackKind(e domain.Event, r domain.ConversationRoute) string {
	if r.ReplyPolicy == "silent" {
		return ""
	}
	terminal := e.EventType == "task.completed" || e.EventType == "task.failed" || e.EventType == "task.declined" || e.EventType == "task.cancelled"
	if terminal {
		return "terminal"
	}
	if r.ReplyPolicy == "terminal_only" {
		return ""
	}
	switch e.EventType {
	case "message.created":
		var m domain.Message
		json.Unmarshal(e.PayloadJSON, &m)
		if m.ReplyToMessageID != "" {
			return "update"
		}
	case "message.received":
		if r.ReplyPolicy == "all" {
			return "update"
		}
	case "task.blocked":
		return "urgent"
	case "message.delivered", "message.acknowledged", "task.accepted", "task.started":
		return "update"
	case "task.progress":
		var p struct {
			Meaningful bool `json:"meaningful"`
		}
		json.Unmarshal(e.PayloadJSON, &p)
		if p.Meaningful || r.ReplyPolicy == "all" {
			return "progress"
		}
	}
	return ""
}

// Scan only successfully projected peer events. Also handles registration and
// cross-origin dependency races. Unique source IDs preserve delivered tombstones.
func (s *Store) backfillCallbacks(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT r.route_json,e.origin_id,e.origin_seq,e.event_id,e.event_type,e.aggregate_type,e.aggregate_id,COALESCE(e.correlation_id,''),COALESCE(e.causation_event_id,''),e.created_at,e.payload_json FROM events e JOIN conversation_routes r ON r.thread_id=COALESCE(json_extract(e.payload_json,'$.thread_id'),(SELECT thread_id FROM messages WHERE message_id=json_extract(e.payload_json,'$.message_id')),(SELECT thread_id FROM tasks WHERE task_id=json_extract(e.payload_json,'$.task_id'))) WHERE e.origin_id<>? AND NOT EXISTS(SELECT 1 FROM pending_projections p WHERE p.event_id=e.event_id) AND NOT EXISTS(SELECT 1 FROM conversation_callback_jobs j WHERE j.source_event_id=e.event_id) ORDER BY e.created_at,e.origin_id,e.origin_seq`, s.localID)
	if err != nil {
		return err
	}
	type candidate struct {
		r domain.ConversationRoute
		e domain.Event
	}
	var cs []candidate
	for rows.Next() {
		var c candidate
		var raw []byte
		var ts string
		if err = rows.Scan(&raw, &c.e.OriginID, &c.e.OriginSeq, &c.e.EventID, &c.e.EventType, &c.e.AggregateType, &c.e.AggregateID, &c.e.CorrelationID, &c.e.CausationEventID, &ts, &c.e.PayloadJSON); err != nil {
			rows.Close()
			return err
		}
		if err = json.Unmarshal(raw, &c.r); err != nil {
			rows.Close()
			return err
		}
		c.e.CreatedAt, err = time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			rows.Close()
			return err
		}
		cs = append(cs, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	now := time.Now()
	for _, c := range cs {
		if c.r.TaskID != "" && strings.HasPrefix(c.e.EventType, "task.") && c.e.AggregateID != c.r.TaskID {
			continue
		}
		kind := callbackKind(c.e, c.r)
		if kind == "" {
			continue
		}
		due := now
		if kind != "terminal" && kind != "urgent" {
			due = due.Add(5 * time.Second)
		}
		raw, err := json.Marshal(c.e)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO conversation_callback_jobs(callback_id,route_thread_id,source_event_id,event_json,kind,next_attempt_at) VALUES(?,?,?,?,?,?)`, "cb_"+c.e.EventID, c.r.ThreadID, c.e.EventID, raw, kind, due.UnixNano()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DueCallbacks(ctx context.Context, limit int) ([]Callback, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = s.backfillCallbacks(ctx, tx); err != nil {
		return nil, err
	}
	// Keep tombstones so backfill cannot resurrect superseded task updates.
	// Frozen batches may already be admitted: never alter their membership.
	// Message replies and other tasks on the same thread remain independent.
	if _, err = tx.ExecContext(ctx, `UPDATE conversation_callback_jobs AS j SET state='suppressed' WHERE j.state='pending' AND j.batch_id='' AND j.kind<>'terminal' AND json_extract(j.event_json,'$.aggregate_type')='task' AND EXISTS(SELECT 1 FROM conversation_callback_jobs t WHERE t.route_thread_id=j.route_thread_id AND t.kind='terminal' AND json_extract(t.event_json,'$.aggregate_id')=json_extract(j.event_json,'$.aggregate_id'))`); err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()
	rows, err := tx.QueryContext(ctx, `SELECT j.callback_id,j.route_thread_id,j.kind,j.batch_id FROM conversation_callback_jobs j JOIN conversation_routes r ON r.thread_id=j.route_thread_id WHERE j.state='pending' AND j.next_attempt_at<=? AND json_extract(r.route_json,'$.reply_policy')<>'silent' AND (json_extract(r.route_json,'$.reply_policy')<>'terminal_only' OR j.kind='terminal') AND (j.kind<>'progress' OR NOT EXISTS(SELECT 1 FROM conversation_callback_jobs p WHERE p.route_thread_id=j.route_thread_id AND p.kind='progress' AND p.delivered_at>?)) ORDER BY CASE WHEN j.kind='terminal' THEN 0 ELSE 1 END,j.next_attempt_at,j.callback_id`, now, now-int64(300*time.Second))
	if err != nil {
		return nil, err
	}
	type seed struct{ id, thread, kind, batch string }
	var seeds []seed
	for rows.Next() {
		var x seed
		if err = rows.Scan(&x.id, &x.thread, &x.kind, &x.batch); err != nil {
			rows.Close()
			return nil, err
		}
		seeds = append(seeds, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var out []Callback
	seen := map[string]bool{}
	responseBytes := 2 // Array brackets; headroom below the bridge's 2 MiB cap.
	for _, x := range seeds {
		key := x.batch
		if key == "" {
			key = x.id
		}
		if seen[key] {
			continue
		}
		if x.batch == "" {
			// Freeze membership at first poll; never mutate an admitted/retrying batch.
			if _, err = tx.ExecContext(ctx, "SAVEPOINT callback_candidate"); err != nil {
				return nil, err
			}
			q := `UPDATE conversation_callback_jobs SET batch_id=? WHERE callback_id=?`
			args := []any{key, x.id}
			if x.kind != "terminal" {
				q = `UPDATE conversation_callback_jobs SET batch_id=? WHERE callback_id IN (SELECT callback_id FROM (SELECT callback_id,next_attempt_at, SUM(length(event_json)) OVER (ORDER BY next_attempt_at,callback_id) AS bytes FROM conversation_callback_jobs WHERE route_thread_id=? AND kind=? AND state='pending' AND batch_id='' AND next_attempt_at<=?) WHERE bytes<=524288 ORDER BY next_attempt_at,callback_id LIMIT 16)`
				args = []any{key, x.thread, x.kind, now}
			}
			if _, err = tx.ExecContext(ctx, q, args...); err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE conversation_callback_jobs SET batch_id=? WHERE callback_id=? AND batch_id=''`, key, x.id); err != nil {
				return nil, err
			}
		}
		var c Callback
		c.CallbackID = key
		var routeRaw []byte
		if err = tx.QueryRowContext(ctx, "SELECT route_json FROM conversation_routes WHERE thread_id=?", x.thread).Scan(&routeRaw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(routeRaw, &c.Route); err != nil {
			return nil, err
		}
		rr, err := tx.QueryContext(ctx, "SELECT event_json,attempts FROM conversation_callback_jobs WHERE batch_id=? AND state='pending' ORDER BY next_attempt_at,callback_id", key)
		if err != nil {
			return nil, err
		}
		for rr.Next() {
			var raw []byte
			var attempts int
			if err = rr.Scan(&raw, &attempts); err != nil {
				rr.Close()
				return nil, err
			}
			var ev domain.Event
			if err = json.Unmarshal(raw, &ev); err != nil {
				rr.Close()
				return nil, err
			}
			c.Events = append(c.Events, ev)
			if len(ev.PayloadJSON) > 256*1024 {
				preview := string(ev.PayloadJSON[:8192])
				c.Events[len(c.Events)-1].PayloadJSON, err = json.Marshal(map[string]any{"callback_payload_omitted": true, "original_bytes": len(ev.PayloadJSON), "preview": preview})
				if err != nil {
					rr.Close()
					return nil, err
				}
			}
			if attempts > c.Attempts {
				c.Attempts = attempts
			}
		}
		err = rr.Err()
		rr.Close()
		if err != nil {
			return nil, err
		}
		seen[key] = true
		// Mark all seed IDs consumed so grouped seeds do not make another batch.
		encoded, err := json.Marshal(c)
		if err != nil {
			return nil, err
		}
		// Old releases could freeze arbitrarily large batches. Keep the persisted
		// membership/identity intact, but expose a bounded, explicit legacy summary.
		if (len(encoded) > 768*1024 || len(c.Events) > 16) && len(c.Events) > 0 {
			summary := domain.Event{EventType: "relay.callback_summary"}
			summary.PayloadJSON, err = json.Marshal(map[string]any{"callback_events_omitted": true, "event_count": len(c.Events), "instruction": "Oversized frozen batch: full events remain in the local Relay store; operator inspection is required. Do not claim a final outcome from this summary."})
			if err != nil {
				return nil, err
			}
			c.Events = []domain.Event{summary}
			encoded, err = json.Marshal(c)
			if err != nil {
				return nil, err
			}
		}
		if responseBytes+len(encoded)+1 > 1024*1024 {
			if x.batch == "" {
				if _, err = tx.ExecContext(ctx, "ROLLBACK TO callback_candidate"); err != nil {
					return nil, err
				}
			}
			if len(encoded)+3 > 1024*1024 {
				if _, err = tx.ExecContext(ctx, `UPDATE conversation_callback_jobs SET last_error='callback routing envelope exceeds response limit; operator reconciliation required' WHERE batch_id=? OR callback_id=?`, key, x.id); err != nil {
					return nil, err
				}
			}
			continue
		}
		responseBytes += len(encoded) + 1
		for _, ev := range c.Events {
			seen["cb_"+ev.EventID] = true
		}
		out = append(out, c)
		if len(out) == limit {
			break
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}
func (s *Store) CompleteCallback(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE conversation_callback_jobs SET state='delivered',delivered_at=COALESCE(delivered_at,?) WHERE batch_id=? OR (batch_id='' AND callback_id=?)`, time.Now().UnixNano(), id, id)
	return callbackResult(result, err)
}
func (s *Store) RetryCallback(ctx context.Context, id, message string, nextTime time.Time) error {
	if len(message) > 2048 {
		message = message[:2048]
	}
	result, err := s.db.ExecContext(ctx, `UPDATE conversation_callback_jobs SET attempts=attempts+1,last_error=?,next_attempt_at=? WHERE state='pending' AND (batch_id=? OR (batch_id='' AND callback_id=?))`, message, nextTime.UnixNano(), id, id)
	return callbackResult(result, err)
}
func callbackResult(r sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("callback not found or not pending")
	}
	return nil
}
