package store

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"agent-relay/internal/domain"
	_ "modernc.org/sqlite"
)

//go:embed 001_init.sql
var schema string

var errProjectionDependency = errors.New("projection dependency missing")

type Store struct {
	db         *sql.DB
	localID    string
	maxPayload int
}

func Open(ctx context.Context, path, localID string, maxPayload int) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, localID: localID, maxPayload: maxPayload}
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000"} {
		if _, err = db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err = db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, err
	}
	_, err = db.ExecContext(ctx, "INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(1,?)", time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error                   { return s.db.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (s *Store) Head(ctx context.Context, origin string) (int64, []byte, error) {
	var n int64
	var h []byte
	err := s.db.QueryRowContext(ctx, "SELECT head_seq,head_hash FROM origin_heads WHERE origin_id=?", origin).Scan(&n, &h)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, nil
	}
	return n, h, err
}
func (s *Store) Append(ctx context.Context, eventType, aggregateType, aggregateID, correlation, causation string, payload any) (domain.Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return domain.Event{}, err
	}
	if len(raw) > s.maxPayload {
		return domain.Event{}, fmt.Errorf("payload too large")
	}
	id, err := domain.NewID("evt")
	if err != nil {
		return domain.Event{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Event{}, err
	}
	defer tx.Rollback()
	seq, prev, err := headTx(ctx, tx, s.localID)
	if err != nil {
		return domain.Event{}, err
	}
	e := domain.Event{OriginID: s.localID, OriginSeq: seq + 1, EventID: id, EventType: eventType, AggregateType: aggregateType, AggregateID: aggregateID, CorrelationID: correlation, CausationEventID: causation, CreatedAt: time.Now().UTC(), PayloadJSON: raw, PrevHash: prev}
	e.EventHash = e.Hash()
	if err = s.insertApply(ctx, tx, e); err != nil {
		return domain.Event{}, err
	}
	if err = tx.Commit(); err != nil {
		return domain.Event{}, err
	}
	return e, nil
}
func (s *Store) Ingest(ctx context.Context, origin string, events []domain.Event) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, e := range events {
		if e.OriginID != origin {
			return fmt.Errorf("forbidden origin")
		}
		if len(e.PayloadJSON) > s.maxPayload {
			return fmt.Errorf("payload too large")
		}
		var existing []byte
		err = tx.QueryRowContext(ctx, "SELECT event_hash FROM events WHERE origin_id=? AND origin_seq=?", e.OriginID, e.OriginSeq).Scan(&existing)
		if err == nil {
			if !bytes.Equal(existing, e.EventHash) {
				return domain.ErrDivergence
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		seq, h, er := headTx(ctx, tx, origin)
		if er != nil {
			return er
		}
		if e.OriginSeq != seq+1 {
			return fmt.Errorf("%w: expected %d", domain.ErrGap, seq+1)
		}
		if !bytes.Equal(e.PrevHash, h) || !e.ValidateHash() {
			return domain.ErrDivergence
		}
		if er = s.insertRemote(ctx, tx, e); er != nil {
			return er
		}
	}
	if err = retryPendingProjections(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}
func headTx(ctx context.Context, tx *sql.Tx, origin string) (int64, []byte, error) {
	var n int64
	var h []byte
	err := tx.QueryRowContext(ctx, "SELECT head_seq,head_hash FROM origin_heads WHERE origin_id=?", origin).Scan(&n, &h)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, nil
	}
	return n, h, err
}
func (s *Store) insertApply(ctx context.Context, tx *sql.Tx, e domain.Event) error {
	if err := insertEvent(ctx, tx, e); err != nil {
		return err
	}
	if err := apply(ctx, tx, e); err != nil {
		return err
	}
	return updateHead(ctx, tx, e)
}
func (s *Store) insertRemote(ctx context.Context, tx *sql.Tx, e domain.Event) error {
	if err := insertEvent(ctx, tx, e); err != nil {
		return err
	}
	err := apply(ctx, tx, e)
	if errors.Is(err, errProjectionDependency) {
		if _, qerr := tx.ExecContext(ctx, "INSERT INTO pending_projections(event_id,reason,created_at) VALUES(?,?,?)", e.EventID, err.Error(), time.Now().UTC().Format(time.RFC3339Nano)); qerr != nil {
			return qerr
		}
	} else if err != nil {
		return err
	}
	return updateHead(ctx, tx, e)
}
func insertEvent(ctx context.Context, tx *sql.Tx, e domain.Event) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", e.OriginID, e.OriginSeq, e.EventID, e.EventType, e.AggregateType, e.AggregateID, null(e.CorrelationID), null(e.CausationEventID), e.CreatedAt.Format(time.RFC3339Nano), []byte(e.PayloadJSON), e.PrevHash, e.EventHash)
	return err
}
func updateHead(ctx context.Context, tx *sql.Tx, e domain.Event) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO origin_heads VALUES(?,?,?,?) ON CONFLICT(origin_id) DO UPDATE SET head_seq=excluded.head_seq,head_hash=excluded.head_hash,updated_at=excluded.updated_at", e.OriginID, e.OriginSeq, e.EventHash, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
func retryPendingProjections(ctx context.Context, tx *sql.Tx) error {
	for {
		rows, err := tx.QueryContext(ctx, `SELECT e.origin_id,e.origin_seq,e.event_id,e.event_type,e.aggregate_type,e.aggregate_id,COALESCE(e.correlation_id,''),COALESCE(e.causation_event_id,''),e.created_at,e.payload_json,e.prev_hash,e.event_hash FROM events e JOIN pending_projections p ON p.event_id=e.event_id ORDER BY e.created_at,e.origin_id,e.origin_seq`)
		if err != nil {
			return err
		}
		var pending []domain.Event
		for rows.Next() {
			var e domain.Event
			var created string
			if err = rows.Scan(&e.OriginID, &e.OriginSeq, &e.EventID, &e.EventType, &e.AggregateType, &e.AggregateID, &e.CorrelationID, &e.CausationEventID, &created, &e.PayloadJSON, &e.PrevHash, &e.EventHash); err != nil {
				rows.Close()
				return err
			}
			e.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
			if err != nil {
				rows.Close()
				return err
			}
			pending = append(pending, e)
		}
		rows.Close()
		progress := false
		for _, e := range pending {
			err = apply(ctx, tx, e)
			if errors.Is(err, errProjectionDependency) {
				continue
			}
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "DELETE FROM pending_projections WHERE event_id=?", e.EventID); err != nil {
				return err
			}
			progress = true
		}
		if !progress {
			return nil
		}
	}
}
func apply(ctx context.Context, tx *sql.Tx, e domain.Event) error {
	switch e.EventType {
	case "message.created":
		var m domain.Message
		if err := json.Unmarshal(e.PayloadJSON, &m); err != nil {
			return err
		}
		if m.SenderID != e.OriginID {
			return fmt.Errorf("message sender not owned by origin")
		}
		if err := domain.ValidateMessage(m, 64<<10); err != nil {
			return err
		}
		if m.ReplyToMessageID != "" {
			var parentThread string
			if err := tx.QueryRowContext(ctx, "SELECT thread_id FROM messages WHERE message_id=?", m.ReplyToMessageID).Scan(&parentThread); errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: parent message %s", errProjectionDependency, m.ReplyToMessageID)
			} else if err != nil {
				return err
			}
			if parentThread != m.ThreadID {
				return fmt.Errorf("reply parent belongs to another thread")
			}
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO threads(thread_id,peer_id,subject,created_at,last_message_at) VALUES(?,?,?,?,?) ON CONFLICT(thread_id) DO UPDATE SET last_message_at=MAX(last_message_at,excluded.last_message_at)", m.ThreadID, peer(m, e.OriginID), null(m.Subject), m.CreatedAt.Format(time.RFC3339Nano), m.CreatedAt.Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO messages(message_id,thread_id,reply_to_message_id,sender_id,recipient_id,kind,priority,subject,body_markdown,ack_required,reply_depth,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", m.MessageID, m.ThreadID, null(m.ReplyToMessageID), m.SenderID, m.RecipientID, m.Kind, m.Priority, null(m.Subject), m.BodyMarkdown, m.AckRequired, m.ReplyDepth, m.CreatedAt.Format(time.RFC3339Nano))
		return err
	case "message.received", "message.delivered", "message.acknowledged":
		var p struct {
			MessageID string    `json:"message_id"`
			At        time.Time `json:"at"`
		}
		if err := json.Unmarshal(e.PayloadJSON, &p); err != nil {
			return err
		}
		var recipient string
		if err := tx.QueryRowContext(ctx, "SELECT recipient_id FROM messages WHERE message_id=?", p.MessageID).Scan(&recipient); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: message %s", errProjectionDependency, p.MessageID)
		} else if err != nil {
			return err
		}
		if recipient != e.OriginID {
			return fmt.Errorf("receipt not owned by recipient")
		}
		col := map[string]string{"message.received": "received_at", "message.delivered": "delivered_at", "message.acknowledged": "acknowledged_at"}[e.EventType]
		_, err := tx.ExecContext(ctx, "UPDATE messages SET "+col+"=COALESCE("+col+",?) WHERE message_id=?", p.At.Format(time.RFC3339Nano), p.MessageID)
		if err != nil {
			return err
		}
		return nil
	case "task.created":
		var t domain.Task
		if err := json.Unmarshal(e.PayloadJSON, &t); err != nil {
			return err
		}
		if t.CreatedBy != e.OriginID {
			return fmt.Errorf("task creator not owned by origin")
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO threads(thread_id,peer_id,subject,created_at,last_message_at) VALUES(?,?,?,?,?) ON CONFLICT(thread_id) DO NOTHING", t.ThreadID, peerTask(t, e.OriginID), t.Objective, t.CreatedAt.Format(time.RFC3339Nano), t.CreatedAt.Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO tasks(task_id,thread_id,created_by,assigned_to,objective,context_json,expected_deliverable,priority,status,update_interval_seconds,created_at,updated_at,last_progress_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", t.TaskID, t.ThreadID, t.CreatedBy, t.AssignedTo, t.Objective, []byte(t.Context), null(t.ExpectedDeliverable), t.Priority, "proposed", t.UpdateIntervalSeconds, t.CreatedAt.Format(time.RFC3339Nano), t.CreatedAt.Format(time.RFC3339Nano), t.CreatedAt.Format(time.RFC3339Nano))
		return err
	case "task.accepted", "task.started", "task.blocked", "task.completed", "task.failed", "task.declined", "task.cancelled", "task.progress", "task.cancel_requested":
		var p map[string]any
		if err := json.Unmarshal(e.PayloadJSON, &p); err != nil {
			return err
		}
		tid, _ := p["task_id"].(string)
		if tid == "" {
			return domain.ErrInvalid
		}
		var owner, assignee, old string
		if err := tx.QueryRowContext(ctx, "SELECT created_by,assigned_to,status FROM tasks WHERE task_id=?", tid).Scan(&owner, &assignee, &old); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: task %s", errProjectionDependency, tid)
		} else if err != nil {
			return err
		}
		if e.EventType == "task.cancel_requested" {
			if e.OriginID != owner {
				return fmt.Errorf("cancellation not owned by creator")
			}
			return nil
		}
		if e.EventType == "task.progress" {
			if e.OriginID != assignee {
				return fmt.Errorf("task progress not owned by assignee")
			}
			summary, _ := p["summary"].(string)
			next, _ := p["next_step"].(string)
			_, err := tx.ExecContext(ctx, "INSERT INTO task_progress VALUES(?,?,?,?,?)", e.EventID, tid, summary, null(next), e.CreatedAt.Format(time.RFC3339Nano))
			if err == nil {
				_, err = tx.ExecContext(ctx, "UPDATE tasks SET last_progress_at=?,updated_at=? WHERE task_id=?", e.CreatedAt.Format(time.RFC3339Nano), e.CreatedAt.Format(time.RFC3339Nano), tid)
			}
			return err
		}
		status := map[string]string{"task.accepted": "accepted", "task.started": "in_progress", "task.blocked": "blocked", "task.completed": "completed", "task.failed": "failed", "task.declined": "declined", "task.cancelled": "cancelled"}[e.EventType]

		if e.OriginID != assignee {
			return fmt.Errorf("task state not owned by assignee")
		}
		if !domain.ValidTaskTransition(old, status) {
			return fmt.Errorf("illegal task transition %s to %s", old, status)
		}
		blocker, _ := p["blocker"].(string)
		result, _ := p["final_result"].(string)
		_, err := tx.ExecContext(ctx, "UPDATE tasks SET status=?,blocker=?,final_result=?,updated_at=?,last_progress_at=? WHERE task_id=?", status, null(blocker), null(result), e.CreatedAt.Format(time.RFC3339Nano), e.CreatedAt.Format(time.RFC3339Nano), tid)
		return err
	case "thread.muted", "thread.closed":
		state := map[string]string{"thread.muted": "muted", "thread.closed": "closed"}[e.EventType]
		result, err := tx.ExecContext(ctx, "UPDATE threads SET state=? WHERE thread_id=?", state, e.AggregateID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: thread %s", errProjectionDependency, e.AggregateID)
		}
		return nil
	}
	return fmt.Errorf("unknown event type %q", e.EventType)
}
func peer(m domain.Message, origin string) string {
	if m.SenderID == origin {
		return m.RecipientID
	}
	return m.SenderID
}
func peerTask(t domain.Task, origin string) string {
	if t.CreatedBy == origin {
		return t.AssignedTo
	}
	return t.CreatedBy
}
func null(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func (s *Store) EventsAfter(ctx context.Context, origin string, after int64, limit int) ([]domain.Event, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT origin_id,origin_seq,event_id,event_type,aggregate_type,aggregate_id,COALESCE(correlation_id,''),COALESCE(causation_event_id,''),created_at,payload_json,prev_hash,event_hash FROM events WHERE origin_id=? AND origin_seq>? ORDER BY origin_seq LIMIT ?", origin, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Event
	for rows.Next() {
		var e domain.Event
		var ts string
		if err = rows.Scan(&e.OriginID, &e.OriginSeq, &e.EventID, &e.EventType, &e.AggregateType, &e.AggregateID, &e.CorrelationID, &e.CausationEventID, &ts, &e.PayloadJSON, &e.PrevHash, &e.EventHash); err != nil {
			return nil, err
		}
		e.CreatedAt, err = time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
func (s *Store) SetCursor(ctx context.Context, peer, origin string, n int64) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO peer_cursors VALUES(?,?,?,?) ON CONFLICT(peer_id,origin_id) DO UPDATE SET confirmed_seq=MAX(confirmed_seq,excluded.confirmed_seq),updated_at=excluded.updated_at", peer, origin, n, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
func (s *Store) Cursor(ctx context.Context, peer, origin string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, "SELECT confirmed_seq FROM peer_cursors WHERE peer_id=? AND origin_id=?", peer, origin).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return n, err
}
func (s *Store) MarkSent(ctx context.Context, through int64) error {
	_, err := s.db.ExecContext(ctx, "UPDATE messages SET sent_at=COALESCE(sent_at,?) WHERE message_id IN (SELECT aggregate_id FROM events WHERE origin_id=? AND origin_seq<=? AND event_type='message.created')", time.Now().UTC().Format(time.RFC3339Nano), s.localID, through)
	return err
}

type MessageView struct {
	domain.Message
	SentAt, ReceivedAt, DeliveredAt, AcknowledgedAt *time.Time
}

func (s *Store) Messages(ctx context.Context, thread string, unack bool, limit int) ([]MessageView, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	q := "SELECT message_id,thread_id,COALESCE(reply_to_message_id,''),sender_id,recipient_id,kind,priority,COALESCE(subject,''),body_markdown,ack_required,reply_depth,created_at,sent_at,received_at,delivered_at,acknowledged_at FROM messages WHERE 1=1"
	var a []any
	if thread != "" {
		q += " AND thread_id=?"
		a = append(a, thread)
	}
	if unack {
		q += " AND ack_required=1 AND acknowledged_at IS NULL"
	}
	q += " ORDER BY created_at DESC LIMIT ?"
	a = append(a, limit)
	rows, err := s.db.QueryContext(ctx, q, a...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MessageView
	for rows.Next() {
		var v MessageView
		var created string
		var sent, recv, deliv, ack sql.NullString
		if err = rows.Scan(&v.MessageID, &v.ThreadID, &v.ReplyToMessageID, &v.SenderID, &v.RecipientID, &v.Kind, &v.Priority, &v.Subject, &v.BodyMarkdown, &v.AckRequired, &v.ReplyDepth, &created, &sent, &recv, &deliv, &ack); err != nil {
			return nil, err
		}
		v.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		v.SentAt = parseNull(sent)
		v.ReceivedAt = parseNull(recv)
		v.DeliveredAt = parseNull(deliv)
		v.AcknowledgedAt = parseNull(ack)
		out = append(out, v)
	}
	return out, rows.Err()
}
func parseNull(v sql.NullString) *time.Time {
	if !v.Valid {
		return nil
	}
	t, e := time.Parse(time.RFC3339Nano, v.String)
	if e != nil {
		return nil
	}
	return &t
}
func (s *Store) Verify(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT DISTINCT origin_id FROM events")
	if err != nil {
		return err
	}
	var origins []string
	for rows.Next() {
		var o string
		rows.Scan(&o)
		origins = append(origins, o)
	}
	rows.Close()
	for _, o := range origins {
		es, err := s.EventsAfter(ctx, o, 0, 1<<30)
		if err != nil {
			return err
		}
		var prev []byte
		for i, e := range es {
			if e.OriginSeq != int64(i+1) || !bytes.Equal(e.PrevHash, prev) || !e.ValidateHash() {
				return fmt.Errorf("invalid chain %s at %d", o, e.OriginSeq)
			}
			prev = e.EventHash
		}
	}
	checks := []struct{ name, query string }{
		{"pending projections", "SELECT COUNT(*) FROM pending_projections"},
		{"orphan replies", "SELECT COUNT(*) FROM messages child LEFT JOIN messages parent ON parent.message_id=child.reply_to_message_id WHERE child.reply_to_message_id IS NOT NULL AND (parent.message_id IS NULL OR parent.thread_id<>child.thread_id)"},
		{"orphan task threads", "SELECT COUNT(*) FROM tasks t LEFT JOIN threads th ON th.thread_id=t.thread_id WHERE th.thread_id IS NULL"},
		{"cursor beyond head", "SELECT COUNT(*) FROM peer_cursors c LEFT JOIN origin_heads h ON h.origin_id=c.origin_id WHERE c.confirmed_seq>COALESCE(h.head_seq,0)"},
	}
	for _, check := range checks {
		var n int
		if err = s.db.QueryRowContext(ctx, check.query).Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("%s: %d", check.name, n)
		}
	}
	var check string
	if err = s.db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&check); err != nil || check != "ok" {
		return fmt.Errorf("integrity: %s: %w", check, err)
	}
	return nil
}
func (s *Store) Backup(ctx context.Context, path string) error {
	if strings.ContainsRune(path, '\x00') {
		return fmt.Errorf("invalid path")
	}
	_, err := s.db.ExecContext(ctx, "VACUUM INTO ?", path)
	return err
}
