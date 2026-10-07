package store

import (
	"agent-relay/internal/domain"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

const conversationSchema = `
CREATE TABLE IF NOT EXISTS conversation_routes(thread_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,route_json BLOB NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS conversation_routes_task ON conversation_routes(task_id) WHERE task_id<>'';
CREATE TABLE IF NOT EXISTS conversation_callback_jobs(callback_id TEXT PRIMARY KEY,route_thread_id TEXT NOT NULL,source_event_id TEXT NOT NULL UNIQUE,event_json BLOB NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at INTEGER NOT NULL,last_error TEXT NOT NULL DEFAULT '',delivered_at INTEGER,batch_id TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS conversation_callback_due ON conversation_callback_jobs(state,next_attempt_at);
`

// BindConversationRoute permits idempotent registration and policy changes only.
// Destination, owner and optional task binding are immutable.
func (s *Store) BindConversationRoute(ctx context.Context, r domain.ConversationRoute) error {
	if r.ReplyPolicy == "" {
		r.ReplyPolicy = "normal"
	}
	switch r.ReplyPolicy {
	case "normal", "all", "terminal_only", "silent":
	default:
		return fmt.Errorf("invalid reply policy")
	}
	if r.OwnerAgentID != s.localID || r.ThreadID == "" || r.Platform == "" || r.ChatID == "" || (r.HermesSessionID == "" && r.HermesSessionKey == "") {
		return fmt.Errorf("invalid local conversation route")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Earliest creating event defines ownership; sending a reply cannot claim a peer thread.
	var owner string
	err = tx.QueryRowContext(ctx, `SELECT origin_id FROM events WHERE event_type IN ('message.created','task.created') AND json_extract(payload_json,'$.thread_id')=? ORDER BY created_at,origin_id,origin_seq LIMIT 1`, r.ThreadID).Scan(&owner)
	if err != nil {
		return err
	}
	if owner != s.localID {
		return fmt.Errorf("thread not locally originated")
	}
	if r.TaskID != "" {
		var thread, creator string
		err = tx.QueryRowContext(ctx, "SELECT thread_id,created_by FROM tasks WHERE task_id=?", r.TaskID).Scan(&thread, &creator)
		if err != nil {
			return err
		}
		if thread != r.ThreadID || creator != s.localID {
			return fmt.Errorf("task not owned by route owner")
		}
	}
	var oldRaw []byte
	err = tx.QueryRowContext(ctx, "SELECT route_json FROM conversation_routes WHERE thread_id=?", r.ThreadID).Scan(&oldRaw)
	if err == nil {
		var old domain.ConversationRoute
		if err = json.Unmarshal(oldRaw, &old); err != nil {
			return err
		}
		old.ReplyPolicy = r.ReplyPolicy
		if old != r {
			return fmt.Errorf("conversation destination immutable")
		}
	} else if err != sql.ErrNoRows {
		return err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO conversation_routes VALUES(?,?,?) ON CONFLICT(thread_id) DO UPDATE SET route_json=excluded.route_json`, r.ThreadID, r.TaskID, raw); err != nil {
		return err
	}
	if err = s.backfillCallbacks(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}
