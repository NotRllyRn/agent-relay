package store

import (
	"agent-relay/internal/domain"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCallbackBacklogResponseBoundAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bounded.db")
	s, err := Open(ctx, path, "a", 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	route := domain.ConversationRoute{ThreadID: "thr_big", OwnerAgentID: "a", Platform: "discord", ChatID: "chat", HermesSessionKey: "session"}
	if _, err = s.Append(ctx, "task.created", "task", "task_big", route.ThreadID, "", domain.Task{TaskID: "task_big", ThreadID: route.ThreadID, CreatedBy: "a", AssignedTo: "b", Objective: "test", Context: json.RawMessage(`{}`), Priority: "normal"}); err != nil {
		t.Fatal(err)
	}
	if err = s.BindConversationRoute(ctx, route); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		ev := domain.Event{EventID: fmt.Sprintf("large_%02d", i), EventType: "message.received", AggregateType: "message", AggregateID: "message", PayloadJSON: json.RawMessage(`{"body":"` + strings.Repeat("x", 100000) + `"}`)}
		kind := "update"
		thread := route.ThreadID
		if i == 39 {
			kind = "terminal"
			ev.EventType = "task.completed"
			thread = "thr_other"
			other := route
			other.ThreadID = thread
			if _, err = s.Append(ctx, "task.created", "task", "task_other", thread, "", domain.Task{TaskID: "task_other", ThreadID: thread, CreatedBy: "a", AssignedTo: "b", Objective: "test", Context: json.RawMessage(`{}`), Priority: "normal"}); err != nil {
				t.Fatal(err)
			}
			if err = s.BindConversationRoute(ctx, other); err != nil {
				t.Fatal(err)
			}
		}
		raw, _ := json.Marshal(ev)
		_, err = s.db.ExecContext(ctx, `INSERT INTO conversation_callback_jobs(callback_id,route_thread_id,source_event_id,event_json,kind,next_attempt_at) VALUES(?,?,?,?,?,0)`, "cb_"+ev.EventID, thread, ev.EventID, raw, kind)
		if err != nil {
			t.Fatal(err)
		}
	}
	jobs, err := s.DueCallbacks(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(jobs)
	if len(raw) >= 2*1024*1024 {
		t.Fatalf("oversized response: %d", len(raw))
	}
	if len(jobs) == 0 || jobs[0].Events[0].EventType != "task.completed" {
		t.Fatal("terminal starved")
	}
	for _, job := range jobs {
		if len(job.Events) > 16 {
			t.Fatal("unbounded membership")
		}
	}
	s.Close()
	s, err = Open(ctx, path, "a", 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.DueCallbacks(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	rawAgain, _ := json.Marshal(again)
	if string(raw) != string(rawAgain) {
		t.Fatal("restart changed frozen callbacks")
	}
	for _, job := range again {
		if err = s.CompleteCallback(ctx, job.CallbackID); err != nil {
			t.Fatal(err)
		}
	}
	remaining, err := s.DueCallbacks(ctx, 100)
	if err != nil || len(remaining) == 0 {
		t.Fatal("remaining events lost", err)
	}
	// Retry preserves logical identity and exposes reconciliation failures.
	id := remaining[0].CallbackID
	if err = s.RetryCallback(ctx, id, "gateway callback uncertain; reconciliation required", time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	status, err := s.CallbackStatus(ctx)
	if err != nil || status.Pending == 0 || len(status.Failures) != 1 || status.Failures[0].CallbackID != id || status.Failures[0].Attempts != 1 {
		t.Fatal("missing reconciliation status", status, err)
	}
	retry, err := s.DueCallbacks(ctx, 100)
	if err != nil || retry[0].CallbackID != id || len(retry[0].Events) != len(remaining[0].Events) {
		t.Fatal("retry identity changed", err)
	}
	// An oversized legacy frozen batch must also be pollable, without changing
	// database membership or losing the full stored events.
	if _, err = s.db.ExecContext(ctx, `UPDATE conversation_callback_jobs SET batch_id='legacy',next_attempt_at=0 WHERE state='pending'`); err != nil {
		t.Fatal(err)
	}
	legacy, err := s.DueCallbacks(ctx, 100)
	if err != nil || len(legacy) != 1 || legacy[0].CallbackID != "legacy" || legacy[0].Events[0].EventType != "relay.callback_summary" {
		t.Fatal("legacy batch unpollable", err)
	}
	var members int
	if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM conversation_callback_jobs WHERE batch_id='legacy'`).Scan(&members); err != nil || members < 20 {
		t.Fatal("legacy membership changed", members, err)
	}
}
