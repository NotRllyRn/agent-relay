package store

import (
	"agent-relay/internal/domain"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestCallbacksPolicyCoalesceThrottleTerminalRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "r.db")
	a, err := Open(ctx, path, "a", 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { a.Close() }()
	b := openTest(t, "b")
	task := domain.Task{TaskID: "task_x", ThreadID: "thr_x", CreatedBy: "a", AssignedTo: "b", Objective: "test", Context: json.RawMessage(`{}`), Priority: "normal", CreatedAt: time.Now()}
	ev, err := a.Append(ctx, "task.created", "task", task.TaskID, task.ThreadID, "", task)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Ingest(ctx, "a", []domain.Event{ev}); err != nil {
		t.Fatal(err)
	}
	route := domain.ConversationRoute{ThreadID: "thr_x", TaskID: "task_x", OwnerAgentID: "a", Platform: "discord", ChatID: "chat", HermesSessionKey: "session"}
	if err = a.BindConversationRoute(ctx, route); err != nil {
		t.Fatal(err)
	}
	send := func(kind string, p map[string]any) {
		t.Helper()
		p["task_id"] = "task_x"
		ev, err := b.Append(ctx, kind, "task", "task_x", "thr_x", "", p)
		if err != nil {
			t.Fatal(err)
		}
		if err = a.Ingest(ctx, "b", []domain.Event{ev}); err != nil {
			t.Fatal(err)
		}
	}
	send("task.accepted", map[string]any{})
	send("task.started", map[string]any{})
	send("task.progress", map[string]any{"summary": "heartbeat"})
	jobs, err := a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 0 {
		t.Fatal("coalesce window", jobs, err)
	}
	a.db.ExecContext(ctx, "UPDATE conversation_callback_jobs SET next_attempt_at=?", time.Now().Add(-time.Minute).UnixNano())
	jobs, err = a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 1 || len(jobs[0].Events) != 2 {
		t.Fatal("normal policy", jobs, err)
	}
	if err = a.CompleteCallback(ctx, jobs[0].CallbackID); err != nil {
		t.Fatal(err)
	}
	send("task.progress", map[string]any{"summary": "real", "meaningful": true})
	send("task.progress", map[string]any{"summary": "real2", "meaningful": true})
	a.db.ExecContext(ctx, "UPDATE conversation_callback_jobs SET next_attempt_at=? WHERE state='pending'", time.Now().Add(-time.Minute).UnixNano())
	jobs, err = a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 1 || len(jobs[0].Events) != 2 {
		t.Fatal("coalesce", jobs, err)
	}
	id := jobs[0].CallbackID
	again, err := a.DueCallbacks(ctx, 10)
	if err != nil || len(again) != 1 || again[0].CallbackID != id {
		t.Fatal("stable batch", again, err)
	}
	if err = a.CompleteCallback(ctx, id); err != nil {
		t.Fatal(err)
	}
	send("task.progress", map[string]any{"summary": "real3", "meaningful": true})
	a.db.ExecContext(ctx, "UPDATE conversation_callback_jobs SET next_attempt_at=? WHERE state='pending'", time.Now().Add(-time.Minute).UnixNano())
	jobs, err = a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 0 {
		t.Fatal("progress throttle", jobs, err)
	}
	send("task.completed", map[string]any{"final_result": "done"})
	a.Close()
	a, err = Open(ctx, path, "a", 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err = a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 1 || jobs[0].Events[0].EventType != "task.completed" {
		t.Fatal("terminal immediate/restart", jobs, err)
	}
	if err = a.CompleteCallback(ctx, jobs[0].CallbackID); err != nil {
		t.Fatal(err)
	}
	if err = a.RebuildProjections(ctx, ""); err != nil {
		t.Fatal(err)
	}
	// Expire throttling: terminal suppression, not the progress timer, must
	// prevent the older pending progress event from resurfacing.
	if _, err = a.db.ExecContext(ctx, "UPDATE conversation_callback_jobs SET delivered_at=? WHERE delivered_at IS NOT NULL", time.Now().Add(-10*time.Minute).UnixNano()); err != nil {
		t.Fatal(err)
	}
	jobs, err = a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 0 {
		t.Fatal("rebuild renotify", jobs, err)
	}
}

func TestTerminalSuppressionPreservesFrozenBatchesAndReplies(t *testing.T) {
	ctx := context.Background()
	a, b := openTest(t, "a"), openTest(t, "b")
	task := domain.Task{TaskID: "task_x", ThreadID: "thr_x", CreatedBy: "a", AssignedTo: "b", Objective: "test", Context: json.RawMessage(`{}`), Priority: "normal", CreatedAt: time.Now()}
	root, err := a.Append(ctx, "task.created", "task", task.TaskID, task.ThreadID, "", task)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Ingest(ctx, "a", []domain.Event{root}); err != nil {
		t.Fatal(err)
	}
	if err = a.BindConversationRoute(ctx, domain.ConversationRoute{ThreadID: task.ThreadID, OwnerAgentID: "a", Platform: "discord", ChatID: "chat", HermesSessionKey: "session"}); err != nil {
		t.Fatal(err)
	}
	send := func(kind, aggregate, id string, payload any) {
		t.Helper()
		ev, err := b.Append(ctx, kind, aggregate, id, task.ThreadID, "", payload)
		if err != nil {
			t.Fatal(err)
		}
		if err = a.Ingest(ctx, "b", []domain.Event{ev}); err != nil {
			t.Fatal(err)
		}
	}
	makeDue := func() {
		t.Helper()
		if _, err := a.db.ExecContext(ctx, "UPDATE conversation_callback_jobs SET next_attempt_at=? WHERE state='pending'", time.Now().Add(-time.Minute).UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	send("task.accepted", "task", task.TaskID, map[string]any{"task_id": task.TaskID})
	send("task.started", "task", task.TaskID, map[string]any{"task_id": task.TaskID})
	makeDue()
	jobs, err := a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 1 {
		t.Fatal(jobs, err)
	}
	frozen := jobs[0].CallbackID
	send("task.progress", "task", task.TaskID, map[string]any{"task_id": task.TaskID, "meaningful": true})
	send("task.completed", "task", task.TaskID, map[string]any{"task_id": task.TaskID, "final_result": "done"})
	reply := msg("reply")
	messageRoot, err := a.Append(ctx, "message.created", "message", "root", task.ThreadID, "", msg("root"))
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Ingest(ctx, "a", []domain.Event{messageRoot}); err != nil {
		t.Fatal(err)
	}
	reply.SenderID, reply.RecipientID, reply.ReplyToMessageID = "b", "a", "root"
	send("message.created", "message", reply.MessageID, reply)
	makeDue()
	jobs, err = a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 3 || jobs[0].Events[0].EventType != "task.completed" {
		t.Fatal("terminal, frozen update and reply expected", jobs, err)
	}
	for _, job := range jobs {
		if job.CallbackID == frozen && len(job.Events) != 2 {
			t.Fatal("frozen batch changed", job)
		}
		if err = a.CompleteCallback(ctx, job.CallbackID); err != nil {
			t.Fatal(err)
		}
	}
	// A late lower-rank update must also remain suppressed after completion.
	send("task.progress", "task", task.TaskID, map[string]any{"task_id": task.TaskID, "meaningful": true, "summary": "late"})
	makeDue()
	jobs, err = a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 0 {
		t.Fatal("late update resurfaced", jobs, err)
	}
}

func TestCallbackIngestRollback(t *testing.T) {
	ctx := context.Background()
	a := openTest(t, "a")
	b := openTest(t, "b")
	root, err := a.Append(ctx, "message.created", "message", "root", "thr_x", "", msg("root"))
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Ingest(ctx, "a", []domain.Event{root}); err != nil {
		t.Fatal(err)
	}
	r := domain.ConversationRoute{ThreadID: "thr_x", OwnerAgentID: "a", Platform: "discord", ChatID: "chat", HermesSessionKey: "session", ReplyPolicy: "all"}
	if err = a.BindConversationRoute(ctx, r); err != nil {
		t.Fatal(err)
	}
	m := msg("reply")
	m.SenderID = "b"
	m.RecipientID = "a"
	m.ReplyToMessageID = "root"
	ev, err := b.Append(ctx, "message.created", "message", m.MessageID, m.ThreadID, "", m)
	if err != nil {
		t.Fatal(err)
	}
	bad := ev
	bad.OriginSeq++
	bad.EventID = "evt_invalid"
	bad.PrevHash = ev.EventHash
	bad.EventType = "conversation.route"
	bad.EventHash = bad.Hash()
	if err = a.Ingest(ctx, "b", []domain.Event{ev, bad}); err == nil {
		t.Fatal("peer route event accepted")
	}
	var n int
	if err = a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM conversation_callback_jobs").Scan(&n); err != nil || n != 0 {
		t.Fatal("rollback leaked callback", n, err)
	}
	head, _, err := a.Head(ctx, "b")
	if err != nil || head != 0 {
		t.Fatal("rollback leaked event", head, err)
	}
	if err = a.Ingest(ctx, "b", []domain.Event{ev}); err != nil {
		t.Fatal(err)
	}
	if err = a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM conversation_callback_jobs").Scan(&n); err != nil || n != 1 {
		t.Fatal("callback not persisted in ingestion", n, err)
	}
}

func TestRouteRejectsPeerOriginAndPolicies(t *testing.T) {
	ctx := context.Background()
	a := openTest(t, "a")
	b := openTest(t, "b")
	m := msg("remote")
	m.SenderID = "b"
	m.RecipientID = "a"
	ev, err := b.Append(ctx, "message.created", "message", m.MessageID, m.ThreadID, "", m)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Ingest(ctx, "b", []domain.Event{ev}); err != nil {
		t.Fatal(err)
	}
	r := domain.ConversationRoute{ThreadID: m.ThreadID, OwnerAgentID: "a", Platform: "discord", ChatID: "chat", HermesSessionKey: "session"}
	if err = a.BindConversationRoute(ctx, r); err == nil {
		t.Fatal("peer-origin route accepted")
	}
	local := msg("local")
	local.ThreadID = "thr_local"
	if _, err = a.Append(ctx, "message.created", "message", local.MessageID, local.ThreadID, "", local); err != nil {
		t.Fatal(err)
	}
	r.ThreadID = local.ThreadID
	r.ReplyPolicy = "silent"
	if err = a.BindConversationRoute(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.ReplyPolicy = "terminal_only"
	if err = a.BindConversationRoute(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.ReplyPolicy = "bad"
	if err = a.BindConversationRoute(ctx, r); err == nil {
		t.Fatal("invalid policy")
	}
}
