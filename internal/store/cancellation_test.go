package store

import (
	"agent-relay/internal/domain"
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestCancellationAfterCompletionDoesNotRewriteTerminalState(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, "local")
	at := time.Now().Add(-time.Hour).UTC()
	task := domain.Task{TaskID: "task", ThreadID: "thread", CreatedBy: "a", AssignedTo: "b", Objective: "work", Context: json.RawMessage(`{}`), Priority: "normal", UpdateIntervalSeconds: 60, CreatedAt: at}
	created := signedEvent("a", 1, nil, "task.created", "task", "task", "thread", task, at)
	if e := s.Ingest(ctx, "a", []domain.Event{created}); e != nil {
		t.Fatal(e)
	}
	accepted := signedEvent("b", 1, nil, "task.accepted", "task", "task", "thread", map[string]any{"task_id": "task", "summary": "yes"}, at.Add(time.Minute))
	completed := signedEvent("b", 2, accepted.EventHash, "task.completed", "task", "task", "thread", map[string]any{"task_id": "task", "summary": "done", "final_result": "ok"}, at.Add(2*time.Minute))
	if e := s.Ingest(ctx, "b", []domain.Event{accepted, completed}); e != nil {
		t.Fatal(e)
	}
	cancel := signedEvent("a", 2, created.EventHash, "task.cancel_requested", "task", "task", "thread", map[string]any{"task_id": "task", "summary": "stop"}, at.Add(3*time.Minute))
	if e := s.Ingest(ctx, "a", []domain.Event{cancel}); e != nil {
		t.Fatal(e)
	}
	got, e := s.GetTask(ctx, "task")
	if e != nil || got.Status != "completed" {
		t.Fatalf("task=%#v err=%v", got, e)
	}
}
