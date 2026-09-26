package app

import (
	"agent-relay/internal/store"
	"context"
	"path/filepath"
	"testing"
)

func svc(t *testing.T, id string) *Service {
	s, e := store.Open(context.Background(), filepath.Join(t.TempDir(), "x.db"), id, 128<<10)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return &Service{LocalID: id, Store: s, MaxMessageBody: 64 << 10}
}
func TestSendReplyAndAckOwnership(t *testing.T) {
	ctx := context.Background()
	a := svc(t, "a")
	r, e := a.Send(ctx, SendInput{Recipient: "b", Body: "one", AckRequired: true})
	if e != nil {
		t.Fatal(e)
	}
	rr, e := a.Reply(ctx, r.ThreadID, r.MessageID, "two", "", "", false)
	if e != nil {
		t.Fatal(e)
	}
	m, e := a.Store.GetMessage(ctx, rr.MessageID)
	if e != nil || m.ReplyToMessageID != r.MessageID || m.BodyMarkdown != "two" {
		t.Fatalf("reply %#v %v", m, e)
	}
	if e = a.Acknowledge(ctx, r.MessageID); e == nil {
		t.Fatal("sender acknowledged own message")
	}
}
func TestTaskLifecycleOwnership(t *testing.T) {
	ctx := context.Background()
	a := svc(t, "a")
	task, e := a.Delegate(ctx, DelegateInput{Recipient: "b", Objective: "work", Context: map[string]any{"x": 1}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = a.UpdateTask(ctx, task.TaskID, "accepted", "ok", "", "", ""); e == nil {
		t.Fatal("creator changed assignee state")
	}
}
func TestReplyRateLimit(t *testing.T) {
	ctx := context.Background()
	s := svc(t, "a")
	s.ThreadRateLimit = 1
	first, err := s.Send(ctx, SendInput{Recipient: "b", Body: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Reply(ctx, first.ThreadID, first.MessageID, "two", "", "", false); err == nil {
		t.Fatal("reply exceeded per-thread rate but was accepted")
	}
}
