package store

import (
	"agent-relay/internal/domain"
	"context"
	"testing"
	"time"
)

func TestConversationRouteBackfillAndDurability(t *testing.T) {
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
	reply := msg("reply")
	reply.SenderID = "b"
	reply.RecipientID = "a"
	reply.ReplyToMessageID = "root"
	ev, err := b.Append(ctx, "message.created", "message", "reply", "thr_x", "", reply)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Ingest(ctx, "b", []domain.Event{ev}); err != nil {
		t.Fatal(err)
	}
	route := domain.ConversationRoute{ThreadID: "thr_x", OwnerAgentID: "a", Platform: "discord", ChatID: "private", HermesSessionKey: "session", ReplyPolicy: "all"}
	if err = a.BindConversationRoute(ctx, route); err != nil {
		t.Fatal(err)
	}
	route.ChatID = "other"
	if err = a.BindConversationRoute(ctx, route); err == nil {
		t.Fatal("destination mutable")
	}
	route.ChatID = "private"
	if err = b.BindConversationRoute(ctx, route); err == nil {
		t.Fatal("peer owner accepted")
	}
	if err = a.Ingest(ctx, "b", []domain.Event{ev}); err != nil {
		t.Fatal(err)
	}
	a.db.ExecContext(ctx, "UPDATE conversation_callback_jobs SET next_attempt_at=?", time.Now().Add(-time.Minute).UnixNano())
	jobs, err := a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%+v err=%v", jobs, err)
	}
	if err = a.RetryCallback(ctx, jobs[0].CallbackID, "failed", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	jobs, err = a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 1 {
		t.Fatal(jobs, err)
	}
	if err = a.CompleteCallback(ctx, jobs[0].CallbackID); err != nil {
		t.Fatal(err)
	}
	if err = a.RebuildProjections(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err = a.BindConversationRoute(ctx, route); err != nil {
		t.Fatal(err)
	}
	jobs, err = a.DueCallbacks(ctx, 10)
	if err != nil || len(jobs) != 0 {
		t.Fatal("renotified", jobs, err)
	}
	es, _ := a.EventsAfter(ctx, "a", 0, 100)
	if len(es) != 1 {
		t.Fatal("route emitted event")
	}
}
