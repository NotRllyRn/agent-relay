package store

import (
	"agent-relay/internal/domain"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func signedEvent(origin string, seq int64, prev []byte, eventType, aggregateType, aggregateID, correlation string, payload any, created time.Time) domain.Event {
	raw, _ := json.Marshal(payload)
	e := domain.Event{OriginID: origin, OriginSeq: seq, EventID: origin + "_evt_" + aggregateID + eventType, EventType: eventType, AggregateType: aggregateType, AggregateID: aggregateID, CorrelationID: correlation, CreatedAt: created.UTC(), PayloadJSON: raw, PrevHash: prev}
	e.EventHash = e.Hash()
	return e
}
func TestDeferredProjectionResolvesAcrossOrigins(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, "local")
	at := time.Now().Add(-time.Minute).UTC()
	receipt := signedEvent("b", 1, nil, "message.received", "message", "m", "t", map[string]any{"message_id": "m", "at": at}, at)
	if e := s.Ingest(ctx, "b", []domain.Event{receipt}); e != nil {
		t.Fatal(e)
	}
	counts, e := s.Counts(ctx)
	if e != nil || counts["pending_projections"] != 1 {
		t.Fatalf("pending=%v err=%v", counts, e)
	}
	m := domain.Message{MessageID: "m", ThreadID: "t", SenderID: "a", RecipientID: "b", Kind: "information", Priority: "normal", BodyMarkdown: "hello", CreatedAt: at.Add(-time.Second)}
	created := signedEvent("a", 1, nil, "message.created", "message", "m", "t", m, m.CreatedAt)
	if e = s.Ingest(ctx, "a", []domain.Event{created}); e != nil {
		t.Fatal(e)
	}
	got, e := s.GetMessage(ctx, "m")
	if e != nil || got.ReceivedAt == nil {
		t.Fatalf("message=%#v err=%v", got, e)
	}
	counts, _ = s.Counts(ctx)
	if counts["pending_projections"] != 0 {
		t.Fatalf("pending=%v", counts)
	}
}
func TestRebuildResolvesDependenciesDeterministically(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, e := Open(ctx, filepath.Join(dir, "r.db"), "local", 128<<10)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	at := time.Now().Add(-time.Hour).UTC()
	receipt := signedEvent("b", 1, nil, "message.received", "message", "m", "t", map[string]any{"message_id": "m", "at": at}, at)
	if e = s.Ingest(ctx, "b", []domain.Event{receipt}); e != nil {
		t.Fatal(e)
	}
	m := domain.Message{MessageID: "m", ThreadID: "t", SenderID: "a", RecipientID: "b", Kind: "information", Priority: "normal", BodyMarkdown: "body", CreatedAt: at.Add(time.Second)}
	created := signedEvent("a", 1, nil, "message.created", "message", "m", "t", m, m.CreatedAt)
	if e = s.Ingest(ctx, "a", []domain.Event{created}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.db.ExecContext(ctx, "DELETE FROM messages"); e != nil {
		t.Fatal(e)
	}
	if e = s.RebuildProjections(ctx, filepath.Join(dir, "backup.db")); e != nil {
		t.Fatal(e)
	}
	got, e := s.GetMessage(ctx, "m")
	if e != nil || got.ReceivedAt == nil || got.BodyMarkdown != "body" {
		t.Fatalf("rebuilt=%#v err=%v", got, e)
	}
}
func TestRejectsForgedMessageSender(t *testing.T) {
	s := openTest(t, "local")
	m := domain.Message{MessageID: "m", ThreadID: "t", SenderID: "other", RecipientID: "b", Kind: "information", Priority: "normal", BodyMarkdown: "x", CreatedAt: time.Now().UTC()}
	e := signedEvent("a", 1, nil, "message.created", "message", "m", "t", m, m.CreatedAt)
	if s.Ingest(context.Background(), "a", []domain.Event{e}) == nil {
		t.Fatal("accepted forged sender")
	}
}
