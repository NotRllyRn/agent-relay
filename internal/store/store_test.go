package store

import (
	"agent-relay/internal/domain"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTest(t *testing.T, id string) *Store {
	t.Helper()
	s, e := Open(context.Background(), filepath.Join(t.TempDir(), "r.db"), id, 128<<10)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func msg(id string) domain.Message {
	return domain.Message{MessageID: id, ThreadID: "thr_x", SenderID: "a", RecipientID: "b", Kind: "question", Priority: "normal", BodyMarkdown: "hello", CreatedAt: time.Now().UTC()}
}
func TestAppendPersistenceProjectionAndVerify(t *testing.T) {
	ctx := context.Background()
	d := t.TempDir()
	p := filepath.Join(d, "r.db")
	s, e := Open(ctx, p, "a", 128<<10)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Append(ctx, "message.created", "message", "msg_x", "thr_x", "", msg("msg_x")); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = Open(ctx, p, "a", 128<<10)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Verify(ctx); e != nil {
		t.Fatal(e)
	}
	ms, e := s.Messages(ctx, "thr_x", false, 20)
	if e != nil || len(ms) != 1 || ms[0].BodyMarkdown != "hello" {
		t.Fatalf("projection: %#v %v", ms, e)
	}
}
func TestIngestDuplicateGapAndDivergence(t *testing.T) {
	ctx := context.Background()
	a := openTest(t, "a")
	b := openTest(t, "b")
	e, er := a.Append(ctx, "message.created", "message", "msg_x", "thr_x", "", msg("msg_x"))
	if er != nil {
		t.Fatal(er)
	}
	if er = b.Ingest(ctx, "a", []domain.Event{e}); er != nil {
		t.Fatal(er)
	}
	if er = b.Ingest(ctx, "a", []domain.Event{e}); er != nil {
		t.Fatal("duplicate", er)
	}
	bad := e
	bad.EventHash = []byte("bad")
	if !errors.Is(b.Ingest(ctx, "a", []domain.Event{bad}), domain.ErrDivergence) {
		t.Fatal("divergence not detected")
	}
	gap := e
	gap.OriginSeq = 3
	gap.EventID = "evt_gap"
	gap.PrevHash = e.EventHash
	gap.EventHash = gap.Hash()
	if !errors.Is(b.Ingest(ctx, "a", []domain.Event{gap}), domain.ErrGap) {
		t.Fatal("gap not detected")
	}
}
func TestReceiptRollsBackWhenUnknown(t *testing.T) {
	s := openTest(t, "a")
	_, e := s.Append(context.Background(), "message.received", "message", "nope", "", "", map[string]any{"message_id": "nope", "at": time.Now()})
	if e == nil {
		t.Fatal("expected projection failure")
	}
	n, _, _ := s.Head(context.Background(), "a")
	if n != 0 {
		t.Fatal("event committed despite projection failure")
	}
}
func TestBackup(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, "a")
	_, _ = s.Append(ctx, "message.created", "message", "m", "t", "", msg("m"))
	p := filepath.Join(t.TempDir(), "backup.db")
	if e := s.Backup(ctx, p); e != nil {
		t.Fatal(e)
	}
	x, e := Open(ctx, p, "a", 128<<10)
	if e != nil {
		t.Fatal(e)
	}
	defer x.Close()
	if e = x.Verify(ctx); e != nil {
		t.Fatal(e)
	}
}
func TestExactPayloadBytesMatter(t *testing.T) {
	e := domain.Event{OriginID: "a", OriginSeq: 1, EventID: "e", EventType: "x", AggregateType: "x", AggregateID: "x", CreatedAt: time.Now().UTC(), PayloadJSON: json.RawMessage(`{"a":1}`)}
	e.EventHash = e.Hash()
	e.PayloadJSON = json.RawMessage(`{ "a": 1 }`)
	if e.ValidateHash() {
		t.Fatal("reencoding accepted")
	}
}
