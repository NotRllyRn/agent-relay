package hermes

import (
	"agent-relay/internal/domain"
	"agent-relay/internal/store"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeliveryUsesStableSessionAndIdempotency(t *testing.T) {
	var sessions, runs atomic.Int32
	var key string
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sessions":
			sessions.Add(1)
			json.NewEncoder(w).Encode(map[string]string{"id": "sess"})
		case "/v1/runs":
			runs.Add(1)
			key = r.Header.Get("Idempotency-Key")
			w.WriteHeader(202)
		default:
			w.WriteHeader(404)
		}
	}))
	defer h.Close()
	ctx := context.Background()
	s, e := store.Open(ctx, filepath.Join(t.TempDir(), "x.db"), "b", 128<<10)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	m := domain.Message{MessageID: "m", ThreadID: "t", SenderID: "a", RecipientID: "b", Kind: "request", Priority: "normal", BodyMarkdown: "do it", CreatedAt: time.Now().UTC()}
	raw, _ := json.Marshal(m)
	ev := domain.Event{OriginID: "a", OriginSeq: 1, EventID: "e", EventType: "message.created", AggregateType: "message", AggregateID: "m", CorrelationID: "t", CreatedAt: time.Now().UTC(), PayloadJSON: raw}
	ev.EventHash = ev.Hash()
	if e = s.Ingest(ctx, "a", []domain.Event{ev}); e != nil {
		t.Fatal(e)
	}
	s.QueueDelivery(ctx, "m")
	w := Worker{s, &Client{h.URL, "key", h.Client()}, "b"}
	if e = w.DeliverOnce(ctx); e != nil {
		t.Fatal(e)
	}
	if sessions.Load() != 1 || runs.Load() != 1 || key != "relay-deliver:m" {
		t.Fatalf("sessions=%d runs=%d key=%s", sessions.Load(), runs.Load(), key)
	}
	m2, e := s.GetMessage(ctx, "m")
	if e != nil || m2.DeliveredAt == nil {
		t.Fatal("not delivered", e)
	}
}
