package peer

import (
	"agent-relay/internal/domain"
	"agent-relay/internal/store"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func db(t *testing.T, id string) *store.Store {
	s, e := store.Open(context.Background(), filepath.Join(t.TempDir(), id+".db"), id, 128<<10)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestSyncConvergesAndRejectsSpoof(t *testing.T) {
	ctx := context.Background()
	a, b := db(t, "a"), db(t, "b")
	m := domain.Message{MessageID: "msg_1", ThreadID: "thr_1", SenderID: "a", RecipientID: "b", Kind: "question", Priority: "normal", BodyMarkdown: "hello", CreatedAt: time.Now().UTC()}
	if _, e := a.Append(ctx, "message.created", "message", m.MessageID, m.ThreadID, "", m); e != nil {
		t.Fatal(e)
	}
	srv := httptest.NewServer((&Server{Store: b, LocalID: "b", Version: "test", Credentials: []PeerCredential{{"a", "secret"}}, MaxBody: 2 << 20, MaxEvents: 256}).Handler())
	defer srv.Close()
	c := Client{srv.Client(), "a", a, 256}
	if e := c.Sync(ctx, "b", srv.URL, "secret"); e != nil {
		t.Fatal(e)
	}
	ms, e := b.Messages(ctx, "", false, 20)
	if e != nil || len(ms) != 1 {
		t.Fatalf("messages=%d err=%v", len(ms), e)
	}
	if e = c.Sync(ctx, "b", srv.URL, "secret"); e != nil {
		t.Fatal("duplicate sync", e)
	}
	es, _ := a.EventsAfter(ctx, "a", 0, 10)
	for i := range es {
		es[i].OriginID = "b"
		es[i].EventHash = es[i].Hash()
	}
	req := SyncRequest{Events: es}
	rr := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/sync", jsonBody(t, req))
	r.Header.Set("Authorization", "Bearer secret")
	r.Header.Set("Content-Type", "application/json")
	((&Server{Store: b, LocalID: "b", Version: "test", Credentials: []PeerCredential{{"a", "secret"}}, MaxBody: 2 << 20, MaxEvents: 256}).Handler()).ServeHTTP(rr, r)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("spoof status %d", rr.Code)
	}
}
func TestHealthUnauthenticatedAndStatusAuthenticated(t *testing.T) {
	s := db(t, "b")
	h := (&Server{Store: s, LocalID: "b", Version: "v", Credentials: []PeerCredential{{"a", "x"}}, MaxBody: 1000, MaxEvents: 10}).Handler()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/healthz", nil))
	if rr.Code != 200 {
		t.Fatal(rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/v1/status", nil))
	if rr.Code != 401 {
		t.Fatal(rr.Code)
	}
}
func jsonBody(t *testing.T, v any) *bytes.Reader {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return bytes.NewReader(b)
}
