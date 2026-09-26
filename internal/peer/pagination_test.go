package peer

import (
	"agent-relay/internal/domain"
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSyncPaginatesBothDirections(t *testing.T) {
	ctx := context.Background()
	a, b := db(t, "a"), db(t, "b")
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("msg_%d", i)
		m := domain.Message{MessageID: id, ThreadID: fmt.Sprintf("thr_%d", i), SenderID: "a", RecipientID: "b", Kind: "information", Priority: "normal", BodyMarkdown: "x", CreatedAt: time.Now().UTC()}
		if _, e := a.Append(ctx, "message.created", "message", id, m.ThreadID, "", m); e != nil {
			t.Fatal(e)
		}
	}
	srv := httptest.NewServer((&Server{Store: b, LocalID: "b", Version: "test", Credentials: []PeerCredential{{ID: "a", Token: "secret"}}, MaxBody: 2 << 20, MaxEvents: 1}).Handler())
	defer srv.Close()
	c := Client{HTTP: srv.Client(), LocalID: "a", Store: a, MaxEvents: 1}
	if e := c.Sync(ctx, "b", srv.URL, "secret"); e != nil {
		t.Fatal(e)
	}
	messages, e := b.Messages(ctx, "", false, 100)
	if e != nil || len(messages) != 3 {
		t.Fatalf("messages=%d err=%v", len(messages), e)
	}
	head, _, e := a.Head(ctx, "b")
	if e != nil || head != 3 {
		t.Fatalf("remote head=%d err=%v", head, e)
	}
}
