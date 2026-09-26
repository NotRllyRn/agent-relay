package mcp

import (
	"agent-relay/internal/app"
	"agent-relay/internal/store"
	"context"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestPingToolIsDiscoverableAndCallsService(t *testing.T) {
	st, e := store.Open(context.Background(), filepath.Join(t.TempDir(), "x.db"), "a", 1024)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	called := ""
	svc := &app.Service{LocalID: "a", Store: st, MaxMessageBody: 1024, PingPeer: func(_ context.Context, id string) (any, error) {
		called = id
		return map[string]any{"reachable": true}, nil
	}}
	httpServer := httptest.NewServer(New(svc, "test"))
	defer httpServer.Close()
	ctx := context.Background()
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "1"}, nil)
	session, e := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	tools, e := session.ListTools(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, tool := range tools.Tools {
		if tool.Name == "relay_ping_peer" {
			found = true
		}
	}
	if !found {
		t.Fatal("relay_ping_peer not discovered")
	}
	if _, e = session.CallTool(ctx, &sdk.CallToolParams{Name: "relay_ping_peer", Arguments: map[string]any{"peer_id": "b"}}); e != nil {
		t.Fatal(e)
	}
	if called != "b" {
		t.Fatalf("called=%q", called)
	}
}
