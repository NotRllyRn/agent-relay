package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAndSecurity(t *testing.T) {
	t.Setenv("OUT", "out")
	t.Setenv("IN", "in")
	p := filepath.Join(t.TempDir(), "c.json")
	os.WriteFile(p, []byte(`{"agent_id":"a","data_dir":"/tmp/a","mcp_listen":"127.0.0.1:7420","peers":[{"id":"b","url":"https://b.example","token_env":"OUT","inbound_token_env":"IN"}]}`), 0600)
	c, e := Load(p)
	if e != nil {
		t.Fatal(e)
	}
	if c.Peers[0].Token != "out" {
		t.Fatal("secret not resolved")
	}
}
func TestRejectPublicHTTP(t *testing.T) {
	c := Config{AgentID: "a", DataDir: "x", MCPListen: "127.0.0.1:1", Peers: []Peer{{ID: "b", URL: "http://public.example", Token: "x", InboundToken: "y"}}}
	if c.Validate() == nil {
		t.Fatal("accepted insecure URL")
	}
}
