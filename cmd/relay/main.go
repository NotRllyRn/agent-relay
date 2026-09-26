package main

import (
	"agent-relay/internal/app"
	"agent-relay/internal/config"
	"agent-relay/internal/hermes"
	relaymcp "agent-relay/internal/mcp"
	"agent-relay/internal/notify"
	"agent-relay/internal/peer"
	"agent-relay/internal/store"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

var version = "0.1.0-dev"

func main() {
	if e := run(os.Args[1:]); e != nil {
		slog.Error("relay failed", "error", e)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "version":
		fmt.Println(version)
		return nil
	case "init":
		return initCmd(args[1:])
	case "serve":
		return serveCmd(args[1:])
	default:
		return operatorCmd(args[0], args[1:])
	}
}
func usage() error {
	return errors.New("usage: relay <init|serve|status|ping|inbox|thread|tasks|task|sync|verify|backup|rebuild-projections|version>")
}
func configFlag(name string, args []string) (*config.Config, error) {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	p := f.String("config", "config.json", "configuration file")
	if e := f.Parse(args); e != nil {
		return nil, e
	}
	return config.Load(*p)
}
func operatorConfig(args []string) (*config.Config, []string, error) {
	path := "config.json"
	var positional []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--config" {
			if i+1 == len(args) {
				return nil, nil, errors.New("--config requires a path")
			}
			path = args[i+1]
			i++
		} else {
			positional = append(positional, args[i])
		}
	}
	c, err := config.Load(path)
	return c, positional, err
}
func initCmd(args []string) error {
	f := flag.NewFlagSet("init", flag.ContinueOnError)
	id := f.String("agent-id", "", "canonical agent ID")
	dir := f.String("data-dir", "", "data directory")
	out := f.String("config", "config.json", "config output")
	if e := f.Parse(args); e != nil {
		return e
	}
	if *id == "" {
		return errors.New("--agent-id is required")
	}
	if *dir == "" {
		*dir = filepath.Join(".", "data")
	}
	if _, e := os.Stat(*out); e == nil {
		return fmt.Errorf("%s already exists", *out)
	}
	if e := os.MkdirAll(*dir, 0700); e != nil {
		return e
	}
	tokenBytes := make([]byte, 32)
	if _, e := rand.Read(tokenBytes); e != nil {
		return e
	}
	token := hex.EncodeToString(tokenBytes)
	doc := map[string]any{"agent_id": *id, "data_dir": *dir, "peer_listen": ":7419", "mcp_listen": "127.0.0.1:7420", "sync_interval_seconds": 30, "hermes": map[string]any{"base_url": "http://127.0.0.1:8642", "api_key_env": "HERMES_API_SERVER_KEY"}, "discord": map[string]any{"enabled": true, "target": "discord", "max_silence_minutes": 60}, "peers": []any{}}
	b, _ := json.MarshalIndent(doc, "", "  ")
	if e := os.WriteFile(*out, append(b, '\n'), 0600); e != nil {
		return e
	}
	s, e := store.Open(context.Background(), filepath.Join(*dir, "relay.db"), *id, 128<<10)
	if e != nil {
		return e
	}
	s.Close()
	fmt.Printf("Initialized %s. Inbound peer token (shown once): %s\n", *out, token)
	return nil
}
func open(c *config.Config) (*store.Store, error) {
	return store.Open(context.Background(), c.DBPath(), c.AgentID, c.MaxEventPayload)
}
func httpClient() *http.Client { return &http.Client{Timeout: 20 * time.Second} }
func creds(c *config.Config) []peer.PeerCredential {
	var x []peer.PeerCredential
	for _, p := range c.Peers {
		x = append(x, peer.PeerCredential{ID: p.ID, Token: p.InboundToken})
	}
	return x
}
func serveCmd(args []string) error {
	c, e := configFlag("serve", args)
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	s, e := store.Open(ctx, c.DBPath(), c.AgentID, c.MaxEventPayload)
	if e != nil {
		return e
	}
	defer s.Close()
	hclient := &hermes.Client{BaseURL: c.Hermes.BaseURL, APIKey: os.Getenv(c.Hermes.APIKeyEnv), HTTP: httpClient()}
	wake := make(chan struct{}, 1)
	svc := &app.Service{LocalID: c.AgentID, Store: s, MaxMessageBody: c.MaxMessageBody, NotifyTasks: c.Discord.Enabled, WakeSync: func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}}
	ps := &peer.Server{Store: s, LocalID: c.AgentID, Version: version, Credentials: creds(c), MaxBody: c.MaxRequestBytes, MaxEvents: c.MaxEvents, HermesHealth: hclient.Health}
	peerHTTP := &http.Server{Addr: c.PeerListen, Handler: ps.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	mux := http.NewServeMux()
	mux.Handle("/mcp", relaymcp.New(svc, version))
	controlHTTP := &http.Server{Addr: c.MCPListen, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	errc := make(chan error, 2)
	go func() {
		if x := peerHTTP.ListenAndServe(); x != nil && !errors.Is(x, http.ErrServerClosed) {
			errc <- x
		}
	}()
	go func() {
		if x := controlHTTP.ListenAndServe(); x != nil && !errors.Is(x, http.ErrServerClosed) {
			errc <- x
		}
	}()
	pc := &peer.Client{HTTP: httpClient(), LocalID: c.AgentID, Store: s, MaxEvents: c.MaxEvents}
	dw := &hermes.Worker{Store: s, Client: hclient, LocalID: c.AgentID}
	nw := &notify.Worker{Store: s, Sender: notify.HermesSender{}, Target: c.Discord.Target}
	go workers(ctx, c, pc, dw, nw, wake)
	slog.Info("agent relay started", "agent_id", c.AgentID, "peer_listen", c.PeerListen, "mcp_listen", c.MCPListen)
	select {
	case <-ctx.Done():
	case e = <-errc:
		cancel()
	}
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	peerHTTP.Shutdown(shutdown)
	controlHTTP.Shutdown(shutdown)
	return e
}
func workers(ctx context.Context, c *config.Config, pc *peer.Client, dw *hermes.Worker, nw *notify.Worker, wake <-chan struct{}) {
	tick := time.NewTicker(time.Duration(c.SyncIntervalSeconds) * time.Second)
	fast := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	defer fast.Stop()
	syncAll := func() {
		for _, p := range c.Peers {
			if e := pc.Sync(ctx, p.ID, p.URL, p.Token); e != nil {
				slog.Warn("peer sync failed", "peer_id", p.ID, "error", e)
			}
		}
	}
	syncAll()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			syncAll()
		case <-wake:
			syncAll()
		case <-fast.C:
			dw.DeliverOnce(ctx)
			if c.Discord.Enabled {
				nw.Once(ctx)
			}
		}
	}
}
func operatorCmd(cmd string, args []string) error {
	c, positional, e := operatorConfig(args)
	if e != nil {
		return e
	}
	s, e := open(c)
	if e != nil {
		return e
	}
	defer s.Close()
	ctx := context.Background()
	pc := &peer.Client{HTTP: httpClient(), LocalID: c.AgentID, Store: s, MaxEvents: c.MaxEvents}
	switch cmd {
	case "verify":
		e = s.Verify(ctx)
		if e == nil {
			fmt.Println("ok")
		}
		return e
	case "backup":
		if len(positional) < 1 {
			return errors.New("backup requires path before --config")
		}
		return s.Backup(ctx, positional[0])
	case "rebuild-projections":
		return s.RebuildProjections(ctx, store.BackupName(c.DBPath()))
	case "inbox":
		v, e := s.Messages(ctx, "", false, 100)
		return printJSON(v, e)
	case "thread":
		if len(positional) < 1 {
			return errors.New("thread requires ID")
		}
		v, e := s.Messages(ctx, positional[0], false, 100)
		return printJSON(v, e)
	case "tasks":
		v, e := s.ListTasks(ctx, "", "", 100)
		return printJSON(v, e)
	case "task":
		if len(positional) < 1 {
			return errors.New("task requires ID")
		}
		v, e := s.GetTask(ctx, positional[0])
		return printJSON(v, e)
	case "sync":
		for _, p := range c.Peers {
			if e = pc.Sync(ctx, p.ID, p.URL, p.Token); e != nil {
				return e
			}
		}
		fmt.Println("ok")
		return nil
	case "ping":
		if len(positional) < 1 {
			return errors.New("ping requires peer ID")
		}
		for _, p := range c.Peers {
			if p.ID == positional[0] {
				st, lat, e := pc.Ping(ctx, p.URL, p.Token)
				if e == nil {
					fmt.Printf("latency_ms=%d\n", lat.Milliseconds())
					return printJSON(st, nil)
				}
				return e
			}
		}
		return errors.New("unknown peer")
	case "status":
		head, _, _ := s.Head(ctx, c.AgentID)
		counts, e := s.Counts(ctx)
		return printJSON(map[string]any{"agent": c.AgentID, "local_head": head, "counts": counts}, e)
	default:
		return usage()
	}
}
func printJSON(v any, e error) error {
	if e != nil {
		return e
	}
	b, e := json.MarshalIndent(v, "", "  ")
	if e == nil {
		fmt.Println(string(b))
	}
	return e
}
