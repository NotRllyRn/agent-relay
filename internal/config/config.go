package config

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type Hermes struct {
	BaseURL   string `json:"base_url"`
	APIKeyEnv string `json:"api_key_env"`
}
type Discord struct {
	Enabled           bool   `json:"enabled"`
	Target            string `json:"target"`
	MaxSilenceMinutes int    `json:"max_silence_minutes"`
}
type Peer struct {
	ID              string `json:"id"`
	URL             string `json:"url"`
	TokenEnv        string `json:"token_env"`
	InboundTokenEnv string `json:"inbound_token_env"`
	Token           string `json:"-"`
	InboundToken    string `json:"-"`
}
type Config struct {
	AgentID, DataDir, PeerListen, MCPListen                                          string
	SyncIntervalSeconds, MaxEvents, MaxRequestBytes, MaxEventPayload, MaxMessageBody int
	AllowInsecurePublicHTTP                                                          bool
	LogFormat                                                                        string
	Hermes                                                                           Hermes
	Discord                                                                          Discord
	Peers                                                                            []Peer
}

func (c *Config) UnmarshalJSON(b []byte) error {
	type raw struct {
		AgentID             string  `json:"agent_id"`
		DataDir             string  `json:"data_dir"`
		PeerListen          string  `json:"peer_listen"`
		MCPListen           string  `json:"mcp_listen"`
		SyncIntervalSeconds int     `json:"sync_interval_seconds"`
		MaxEvents           int     `json:"max_events"`
		MaxRequestBytes     int     `json:"max_request_bytes"`
		MaxEventPayload     int     `json:"max_event_payload"`
		MaxMessageBody      int     `json:"max_message_body"`
		Allow               bool    `json:"allow_insecure_public_http"`
		LogFormat           string  `json:"log_format"`
		Hermes              Hermes  `json:"hermes"`
		Discord             Discord `json:"discord"`
		Peers               []Peer  `json:"peers"`
	}
	var r raw
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	*c = Config{r.AgentID, r.DataDir, r.PeerListen, r.MCPListen, r.SyncIntervalSeconds, r.MaxEvents, r.MaxRequestBytes, r.MaxEventPayload, r.MaxMessageBody, r.Allow, r.LogFormat, r.Hermes, r.Discord, r.Peers}
	return nil
}
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return nil, err
	}
	c.defaults()
	for i := range c.Peers {
		c.Peers[i].Token = os.Getenv(c.Peers[i].TokenEnv)
		c.Peers[i].InboundToken = os.Getenv(c.Peers[i].InboundTokenEnv)
	}
	if err = c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}
func (c *Config) defaults() {
	if c.PeerListen == "" {
		c.PeerListen = ":7419"
	}
	if c.MCPListen == "" {
		c.MCPListen = "127.0.0.1:7420"
	}
	if c.SyncIntervalSeconds == 0 {
		c.SyncIntervalSeconds = 30
	}
	if c.MaxEvents == 0 {
		c.MaxEvents = 256
	}
	if c.MaxRequestBytes == 0 {
		c.MaxRequestBytes = 2 << 20
	}
	if c.MaxEventPayload == 0 {
		c.MaxEventPayload = 128 << 10
	}
	if c.MaxMessageBody == 0 {
		c.MaxMessageBody = 64 << 10
	}
	if c.Discord.Target == "" {
		c.Discord.Target = "discord"
	}
	if c.Discord.MaxSilenceMinutes == 0 {
		c.Discord.MaxSilenceMinutes = 60
	}
}
func (c *Config) Validate() error {
	if c.AgentID == "" || c.DataDir == "" {
		return fmt.Errorf("agent_id and data_dir are required")
	}
	if host, _, e := net.SplitHostPort(c.MCPListen); e != nil || !(host == "127.0.0.1" || host == "localhost" || host == "::1") {
		return fmt.Errorf("mcp_listen must be loopback")
	}
	seen := map[string]bool{}
	for _, p := range c.Peers {
		if p.ID == "" || p.ID == c.AgentID || seen[p.ID] {
			return fmt.Errorf("invalid peer id %q", p.ID)
		}
		seen[p.ID] = true
		u, e := url.Parse(p.URL)
		if e != nil || u.Host == "" {
			return fmt.Errorf("invalid peer URL")
		}
		if u.Scheme == "http" && !c.AllowInsecurePublicHTTP && !privateHost(u.Hostname()) {
			return fmt.Errorf("peer %s uses insecure public HTTP", p.ID)
		}
		if p.Token == "" || p.InboundToken == "" {
			return fmt.Errorf("peer %s token environment is unset", p.ID)
		}
	}
	return nil
}
func privateHost(h string) bool {
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}
func (c *Config) DBPath() string { return filepath.Join(c.DataDir, "relay.db") }
