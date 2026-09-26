package peer

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"agent-relay/internal/domain"
	"agent-relay/internal/store"
)

type PeerCredential struct{ ID, Token string }
type Status struct {
	Relay       RelayStatus       `json:"relay"`
	Hermes      any               `json:"hermes"`
	Replication ReplicationStatus `json:"replication"`
	CheckedAt   time.Time         `json:"checked_at"`
}
type RelayStatus struct {
	Healthy       bool   `json:"healthy"`
	Version       string `json:"version"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	Database      string `json:"database"`
}
type ReplicationStatus struct {
	SelfHead         int64 `json:"self_head"`
	StoredCallerHead int64 `json:"stored_caller_head"`
}
type SyncRequest struct {
	KnownPeerSeq int64          `json:"known_peer_seq"`
	Events       []domain.Event `json:"events"`
}
type SyncResponse struct {
	AcceptedCallerThrough int64          `json:"accepted_caller_through"`
	SelfHead              int64          `json:"self_head"`
	Events                []domain.Event `json:"events"`
	More                  bool           `json:"more"`
}
type ErrorResponse struct {
	Error   string         `json:"error"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}
type HermesHealth func(context.Context) any
type Server struct {
	Store              *store.Store
	LocalID, Version   string
	Credentials        []PeerCredential
	MaxBody, MaxEvents int
	Started            time.Time
	HermesHealth       HermesHealth
	Logger             *slog.Logger
	mu                 sync.Mutex
	limits             map[string]*bucket
}
type bucket struct {
	at     time.Time
	tokens float64
}

func (s *Server) Handler() http.Handler {
	if s.Started.IsZero() {
		s.Started = time.Now()
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	s.limits = map[string]*bucket{}
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	m.Handle("GET /v1/status", s.auth(http.HandlerFunc(s.status)))
	m.Handle("POST /v1/sync", s.auth(http.HandlerFunc(s.sync)))
	return security(m)
}
func security(n http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		n.ServeHTTP(w, r)
	})
}
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			writeErr(w, 401, "unauthorized", "bearer token required", nil)
			return
		}
		token := strings.TrimPrefix(h, "Bearer ")
		id := ""
		for _, c := range s.Credentials {
			if len(token) == len(c.Token) && subtle.ConstantTimeCompare([]byte(token), []byte(c.Token)) == 1 {
				id = c.ID
			}
		}
		if id == "" {
			writeErr(w, 401, "unauthorized", "invalid credential", nil)
			return
		}
		if !s.allow(id, r.URL.Path) {
			w.Header().Set("Retry-After", "1")
			writeErr(w, 429, "rate_limited", "request rate exceeded", nil)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), peerKey{}, id)))
	})
}

type peerKey struct{}

func (s *Server) allow(id, path string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := id + path
	b := s.limits[key]
	now := time.Now()
	rate, burst := 2.0, 10.0
	if path == "/v1/status" {
		rate, burst = 1, 5
	}
	if b == nil {
		b = &bucket{now, float64(burst)}
		s.limits[key] = b
	}
	b.tokens += now.Sub(b.at).Seconds() * rate
	if b.tokens > float64(burst) {
		b.tokens = float64(burst)
	}
	b.at = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	caller := r.Context().Value(peerKey{}).(string)
	self, _, e := s.Store.Head(r.Context(), s.LocalID)
	if e != nil {
		writeErr(w, 500, "internal_error", "database unavailable", nil)
		return
	}
	other, _, _ := s.Store.Head(r.Context(), caller)
	h := any(map[string]string{"liveness": "unknown", "readiness": "unknown"})
	if s.HermesHealth != nil {
		h = s.HermesHealth(r.Context())
	}
	writeJSON(w, 200, Status{Relay: RelayStatus{true, s.Version, int64(time.Since(s.Started).Seconds()), "healthy"}, Hermes: h, Replication: ReplicationStatus{self, other}, CheckedAt: time.Now().UTC()})
}
func (s *Server) sync(w http.ResponseWriter, r *http.Request) {
	caller := r.Context().Value(peerKey{}).(string)
	mediaType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
	if mediaType != "application/json" {
		writeErr(w, http.StatusUnsupportedMediaType, "invalid_event", "Content-Type must be application/json", nil)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(s.MaxBody))
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	var req SyncRequest
	if e := d.Decode(&req); e != nil {
		code := "invalid_event"
		status := 400
		if strings.Contains(e.Error(), "request body too large") {
			code = "payload_too_large"
			status = 413
		}
		writeErr(w, status, code, "invalid sync request", nil)
		return
	}
	if e := d.Decode(&struct{}{}); e != io.EOF {
		writeErr(w, http.StatusBadRequest, "invalid_event", "request must contain one JSON object", nil)
		return
	}
	if len(req.Events) > s.MaxEvents {
		writeErr(w, 413, "payload_too_large", "too many events", nil)
		return
	}
	if e := s.Store.Ingest(r.Context(), caller, req.Events); e != nil {
		switch {
		case errors.Is(e, domain.ErrGap):
			n, _, _ := s.Store.Head(r.Context(), caller)
			writeErr(w, 409, "event_gap", e.Error(), map[string]any{"expected_seq": n + 1})
		case errors.Is(e, domain.ErrDivergence):
			writeErr(w, 409, "event_divergence", e.Error(), nil)
		case strings.Contains(e.Error(), "forbidden origin"):
			writeErr(w, 403, "forbidden_origin", e.Error(), nil)
		default:
			s.Logger.Error("sync ingest", "peer_id", caller, "error", e)
			writeErr(w, 400, "invalid_event", e.Error(), nil)
		}
		return
	}
	for _, event := range req.Events {
		if event.EventType == "task.created" {
			if err := s.Store.QueueTaskDelivery(r.Context(), event.AggregateID); err != nil {
				writeErr(w, 500, "internal_error", "could not queue task delivery", nil)
				return
			}
			continue
		}
		if event.EventType != "message.created" {
			continue
		}
		seen, err := s.Store.HasEvent(r.Context(), "message.received", event.AggregateID)
		if err != nil {
			writeErr(w, 500, "internal_error", "database error", nil)
			return
		}
		if !seen {
			if _, err = s.Store.Append(r.Context(), "message.received", "message", event.AggregateID, event.CorrelationID, event.EventID, map[string]any{"message_id": event.AggregateID, "at": time.Now().UTC()}); err != nil {
				writeErr(w, 500, "internal_error", "could not create receipt", nil)
				return
			}
		}
		if err = s.Store.QueueDelivery(r.Context(), event.AggregateID); err != nil {
			writeErr(w, 500, "internal_error", "could not queue delivery", nil)
			return
		}
	}
	accepted, _, _ := s.Store.Head(r.Context(), caller)
	self, _, _ := s.Store.Head(r.Context(), s.LocalID)
	events, e := s.Store.EventsAfter(r.Context(), s.LocalID, req.KnownPeerSeq, s.MaxEvents)
	if e != nil {
		writeErr(w, 500, "internal_error", "database error", nil)
		return
	}
	writeJSON(w, 200, SyncResponse{accepted, self, events, len(events) == s.MaxEvents})
}
func writeErr(w http.ResponseWriter, status int, code, msg string, details map[string]any) {
	writeJSONStatus(w, status, ErrorResponse{code, msg, details})
}
func writeJSON(w http.ResponseWriter, status int, v any) { writeJSONStatus(w, status, v) }
func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

type Client struct {
	HTTP      *http.Client
	LocalID   string
	Store     *store.Store
	MaxEvents int
}

func (c *Client) Sync(ctx context.Context, peerID, baseURL, token string) error {
	for batch := 0; batch < 10000; batch++ {
		more, err := c.syncOnce(ctx, peerID, baseURL, token)
		if err != nil {
			_ = c.Store.RecordPeerError(ctx, peerID, err)
			return err
		}
		if !more {
			return c.Store.RecordPeerSuccess(ctx, peerID, map[string]any{"synced": true})
		}
	}
	return fmt.Errorf("sync exceeded batch safety limit")
}
func (c *Client) syncOnce(ctx context.Context, peerID, baseURL, token string) (bool, error) {
	cursor, err := c.Store.Cursor(ctx, peerID, c.LocalID)
	if err != nil {
		return false, err
	}
	known, _, err := c.Store.Head(ctx, peerID)
	if err != nil {
		return false, err
	}
	events, err := c.Store.EventsAfter(ctx, c.LocalID, cursor, c.MaxEvents)
	if err != nil {
		return false, err
	}
	body, err := json.Marshal(SyncRequest{KnownPeerSeq: known, Events: events})
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/v1/sync", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		var apiErr ErrorResponse
		if json.Unmarshal(b, &apiErr) == nil && apiErr.Error == "event_gap" {
			if n, ok := apiErr.Details["expected_seq"].(float64); ok && n > 0 {
				if err = c.Store.ResetCursor(ctx, peerID, c.LocalID, int64(n)-1); err != nil {
					return false, err
				}
				return true, nil
			}
		}
		return false, fmt.Errorf("sync HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var out SyncResponse
	if err = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&out); err != nil {
		return false, err
	}
	if err = c.Store.Ingest(ctx, peerID, out.Events); err != nil {
		return false, err
	}
	if err = c.Store.SetCursor(ctx, peerID, c.LocalID, out.AcceptedCallerThrough); err != nil {
		return false, err
	}
	if err = c.Store.MarkSent(ctx, out.AcceptedCallerThrough); err != nil {
		return false, err
	}
	return out.More || len(events) == c.MaxEvents, nil
}
func (c *Client) Ping(ctx context.Context, baseURL, token string) (Status, time.Duration, error) {
	start := time.Now()
	req, e := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(baseURL, "/")+"/v1/status", nil)
	if e != nil {
		return Status{}, 0, e
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, e := c.HTTP.Do(req)
	lat := time.Since(start)
	if e != nil {
		return Status{}, lat, e
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return Status{}, lat, fmt.Errorf("status HTTP %d", resp.StatusCode)
	}
	var st Status
	e = json.NewDecoder(io.LimitReader(resp.Body, 256<<10)).Decode(&st)
	return st, lat, e
}
