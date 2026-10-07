// Package control exposes private bridge operations only on the loopback listener.
package control

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"agent-relay/internal/domain"
	"agent-relay/internal/store"
)

func Handler(s *store.Store, token string) http.Handler {
	mux := http.NewServeMux()
	decode := func(w http.ResponseWriter, r *http.Request, v any) bool {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		if err := d.Decode(v); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return false
		}
		var extra any
		if err := d.Decode(&extra); err != io.EOF {
			http.Error(w, "expected one JSON object", http.StatusBadRequest)
			return false
		}
		return true
	}
	respond := func(w http.ResponseWriter, v any, err error) {
		if err != nil {
			http.Error(w, "operation rejected", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("POST /v1/local/routes", func(w http.ResponseWriter, r *http.Request) {
		var route domain.ConversationRoute
		if !decode(w, r, &route) {
			return
		}
		respond(w, map[string]bool{"ok": true}, s.BindConversationRoute(r.Context(), route))
	})
	mux.HandleFunc("GET /v1/local/callbacks/next", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		jobs, err := s.DueCallbacks(r.Context(), n)
		if jobs == nil {
			jobs = []store.Callback{}
		}
		respond(w, jobs, err)
	})
	mux.HandleFunc("GET /v1/local/callbacks/status", func(w http.ResponseWriter, r *http.Request) {
		status, err := s.CallbackStatus(r.Context())
		respond(w, status, err)
	})
	mux.HandleFunc("POST /v1/local/callbacks/{id}/complete", func(w http.ResponseWriter, r *http.Request) {
		var body struct{}
		if !decode(w, r, &body) {
			return
		}
		respond(w, map[string]bool{"ok": true}, s.CompleteCallback(r.Context(), r.PathValue("id")))
	})
	mux.HandleFunc("POST /v1/local/callbacks/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Error string `json:"error"`
		}
		if !decode(w, r, &body) {
			return
		}
		respond(w, map[string]bool{"ok": true}, s.RetryCallback(r.Context(), r.PathValue("id"), body.Error, time.Now().Add(30*time.Second)))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			http.Error(w, "loopback required", http.StatusForbidden)
			return
		}
		if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
