package control

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"agent-relay/internal/store"
)

func TestLocalControlsRejectNonLocalAndUnauthenticated(t *testing.T) {
	s, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "relay.db"), "wilbur", 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := Handler(s, "test-token")
	for _, tc := range []struct {
		remote, token string
		want          int
	}{
		{"192.0.2.1:42", "test-token", http.StatusForbidden},
		{"127.0.0.1:42", "", http.StatusUnauthorized},
		{"127.0.0.1:42", "test-token", http.StatusOK},
	} {
		r := httptest.NewRequest("GET", "/v1/local/callbacks/next", nil)
		r.RemoteAddr = tc.remote
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("got %d want %d: %s", w.Code, tc.want, w.Body.String())
		}
	}
	for _, tc := range []struct {
		token string
		want  int
	}{{"", http.StatusUnauthorized}, {"test-token", http.StatusOK}} {
		r := httptest.NewRequest("GET", "/v1/local/callbacks/status", nil)
		r.RemoteAddr = "127.0.0.1:42"
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("status got %d want %d", w.Code, tc.want)
		}
		if tc.want == http.StatusOK && !strings.Contains(w.Body.String(), `"pending":0`) {
			t.Fatal("missing status", w.Body.String())
		}
	}
}

func TestLocalControlsRejectMalformedRoute(t *testing.T) {
	s, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "relay.db"), "wilbur", 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := httptest.NewRequest("POST", "/v1/local/routes", strings.NewReader(`{"owner_agent_id":"maya","unknown":true}`))
	r.RemoteAddr = "127.0.0.1:42"
	w := httptest.NewRecorder()
	Handler(s, "").ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d", w.Code)
	}
}
