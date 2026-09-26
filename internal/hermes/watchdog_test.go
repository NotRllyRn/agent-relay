package hermes

import (
	"agent-relay/internal/domain"
	"agent-relay/internal/store"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskDeliveryAndWatchdogAreIdempotent(t *testing.T) {
	var runs atomic.Int32
	keys := make(chan string, 10)
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sessions":
			json.NewEncoder(w).Encode(map[string]string{"id": "sess"})
		case "/v1/runs":
			runs.Add(1)
			keys <- r.Header.Get("Idempotency-Key")
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer h.Close()
	ctx := context.Background()
	s, e := store.Open(ctx, filepath.Join(t.TempDir(), "x.db"), "b", 128<<10)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	task := domain.Task{TaskID: "task_x", ThreadID: "thr_x", CreatedBy: "b", AssignedTo: "b", Objective: "work", Context: json.RawMessage(`{}`), Priority: "normal", Status: "proposed", UpdateIntervalSeconds: 1, CreatedAt: time.Now().UTC()}
	if _, e = s.Append(ctx, "task.created", "task", task.TaskID, task.ThreadID, "", task); e != nil {
		t.Fatal(e)
	}
	if e = s.QueueTaskDelivery(ctx, task.TaskID); e != nil {
		t.Fatal(e)
	}
	w := Worker{Store: s, Client: &Client{BaseURL: h.URL, APIKey: "key", HTTP: h.Client()}, LocalID: "b"}
	if e = w.DeliverTasksOnce(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Append(ctx, "task.accepted", "task", task.TaskID, task.ThreadID, "", map[string]any{"task_id": task.TaskID, "summary": "accepted"}); e != nil {
		t.Fatal(e)
	}
	time.Sleep(1100 * time.Millisecond)
	if e = w.WatchdogOnce(ctx, time.Hour); e != nil {
		t.Fatal(e)
	}
	if e = w.WatchdogOnce(ctx, time.Hour); e != nil {
		t.Fatal(e)
	}
	if runs.Load() != 2 {
		t.Fatalf("runs=%d, want task delivery plus one watchdog", runs.Load())
	}
	first, second := <-keys, <-keys
	if first != "relay-deliver-task:task_x" || !strings.HasPrefix(second, "relay-progress:task_x:") {
		t.Fatalf("keys %q %q", first, second)
	}
}
