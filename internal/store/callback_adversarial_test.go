package store

import (
	"agent-relay/internal/domain"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestCallbackAdversarialBounds(t *testing.T) {
	for _, mode := range []string{"seed_order", "legacy_count", "legacy_metadata", "large_payload"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s := openTest(t, "a")
			route := domain.ConversationRoute{ThreadID: "thr", OwnerAgentID: "a", Platform: "discord", ChatID: "chat", HermesSessionKey: "session"}
			if _, err := s.Append(ctx, "task.created", "task", "task", route.ThreadID, "", domain.Task{TaskID: "task", ThreadID: route.ThreadID, CreatedBy: "a", AssignedTo: "b", Objective: "test", Context: json.RawMessage(`{}`), Priority: "normal"}); err != nil {
				t.Fatal(err)
			}
			if err := s.BindConversationRoute(ctx, route); err != nil {
				t.Fatal(err)
			}
			count := 20
			if mode == "legacy_metadata" || mode == "large_payload" {
				count = 1
			}
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("evt_%02d", i)
				due := int64(i + 1)
				if i == count-1 {
					due = 0
				}
				ev := domain.Event{EventID: id, EventType: "message.received", PayloadJSON: json.RawMessage(`{}`)}
				if mode == "legacy_metadata" {
					ev.CorrelationID = strings.Repeat("x", 2*1024*1024)
				}
				if mode == "large_payload" {
					ev.PayloadJSON = json.RawMessage(`{"body":"` + strings.Repeat("x", 2*1024*1024) + `"}`)
				}
				raw, err := json.Marshal(ev)
				if err != nil {
					t.Fatal(err)
				}
				batch := ""
				if strings.HasPrefix(mode, "legacy") {
					batch = "legacy"
				}
				if _, err = s.db.ExecContext(ctx, `INSERT INTO conversation_callback_jobs(callback_id,route_thread_id,source_event_id,event_json,kind,next_attempt_at,batch_id) VALUES(?,?,?,?,?,?,?)`, "cb_"+id, route.ThreadID, id, raw, "update", due, batch); err != nil {
					t.Fatal(err)
				}
			}
			jobs, err := s.DueCallbacks(ctx, 100)
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) == 0 {
				t.Fatal("unpollable callback")
			}
			raw, err := json.Marshal(jobs)
			if err != nil || len(raw) > 1024*1024 {
				t.Fatal("response bound", len(raw), err)
			}
			for _, job := range jobs {
				if len(job.Events) > 16 {
					t.Fatal("membership count", len(job.Events))
				}
			}
			var maxMembers int
			if err = s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(n),0) FROM (SELECT COUNT(*) n FROM conversation_callback_jobs WHERE batch_id<>'' GROUP BY batch_id)`).Scan(&maxMembers); err != nil {
				t.Fatal(err)
			}
			if mode == "seed_order" && maxMembers > 16 {
				t.Fatal("seed overflow", maxMembers)
			}
			if strings.HasPrefix(mode, "legacy") && jobs[0].Events[0].EventType != "relay.callback_summary" {
				t.Fatal("legacy summary absent")
			}
		})
	}
}
