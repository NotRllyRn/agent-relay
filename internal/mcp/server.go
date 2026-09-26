package mcp

import (
	"agent-relay/internal/app"
	"context"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"net/http"
	"time"
)

type Server struct {
	Service *app.Service
	Handler http.Handler
}
type sendArgs struct {
	Recipient   string `json:"recipient" jsonschema:"required"`
	Subject     string `json:"subject,omitempty"`
	Body        string `json:"body" jsonschema:"required"`
	Kind        string `json:"kind,omitempty"`
	Priority    string `json:"priority,omitempty"`
	AckRequired bool   `json:"ack_required,omitempty"`
}
type replyArgs struct {
	ThreadID         string `json:"thread_id" jsonschema:"required"`
	ReplyToMessageID string `json:"reply_to_message_id" jsonschema:"required"`
	Body             string `json:"body" jsonschema:"required"`
	Kind             string `json:"kind,omitempty"`
	Priority         string `json:"priority,omitempty"`
	AckRequired      bool   `json:"ack_required,omitempty"`
}
type ackArgs struct {
	MessageID string `json:"message_id" jsonschema:"required"`
}
type inboxArgs struct {
	UnacknowledgedOnly bool   `json:"unacknowledged_only,omitempty"`
	ThreadID           string `json:"thread_id,omitempty"`
	Limit              int    `json:"limit,omitempty"`
}
type threadArgs struct {
	ThreadID string `json:"thread_id" jsonschema:"required"`
	Limit    int    `json:"limit,omitempty"`
}
type delegateArgs struct {
	Recipient             string `json:"recipient" jsonschema:"required"`
	Objective             string `json:"objective" jsonschema:"required"`
	Context               any    `json:"context,omitempty"`
	ExpectedDeliverable   string `json:"expected_deliverable,omitempty"`
	Priority              string `json:"priority,omitempty"`
	UpdateIntervalMinutes int    `json:"update_interval_minutes,omitempty"`
}
type updateArgs struct {
	TaskID      string `json:"task_id" jsonschema:"required"`
	Status      string `json:"status,omitempty"`
	Summary     string `json:"summary" jsonschema:"required"`
	NextStep    string `json:"next_step,omitempty"`
	Blocker     string `json:"blocker,omitempty"`
	FinalResult string `json:"final_result,omitempty"`
}
type listTaskArgs struct {
	Status     string `json:"status,omitempty"`
	AssignedTo string `json:"assigned_to,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}
type searchArgs struct {
	Query string `json:"query" jsonschema:"required"`
	Limit int    `json:"limit,omitempty"`
}

func New(service *app.Service, version string) *Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "agent-relay", Version: version}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "relay_send_message", Description: "Send a durable message to a configured peer"}, func(ctx context.Context, _ *mcp.CallToolRequest, a sendArgs) (*mcp.CallToolResult, any, error) {
		v, e := service.Send(ctx, app.SendInput{a.Recipient, a.Subject, a.Body, a.Kind, a.Priority, a.AckRequired})
		return nil, v, e
	})
	mcp.AddTool(s, &mcp.Tool{Name: "relay_reply", Description: "Reply in an existing relay thread"}, func(ctx context.Context, _ *mcp.CallToolRequest, a replyArgs) (*mcp.CallToolResult, any, error) {
		v, e := service.Reply(ctx, a.ThreadID, a.ReplyToMessageID, a.Body, a.Kind, a.Priority, a.AckRequired)
		return nil, v, e
	})
	mcp.AddTool(s, &mcp.Tool{Name: "relay_acknowledge", Description: "Acknowledge an inbound message"}, func(ctx context.Context, _ *mcp.CallToolRequest, a ackArgs) (*mcp.CallToolResult, any, error) {
		e := service.Acknowledge(ctx, a.MessageID)
		return nil, map[string]bool{"acknowledged": e == nil}, e
	})
	mcp.AddTool(s, &mcp.Tool{Name: "relay_inbox", Description: "List relay messages"}, func(ctx context.Context, _ *mcp.CallToolRequest, a inboxArgs) (*mcp.CallToolResult, any, error) {
		v, e := service.Store.Messages(ctx, a.ThreadID, a.UnacknowledgedOnly, a.Limit)
		return nil, v, e
	})
	mcp.AddTool(s, &mcp.Tool{Name: "relay_get_thread", Description: "Get messages in a relay thread"}, func(ctx context.Context, _ *mcp.CallToolRequest, a threadArgs) (*mcp.CallToolResult, any, error) {
		v, e := service.Store.Messages(ctx, a.ThreadID, false, a.Limit)
		return nil, v, e
	})
	mcp.AddTool(s, &mcp.Tool{Name: "relay_delegate_task", Description: "Delegate a durable task to a peer"}, func(ctx context.Context, _ *mcp.CallToolRequest, a delegateArgs) (*mcp.CallToolResult, any, error) {
		v, e := service.Delegate(ctx, app.DelegateInput{a.Recipient, a.Objective, a.Context, a.ExpectedDeliverable, a.Priority, a.UpdateIntervalMinutes})
		return nil, v, e
	})
	mcp.AddTool(s, &mcp.Tool{Name: "relay_update_task", Description: "Update an assigned relay task"}, func(ctx context.Context, _ *mcp.CallToolRequest, a updateArgs) (*mcp.CallToolResult, any, error) {
		v, e := service.UpdateTask(ctx, a.TaskID, a.Status, a.Summary, a.NextStep, a.Blocker, a.FinalResult)
		return nil, v, e
	})
	mcp.AddTool(s, &mcp.Tool{Name: "relay_list_tasks", Description: "List relay tasks"}, func(ctx context.Context, _ *mcp.CallToolRequest, a listTaskArgs) (*mcp.CallToolResult, any, error) {
		v, e := service.Store.ListTasks(ctx, a.Status, a.AssignedTo, a.Limit)
		return nil, v, e
	})
	mcp.AddTool(s, &mcp.Tool{Name: "relay_search_history", Description: "Search relay message and task history"}, func(ctx context.Context, _ *mcp.CallToolRequest, a searchArgs) (*mcp.CallToolResult, any, error) {
		v, e := service.Store.Search(ctx, a.Query, a.Limit)
		return nil, v, e
	})
	h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, &mcp.StreamableHTTPOptions{JSONResponse: true, SessionTimeout: 2 * time.Hour})
	return &Server{service, h}
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.Handler.ServeHTTP(w, r) }
