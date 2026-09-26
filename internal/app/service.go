package app

import (
	"agent-relay/internal/domain"
	"agent-relay/internal/store"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Service struct {
	LocalID        string
	Store          *store.Store
	MaxMessageBody int
	WakeSync       func()
	NotifyTasks    bool
}
type SendInput struct {
	Recipient, Subject, Body, Kind, Priority string
	AckRequired                              bool
}
type SendResult struct {
	MessageID     string `json:"message_id"`
	ThreadID      string `json:"thread_id"`
	DeliveryState string `json:"delivery_state"`
}

func (s *Service) Send(ctx context.Context, in SendInput) (SendResult, error) {
	mid, e := domain.NewID("msg")
	if e != nil {
		return SendResult{}, e
	}
	tid, e := domain.NewID("thr")
	if e != nil {
		return SendResult{}, e
	}
	if in.Kind == "" {
		in.Kind = "information"
	}
	if in.Priority == "" {
		in.Priority = "normal"
	}
	m := domain.Message{MessageID: mid, ThreadID: tid, SenderID: s.LocalID, RecipientID: in.Recipient, Kind: in.Kind, Priority: in.Priority, Subject: in.Subject, BodyMarkdown: in.Body, AckRequired: in.AckRequired, CreatedAt: time.Now().UTC()}
	if e = domain.ValidateMessage(m, s.MaxMessageBody); e != nil {
		return SendResult{}, e
	}
	_, e = s.Store.Append(ctx, "message.created", "message", mid, tid, "", m)
	if e == nil {
		s.wake()
	}
	return SendResult{mid, tid, "queued"}, e
}
func (s *Service) Reply(ctx context.Context, thread, parent, body, kind, priority string, ack bool) (SendResult, error) {
	p, e := s.Store.GetMessage(ctx, parent)
	if e != nil {
		return SendResult{}, e
	}
	if p.ThreadID != thread {
		return SendResult{}, fmt.Errorf("parent does not belong to thread")
	}
	if p.ReplyDepth >= 12 {
		return SendResult{}, fmt.Errorf("maximum reply depth exceeded")
	}
	recipient := p.SenderID
	if recipient == s.LocalID {
		recipient = p.RecipientID
	}
	mid, _ := domain.NewID("msg")
	if kind == "" {
		kind = "information"
	}
	if priority == "" {
		priority = "normal"
	}
	m := domain.Message{MessageID: mid, ThreadID: thread, ReplyToMessageID: parent, SenderID: s.LocalID, RecipientID: recipient, Kind: kind, Priority: priority, BodyMarkdown: body, AckRequired: ack, ReplyDepth: p.ReplyDepth + 1, CreatedAt: time.Now().UTC()}
	if e = domain.ValidateMessage(m, s.MaxMessageBody); e != nil {
		return SendResult{}, e
	}
	_, e = s.Store.Append(ctx, "message.created", "message", mid, thread, "", m)
	if e == nil {
		s.wake()
	}
	return SendResult{mid, thread, "queued"}, e
}
func (s *Service) Acknowledge(ctx context.Context, id string) error {
	m, e := s.Store.GetMessage(ctx, id)
	if e != nil {
		return e
	}
	if m.RecipientID != s.LocalID {
		return fmt.Errorf("only recipient can acknowledge")
	}
	yes, e := s.Store.HasEvent(ctx, "message.acknowledged", id)
	if e != nil || yes {
		return e
	}
	_, e = s.Store.Append(ctx, "message.acknowledged", "message", id, m.ThreadID, "", map[string]any{"message_id": id, "at": time.Now().UTC()})
	if e == nil {
		s.wake()
	}
	return e
}

type DelegateInput struct {
	Recipient, Objective          string
	Context                       any
	ExpectedDeliverable, Priority string
	UpdateIntervalMinutes         int
}

func (s *Service) Delegate(ctx context.Context, in DelegateInput) (domain.Task, error) {
	tid, _ := domain.NewID("task")
	thr, _ := domain.NewID("thr")
	raw, e := json.Marshal(in.Context)
	if e != nil {
		return domain.Task{}, e
	}
	if in.Priority == "" {
		in.Priority = "normal"
	}
	if in.UpdateIntervalMinutes <= 0 {
		in.UpdateIntervalMinutes = 60
	}
	t := domain.Task{TaskID: tid, ThreadID: thr, CreatedBy: s.LocalID, AssignedTo: in.Recipient, Objective: in.Objective, Context: raw, ExpectedDeliverable: in.ExpectedDeliverable, Priority: in.Priority, Status: "proposed", UpdateIntervalSeconds: in.UpdateIntervalMinutes * 60, CreatedAt: time.Now().UTC()}
	_, e = s.Store.Append(ctx, "task.created", "task", tid, thr, "", t)
	if e == nil {
		s.wake()
	}
	return t, e
}
func (s *Service) UpdateTask(ctx context.Context, id, status, summary, next, blocker, result string) (domain.Event, error) {
	t, e := s.Store.GetTask(ctx, id)
	if e != nil {
		return domain.Event{}, e
	}
	if t.AssignedTo != s.LocalID {
		return domain.Event{}, fmt.Errorf("only assignee can update task")
	}
	eventType := "task.progress"
	if status != "" {
		if !domain.ValidTaskTransition(t.Status, status) {
			return domain.Event{}, fmt.Errorf("illegal transition")
		}
		eventType = map[string]string{"accepted": "task.accepted", "in_progress": "task.started", "blocked": "task.blocked", "completed": "task.completed", "failed": "task.failed", "declined": "task.declined", "cancelled": "task.cancelled"}[status]
		if eventType == "" {
			return domain.Event{}, fmt.Errorf("invalid status")
		}
	}
	p := map[string]any{"task_id": id, "summary": summary, "next_step": next, "blocker": blocker, "final_result": result}
	ev, e := s.Store.Append(ctx, eventType, "task", id, t.ThreadID, "", p)
	if e == nil && s.NotifyTasks && (status != "" || eventType == "task.progress") {
		nid := "notif_" + ev.EventID
		body := fmt.Sprintf("%s — task %s: %s", s.LocalID, id, summary)
		if status != "" {
			body = fmt.Sprintf("%s — task %s is %s. %s", s.LocalID, id, status, summary)
		}
		e = s.Store.EnqueueNotification(ctx, nid, id, body)
	}
	if e == nil {
		s.wake()
	}
	return ev, e
}
func (s *Service) RequestCancellation(ctx context.Context, id, summary string) error {
	t, e := s.Store.GetTask(ctx, id)
	if e != nil {
		return e
	}
	if t.CreatedBy != s.LocalID {
		return fmt.Errorf("only creator can request cancellation")
	}
	_, e = s.Store.Append(ctx, "task.cancel_requested", "task", id, t.ThreadID, "", map[string]any{"task_id": id, "summary": summary})
	if e == nil {
		s.wake()
	}
	return e
}
func (s *Service) wake() {
	if s.WakeSync != nil {
		s.WakeSync()
	}
}
