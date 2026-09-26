package domain

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const EventVersion = "1"

var (
	ErrGap        = errors.New("event gap")
	ErrDivergence = errors.New("event divergence")
	ErrInvalid    = errors.New("invalid event")
)

type Event struct {
	OriginID         string          `json:"origin_id"`
	OriginSeq        int64           `json:"origin_seq"`
	EventID          string          `json:"event_id"`
	EventType        string          `json:"event_type"`
	AggregateType    string          `json:"aggregate_type"`
	AggregateID      string          `json:"aggregate_id"`
	CorrelationID    string          `json:"correlation_id,omitempty"`
	CausationEventID string          `json:"causation_event_id,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	PayloadJSON      json.RawMessage `json:"payload_json"`
	PrevHash         []byte          `json:"prev_hash,omitempty"`
	EventHash        []byte          `json:"event_hash"`
}

func NewID(prefix string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(b), nil
}

func (e Event) Hash() []byte {
	payload := sha256.Sum256(e.PayloadJSON)
	parts := []string{EventVersion, e.OriginID, strconv.FormatInt(e.OriginSeq, 10), e.EventID, e.EventType, e.AggregateType, e.AggregateID, e.CreatedAt.UTC().Format(time.RFC3339Nano), hex.EncodeToString(e.PrevHash), hex.EncodeToString(payload[:])}
	h := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return h[:]
}

func (e Event) ValidateHash() bool {
	return len(e.EventHash) == sha256.Size && string(e.EventHash) == string(e.Hash())
}

type ArtifactRef struct{ Kind, Value, HostID, Label, SHA256 string }
type Message struct {
	MessageID        string    `json:"message_id"`
	ThreadID         string    `json:"thread_id"`
	ReplyToMessageID string    `json:"reply_to_message_id,omitempty"`
	SenderID         string    `json:"sender_id"`
	RecipientID      string    `json:"recipient_id"`
	Kind             string    `json:"kind"`
	Priority         string    `json:"priority"`
	Subject          string    `json:"subject,omitempty"`
	BodyMarkdown     string    `json:"body_markdown"`
	AckRequired      bool      `json:"ack_required"`
	ReplyDepth       int       `json:"reply_depth"`
	CreatedAt        time.Time `json:"created_at"`
}
type Task struct {
	TaskID                string          `json:"task_id"`
	ThreadID              string          `json:"thread_id"`
	CreatedBy             string          `json:"created_by"`
	AssignedTo            string          `json:"assigned_to"`
	Objective             string          `json:"objective"`
	Context               json.RawMessage `json:"context"`
	ExpectedDeliverable   string          `json:"expected_deliverable,omitempty"`
	Priority              string          `json:"priority"`
	Status                string          `json:"status"`
	Blocker               string          `json:"blocker,omitempty"`
	FinalResult           string          `json:"final_result,omitempty"`
	UpdateIntervalSeconds int             `json:"update_interval_seconds"`
	CreatedAt             time.Time       `json:"created_at"`
}
type TaskUpdate struct {
	TaskID, Summary, NextStep, Blocker, FinalResult string
	Status                                          string
	Meaningful                                      bool
	ArtifactRefs                                    []ArtifactRef
}

var transitions = map[string]map[string]bool{
	"proposed":    {"accepted": true, "declined": true, "cancelled": true},
	"accepted":    {"in_progress": true, "blocked": true, "completed": true, "failed": true, "cancelled": true},
	"in_progress": {"blocked": true, "completed": true, "failed": true, "cancelled": true},
	"blocked":     {"in_progress": true, "completed": true, "failed": true, "cancelled": true},
}

func ValidTaskTransition(from, to string) bool { return from == to || transitions[from][to] }
func ValidateMessage(m Message, maxBody int) error {
	if m.MessageID == "" || m.ThreadID == "" || m.SenderID == "" || m.RecipientID == "" || strings.TrimSpace(m.BodyMarkdown) == "" {
		return fmt.Errorf("message fields: %w", ErrInvalid)
	}
	if len(m.BodyMarkdown) > maxBody {
		return fmt.Errorf("message body exceeds %d bytes", maxBody)
	}
	if m.Kind == "" {
		m.Kind = "information"
	}
	if !oneOf(m.Kind, "information", "question", "request", "result") {
		return fmt.Errorf("invalid kind")
	}
	if m.Priority == "" {
		m.Priority = "normal"
	}
	if !oneOf(m.Priority, "normal", "high", "urgent") {
		return fmt.Errorf("invalid priority")
	}
	if m.ReplyDepth > 12 {
		return fmt.Errorf("maximum reply depth exceeded")
	}
	return nil
}
func oneOf(v string, xs ...string) bool {
	for _, x := range xs {
		if v == x {
			return true
		}
	}
	return false
}
