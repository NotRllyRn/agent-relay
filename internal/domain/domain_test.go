package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventHashDetectsTampering(t *testing.T) {
	e := Event{OriginID: "a", OriginSeq: 1, EventID: "evt_x", EventType: "message.created", AggregateType: "message", AggregateID: "msg_x", CreatedAt: time.Unix(1, 2).UTC(), PayloadJSON: json.RawMessage(`{"x":1}`)}
	e.EventHash = e.Hash()
	if !e.ValidateHash() {
		t.Fatal("valid hash rejected")
	}
	e.PayloadJSON = json.RawMessage(`{"x":2}`)
	if e.ValidateHash() {
		t.Fatal("tampering accepted")
	}
}
func TestTaskTransitions(t *testing.T) {
	if !ValidTaskTransition("blocked", "in_progress") {
		t.Fatal("resume should be legal")
	}
	if ValidTaskTransition("completed", "in_progress") {
		t.Fatal("terminal task reopened")
	}
}
func TestMessageValidation(t *testing.T) {
	m := Message{MessageID: "m", ThreadID: "t", SenderID: "a", RecipientID: "b", Kind: "question", Priority: "normal", BodyMarkdown: "hi"}
	if err := ValidateMessage(m, 64); err != nil {
		t.Fatal(err)
	}
	m.ReplyDepth = 13
	if ValidateMessage(m, 64) == nil {
		t.Fatal("depth accepted")
	}
}
