package notify

import (
	"agent-relay/internal/store"
	"context"
	"path/filepath"
	"testing"
)

type fakeSender struct {
	calls int
	fail  bool
}

func (f *fakeSender) Send(context.Context, string, string) error {
	f.calls++
	if f.fail {
		return context.DeadlineExceeded
	}
	return nil
}
func TestWorkerSendsOnce(t *testing.T) {
	s, e := store.Open(context.Background(), filepath.Join(t.TempDir(), "x.db"), "a", 1024)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.EnqueueNotification(context.Background(), "n", "t", "hello")
	f := &fakeSender{}
	w := Worker{s, f, "discord"}
	if e = w.Once(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = w.Once(context.Background()); e != nil {
		t.Fatal(e)
	}
	if f.calls != 1 {
		t.Fatalf("calls=%d", f.calls)
	}
}
