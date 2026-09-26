package notify

import (
	"agent-relay/internal/store"
	"context"
	"fmt"
	"os/exec"
	"time"
)

type Sender interface {
	Send(context.Context, string, string) error
}
type HermesSender struct{ Binary string }

func (h HermesSender) Send(ctx context.Context, target, body string) error {
	b := h.Binary
	if b == "" {
		b = "hermes"
	}
	out, e := exec.CommandContext(ctx, b, "send", "--to", target, body).CombinedOutput()
	if e != nil {
		return fmt.Errorf("hermes send: %w: %s", e, string(out))
	}
	return nil
}

type Worker struct {
	Store  *store.Store
	Sender Sender
	Target string
}

func (w *Worker) Once(ctx context.Context) error {
	jobs, e := w.Store.DueNotifications(ctx, 20)
	if e != nil {
		return e
	}
	for _, j := range jobs {
		e = w.Sender.Send(ctx, w.Target, j.Body)
		if e == nil {
			w.Store.FinishNotification(ctx, j.ID)
		} else {
			n := j.Attempts + 1
			w.Store.RetryNotification(ctx, j.ID, n, e, time.Now().Add(notificationBackoff(n)))
		}
	}
	return nil
}
func notificationBackoff(n int) time.Duration {
	d := []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}
	if n <= len(d) {
		return d[n-1]
	}
	return 30 * time.Minute
}
