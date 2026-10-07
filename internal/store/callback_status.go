package store

import "context"

// CallbackStatus is local-only operator visibility, never part of peer sync.
type CallbackStatus struct {
	Pending           int               `json:"pending"`
	OldestDueUnixNano int64             `json:"oldest_due_unix_nano"`
	Failures          []CallbackFailure `json:"failures"`
}
type CallbackFailure struct {
	CallbackID          string `json:"callback_id"`
	Attempts            int    `json:"attempts"`
	LastError           string `json:"last_error"`
	NextAttemptUnixNano int64  `json:"next_attempt_unix_nano"`
}

func (s *Store) CallbackStatus(ctx context.Context) (CallbackStatus, error) {
	result := CallbackStatus{Failures: []CallbackFailure{}}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MIN(next_attempt_at),0) FROM conversation_callback_jobs WHERE state='pending'`).Scan(&result.Pending, &result.OldestDueUnixNano); err != nil {
		return result, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT COALESCE(NULLIF(batch_id,''),callback_id),MAX(attempts),MAX(last_error),MAX(next_attempt_at) FROM conversation_callback_jobs WHERE state='pending' AND last_error<>'' GROUP BY COALESCE(NULLIF(batch_id,''),callback_id) ORDER BY MAX(next_attempt_at),1 LIMIT 100`)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var failure CallbackFailure
		if err = rows.Scan(&failure.CallbackID, &failure.Attempts, &failure.LastError, &failure.NextAttemptUnixNano); err != nil {
			return result, err
		}
		result.Failures = append(result.Failures, failure)
	}
	return result, rows.Err()
}
