package strand

import (
	"context"
	"encoding/json"
	"time"
)

// TimerScheduler scans for due timers and appends timeout commands to entity inboxes.
type TimerScheduler[S any] struct {
	store  Store[S]
	engine *Engine[S]
}

// NewTimerScheduler creates a new TimerScheduler.
func NewTimerScheduler[S any](store Store[S], engine *Engine[S]) *TimerScheduler[S] {
	return &TimerScheduler[S]{
		store:  store,
		engine: engine,
	}
}

// PollDueTimers checks for timers whose due_at timestamp is due and appends their timeout commands.
func (t *TimerScheduler[S]) PollDueTimers(ctx context.Context, now time.Time, batchSize int) (int, error) {
	dueTimers, err := t.store.FetchDueTimers(ctx, now, batchSize)
	if err != nil || len(dueTimers) == 0 {
		return 0, err
	}

	triggered := 0
	for _, op := range dueTimers {
		cmdPayload := op.Command.Payload
		if len(cmdPayload) == 0 {
			cmdPayload, _ = json.Marshal(op)
		}
		// Append timeout command to entity command sequence
		_, err := t.store.AppendCommand(ctx, op.Command.MachineID, op.Command.Type, json.RawMessage(cmdPayload))
		if err == nil {
			triggered++
			if t.engine != nil {
				_ = t.engine.Drain(ctx, op.Command.MachineID)
			}
		}
	}

	return triggered, nil
}

// StartWorker runs a background polling loop for due timers.
func (t *TimerScheduler[S]) StartWorker(ctx context.Context, pollInterval time.Duration) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = t.PollDueTimers(ctx, time.Now(), 50)
		}
	}
}
