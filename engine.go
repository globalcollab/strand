package strand

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Engine orchestrates the stateless execution loop for entity machines of type S.
type Engine[S any] struct {
	store     Store[S]
	processor Processor[S]
}

// NewEngine creates a new Engine instance.
func NewEngine[S any](store Store[S], processor Processor[S]) *Engine[S] {
	return &Engine[S]{
		store:     store,
		processor: processor,
	}
}

// ProcessOne attempts to execute a single pending command for the given machineID.
// If an optimistic CAS conflict occurs, it reloads the updated state and retries.
func (e *Engine[S]) ProcessOne(ctx context.Context, machineID string) error {
	maxRetries := 20
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		snapshot, cmd, err := e.store.LoadNext(ctx, machineID)
		if err != nil {
			return err
		}

		result, err := e.processor.Apply(ctx, snapshot, cmd)
		if err != nil {
			// Handle non-retryable business failure or retryable error
			_ = e.store.MarkCommandFailed(ctx, machineID, cmd, err, false, 0)
			return fmt.Errorf("strand: processor apply error for cmd %s (seq %d): %w", cmd.CommandID, cmd.Sequence, err)
		}

		err = e.store.Commit(ctx, CommitRequest[S]{
			MachineID:       machineID,
			ExpectedVersion: snapshot.Version,
			CommandSequence: cmd.Sequence,
			Command:         cmd,
			Result:          result,
		})

		if err == nil {
			return nil
		}

		if errors.Is(err, ErrConflict) {
			// Optimistic concurrency collision: another worker committed first.
			// Reload the new authoritative state and retry.
			lastErr = err
			time.Sleep(time.Duration(attempt*5) * time.Millisecond)
			continue
		}

		return err
	}
	return lastErr
}

// Drain continuously processes all pending commands for machineID until no commands remain.
func (e *Engine[S]) Drain(ctx context.Context, machineID string) error {
	for {
		err := e.ProcessOne(ctx, machineID)
		if errors.Is(err, ErrNoPendingCommand) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// Send appends a command to a machine and drains its execution queue.
func (e *Engine[S]) Send(ctx context.Context, machineID string, cmdType string, payload any) (Command, error) {
	cmd, err := e.store.AppendCommand(ctx, machineID, cmdType, payload)
	if err != nil {
		return Command{}, err
	}

	// Drain pending commands synchronously or let background workers process
	_ = e.Drain(ctx, machineID)
	return cmd, nil
}
