package strand

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNoPendingCommand is returned when there are no eligible commands to process.
	ErrNoPendingCommand = errors.New("strand: no pending command available")

	// ErrConflict is returned when a CAS check fails (version or sequence mismatch).
	ErrConflict = errors.New("strand: optimistic concurrency conflict")

	// ErrEntityNotFound is returned when an entity instance does not exist.
	ErrEntityNotFound = errors.New("strand: entity instance not found")

	// ErrInvalidSequence is returned when a command sequence gap occurs.
	ErrInvalidSequence = errors.New("strand: invalid command sequence gap")
)

// Processor defines the pure state transition logic for an entity type S.
type Processor[S any] interface {
	Apply(ctx context.Context, snapshot Snapshot[S], cmd Command) (Result[S], error)
}

// CommitRequest carries the parameters needed for an atomic CAS commit.
type CommitRequest[S any] struct {
	MachineID       string
	ExpectedVersion uint64
	CommandSequence uint64
	Command         Command
	Result          Result[S]
}

// Store abstraction for persistent entity states, command queues, outboxes, and timers.
type Store[S any] interface {
	// GetSnapshot returns the current state snapshot for a machine instance.
	GetSnapshot(ctx context.Context, machineID string) (Snapshot[S], error)

	// LoadNext returns the current snapshot and the next eligible command (last_applied_sequence + 1).
	LoadNext(ctx context.Context, machineID string) (Snapshot[S], Command, error)

	// AppendCommand enqueues a new command to the machine's sequence log.
	AppendCommand(ctx context.Context, machineID string, cmdType string, payload any) (Command, error)

	// Commit performs an atomic compare-and-swap update of:
	// - State S
	// - Version (version + 1)
	// - LastAppliedSequence (CommandSequence)
	// - Outbox effects
	// - Timer operations
	Commit(ctx context.Context, req CommitRequest[S]) error

	// MarkCommandFailed records a processing failure or rejection for a command.
	MarkCommandFailed(ctx context.Context, machineID string, cmd Command, err error, retryable bool, backoff time.Duration) error

	// FetchPendingEffects fetches outbox effects to be processed.
	FetchPendingEffects(ctx context.Context, limit int) ([]Effect, error)

	// MarkEffectComplete removes completed outbox effects.
	MarkEffectComplete(ctx context.Context, effectID string) error

	// UnclaimEffect releases an in-flight outbox effect claim on error so it can be retried.
	UnclaimEffect(ctx context.Context, effectID string) error

	// FetchDueTimers retrieves timers whose due_at timestamp is <= now.
	FetchDueTimers(ctx context.Context, now time.Time, limit int) ([]TimerOperation, error)
}
