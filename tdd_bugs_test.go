package strand_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/globalcollab/strand"
	"github.com/globalcollab/strand/statechart"
)

// Bug 1 TDD Test: Due Timers must be removed from timer queue after being fetched so they don't fire endlessly
func TestTDBug1_InfiniteTimerFiring(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "tdd-timer-bug1"

	store := strand.NewRedisStore[*MultiPodState](client, "tdd_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	now := time.Now()
	// Schedule a due timer
	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Active"},
			Timers: []strand.TimerOperation{
				{
					Name:       "due-timer-1",
					Generation: 1,
					DueAt:      now.Add(-10 * time.Second),
					Command:    strand.Command{MachineID: machineID, Type: "Timeout"},
				},
			},
		},
	})

	// First fetch should return 1 due timer
	due1, err := store.FetchDueTimers(ctx, now, 10)
	if err != nil || len(due1) != 1 {
		t.Fatalf("First FetchDueTimers failed: count=%d err=%v", len(due1), err)
	}

	// Second fetch right after MUST return 0 due timers (because it was claimed/cleared!)
	due2, err := store.FetchDueTimers(ctx, now, 10)
	if len(due2) != 0 {
		t.Errorf("BUG 1 DETECTED: Due timer was not cleared from Redis ZSET! Second fetch returned %d timers", len(due2))
	}
}

// Bug 2 TDD Test: Failed outbox effect must release claim or allow retry
func TestTDBug2_OutboxFailedHandlerRetry(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "tdd-outbox-bug2"

	store := strand.NewRedisStore[*MultiPodState](client, "tdd_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Active"},
			Effects: []strand.Effect{
				{ID: "fail-eff-1", Kind: "SendEmail", IdempotencyKey: "email-1"},
			},
		},
	})

	attempts := 0
	outbox := strand.NewOutboxProcessor[*MultiPodState](store, func(ctx context.Context, effect strand.Effect) error {
		attempts++
		if attempts == 1 {
			return fmt.Errorf("transient email gateway timeout") // First attempt fails!
		}
		return nil // Second attempt succeeds!
	})

	// Attempt 1: Fails
	_, _ = outbox.ProcessNext(ctx, 10)

	// Attempt 2: Should retry and succeed
	processed, _ := outbox.ProcessNext(ctx, 10)

	if processed != 1 || attempts != 2 {
		t.Errorf("BUG 2 DETECTED: Failed outbox effect was locked in claimed state and never retried! attempts=%d processed=%d", attempts, processed)
	}
}

// Bug 3 TDD Test: Nil State Default Factory Panic
func TestTDBug3_NilStateFactoryHandling(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "tdd-nil-bug3"

	// Store initialized with NIL factory function
	store := strand.NewRedisStore[*MultiPodState](client, "tdd_strand", nil)

	processor := statechart.New[*MultiPodState]("NilTest").
		Initial("Active").
		State("Active",
			statechart.On("Ping", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				if state == nil {
					state = &MultiPodState{CurrentState: "Active"}
				}
				state.Count = 42
				return state, nil, nil, nil
			}),
		).
		Build()

	engine := strand.NewEngine[*MultiPodState](store, processor)

	_, err := engine.Send(ctx, machineID, "Ping", nil)
	if err != nil {
		t.Errorf("BUG 3 DETECTED: Engine failed on nil initial state factory: %v", err)
	}
}

// Bug 4 TDD Test: MemoryStore In-Flight Command Duplicate Load Gating
func TestTDBug4_MemoryStoreConcurrentLoadGating(t *testing.T) {
	ctx := context.Background()
	machineID := "tdd-mem-bug4"

	store := strand.NewMemoryStore[*MultiPodState](func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	_, _ = store.AppendCommand(ctx, machineID, "Inc", nil)

	// Worker 1 loads next command
	snap1, cmd1, err1 := store.LoadNext(ctx, machineID)
	if err1 != nil {
		t.Fatalf("Worker 1 LoadNext failed: %v", err1)
	}

	// Worker 2 attempts LoadNext before Worker 1 commits -> should return ErrNoPendingCommand (in-flight gating)
	_, _, err2 := store.LoadNext(ctx, machineID)
	if err2 != strand.ErrNoPendingCommand {
		t.Errorf("BUG 4 DETECTED: MemoryStore returned same in-flight command to Worker 2! err2=%v", err2)
	}

	_ = snap1
	_ = cmd1
}

// Bug 5 TDD Test: Timer MachineID Auto-population & Validation
func TestTDBug5_TimerMachineIDValidation(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "tdd-timer-bug5"

	store := strand.NewRedisStore[*MultiPodState](client, "tdd_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	now := time.Now()

	// Commit a timer where op.Command.MachineID was left empty by developer
	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Active"},
			Timers: []strand.TimerOperation{
				{
					Name:       "timer-no-id",
					Generation: 1,
					DueAt:      now.Add(-1 * time.Second),
					Command:    strand.Command{Type: "Timeout"}, // MachineID missing!
				},
			},
		},
	})

	due, err := store.FetchDueTimers(ctx, now, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("FetchDueTimers failed: %v", err)
	}

	if due[0].Command.MachineID != machineID {
		t.Errorf("BUG 5 DETECTED: Timer MachineID was not automatically populated from parent machine! Got '%s', expected '%s'", due[0].Command.MachineID, machineID)
	}
}
