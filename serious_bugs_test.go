package strand_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/globalcollab/strand"
	"github.com/globalcollab/strand/statechart"
)

// Serious Bug 6: Engine.ProcessOne must automatically retry on CAS conflict instead of returning raw ErrConflict
func TestSeriousBug6_EngineAutomaticCASRetry(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "serious-bug6-cas"

	store := strand.NewRedisStore[*MultiPodState](client, "serious_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active", Count: 0}
	})

	processor := statechart.New[*MultiPodState]("CASTest").
		Initial("Active").
		State("Active",
			statechart.On("Inc", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				state.Count++
				return state, nil, nil, nil
			}),
		).
		Build()

	engine := strand.NewEngine[*MultiPodState](store, processor)

	_, _ = store.AppendCommand(ctx, machineID, "Inc", nil)

	// Simulate concurrent conflict: artificially advance version in Redis to force CAS conflict on first attempt
	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Active", Count: 1},
		},
	})

	// Append command sequence 2
	_, _ = store.AppendCommand(ctx, machineID, "Inc", nil)

	// ProcessOne should handle snapshot reload automatically on conflict without returning ErrConflict!
	err := engine.ProcessOne(ctx, machineID)
	if err == strand.ErrConflict {
		t.Fatalf("BUG 6 DETECTED: Engine.ProcessOne returned raw ErrConflict without retrying automatically!")
	}
}

// Serious Bug 7: TimerScheduler must preserve and pass Generation payload to state machine
func TestSeriousBug7_TimerGenerationPayloadPreservation(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "serious-bug7-gen"

	store := strand.NewRedisStore[*MultiPodState](client, "serious_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	var receivedGen uint64
	processor := statechart.New[*MultiPodState]("GenTest").
		Initial("Active").
		State("Active",
			statechart.On("Timeout", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				var p strand.TimerOperation
				if err := json.Unmarshal(cmd.Payload, &p); err != nil {
					var rawMap map[string]interface{}
					_ = json.Unmarshal(cmd.Payload, &rawMap)
					if g, ok := rawMap["generation"].(float64); ok {
						receivedGen = uint64(g)
					}
				} else {
					receivedGen = p.Generation
				}
				return state, nil, nil, nil
			}),
		).
		Build()

	engine := strand.NewEngine[*MultiPodState](store, processor)
	scheduler := strand.NewTimerScheduler[*MultiPodState](store, engine)

	now := time.Now()
	cmd1, _ := store.AppendCommand(ctx, machineID, "Init", nil)

	// Schedule timer with Generation 42
	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: cmd1.Sequence,
		Command:         cmd1,
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Active"},
			Timers: []strand.TimerOperation{
				{
					Name:       "answer-timeout",
					Generation: 42,
					DueAt:      now.Add(-1 * time.Second),
					Command: strand.Command{
						Type:    "Timeout",
						Payload: json.RawMessage(`{"generation":42}`),
					},
				},
			},
		},
	})

	triggered, err := scheduler.PollDueTimers(ctx, now, 10)
	if err != nil || triggered != 1 {
		t.Fatalf("PollDueTimers failed: triggered=%d err=%v", triggered, err)
	}

	if receivedGen != 42 {
		t.Errorf("BUG 7 DETECTED: Timer generation was lost when scheduler appended command! Got gen=%d, expected 42", receivedGen)
	}
}

// Serious Bug 8: MemoryStore State Pointer Reference Isolation
func TestSeriousBug8_MemoryStoreReferenceIsolation(t *testing.T) {
	ctx := context.Background()
	machineID := "serious-bug8-mem"

	store := strand.NewMemoryStore[*MultiPodState](func() *MultiPodState {
		return &MultiPodState{
			CurrentState: "Active",
			History:      []string{"init"},
		}
	})

	processor := statechart.New[*MultiPodState]("MemIsoTest").
		Initial("Active").
		State("Active",
			statechart.On("AddHistory", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				state.History = append(state.History, "step1")
				return state, nil, nil, nil
			}),
		).
		Build()

	engine := strand.NewEngine[*MultiPodState](store, processor)

	_, _ = store.AppendCommand(ctx, machineID, "AddHistory", nil)
	_ = engine.Drain(ctx, machineID)

	snap1, _ := store.GetSnapshot(ctx, machineID)

	// Mutate snap1.History externally
	snap1.State.History[0] = "MUTATED"

	// Fetch fresh snapshot from store: History[0] MUST STILL BE "init" (isolated copy)!
	snap2, _ := store.GetSnapshot(ctx, machineID)
	if snap2.State.History[0] == "MUTATED" {
		t.Errorf("BUG 8 DETECTED: MemoryStore leaked state pointer reference! External mutation corrupted committed store state!")
	}
}

// Serious Bug 9: Outbox Batch Cleanup & Remaining Item Calculations
func TestSeriousBug9_OutboxBatchCleanup(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "serious-bug9-outbox"

	store := strand.NewRedisStore[*MultiPodState](client, "serious_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	effects := make([]strand.Effect, 100)
	for i := 0; i < 100; i++ {
		effects[i] = strand.Effect{
			ID:             fmt.Sprintf("eff-%d", i),
			Kind:           "Log",
			IdempotencyKey: fmt.Sprintf("key-%d", i),
		}
	}

	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result: strand.Result[*MultiPodState]{
			State:   &MultiPodState{CurrentState: "Active"},
			Effects: effects,
		},
	})

	outbox := strand.NewOutboxProcessor[*MultiPodState](store, func(ctx context.Context, effect strand.Effect) error {
		return nil
	})

	// Process batch of 10 items
	n, err := outbox.ProcessNext(ctx, 10)
	if err != nil || n != 10 {
		t.Fatalf("ProcessNext failed: n=%d err=%v", n, err)
	}

	// Remaining pending effects should be 90
	pending, err := store.FetchPendingEffects(ctx, 100)
	if err != nil || len(pending) != 90 {
		t.Errorf("BUG 9 DETECTED: Outbox fetch returned invalid count after batch claim! got %d, expected 90", len(pending))
	}
}

// Serious Bug 10: Concurrent Outbox Unclaim & Claim Collision Protection
func TestSeriousBug10_OutboxUnclaimRetrySafety(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "serious-bug10-unclaim"

	store := strand.NewRedisStore[*MultiPodState](client, "serious_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	eff := strand.Effect{
		ID:             "eff-retry-1",
		Kind:           "Webhook",
		IdempotencyKey: "key-1",
	}

	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result: strand.Result[*MultiPodState]{
			State:   &MultiPodState{CurrentState: "Active"},
			Effects: []strand.Effect{eff},
		},
	})

	// Worker 1 fetches & claims effect
	pending1, _ := store.FetchPendingEffects(ctx, 1)
	if len(pending1) != 1 {
		t.Fatalf("Expected 1 pending effect, got %d", len(pending1))
	}

	// Worker 2 attempts to fetch effect immediately: should return 0 since Worker 1 claimed it
	pending2, _ := store.FetchPendingEffects(ctx, 1)
	if len(pending2) != 0 {
		t.Errorf("BUG 10 DETECTED: Worker 2 claimed an already-claimed effect! got %d", len(pending2))
	}

	// Worker 1 fails and unclaims effect
	_ = store.UnclaimEffect(ctx, eff.ID)

	// Worker 2 fetches effect after unclaim: should successfully receive it for retry!
	pending3, _ := store.FetchPendingEffects(ctx, 1)
	if len(pending3) != 1 {
		t.Errorf("BUG 10 DETECTED: Failed effect was not made available after UnclaimEffect! got %d", len(pending3))
	}
}

// Serious Bug 11: Worker Pod Crash Claim Lease Recovery
func TestSeriousBug11_WorkerCrashClaimLeaseRecovery(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "serious-bug11-crash-lease"

	store := strand.NewRedisStore[*MultiPodState](client, "serious_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	eff := strand.Effect{
		ID:             "eff-crashed-worker",
		Kind:           "Payment",
		IdempotencyKey: "pay-1",
	}

	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result: strand.Result[*MultiPodState]{
			State:   &MultiPodState{CurrentState: "Active"},
			Effects: []strand.Effect{eff},
		},
	})

	// Worker Pod A claims effect at timestamp T-15s (Simulating Pod A crashing 15 seconds ago)
	staleTs := time.Now().Add(-15 * time.Second).UnixMilli()
	_ = client.HSet(ctx, "serious_strand:outbox_claimed", eff.ID, staleTs).Err()

	// Worker Pod B runs FetchPendingEffects: should recover the orphaned claim after lease expiration!
	recovered, err := store.FetchPendingEffects(ctx, 1)
	if err != nil || len(recovered) != 1 {
		t.Fatalf("BUG 11 DETECTED: Worker Pod B failed to recover orphaned outbox claim after Pod A crash! got %d, err %v", len(recovered), err)
	}

	if recovered[0].ID != eff.ID {
		t.Errorf("Recovered effect ID mismatch: got %s, expected %s", recovered[0].ID, eff.ID)
	}
}

// Serious Bug 12: Outbox Batch Partial Error Processing Recovery Leak
func TestSeriousBug12_OutboxBatchPartialErrorRecovery(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "serious-bug12-batch-err"

	store := strand.NewRedisStore[*MultiPodState](client, "serious_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	effects := []strand.Effect{
		{ID: "eff-batch-1", Kind: "Log", IdempotencyKey: "k1"},
		{ID: "eff-batch-2", Kind: "Log", IdempotencyKey: "k2"},
		{ID: "eff-batch-3", Kind: "Log", IdempotencyKey: "k3"},
	}

	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result: strand.Result[*MultiPodState]{
			State:   &MultiPodState{CurrentState: "Active"},
			Effects: effects,
		},
	})

	// Outbox handler fails on the first processed effect
	firstProcessed := ""
	outbox := strand.NewOutboxProcessor[*MultiPodState](store, func(ctx context.Context, effect strand.Effect) error {
		if firstProcessed == "" {
			firstProcessed = effect.ID
			return fmt.Errorf("simulated network handler failure for %s", effect.ID)
		}
		return nil
	})

	n, err := outbox.ProcessNext(ctx, 3)
	if err == nil {
		t.Fatalf("Expected outbox.ProcessNext to return handler error, got n=%d", n)
	}

	// Fetch remaining pending effects: all 3 effects MUST still be pending and available!
	pending, err := store.FetchPendingEffects(ctx, 10)
	if err != nil || len(pending) != 3 {
		t.Errorf("BUG 12 DETECTED: Untouched batch items were leaked in claimed state after handler failure! got %d, expected 3", len(pending))
	}
}
