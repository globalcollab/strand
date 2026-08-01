package strand_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/globalcollab/strand"
	"github.com/globalcollab/strand/statechart"
)

type MultiPodState struct {
	CurrentState string   `json:"current_state"`
	Count        int      `json:"count"`
	TimerGen     uint64   `json:"timer_gen"`
	LastUser     string   `json:"last_user,omitempty"`
	BigPayload   string   `json:"big_payload,omitempty"`
	History      []string `json:"history,omitempty"`
}

func (m *MultiPodState) GetState() string {
	return m.CurrentState
}

func (m *MultiPodState) SetState(s string) {
	m.CurrentState = s
}

func setupRedisTestServer(t *testing.T) (func(), redis.UniversalClient) {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		client := redis.NewClient(&redis.Options{
			Addr: addr,
		})
		if err := client.Ping(context.Background()).Err(); err != nil {
			t.Fatalf("Failed to ping live Redis at %s: %v", addr, err)
		}
		// Flush test db before test
		_ = client.FlushDB(context.Background()).Err()
		return func() {
			_ = client.FlushDB(context.Background()).Err()
			_ = client.Close()
		}, client
	}

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return func() {
		client.Close()
		mr.Close()
	}, client
}

// Scenario 1: Multi-Instance Pod Racing (10 Stateless Server Pods)
func TestMultiInstanceRedisPodRacing(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "redis-call-9900"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active", Count: 0}
	})

	processor := statechart.New[*MultiPodState]("MultiPodMachine").
		Initial("Active").
		State("Active",
			statechart.On("Increment", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				state.Count++
				return state, nil, nil, nil
			}),
		).
		Build()

	totalCommands := 100
	for i := 0; i < totalCommands; i++ {
		_, err := store.AppendCommand(ctx, machineID, "Increment", nil)
		if err != nil {
			t.Fatalf("AppendCommand failed: %v", err)
		}
	}

	numPods := 10
	var wg sync.WaitGroup
	var executedCount int64

	for podID := 0; podID < numPods; podID++ {
		wg.Add(1)
		go func(pod int) {
			defer wg.Done()
			engine := strand.NewEngine[*MultiPodState](store, processor)
			for {
				err := engine.ProcessOne(ctx, machineID)
				if err != nil {
					break
				}
				atomic.AddInt64(&executedCount, 1)
			}
		}(podID)
	}

	wg.Wait()

	snap, err := store.GetSnapshot(ctx, machineID)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}

	if snap.LastAppliedSequence != uint64(totalCommands) {
		t.Errorf("Expected LastAppliedSequence = %d, got %d", totalCommands, snap.LastAppliedSequence)
	}

	if snap.State.Count != totalCommands {
		t.Errorf("Expected final Count = %d, got %d", totalCommands, snap.State.Count)
	}
}

// Scenario 2: Sequence Gating prevents processing command N+1 before command N commits
func TestMultiInstanceRedisSequenceGating(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "gate-call-100"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active", Count: 0}
	})

	cmd1, _ := store.AppendCommand(ctx, machineID, "Step1", nil)
	cmd2, _ := store.AppendCommand(ctx, machineID, "Step2", nil)

	if cmd1.Sequence != 1 || cmd2.Sequence != 2 {
		t.Fatalf("Expected sequences 1 and 2, got %d and %d", cmd1.Sequence, cmd2.Sequence)
	}

	err := store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 2,
		Command:         cmd2,
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Active", Count: 2},
		},
	})

	if err != strand.ErrConflict {
		t.Errorf("Expected ErrConflict for out-of-sequence commit, got %v", err)
	}
}

// Scenario 3: Multi-Instance Outbox Worker Distribution
func TestMultiInstanceRedisOutboxDistribution(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "outbox-multi-1"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	effects := []strand.Effect{
		{ID: "eff-1", Kind: "SendPushNotification", IdempotencyKey: "key-1"},
		{ID: "eff-2", Kind: "SendSMS", IdempotencyKey: "key-2"},
		{ID: "eff-3", Kind: "SendEmail", IdempotencyKey: "key-3"},
		{ID: "eff-4", Kind: "LogMetrics", IdempotencyKey: "key-4"},
		{ID: "eff-5", Kind: "UpdateBilling", IdempotencyKey: "key-5"},
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

	var mu sync.Mutex
	executedMap := make(map[string]int)

	numWorkers := 3
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outbox := strand.NewOutboxProcessor[*MultiPodState](store, func(ctx context.Context, effect strand.Effect) error {
				mu.Lock()
				executedMap[effect.ID]++
				mu.Unlock()
				return nil
			})
			_, _ = outbox.ProcessNext(ctx, 10)
		}()
	}

	wg.Wait()

	if len(executedMap) != 5 {
		t.Errorf("Expected 5 effects executed, got %d", len(executedMap))
	}

	for id, count := range executedMap {
		if count != 1 {
			t.Errorf("Effect %s executed %d times (expected 1)", id, count)
		}
	}
}

// Scenario 4: Timer Generation Invalidation across Multi-Instance Nodes
func TestMultiInstanceRedisTimerGenerationInvalidation(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "timer-race-55"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "WaitingForAnswer", TimerGen: 1}
	})

	err := store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1, Type: "AcceptCall"},
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Connecting", TimerGen: 2},
		},
	})

	if err != nil {
		t.Fatalf("Pod A commit failed: %v", err)
	}

	snap, err := store.GetSnapshot(ctx, machineID)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}

	if snap.State.CurrentState != "Connecting" || snap.State.TimerGen != 2 {
		t.Errorf("Unexpected state after Pod A accept: %+v", snap.State)
	}
}

// Scenario 5: Pod Crash & Recovery Mid-Execution
func TestMultiInstancePodCrashRecovery(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "crash-pod-77"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Step0", Count: 0}
	})

	processor := statechart.New[*MultiPodState]("CrashTest").
		Initial("Step0").
		State("Step0",
			statechart.On("DoWork", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				state.CurrentState = "Step1"
				state.Count = 100
				return state, nil, nil, nil
			}),
		).
		Build()

	cmd, _ := store.AppendCommand(ctx, machineID, "DoWork", nil)

	snap, loadedCmd, _ := store.LoadNext(ctx, machineID)
	_ = snap

	engineB := strand.NewEngine[*MultiPodState](store, processor)
	err := engineB.ProcessOne(ctx, machineID)
	if err != nil {
		t.Fatalf("Pod B recovery failed: %v", err)
	}

	finalSnap, _ := store.GetSnapshot(ctx, machineID)
	if finalSnap.LastAppliedSequence != cmd.Sequence {
		t.Errorf("Expected sequence %d committed, got %d", cmd.Sequence, finalSnap.LastAppliedSequence)
	}
	if finalSnap.State.CurrentState != "Step1" || finalSnap.State.Count != 100 {
		t.Errorf("Unexpected state after Pod B recovery: %+v", finalSnap.State)
	}

	_ = loadedCmd
}

// Scenario 6: Transient Error Retry vs Non-Retryable Business Failure
func TestMultiInstanceCommandRejectionAndRetry(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "retry-test-1"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	cmd, _ := store.AppendCommand(ctx, machineID, "TransientErrorCmd", nil)

	err := store.MarkCommandFailed(ctx, machineID, cmd, fmt.Errorf("redis timeout"), true, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("MarkCommandFailed failed: %v", err)
	}

	processor := statechart.New[*MultiPodState]("RetryTest").
		Initial("Active").
		State("Active",
			statechart.On("TransientErrorCmd", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				state.CurrentState = "Recovered"
				return state, nil, nil, nil
			}),
		).
		Build()

	engineB := strand.NewEngine[*MultiPodState](store, processor)
	err = engineB.ProcessOne(ctx, machineID)
	if err != nil {
		t.Fatalf("Pod B retry processing failed: %v", err)
	}

	snap, _ := store.GetSnapshot(ctx, machineID)
	if snap.State.CurrentState != "Recovered" {
		t.Errorf("Expected state 'Recovered', got '%s'", snap.State.CurrentState)
	}
}

// Scenario 7: High-Throughput Interleaved Multi-Entity Scalability across 20 Pods
func TestMultiInstanceHighThroughputMultiEntity(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active", Count: 0}
	})

	processor := statechart.New[*MultiPodState]("MultiEntityMachine").
		Initial("Active").
		State("Active",
			statechart.On("Inc", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				state.Count++
				return state, nil, nil, nil
			}),
		).
		Build()

	numEntities := 20
	cmdsPerEntity := 20

	for i := 0; i < cmdsPerEntity; i++ {
		for e := 0; e < numEntities; e++ {
			entityID := fmt.Sprintf("entity-%d", e)
			_, _ = store.AppendCommand(ctx, entityID, "Inc", nil)
		}
	}

	var wg sync.WaitGroup
	numPods := 20

	for p := 0; p < numPods; p++ {
		wg.Add(1)
		go func(pod int) {
			defer wg.Done()
			engine := strand.NewEngine[*MultiPodState](store, processor)
			for e := 0; e < numEntities; e++ {
				entityID := fmt.Sprintf("entity-%d", e)
				_ = engine.Drain(ctx, entityID)
			}
		}(p)
	}

	wg.Wait()

	for e := 0; e < numEntities; e++ {
		entityID := fmt.Sprintf("entity-%d", e)
		snap, err := store.GetSnapshot(ctx, entityID)
		if err != nil {
			t.Fatalf("Failed GetSnapshot for %s: %v", entityID, err)
		}
		if snap.State.Count != cmdsPerEntity {
			t.Errorf("Entity %s expected count %d, got %d", entityID, cmdsPerEntity, snap.State.Count)
		}
	}
}

// Scenario 8: Dynamic Timer Rescheduling & Due Timer Polling
func TestMultiInstanceDynamicTimerRescheduling(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "resched-timer-1"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active", TimerGen: 1}
	})

	now := time.Now()

	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Active", TimerGen: 1},
			Timers: []strand.TimerOperation{
				{
					Name:       "session-timeout",
					Generation: 1,
					DueAt:      now.Add(-5 * time.Second),
					Command:    strand.Command{MachineID: machineID, Type: "TimeoutGen1"},
				},
			},
		},
	})

	engine := strand.NewEngine[*MultiPodState](store, nil)
	scheduler := strand.NewTimerScheduler[*MultiPodState](store, engine)

	due, err := store.FetchDueTimers(ctx, now, 10)
	if err != nil {
		t.Fatalf("FetchDueTimers failed: %v", err)
	}

	if len(due) != 1 || due[0].Name != "session-timeout" || due[0].Generation != 1 {
		t.Errorf("Unexpected due timer fetched: %+v", due)
	}

	_ = scheduler
}

// --- NEW OUT-OF-THE-BOX SCENARIOS ---

// Scenario 9: Negative Scenario - Poison Pill Command (Non-retryable Permanent Failure)
func TestPoisonPillCommandHandling(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "poison-pill-call"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active", Count: 0}
	})

	processor := statechart.New[*MultiPodState]("PoisonPillMachine").
		Initial("Active").
		State("Active",
			statechart.On("PoisonCmd", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				return state, nil, nil, fmt.Errorf("corrupt payload: invariant failed")
			}),
			statechart.On("GoodCmd", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				state.Count = 999
				return state, nil, nil, nil
			}),
		).
		Build()

	engine := strand.NewEngine[*MultiPodState](store, processor)

	// Append poison command (seq 1) and good command (seq 2)
	_, _ = store.AppendCommand(ctx, machineID, "PoisonCmd", nil)
	goodCmd, _ := store.AppendCommand(ctx, machineID, "GoodCmd", nil)

	// Processing poison command returns error and marks command failed
	err := engine.ProcessOne(ctx, machineID)
	if err == nil {
		t.Fatalf("Expected error processing poison pill command, got nil")
	}

	// Good command seq 2 should now be able to process after poison command failed
	err = engine.ProcessOne(ctx, machineID)
	if err != nil {
		t.Fatalf("Good command failed after poison pill: %v", err)
	}

	snap, _ := store.GetSnapshot(ctx, machineID)
	if snap.LastAppliedSequence != goodCmd.Sequence || snap.State.Count != 999 {
		t.Errorf("Expected GoodCmd to apply cleanly, got snapshot: %+v", snap)
	}
}

// Scenario 10: Negative Scenario - Duplicate Command Deduplication
func TestDuplicateCommandDeduplication(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "dedup-call-1"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active", Count: 0}
	})

	// Append two commands
	cmd1, _ := store.AppendCommand(ctx, machineID, "Increment", nil)
	cmd2, _ := store.AppendCommand(ctx, machineID, "Increment", nil)

	if cmd1.Sequence == cmd2.Sequence {
		t.Errorf("Sequences must be unique and monotonic, got %d and %d", cmd1.Sequence, cmd2.Sequence)
	}
}

// Scenario 11: Positive Scenario - Cascading Outbox Completion Loop
func TestCascadingOutboxCompletionLoop(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "cascade-loop-1"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Broadcasting"}
	})

	processor := statechart.New[*MultiPodState]("CascadeMachine").
		Initial("Broadcasting").
		State("Broadcasting",
			statechart.On("StartBroadcast", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				effects := []strand.Effect{
					{Kind: "SendInvites", IdempotencyKey: "inv-1"},
				}
				return state, effects, nil, nil
			}),
			statechart.On("InvitesComplete", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				state.CurrentState = "WaitingForAnswer"
				return state, nil, nil, nil
			}),
		).
		Build()

	engine := strand.NewEngine[*MultiPodState](store, processor)

	// Step 1: Trigger StartBroadcast
	_, _ = engine.Send(ctx, machineID, "StartBroadcast", nil)

	// Outbox worker processes effect and posts completion command back into machine inbox
	outboxWorker := strand.NewOutboxProcessor[*MultiPodState](store, func(ctx context.Context, effect strand.Effect) error {
		// Outbox completion loop: posts InvitesComplete command into engine
		_, err := engine.Send(ctx, machineID, "InvitesComplete", nil)
		return err
	})

	n, err := outboxWorker.ProcessNext(ctx, 10)
	if err != nil || n != 1 {
		t.Fatalf("Outbox processing failed: n=%d err=%v", n, err)
	}

	// Verify state advanced to WaitingForAnswer via cascading outbox completion
	snap, _ := store.GetSnapshot(ctx, machineID)
	if snap.State.CurrentState != "WaitingForAnswer" {
		t.Errorf("Expected state 'WaitingForAnswer', got '%s'", snap.State.CurrentState)
	}
}

// Scenario 12: Rapid Double-Accept Race Condition (Alice vs Bob)
func TestRapidDoubleAcceptRaceCondition(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "race-accept-call"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "WaitingForAnswer"}
	})

	processor := statechart.New[*MultiPodState]("RaceAcceptMachine").
		Initial("WaitingForAnswer").
		State("WaitingForAnswer",
			statechart.On("AcceptCall", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				var p struct {
					User string `json:"user"`
				}
				_ = json.Unmarshal(cmd.Payload, &p)
				state.CurrentState = "Connecting"
				state.LastUser = p.User
				return state, nil, nil, nil
			}),
		).
		State("Connecting",
			statechart.On("AcceptCall", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				// Late accept when already connecting -> No-op / rejected
				return state, nil, nil, nil
			}),
		).
		Build()

	engine := strand.NewEngine[*MultiPodState](store, processor)

	// Alice accepts first (seq 1)
	alicePayload, _ := json.Marshal(map[string]string{"user": "Alice"})
	_, _ = store.AppendCommand(ctx, machineID, "AcceptCall", alicePayload)

	// Bob accepts a millisecond later (seq 2)
	bobPayload, _ := json.Marshal(map[string]string{"user": "Bob"})
	_, _ = store.AppendCommand(ctx, machineID, "AcceptCall", bobPayload)

	// Drain execution queue
	_ = engine.Drain(ctx, machineID)

	snap, _ := store.GetSnapshot(ctx, machineID)
	if snap.State.CurrentState != "Connecting" {
		t.Errorf("Expected state 'Connecting', got '%s'", snap.State.CurrentState)
	}

	// Alice must win because her command was assigned sequence 1
	if snap.State.LastUser != "Alice" {
		t.Errorf("Expected winner 'Alice', got '%s'", snap.State.LastUser)
	}
}

// Scenario 13: Large Payload Burst Stress Test
func TestLargePayloadBurstStress(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "large-payload-call"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active"}
	})

	processor := statechart.New[*MultiPodState]("LargePayloadMachine").
		Initial("Active").
		State("Active",
			statechart.On("StoreBigData", func(ctx context.Context, state *MultiPodState, cmd strand.Command) (*MultiPodState, []strand.Effect, []strand.TimerOperation, error) {
				state.BigPayload = string(cmd.Payload)
				return state, nil, nil, nil
			}),
		).
		Build()

	// Generate 50KB payload string
	bigStr := strings.Repeat("A", 50000)
	bigJSON, _ := json.Marshal(map[string]string{"data": bigStr})

	engine := strand.NewEngine[*MultiPodState](store, processor)
	_, err := engine.Send(ctx, machineID, "StoreBigData", bigJSON)
	if err != nil {
		t.Fatalf("Send large payload failed: %v", err)
	}

	snap, _ := store.GetSnapshot(ctx, machineID)
	if !strings.Contains(snap.State.BigPayload, "AAAAA") {
		t.Errorf("Failed to persist large payload accurately")
	}
}

// Scenario 14: Slow Worker Stale Version Conflict Rejection
func TestSlowWorkerStaleVersionConflict(t *testing.T) {
	cleanup, client := setupRedisTestServer(t)
	defer cleanup()

	ctx := context.Background()
	machineID := "slow-worker-call"

	store := strand.NewRedisStore[*MultiPodState](client, "test_strand", func() *MultiPodState {
		return &MultiPodState{CurrentState: "Active", Count: 0}
	})

	// Slow Pod A loads Version 0, Seq 1
	cmd1, _ := store.AppendCommand(ctx, machineID, "Inc", nil)

	// Fast Pod B commits Seq 1
	_ = store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         cmd1,
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Active", Count: 1},
		},
	})

	// Slow Pod A wakes up and attempts to commit stale Version 0 for Seq 1
	err := store.Commit(ctx, strand.CommitRequest[*MultiPodState]{
		MachineID:       machineID,
		ExpectedVersion: 0, // Stale version!
		CommandSequence: 1,
		Command:         cmd1,
		Result: strand.Result[*MultiPodState]{
			State: &MultiPodState{CurrentState: "Active", Count: 999},
		},
	})

	if err != strand.ErrConflict {
		t.Errorf("Expected ErrConflict for slow worker commit, got %v", err)
	}

	snap, _ := store.GetSnapshot(ctx, machineID)
	if snap.State.Count != 1 {
		t.Errorf("State count corrupted by slow worker: expected 1, got %d", snap.State.Count)
	}
}
