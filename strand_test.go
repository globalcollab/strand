package strand_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/globalcollab/strand"
	"github.com/globalcollab/strand/statechart"
)

type CounterState struct {
	CurrentState string `json:"current_state"`
	Count        int    `json:"count"`
}

func (c *CounterState) GetState() string {
	return c.CurrentState
}

func (c *CounterState) SetState(s string) {
	c.CurrentState = s
}

func buildCounterMachine() *statechart.Statechart[*CounterState] {
	return statechart.New[*CounterState]("Counter").
		Initial("Active").
		State("Active",
			statechart.On("Increment", func(ctx context.Context, state *CounterState, cmd strand.Command) (*CounterState, []strand.Effect, []strand.TimerOperation, error) {
				state.Count++
				return state, nil, nil, nil
			}),
		).
		Build()
}

func TestCommandOrderingAndConcurrentWorkerCAS(t *testing.T) {
	ctx := context.Background()
	store := strand.NewMemoryStore[*CounterState](func() *CounterState {
		return &CounterState{CurrentState: "Active", Count: 0}
	})
	engine := strand.NewEngine[*CounterState](store, buildCounterMachine())

	machineID := "counter-101"

	// Append 50 Increment commands
	numCommands := 50
	for i := 0; i < numCommands; i++ {
		_, err := store.AppendCommand(ctx, machineID, "Increment", nil)
		if err != nil {
			t.Fatalf("Failed to append command: %v", err)
		}
	}

	// Spin up 10 concurrent workers to drain the command queue
	var wg sync.WaitGroup
	numWorkers := 10
	var successCount int64

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := engine.ProcessOne(ctx, machineID)
				if errors.Is(err, strand.ErrNoPendingCommand) {
					snap, _ := store.GetSnapshot(ctx, machineID)
					if snap.LastAppliedSequence >= uint64(numCommands) {
						return
					}
					time.Sleep(1 * time.Millisecond)
					continue
				}
				if err == nil {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	if successCount != int64(numCommands) {
		t.Errorf("Expected %d total successful command executions, got %d", numCommands, successCount)
	}

	snap, err := store.GetSnapshot(ctx, machineID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	// Verify final state sequence
	if snap.LastAppliedSequence != uint64(numCommands) {
		t.Errorf("Expected LastAppliedSequence %d, got %d", numCommands, snap.LastAppliedSequence)
	}
	if snap.State.Count != numCommands {
		t.Errorf("Expected final Count %d, got %d", numCommands, snap.State.Count)
	}
}

func TestOutboxEffectExecution(t *testing.T) {
	ctx := context.Background()
	store := strand.NewMemoryStore[*CounterState](func() *CounterState {
		return &CounterState{CurrentState: "Active", Count: 0}
	})

	machine := statechart.New[*CounterState]("OutboxTest").
		Initial("Active").
		State("Active",
			statechart.On("DoEffect", func(ctx context.Context, state *CounterState, cmd strand.Command) (*CounterState, []strand.Effect, []strand.TimerOperation, error) {
				effects := []strand.Effect{
					{
						Kind:           "SendEmail",
						IdempotencyKey: "email-key-1",
					},
				}
				return state, effects, nil, nil
			}),
		).
		Build()

	engine := strand.NewEngine[*CounterState](store, machine)
	machineID := "outbox-test-1"

	_, err := engine.Send(ctx, machineID, "DoEffect", nil)
	if err != nil {
		t.Fatalf("Engine send failed: %v", err)
	}

	var executedEffects []string
	outbox := strand.NewOutboxProcessor[*CounterState](store, func(ctx context.Context, effect strand.Effect) error {
		executedEffects = append(executedEffects, effect.Kind)
		return nil
	})

	n, err := outbox.ProcessNext(ctx, 10)
	if err != nil {
		t.Fatalf("Outbox processing failed: %v", err)
	}

	if n != 1 {
		t.Errorf("Expected 1 effect processed, got %d", n)
	}

	if len(executedEffects) != 1 || executedEffects[0] != "SendEmail" {
		t.Errorf("Unexpected executed effects: %v", executedEffects)
	}
}
