package statechart_test

import (
	"context"
	"testing"
	"time"

	"github.com/globalcollab/strand"
	"github.com/globalcollab/strand/statechart"
)

type TestState struct {
	CurrentState string `json:"current_state"`
	Val          string `json:"val"`
}

func (t *TestState) GetState() string {
	return t.CurrentState
}

func (t *TestState) SetState(s string) {
	t.CurrentState = s
}

func TestStatechartTransitions(t *testing.T) {
	ctx := context.Background()

	chart := statechart.New[*TestState]("TestChart").
		Initial("StateA").
		State("StateA",
			statechart.On("ToB", func(ctx context.Context, state *TestState, cmd strand.Command) (*TestState, []strand.Effect, []strand.TimerOperation, error) {
				state.CurrentState = "StateB"
				state.Val = "transitioned_b"
				effects := []strand.Effect{{Kind: "EffectA"}}
				timers := []strand.TimerOperation{{Name: "TimerA", DueAt: time.Now()}}
				return state, effects, timers, nil
			}),
		).
		State("StateB",
			statechart.On("ToC", func(ctx context.Context, state *TestState, cmd strand.Command) (*TestState, []strand.Effect, []strand.TimerOperation, error) {
				state.CurrentState = "StateC"
				return state, nil, nil, nil
			}),
		).
		Build()

	// Initial State A -> ToB -> State B
	snap := strand.Snapshot[*TestState]{
		State: &TestState{CurrentState: "StateA"},
	}
	cmdB := strand.Command{Type: "ToB"}

	res, err := chart.Apply(ctx, snap, cmdB)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if res.State.CurrentState != "StateB" || res.State.Val != "transitioned_b" {
		t.Errorf("Unexpected state after ToB: %+v", res.State)
	}
	if len(res.Effects) != 1 || res.Effects[0].Kind != "EffectA" {
		t.Errorf("Unexpected effects: %+v", res.Effects)
	}
	if len(res.Timers) != 1 || res.Timers[0].Name != "TimerA" {
		t.Errorf("Unexpected timers: %+v", res.Timers)
	}

	// State B -> Unhandled event -> No change
	cmdUnhandled := strand.Command{Type: "UnknownEvent"}
	snapB := strand.Snapshot[*TestState]{State: res.State}
	resUnhandled, err := chart.Apply(ctx, snapB, cmdUnhandled)
	if err != nil {
		t.Fatalf("Apply unhandled failed: %v", err)
	}

	if resUnhandled.Changed {
		t.Errorf("Expected Changed = false for unhandled event")
	}
	if resUnhandled.State.CurrentState != "StateB" {
		t.Errorf("State should remain StateB, got %s", resUnhandled.State.CurrentState)
	}
}
