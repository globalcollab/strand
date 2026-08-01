package statechart

import (
	"context"
	"fmt"
	"reflect"

	"github.com/globalcollab/strand"
)

// StateGetterSetter is an interface that state structs can implement to support generic state string tracking.
type StateGetterSetter interface {
	GetState() string
	SetState(s string)
}

// HandlerFunc defines a transition handler signature for event commands.
type HandlerFunc[S any] func(ctx context.Context, state S, cmd strand.Command) (S, []strand.Effect, []strand.TimerOperation, error)

// StateConfig configures event handlers and timers for a given state name.
type StateConfig[S any] struct {
	Name     string
	Handlers map[string]HandlerFunc[S]
}

// Builder constructs a Statechart Processor for type S.
type Builder[S StateGetterSetter] struct {
	name         string
	initialState string
	states       map[string]*StateConfig[S]
}

// New creates a new Statechart Builder.
func New[S StateGetterSetter](name string) *Builder[S] {
	return &Builder[S]{
		name:   name,
		states: make(map[string]*StateConfig[S]),
	}
}

// Initial sets the initial state string name.
func (b *Builder[S]) Initial(state string) *Builder[S] {
	b.initialState = state
	return b
}

// State registers a state name with transition handlers.
func (b *Builder[S]) State(stateName string, handlers ...StateOption[S]) *Builder[S] {
	cfg := &StateConfig[S]{
		Name:     stateName,
		Handlers: make(map[string]HandlerFunc[S]),
	}
	for _, opt := range handlers {
		opt(cfg)
	}
	b.states[stateName] = cfg
	return b
}

// StateOption configures a StateConfig.
type StateOption[S StateGetterSetter] func(cfg *StateConfig[S])

// On registers a command type transition handler for a state.
func On[S StateGetterSetter](commandType string, handler HandlerFunc[S]) StateOption[S] {
	return func(cfg *StateConfig[S]) {
		cfg.Handlers[commandType] = handler
	}
}

// Statechart implements Processor[S].
type Statechart[S StateGetterSetter] struct {
	builder *Builder[S]
}

// Build finalizes the statechart processor.
func (b *Builder[S]) Build() *Statechart[S] {
	return &Statechart[S]{builder: b}
}

func isNilState[S any](s S) bool {
	var zero S
	if any(s) == any(zero) {
		return true
	}
	v := reflect.ValueOf(s)
	return (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) && v.IsNil()
}

// Apply executes the statechart transition logic.
func (sc *Statechart[S]) Apply(ctx context.Context, snapshot strand.Snapshot[S], cmd strand.Command) (strand.Result[S], error) {
	currentState := ""
	if !isNilState(snapshot.State) {
		currentState = snapshot.State.GetState()
	}
	if currentState == "" {
		currentState = sc.builder.initialState
		if !isNilState(snapshot.State) {
			snapshot.State.SetState(currentState)
		}
	}

	stateCfg, exists := sc.builder.states[currentState]
	if !exists {
		return strand.Result[S]{State: snapshot.State}, fmt.Errorf("statechart: unknown state '%s'", currentState)
	}

	handler, hasHandler := stateCfg.Handlers[cmd.Type]
	if !hasHandler {
		// Event not handled in current state: return unchanged state (no-op)
		return strand.Result[S]{
			State:   snapshot.State,
			Changed: false,
		}, nil
	}

	newState, effects, timers, err := handler(ctx, snapshot.State, cmd)
	if err != nil {
		return strand.Result[S]{State: snapshot.State}, err
	}

	return strand.Result[S]{
		State:   newState,
		Changed: true,
		Effects: effects,
		Timers:  timers,
	}, nil
}
