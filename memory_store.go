package strand

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type memoryMachine[S any] struct {
	snapshot        Snapshot[S]
	nextSequence    uint64
	commands        map[uint64]Command // sequence -> Command
	timers          map[string]TimerOperation // timer_name -> TimerOperation
}

// MemoryStore is an in-memory, thread-safe implementation of Store[S].
type MemoryStore[S any] struct {
	mu           sync.RWMutex
	machines     map[string]*memoryMachine[S]
	initialState func() S
	outbox       map[string]Effect // effect_id -> Effect
}

// NewMemoryStore creates a new thread-safe MemoryStore instance.
func NewMemoryStore[S any](initialState func() S) *MemoryStore[S] {
	return &MemoryStore[S]{
		machines:     make(map[string]*memoryMachine[S]),
		initialState: initialState,
		outbox:       make(map[string]Effect),
	}
}

func (m *MemoryStore[S]) getOrCreateMachine(machineID string) *memoryMachine[S] {
	machine, exists := m.machines[machineID]
	if !exists {
		var init S
		if m.initialState != nil {
			init = m.initialState()
		}
		machine = &memoryMachine[S]{
			snapshot: Snapshot[S]{
				MachineID:           machineID,
				State:               init,
				Version:             0,
				LastAppliedSequence: 0,
			},
			nextSequence: 1,
			commands:     make(map[uint64]Command),
			timers:       make(map[string]TimerOperation),
		}
		m.machines[machineID] = machine
	}
	return machine
}

func (m *MemoryStore[S]) AppendCommand(ctx context.Context, machineID string, cmdType string, payload any) (Command, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	machine := m.getOrCreateMachine(machineID)

	var payloadRaw json.RawMessage
	if payload != nil {
		switch p := payload.(type) {
		case json.RawMessage:
			payloadRaw = p
		case []byte:
			payloadRaw = p
		default:
			bytes, err := json.Marshal(payload)
			if err != nil {
				return Command{}, fmt.Errorf("strand: failed to marshal command payload: %w", err)
			}
			payloadRaw = bytes
		}
	}

	seq := machine.nextSequence
	machine.nextSequence++

	cmd := Command{
		MachineID:  machineID,
		CommandID:  fmt.Sprintf("%s-%d", machineID, seq),
		Sequence:   seq,
		Type:       cmdType,
		Payload:    payloadRaw,
		AcceptedAt: time.Now(),
		Status:     StatusPending,
		Attempts:   0,
	}

	machine.commands[seq] = cmd
	return cmd, nil
}

func cloneState[S any](state S) S {
	var cloned S
	bytes, err := json.Marshal(state)
	if err != nil {
		return state
	}
	_ = json.Unmarshal(bytes, &cloned)
	return cloned
}

func (m *MemoryStore[S]) GetSnapshot(ctx context.Context, machineID string) (Snapshot[S], error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	machine, exists := m.machines[machineID]
	if !exists {
		return Snapshot[S]{}, ErrEntityNotFound
	}
	snap := machine.snapshot
	snap.State = cloneState(snap.State)
	return snap, nil
}

func (m *MemoryStore[S]) LoadNext(ctx context.Context, machineID string) (Snapshot[S], Command, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	machine, exists := m.machines[machineID]
	if !exists {
		return Snapshot[S]{}, Command{}, ErrEntityNotFound
	}

	targetSeq := machine.snapshot.LastAppliedSequence + 1
	maxSeq := machine.nextSequence - 1
	for targetSeq <= maxSeq {
		cmd, exists := machine.commands[targetSeq]
		if exists && (cmd.Status == StatusPending || cmd.Status == "") {
			cmd.Status = StatusProcessing
			machine.commands[targetSeq] = cmd
			snap := machine.snapshot
			snap.State = cloneState(snap.State)
			return snap, cmd, nil
		}
		targetSeq++
	}

	return Snapshot[S]{}, Command{}, ErrNoPendingCommand
}

func (m *MemoryStore[S]) Commit(ctx context.Context, req CommitRequest[S]) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	machine, exists := m.machines[req.MachineID]
	if !exists {
		return ErrEntityNotFound
	}

	// CAS validation
	if machine.snapshot.Version != req.ExpectedVersion || machine.snapshot.LastAppliedSequence+1 != req.CommandSequence {
		if c, ok := machine.commands[req.CommandSequence]; ok && c.Status == StatusProcessing {
			c.Status = StatusPending
			machine.commands[req.CommandSequence] = c
		}
		return ErrConflict
	}

	// Apply transition changes with deep clone
	machine.snapshot.State = cloneState(req.Result.State)
	machine.snapshot.Version++
	machine.snapshot.LastAppliedSequence = req.CommandSequence

	// Update command status
	cmd := machine.commands[req.CommandSequence]
	cmd.Status = StatusCompleted
	machine.commands[req.CommandSequence] = cmd

	// Store outbox effects
	for i, eff := range req.Result.Effects {
		if eff.ID == "" {
			eff.ID = fmt.Sprintf("%s/%d/%d", req.MachineID, req.CommandSequence, i)
		}
		eff.MachineID = req.MachineID
		eff.SourceSequence = req.CommandSequence
		eff.Status = "pending"
		m.outbox[eff.ID] = eff
	}

	// Process timer operations
	for _, t := range req.Result.Timers {
		if !t.Cancel {
			if t.Command.MachineID == "" {
				t.Command.MachineID = req.MachineID
			}
			machine.timers[t.Name] = t
		} else {
			delete(machine.timers, t.Name)
		}
	}

	return nil
}

func (m *MemoryStore[S]) UnclaimEffect(ctx context.Context, effectID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil
}

func (m *MemoryStore[S]) MarkCommandFailed(ctx context.Context, machineID string, cmd Command, reason error, retryable bool, retryDelay time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	machine, exists := m.machines[machineID]
	if !exists {
		return ErrEntityNotFound
	}

	c, exists := machine.commands[cmd.Sequence]
	if exists {
		if retryable {
			c.Status = StatusPending
		} else {
			c.Status = StatusFailed
			if reason != nil {
				c.Error = reason.Error()
			}
			machine.snapshot.LastAppliedSequence = cmd.Sequence
		}
		machine.commands[cmd.Sequence] = c
	}
	return nil
}

func (m *MemoryStore[S]) FetchPendingEffects(ctx context.Context, limit int) ([]Effect, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []Effect
	for _, eff := range m.outbox {
		if eff.Status == "pending" {
			result = append(result, eff)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *MemoryStore[S]) MarkEffectComplete(ctx context.Context, effectID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.outbox, effectID)
	return nil
}

func (m *MemoryStore[S]) FetchDueTimers(ctx context.Context, now time.Time, limit int) ([]TimerOperation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var due []TimerOperation
	for _, machine := range m.machines {
		for name, timer := range machine.timers {
			if !timer.DueAt.After(now) {
				due = append(due, timer)
				delete(machine.timers, name)
				if limit > 0 && len(due) >= limit {
					break
				}
			}
		}
		if limit > 0 && len(due) >= limit {
			break
		}
	}
	return due, nil
}
