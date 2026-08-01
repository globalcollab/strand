package strand

import (
	"encoding/json"
	"time"
)

// CommandStatus represents the execution status of an inbox command.
type CommandStatus string

const (
	StatusPending    CommandStatus = "pending"
	StatusProcessing CommandStatus = "processing"
	StatusCompleted  CommandStatus = "completed"
	StatusFailed     CommandStatus = "failed"
	StatusRejected   CommandStatus = "rejected"
)

// Command represents an ordered input target to a specific entity instance (strand).
type Command struct {
	MachineID   string          `json:"machine_id"`
	CommandID   string          `json:"command_id"`
	Sequence    uint64          `json:"sequence"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	AcceptedAt  time.Time       `json:"accepted_at"`
	Correlation string          `json:"correlation,omitempty"`
	Causation   string          `json:"causation,omitempty"`
	Status      CommandStatus   `json:"status"`
	Attempts    int             `json:"attempts"`
	Error       string          `json:"error,omitempty"`
}

// Effect represents an asynchronous side-effect intent committed to the transactional outbox.
type Effect struct {
	ID             string          `json:"id"` // Unique stable key: {machineID}/{sequence}/{effectIndex}
	MachineID      string          `json:"machine_id"`
	SourceSequence uint64          `json:"source_sequence"`
	Kind           string          `json:"kind"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Attempts       int             `json:"attempts"`
}

// TimerOperation represents an intent to schedule or cancel a future timer command.
type TimerOperation struct {
	Name       string          `json:"name"`
	Generation uint64          `json:"generation"`
	DueAt      time.Time       `json:"due_at"`
	Command    Command         `json:"command"`
	Cancel     bool            `json:"cancel"`
}

// Result represents the pure output of a state machine transition step.
type Result[S any] struct {
	State   S                `json:"state"`
	Changed bool             `json:"changed"`
	Effects []Effect         `json:"effects,omitempty"`
	Timers  []TimerOperation `json:"timers,omitempty"`
}
