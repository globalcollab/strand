package main

import (
	"encoding/json"
)

// State constants for CallState
const (
	StateNew              = "New"
	StateAuthenticating   = "Authenticating"
	StateBroadcasting     = "Broadcasting"
	StateWaitingForAnswer = "WaitingForAnswer"
	StateConnecting       = "Connecting"
	StateEnding           = "Ending"
	StateEnded            = "Ended"
)

// CallState tracks telephony call session details.
type CallState struct {
	CurrentState string   `json:"current_state"`
	CallID       string   `json:"call_id"`
	CallerID     string   `json:"caller_id"`
	Invitees     []string `json:"invitees"`
	AcceptedBy   string   `json:"accepted_by,omitempty"`
	EndReason    string   `json:"end_reason,omitempty"`
	TimerGen     uint64   `json:"timer_gen"`
}

func (c *CallState) GetState() string {
	return c.CurrentState
}

func (c *CallState) SetState(s string) {
	c.CurrentState = s
}

// Command Payload structs
type ReceiveCallPayload struct {
	CallID   string   `json:"call_id"`
	CallerID string   `json:"caller_id"`
	Invitees []string `json:"invitees"`
}

type AcceptCallPayload struct {
	UserID string `json:"user_id"`
}

type TimeoutPayload struct {
	TimerName  string `json:"timer_name"`
	Generation uint64 `json:"generation"`
}

func parsePayload[T any](raw json.RawMessage) (T, error) {
	var val T
	err := json.Unmarshal(raw, &val)
	return val, err
}
