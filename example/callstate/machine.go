package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/globalcollab/strand"
	"github.com/globalcollab/strand/statechart"
)

func buildCallStateMachine() *statechart.Statechart[*CallState] {
	return statechart.New[*CallState]("CallState").
		Initial(StateNew).
		State(StateNew,
			statechart.On("ReceiveCall", handleReceiveCall),
		).
		State(StateAuthenticating,
			statechart.On("AuthSucceeded", handleAuthSucceeded),
			statechart.On("AuthFailed", handleAuthFailed),
		).
		State(StateBroadcasting,
			statechart.On("InvitesBroadcasted", handleInvitesBroadcasted),
		).
		State(StateWaitingForAnswer,
			statechart.On("AcceptCall", handleAcceptCall),
			statechart.On("CallerHangup", handleCallerHangup),
			statechart.On("AnswerTimeout", handleAnswerTimeout),
		).
		State(StateConnecting,
			statechart.On("CallerHangup", handleCallerHangup),
		).
		Build()
}

func handleReceiveCall(ctx context.Context, state *CallState, cmd strand.Command) (*CallState, []strand.Effect, []strand.TimerOperation, error) {
	p, err := parsePayload[ReceiveCallPayload](cmd.Payload)
	if err != nil {
		return state, nil, nil, err
	}

	state.CallID = p.CallID
	state.CallerID = p.CallerID
	state.Invitees = p.Invitees
	state.CurrentState = StateAuthenticating

	authPayload, _ := json.Marshal(map[string]string{"caller_id": p.CallerID})
	effects := []strand.Effect{
		{
			Kind:           "AuthenticateCaller",
			IdempotencyKey: fmt.Sprintf("auth-%s", p.CallID),
			Payload:        authPayload,
		},
	}

	return state, effects, nil, nil
}

func handleAuthSucceeded(ctx context.Context, state *CallState, cmd strand.Command) (*CallState, []strand.Effect, []strand.TimerOperation, error) {
	state.CurrentState = StateBroadcasting

	invPayload, _ := json.Marshal(map[string]interface{}{"call_id": state.CallID, "invitees": state.Invitees})
	effects := []strand.Effect{
		{
			Kind:           "BroadcastInvites",
			IdempotencyKey: fmt.Sprintf("broadcast-%s", state.CallID),
			Payload:        invPayload,
		},
	}

	return state, effects, nil, nil
}

func handleAuthFailed(ctx context.Context, state *CallState, cmd strand.Command) (*CallState, []strand.Effect, []strand.TimerOperation, error) {
	state.CurrentState = StateEnded
	state.EndReason = "Authentication Failed"
	return state, nil, nil, nil
}

func handleInvitesBroadcasted(ctx context.Context, state *CallState, cmd strand.Command) (*CallState, []strand.Effect, []strand.TimerOperation, error) {
	state.CurrentState = StateWaitingForAnswer
	state.TimerGen++

	timeoutPayload, _ := json.Marshal(TimeoutPayload{
		TimerName:  "answer-timeout",
		Generation: state.TimerGen,
	})

	timers := []strand.TimerOperation{
		{
			Name:       "answer-timeout",
			Generation: state.TimerGen,
			DueAt:      time.Now().Add(30 * time.Second),
			Command: strand.Command{
				MachineID: state.CallID,
				Type:      "AnswerTimeout",
				Payload:   timeoutPayload,
			},
		},
	}

	return state, nil, timers, nil
}

func handleAcceptCall(ctx context.Context, state *CallState, cmd strand.Command) (*CallState, []strand.Effect, []strand.TimerOperation, error) {
	p, err := parsePayload[AcceptCallPayload](cmd.Payload)
	if err != nil {
		return state, nil, nil, err
	}

	state.CurrentState = StateConnecting
	state.AcceptedBy = p.UserID

	// Cancel the answer timeout
	timers := []strand.TimerOperation{
		{
			Name:   "answer-timeout",
			Cancel: true,
		},
	}

	connPayload, _ := json.Marshal(map[string]string{"call_id": state.CallID, "accepted_by": p.UserID})
	effects := []strand.Effect{
		{
			Kind:           "ConnectCall",
			IdempotencyKey: fmt.Sprintf("connect-%s", state.CallID),
			Payload:        connPayload,
		},
	}

	return state, effects, timers, nil
}

func handleCallerHangup(ctx context.Context, state *CallState, cmd strand.Command) (*CallState, []strand.Effect, []strand.TimerOperation, error) {
	state.CurrentState = StateEnded
	state.EndReason = "Caller Hangup"
	return state, nil, nil, nil
}

func handleAnswerTimeout(ctx context.Context, state *CallState, cmd strand.Command) (*CallState, []strand.Effect, []strand.TimerOperation, error) {
	p, err := parsePayload[TimeoutPayload](cmd.Payload)
	if err != nil {
		return state, nil, nil, err
	}

	// Generation check: ignore stale timers
	if p.Generation != state.TimerGen {
		return state, nil, nil, nil // No-op
	}

	state.CurrentState = StateEnded
	state.EndReason = "No Answer Timeout"

	notifyPayload, _ := json.Marshal(map[string]string{"call_id": state.CallID, "reason": "no_answer"})
	effects := []strand.Effect{
		{
			Kind:           "NotifyMissedCall",
			IdempotencyKey: fmt.Sprintf("missed-%s", state.CallID),
			Payload:        notifyPayload,
		},
	}

	return state, effects, nil, nil
}
