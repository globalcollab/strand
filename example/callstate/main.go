package main

import (
	"context"
	"fmt"
	"log"

	"github.com/globalcollab/strand"
)

func main() {
	ctx := context.Background()
	callID := "call-88192"

	// Initialize In-Memory Store for lightweight, zero-dependency execution
	store := strand.NewMemoryStore[*CallState](func() *CallState {
		return &CallState{}
	})

	processor := buildCallStateMachine()
	engine := strand.NewEngine[*CallState](store, processor)

	// Set up Outbox Processor
	outbox := strand.NewOutboxProcessor[*CallState](store, func(ctx context.Context, effect strand.Effect) error {
		fmt.Printf(" [OUTBOX] Executed Effect: Kind=%s | ID=%s | Key=%s\n", effect.Kind, effect.ID, effect.IdempotencyKey)
		return nil
	})

	fmt.Println("================================================================")
	fmt.Println("🚀 STRAND: Stateless Durable State Machine Telephony Workflow")
	fmt.Println("================================================================")

	// 1. Receive Call
	fmt.Println("\n1. Appending ReceiveCall Command...")
	_, err := engine.Send(ctx, callID, "ReceiveCall", ReceiveCallPayload{
		CallID:   callID,
		CallerID: "user-alice",
		Invitees: []string{"user-bob", "user-charlie"},
	})
	if err != nil {
		log.Fatalf("Failed Send: %v", err)
	}

	snap, _ := store.GetSnapshot(ctx, callID)
	fmt.Printf("   State after ReceiveCall: %s\n", snap.State.CurrentState)

	// Process Outbox
	_, _ = outbox.ProcessNext(ctx, 10)

	// 2. Auth Succeeded
	fmt.Println("\n2. Appending AuthSucceeded Command...")
	_, _ = engine.Send(ctx, callID, "AuthSucceeded", nil)
	snap, _ = store.GetSnapshot(ctx, callID)
	fmt.Printf("   State after AuthSucceeded: %s\n", snap.State.CurrentState)

	// Process Outbox
	_, _ = outbox.ProcessNext(ctx, 10)

	// 3. Invites Broadcasted
	fmt.Println("\n3. Appending InvitesBroadcasted Command...")
	_, _ = engine.Send(ctx, callID, "InvitesBroadcasted", nil)
	snap, _ = store.GetSnapshot(ctx, callID)
	fmt.Printf("   State after InvitesBroadcasted: %s (Timer Generation: %d)\n", snap.State.CurrentState, snap.State.TimerGen)

	// 4. Accept Call (User Bob accepts)
	fmt.Println("\n4. Appending AcceptCall Command (Bob accepts)...")
	_, _ = engine.Send(ctx, callID, "AcceptCall", AcceptCallPayload{UserID: "user-bob"})
	snap, _ = store.GetSnapshot(ctx, callID)
	fmt.Printf("   State after AcceptCall: %s (AcceptedBy: %s)\n", snap.State.CurrentState, snap.State.AcceptedBy)

	// Process Outbox
	_, _ = outbox.ProcessNext(ctx, 10)

	// 5. Simulate Stale Timer Callback
	fmt.Println("\n5. Simulating Stale AnswerTimeout Callback (Generation 1)...")
	timeoutPayload, _ := parsePayload[TimeoutPayload](nil)
	timeoutPayload.TimerName = "answer-timeout"
	timeoutPayload.Generation = 1 // Stale generation

	_, _ = engine.Send(ctx, callID, "AnswerTimeout", timeoutPayload)
	snap, _ = store.GetSnapshot(ctx, callID)
	fmt.Printf("   State after Stale AnswerTimeout: %s (Safely Ignored! Reason: %s)\n", snap.State.CurrentState, snap.State.EndReason)

	fmt.Println("\n================================================================")
	fmt.Println("✅ STRAND Execution Completed Successfully!")
	fmt.Println("================================================================")
}
