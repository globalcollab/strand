package strand

import (
	"context"
	"fmt"
	"time"
)

// EffectHandler executes a side effect outbox intent idempotently.
type EffectHandler func(ctx context.Context, effect Effect) error

// OutboxProcessor polls pending outbox effects and executes them using an EffectHandler.
type OutboxProcessor[S any] struct {
	store   Store[S]
	handler EffectHandler
}

// NewOutboxProcessor creates a new OutboxProcessor instance.
func NewOutboxProcessor[S any](store Store[S], handler EffectHandler) *OutboxProcessor[S] {
	return &OutboxProcessor[S]{
		store:   store,
		handler: handler,
	}
}

// ProcessNext claims and dispatches up to limit pending outbox effects.
func (o *OutboxProcessor[S]) ProcessNext(ctx context.Context, limit int) (int, error) {
	effects, err := o.store.FetchPendingEffects(ctx, limit)
	if err != nil {
		return 0, err
	}

	processed := 0
	for i, eff := range effects {
		if o.handler != nil {
			if err := o.handler(ctx, eff); err != nil {
				_ = o.store.UnclaimEffect(ctx, eff.ID)
				// Unclaim remaining unclaimed effects in this batch so they are available immediately
				for j := i + 1; j < len(effects); j++ {
					_ = o.store.UnclaimEffect(ctx, effects[j].ID)
				}
				return processed, fmt.Errorf("strand: outbox effect handler failed for %s: %w", eff.ID, err)
			}
		}
		if err := o.store.MarkEffectComplete(ctx, eff.ID); err != nil {
			return processed, fmt.Errorf("strand: failed to mark effect complete %s: %w", eff.ID, err)
		}
		processed++
	}

	return processed, nil
}

// StartWorker runs a background polling loop for outbox effects.
func (o *OutboxProcessor[S]) StartWorker(ctx context.Context, interval time.Duration, batchSize int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = o.ProcessNext(ctx, batchSize)
		}
	}
}
