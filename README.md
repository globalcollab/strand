# Strand: Distributed Durable Entity Runtime for Go

`github.com/globalcollab/strand`

[![Go Reference](https://pkg.go.dev/badge/github.com/globalcollab/strand.svg)](https://pkg.go.dev/github.com/globalcollab/strand)
[![Release](https://img.shields.io/github/v/release/globalcollab/strand)](https://github.com/globalcollab/strand/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Strand** is a high-performance, stateless, production-grade distributed durable-entity runtime for Go. It enables developer teams to build fault-tolerant, stateful workflows (such as payment pipelines, call state machines, order fulfillment, and user lifecycles) using pure state-machine transitions backed by Redis or in-memory stores.

---

## Core Features

- **Stateless Worker Pod Scaling**: Deploy any number of worker instances without sticky sessions or complex cluster coordination.
- **Optimistic Concurrency Control (CAS)**: Uses atomic Lua-scripted Compare-And-Swap validation to eliminate race conditions under heavy worker contention.
- **Guaranteed Order Inbox**: Strict sequence-gated command processing per entity instance.
- **Transactional Outbox & Idempotency**: Asynchronous side-effect execution with worker crash recovery leases and at-least-once delivery guarantees.
- **Precision Durable Timers**: Schedule and cancel future timer commands with generation invalidation to prevent stale timer firing.
- **Fluent Statechart DSL**: Type-safe declarative state machine builder package (`strand/statechart`).

---

## Installation

```bash
go get github.com/globalcollab/strand@v0.0.1
```

---

## Architecture Overview

```
                      +-----------------------------+
                      |   Stateless Worker Pods     |
                      |  (Engine / State Processor) |
                      +--------------+--------------+
                                     |
                          Atomic Lua | Compare-And-Swap
                                     v
                      +-----------------------------+
                      |       Redis Persistent      |
                      |   (State, Inbox, Outbox)    |
                      +-----------------------------+
```

---

## Quickstart

### 1. Define Entity State & Transitions

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/globalcollab/strand"
	"github.com/globalcollab/strand/statechart"
	"github.com/redis/go-redis/v9"
)

type OrderState struct {
	CurrentState string `json:"current_state"`
	OrderID      string `json:"order_id"`
	Amount       int    `json:"amount"`
}

func (s *OrderState) GetState() string          { return s.CurrentState }
func (s *OrderState) SetState(stateName string) { s.CurrentState = stateName }

func main() {
	ctx := context.Background()

	// 1. Build Statechart
	processor := statechart.New[*OrderState]("OrderFlow").
		Initial("Pending").
		State("Pending",
			statechart.On("Pay", func(ctx context.Context, state *OrderState, cmd strand.Command) (*OrderState, []strand.Effect, []strand.TimerOperation, error) {
				state.CurrentState = "Paid"
				effects := []strand.Effect{
					{
						Kind:           "SendReceiptEmail",
						IdempotencyKey: fmt.Sprintf("receipt-%s", state.OrderID),
					},
				}
				return state, effects, nil, nil
			}),
		).
		Build()

	// 2. Initialize Redis Store & Engine
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	store := strand.NewRedisStore[*OrderState](rdb, "prod_strand", func() *OrderState {
		return &OrderState{CurrentState: "Pending"}
	})

	engine := strand.NewEngine[*OrderState](store, processor)

	// 3. Append & Process Command
	machineID := "order-1001"
	_, err := engine.Send(ctx, machineID, "Pay", nil)
	if err != nil {
		panic(err)
	}

	// 4. Run Worker Loop
	_ = engine.ProcessOne(ctx, machineID)

	// 5. Query Snapshot
	snap, _ := store.GetSnapshot(ctx, machineID)
	fmt.Printf("Order %s status: %s (version: %d)\n", snap.State.OrderID, snap.State.CurrentState, snap.Version)
}
```

---

## Outbox Side-Effect Processing

Process asynchronous side effects idempotently with automatic crash recovery:

```go
outbox := strand.NewOutboxProcessor[*OrderState](store, func(ctx context.Context, eff strand.Effect) error {
	switch eff.Kind {
	case "SendReceiptEmail":
		// Execute side effect (e.g. call email API or database)
		return nil
	default:
		return nil
	}
})

// Run background worker polling every 100ms
go outbox.StartWorker(context.Background(), 100*time.Millisecond, 50)
```

---

## Performance Benchmarks

Tested on Apple M4 Pro (Go 1.22, Redis 7 Alpine):

| Benchmark Suite | Throughput / Latency | Memory / Allocs |
| :--- | :--- | :--- |
| **`MemoryStore_SendAndProcess`** | **~788,840 ops/sec** (1.7 µs/op) | 1.3 KB/op (24 allocs) |
| **`OutboxProcessor_Throughput`** | **~4,760 items/sec** (209 µs / 50-item batch) | 0.5 KB/op (18 allocs) |
| **`RedisStore_ConcurrentMultiEntity`** | **~2,050 parallel ops/sec** | 7.7 KB/op (186 allocs) |

```bash
REDIS_ADDR="127.0.0.1:6379" go test -bench=. -benchmem ./...
```

---

## License

Distributed under the MIT License. See `LICENSE` for details.
