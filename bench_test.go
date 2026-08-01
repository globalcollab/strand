package strand_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/globalcollab/strand"
	"github.com/globalcollab/strand/statechart"
	"github.com/redis/go-redis/v9"
)

type BenchState struct {
	Count int `json:"count"`
}

func (s *BenchState) GetState() string          { return "Active" }
func (s *BenchState) SetState(stateName string) {}

func setupBenchMachine() strand.Processor[*BenchState] {
	return statechart.New[*BenchState]("BenchMachine").
		Initial("Active").
		State("Active",
			statechart.On("Increment", func(ctx context.Context, state *BenchState, cmd strand.Command) (*BenchState, []strand.Effect, []strand.TimerOperation, error) {
				state.Count++
				effects := []strand.Effect{
					{
						Kind:           "LogMetric",
						IdempotencyKey: fmt.Sprintf("metric-%d", state.Count),
					},
				}
				return state, effects, nil, nil
			}),
		).
		Build()
}

func BenchmarkMemoryStore_SendAndProcess(b *testing.B) {
	ctx := context.Background()
	processor := setupBenchMachine()
	store := strand.NewMemoryStore[*BenchState](func() *BenchState {
		return &BenchState{Count: 0}
	})
	engine := strand.NewEngine[*BenchState](store, processor)
	machineID := "bench-mem-1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Send(ctx, machineID, "Increment", nil)
		if err != nil {
			b.Fatalf("Send error: %v", err)
		}
	}
}

func BenchmarkRedisStore_SendAndProcess(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Skip("Redis server not available at 127.0.0.1:6379")
	}

	ctx := context.Background()
	processor := setupBenchMachine()
	store := strand.NewRedisStore[*BenchState](rdb, "bench_strand", func() *BenchState {
		return &BenchState{Count: 0}
	})
	engine := strand.NewEngine[*BenchState](store, processor)
	machineID := "bench-redis-1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Send(ctx, machineID, "Increment", nil)
		if err != nil {
			b.Fatalf("Send error: %v", err)
		}
	}
}

func BenchmarkRedisStore_ConcurrentMultiEntity(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Skip("Redis server not available at 127.0.0.1:6379")
	}

	ctx := context.Background()
	processor := setupBenchMachine()
	store := strand.NewRedisStore[*BenchState](rdb, "bench_strand_concurrent", func() *BenchState {
		return &BenchState{Count: 0}
	})
	engine := strand.NewEngine[*BenchState](store, processor)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerID := fmt.Sprintf("bench-entity-%d", time.Now().UnixNano())
		for pb.Next() {
			_, err := engine.Send(ctx, workerID, "Increment", nil)
			if err != nil {
				b.Errorf("Send error: %v", err)
			}
		}
	})
}

func BenchmarkOutboxProcessor_Throughput(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Skip("Redis server not available at 127.0.0.1:6379")
	}

	ctx := context.Background()
	store := strand.NewRedisStore[*BenchState](rdb, "bench_strand_outbox", func() *BenchState {
		return &BenchState{Count: 0}
	})

	outbox := strand.NewOutboxProcessor[*BenchState](store, func(ctx context.Context, effect strand.Effect) error {
		return nil
	})

	// Pre-populate outbox effects
	machineID := "bench-outbox-entity"
	effects := make([]strand.Effect, 100)
	for i := 0; i < 100; i++ {
		effects[i] = strand.Effect{
			ID:             fmt.Sprintf("eff-bench-%d", i),
			Kind:           "LogMetric",
			IdempotencyKey: fmt.Sprintf("key-bench-%d", i),
		}
	}
	_ = store.Commit(ctx, strand.CommitRequest[*BenchState]{
		MachineID:       machineID,
		ExpectedVersion: 0,
		CommandSequence: 1,
		Command:         strand.Command{Sequence: 1},
		Result:          strand.Result[*BenchState]{State: &BenchState{Count: 1}, Effects: effects},
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = outbox.ProcessNext(ctx, 50)
	}
}

// BenchmarkMultiInstancePodRacing_SingleEntityContention simulates 5 independent worker pods
// all racing to process commands concurrently on the EXACT SAME entity instance.
func BenchmarkMultiInstancePodRacing_SingleEntityContention(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Skip("Redis server not available at 127.0.0.1:6379")
	}

	ctx := context.Background()
	processor := setupBenchMachine()
	const numPods = 5
	engines := make([]*strand.Engine[*BenchState], numPods)

	for i := 0; i < numPods; i++ {
		store := strand.NewRedisStore[*BenchState](rdb, "bench_pod_racing", func() *BenchState {
			return &BenchState{Count: 0}
		})
		engines[i] = strand.NewEngine[*BenchState](store, processor)
	}

	sharedEntityID := "shared-pod-racing-entity"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		podIdx := 0
		for pb.Next() {
			engine := engines[podIdx%numPods]
			podIdx++
			_, err := engine.Send(ctx, sharedEntityID, "Increment", nil)
			if err != nil {
				b.Errorf("Multi-instance pod send error: %v", err)
			}
		}
	})
}

// BenchmarkMultiInstancePodCluster_100Entities simulates 10 independent worker pods
// executing commands distributed across 100 entity state machines concurrently.
func BenchmarkMultiInstancePodCluster_100Entities(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Skip("Redis server not available at 127.0.0.1:6379")
	}

	ctx := context.Background()
	processor := setupBenchMachine()
	const numPods = 10
	engines := make([]*strand.Engine[*BenchState], numPods)

	for i := 0; i < numPods; i++ {
		store := strand.NewRedisStore[*BenchState](rdb, "bench_pod_cluster", func() *BenchState {
			return &BenchState{Count: 0}
		})
		engines[i] = strand.NewEngine[*BenchState](store, processor)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			engine := engines[i%numPods]
			entityID := fmt.Sprintf("cluster-entity-%d", i%100)
			i++
			_, err := engine.Send(ctx, entityID, "Increment", nil)
			if err != nil {
				b.Errorf("Multi-instance cluster send error: %v", err)
			}
		}
	})
}
