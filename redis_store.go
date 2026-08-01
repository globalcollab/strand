package strand

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	luaCommitScript = `
local machine_key = KEYS[1]
local cmd_hash_key = KEYS[2]

local expected_ver = tonumber(ARGV[1])
local command_seq = tonumber(ARGV[2])
local new_state = ARGV[3]

local current_ver = tonumber(redis.call('HGET', machine_key, 'version') or '0')
local current_seq = tonumber(redis.call('HGET', machine_key, 'last_applied_sequence') or '0')

if current_ver ~= expected_ver or (current_seq + 1) ~= command_seq then
    redis.call('HSET', cmd_hash_key, 'status', 'pending')
    return redis.error_reply("ERR_CAS_CONFLICT")
end

-- Update machine instance state, version, and sequence
redis.call('HSET', machine_key, 'version', expected_ver + 1, 'last_applied_sequence', command_seq, 'state', new_state)

-- Mark command completed
redis.call('HSET', cmd_hash_key, 'status', 'completed')

return "OK"
`

	luaClaimEffectsScript = `
local outbox_key = KEYS[1]
local claimed_key = KEYS[2]
local limit = tonumber(ARGV[1])
local now_ts = tonumber(ARGV[2])
local lease_ttl = tonumber(ARGV[3])

local all_effects = redis.call('HGETALL', outbox_key)
local results = {}
local count = 0

for i = 1, #all_effects, 2 do
    local effect_id = all_effects[i]
    local effect_json = all_effects[i+1]

    local existing_claim = redis.call('HGET', claimed_key, effect_id)
    local can_claim = false
    if not existing_claim then
        can_claim = true
    else
        local claim_ts = tonumber(existing_claim) or 0
        if lease_ttl > 0 and (now_ts - claim_ts) > lease_ttl then
            can_claim = true
        end
    end

    if can_claim then
        redis.call('HSET', claimed_key, effect_id, now_ts)
        table.insert(results, effect_json)
        count = count + 1
        if limit > 0 and count >= limit then
            break
        end
    end
end

return results
`

	luaFetchAndRemoveDueTimersScript = `
local timers_key = KEYS[1]
local meta_key = KEYS[2]
local now_score = ARGV[1]
local limit = tonumber(ARGV[2])

local members = redis.call('ZRANGEBYSCORE', timers_key, '-inf', now_score, 'LIMIT', 0, limit)
local results = {}

for i = 1, #members do
    local m = members[i]
    local raw = redis.call('HGET', meta_key, m)
    if raw then
        table.insert(results, raw)
        redis.call('ZREM', timers_key, m)
        redis.call('HDEL', meta_key, m)
    end
end

return results
`
)

// RedisStore implements Store[S] using Redis / Dragonfly with atomic Lua scripts.
type RedisStore[S any] struct {
	client       redis.UniversalClient
	prefix       string
	initialState func() S
}

// NewRedisStore creates a new RedisStore instance.
func NewRedisStore[S any](client redis.UniversalClient, prefix string, initialState func() S) *RedisStore[S] {
	if prefix == "" {
		prefix = "strand"
	}
	return &RedisStore[S]{
		client:       client,
		prefix:       prefix,
		initialState: initialState,
	}
}

func (r *RedisStore[S]) machineKey(machineID string) string {
	return fmt.Sprintf("%s:machine:%s", r.prefix, machineID)
}

func (r *RedisStore[S]) commandKey(machineID string, seq uint64) string {
	return fmt.Sprintf("%s:cmd:%s:%d", r.prefix, machineID, seq)
}

func (r *RedisStore[S]) outboxKey() string {
	return fmt.Sprintf("%s:outbox", r.prefix)
}

func (r *RedisStore[S]) timersKey() string {
	return fmt.Sprintf("%s:timers", r.prefix)
}

func (r *RedisStore[S]) AppendCommand(ctx context.Context, machineID string, cmdType string, payload any) (Command, error) {
	mKey := r.machineKey(machineID)

	seq, err := r.client.HIncrBy(ctx, mKey, "next_sequence", 1).Result()
	if err != nil {
		return Command{}, fmt.Errorf("strand: failed to generate command sequence: %w", err)
	}

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
				return Command{}, fmt.Errorf("strand: failed to marshal payload: %w", err)
			}
			payloadRaw = bytes
		}
	}

	cmd := Command{
		MachineID:  machineID,
		CommandID:  fmt.Sprintf("%s-%d", machineID, seq),
		Sequence:   uint64(seq),
		Type:       cmdType,
		Payload:    payloadRaw,
		AcceptedAt: time.Now(),
		Status:     StatusPending,
		Attempts:   0,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return Command{}, fmt.Errorf("strand: failed to marshal command struct: %w", err)
	}

	cmdKey := r.commandKey(machineID, uint64(seq))
	err = r.client.HSet(ctx, cmdKey, map[string]interface{}{
		"data":   string(cmdBytes),
		"status": string(StatusPending),
	}).Err()

	if err != nil {
		return Command{}, fmt.Errorf("strand: failed to store command in redis: %w", err)
	}

	return cmd, nil
}

func (r *RedisStore[S]) GetSnapshot(ctx context.Context, machineID string) (Snapshot[S], error) {
	mKey := r.machineKey(machineID)
	vals, err := r.client.HMGet(ctx, mKey, "state", "version", "last_applied_sequence").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Snapshot[S]{}, err
	}

	var state S
	var version, lastSeq uint64

	if vals[0] != nil {
		if stateStr, ok := vals[0].(string); ok && stateStr != "" {
			if err := json.Unmarshal([]byte(stateStr), &state); err != nil {
				return Snapshot[S]{}, fmt.Errorf("strand: failed to unmarshal state: %w", err)
			}
		}
	} else if r.initialState != nil {
		state = r.initialState()
	} else {
		var zero S
		t := reflect.TypeOf(zero)
		if t != nil && t.Kind() == reflect.Ptr {
			state = reflect.New(t.Elem()).Interface().(S)
		}
	}

	if vals[1] != nil {
		if verStr, ok := vals[1].(string); ok {
			v, _ := strconv.ParseUint(verStr, 10, 64)
			version = v
		}
	}

	if vals[2] != nil {
		if seqStr, ok := vals[2].(string); ok {
			s, _ := strconv.ParseUint(seqStr, 10, 64)
			lastSeq = s
		}
	}

	return Snapshot[S]{
		MachineID:           machineID,
		State:               state,
		Version:             version,
		LastAppliedSequence: lastSeq,
	}, nil
}

func (r *RedisStore[S]) LoadNext(ctx context.Context, machineID string) (Snapshot[S], Command, error) {
	mKey := r.machineKey(machineID)
	vals, err := r.client.HMGet(ctx, mKey, "state", "version", "last_applied_sequence").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Snapshot[S]{}, Command{}, err
	}

	var state S
	var version, lastSeq uint64

	if vals[0] != nil {
		if stateStr, ok := vals[0].(string); ok && stateStr != "" {
			if err := json.Unmarshal([]byte(stateStr), &state); err != nil {
				return Snapshot[S]{}, Command{}, fmt.Errorf("strand: failed to unmarshal state: %w", err)
			}
		}
	} else if r.initialState != nil {
		state = r.initialState()
	} else {
		var zero S
		t := reflect.TypeOf(zero)
		if t != nil && t.Kind() == reflect.Ptr {
			state = reflect.New(t.Elem()).Interface().(S)
		}
	}

	if vals[1] != nil {
		if verStr, ok := vals[1].(string); ok {
			v, _ := strconv.ParseUint(verStr, 10, 64)
			version = v
		}
	}

	if vals[2] != nil {
		if seqStr, ok := vals[2].(string); ok {
			s, _ := strconv.ParseUint(seqStr, 10, 64)
			lastSeq = s
		}
	}

	nextSeq := lastSeq + 1
	cmdKey := r.commandKey(machineID, nextSeq)

	cmdVals, err := r.client.HMGet(ctx, cmdKey, "data", "status").Result()
	if err != nil || cmdVals[0] == nil {
		return Snapshot[S]{}, Command{}, ErrNoPendingCommand
	}

	statusStr, _ := cmdVals[1].(string)
	if statusStr != string(StatusPending) {
		return Snapshot[S]{}, Command{}, ErrNoPendingCommand
	}

	var cmd Command
	dataStr, _ := cmdVals[0].(string)
	if err := json.Unmarshal([]byte(dataStr), &cmd); err != nil {
		return Snapshot[S]{}, Command{}, fmt.Errorf("strand: failed to unmarshal command: %w", err)
	}

	snapshot := Snapshot[S]{
		MachineID:           machineID,
		State:               state,
		Version:             version,
		LastAppliedSequence: lastSeq,
	}

	return snapshot, cmd, nil
}

func (r *RedisStore[S]) Commit(ctx context.Context, req CommitRequest[S]) error {
	mKey := r.machineKey(req.MachineID)
	cmdKey := r.commandKey(req.MachineID, req.CommandSequence)

	stateBytes, err := json.Marshal(req.Result.State)
	if err != nil {
		return fmt.Errorf("strand: failed to marshal state for commit: %w", err)
	}

	// Execute atomic CAS Lua script
	res, err := r.client.Eval(ctx, luaCommitScript, []string{mKey, cmdKey},
		req.ExpectedVersion, req.CommandSequence, string(stateBytes),
	).Result()

	if err != nil {
		if err.Error() == "ERR_CAS_CONFLICT" || err.Error() == "ERR ERR_CAS_CONFLICT" {
			return ErrConflict
		}
		return fmt.Errorf("strand: commit redis script error: %w", err)
	}

	if res != "OK" {
		return ErrConflict
	}

	// Store outbox effects
	pipe := r.client.Pipeline()
	for i, eff := range req.Result.Effects {
		if eff.ID == "" {
			eff.ID = fmt.Sprintf("%s/%d/%d", req.MachineID, req.CommandSequence, i)
		}
		eff.MachineID = req.MachineID
		eff.SourceSequence = req.CommandSequence
		eff.Status = "pending"

		effBytes, _ := json.Marshal(eff)
		pipe.HSet(ctx, r.outboxKey(), eff.ID, string(effBytes))
	}

	// Process timer operations
	for _, t := range req.Result.Timers {
		timerKey := fmt.Sprintf("%s:%s", req.MachineID, t.Name)
		if t.Cancel {
			pipe.ZRem(ctx, r.timersKey(), timerKey)
		} else {
			if t.Command.MachineID == "" {
				t.Command.MachineID = req.MachineID
			}
			tBytes, _ := json.Marshal(t)
			pipe.HSet(ctx, fmt.Sprintf("%s:timer_meta", r.prefix), timerKey, string(tBytes))
			pipe.ZAdd(ctx, r.timersKey(), redis.Z{
				Score:  float64(t.DueAt.UnixNano()),
				Member: timerKey,
			})
		}
	}

	_, _ = pipe.Exec(ctx)
	return nil
}

func (r *RedisStore[S]) MarkCommandFailed(ctx context.Context, machineID string, cmd Command, reason error, retryable bool, retryDelay time.Duration) error {
	mKey := r.machineKey(machineID)
	cKey := r.commandKey(machineID, cmd.Sequence)

	status := string(StatusFailed)
	if retryable {
		status = string(StatusPending)
	}

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, cKey, "status", status)
	if reason != nil {
		pipe.HSet(ctx, cKey, "error", reason.Error())
	}
	if !retryable {
		pipe.HSet(ctx, mKey, "last_applied_sequence", cmd.Sequence)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStore[S]) outboxClaimedKey() string {
	return fmt.Sprintf("%s:outbox_claimed", r.prefix)
}

func (r *RedisStore[S]) FetchPendingEffects(ctx context.Context, limit int) ([]Effect, error) {
	nowMs := time.Now().UnixMilli()
	leaseTTLMs := 10000 // 10s default lease timeout
	res, err := r.client.Eval(ctx, luaClaimEffectsScript, []string{r.outboxKey(), r.outboxClaimedKey()}, limit, nowMs, leaseTTLMs).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	var results []Effect
	if rawList, ok := res.([]interface{}); ok {
		for _, item := range rawList {
			if str, ok := item.(string); ok {
				var eff Effect
				if err := json.Unmarshal([]byte(str), &eff); err == nil {
					results = append(results, eff)
				}
			}
		}
	}
	return results, nil
}

func (r *RedisStore[S]) MarkEffectComplete(ctx context.Context, effectID string) error {
	pipe := r.client.Pipeline()
	pipe.HDel(ctx, r.outboxKey(), effectID)
	pipe.HDel(ctx, r.outboxClaimedKey(), effectID)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStore[S]) UnclaimEffect(ctx context.Context, effectID string) error {
	return r.client.HDel(ctx, r.outboxClaimedKey(), effectID).Err()
}

func (r *RedisStore[S]) FetchDueTimers(ctx context.Context, now time.Time, limit int) ([]TimerOperation, error) {
	res, err := r.client.Eval(ctx, luaFetchAndRemoveDueTimersScript, []string{r.timersKey(), fmt.Sprintf("%s:timer_meta", r.prefix)}, now.UnixNano(), limit).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	var due []TimerOperation
	if rawList, ok := res.([]interface{}); ok {
		for _, item := range rawList {
			if str, ok := item.(string); ok {
				var t TimerOperation
				if err := json.Unmarshal([]byte(str), &t); err == nil {
					due = append(due, t)
				}
			}
		}
	}
	return due, nil
}
