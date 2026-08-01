package strand

// Snapshot wraps an entity's state together with optimistic versioning metadata.
type Snapshot[S any] struct {
	MachineID           string `json:"machine_id"`
	State               S      `json:"state"`
	Version             uint64 `json:"version"`
	LastAppliedSequence uint64 `json:"last_applied_sequence"`
}
