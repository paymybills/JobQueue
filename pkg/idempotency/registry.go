package idempotency

import (
	"sync"

	"github.com/aniro/jobqueue/pkg/models"
)

// Registry tracks idempotency keys to prevent duplicate job processing.
//
// How it works:
//   - On job submission, the producer checks if the idempotency key exists.
//   - If it does, the original result is returned (exactly-once semantics).
//   - If it doesn't, the key is registered as "in-flight" and processing proceeds.
//   - On completion, the result is stored for future lookups.
//
// Why:
//   - Network retries, client timeouts, or user double-clicks can cause
//     duplicate submissions. The registry ensures they are a no-op.
//   - RWMutex allows concurrent reads (duplicate checks) with exclusive writes.
type Registry struct {
	mu      sync.RWMutex
	keys    map[string]*Entry
}

// Entry represents the state of an idempotency key.
type Entry struct {
	JobID    string              `json:"job_id"`
	State    models.JobState     `json:"state"`
	Result   *models.JobResult   `json:"result,omitempty"`
}

// NewRegistry creates a new idempotency registry.
func NewRegistry() *Registry {
	return &Registry{
		keys: make(map[string]*Entry),
	}
}

// Check returns the existing entry for a key, if it exists.
// Uses a read lock for concurrent access.
func (r *Registry) Check(key string) (*Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, exists := r.keys[key]
	return entry, exists
}

// Register marks a key as in-flight. Returns false if the key already exists.
func (r *Registry) Register(key string, jobID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.keys[key]; exists {
		return false // Duplicate
	}

	r.keys[key] = &Entry{
		JobID: jobID,
		State: models.StatePending,
	}
	return true
}

// Complete stores the final result for a key.
func (r *Registry) Complete(key string, result *models.JobResult) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, exists := r.keys[key]; exists {
		entry.Result = result
		entry.State = result.State
	}
}

// Size returns the number of tracked keys.
func (r *Registry) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.keys)
}
