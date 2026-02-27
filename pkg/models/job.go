package models

import (
	"fmt"
	"time"

	"crypto/rand"
	"encoding/hex"
)

// JobState represents the lifecycle state of a job.
type JobState int

const (
	StatePending      JobState = iota // Waiting in queue
	StateProcessing                   // Being processed by a worker
	StateCompleted                    // Successfully processed
	StateFailed                       // Failed but may be retried
	StateDeadLettered                 // Exhausted retries, moved to DLQ
)

func (s JobState) String() string {
	switch s {
	case StatePending:
		return "PENDING"
	case StateProcessing:
		return "PROCESSING"
	case StateCompleted:
		return "COMPLETED"
	case StateFailed:
		return "FAILED"
	case StateDeadLettered:
		return "DEAD_LETTERED"
	default:
		return "UNKNOWN"
	}
}

// Job represents a unit of work in the queue.
type Job struct {
	ID             string    `json:"id"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Payload        string    `json:"payload"`
	Priority       int       `json:"priority"` // Higher = more urgent
	State          JobState  `json:"state"`
	RetryCount     int       `json:"retry_count"`
	MaxRetries     int       `json:"max_retries"`
	CreatedAt      time.Time `json:"created_at"`
	LastAttempt    time.Time `json:"last_attempt,omitempty"`
	NextRetryAt    time.Time `json:"next_retry_at,omitempty"`
	Error          string    `json:"error,omitempty"`
}

// JobResult represents the outcome of processing a job.
type JobResult struct {
	JobID   string   `json:"job_id"`
	Success bool     `json:"success"`
	State   JobState `json:"state"`
	Error   string   `json:"error,omitempty"`
}

// NewJob creates a new job with defaults.
func NewJob(payload string, idempotencyKey string, priority int, maxRetries int) *Job {
	return &Job{
		ID:             generateID(),
		IdempotencyKey: idempotencyKey,
		Payload:        payload,
		Priority:       priority,
		State:          StatePending,
		RetryCount:     0,
		MaxRetries:     maxRetries,
		CreatedAt:      time.Now(),
	}
}

func generateID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("failed to generate ID: %v", err))
	}
	return hex.EncodeToString(b)
}
