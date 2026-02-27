package retry

import (
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/aniro/jobqueue/pkg/dlq"
	"github.com/aniro/jobqueue/pkg/idempotency"
	"github.com/aniro/jobqueue/pkg/models"
	"github.com/aniro/jobqueue/pkg/queue"
)

// Manager handles retry logic with exponential backoff and DLQ routing.
//
// How backoff works:
//   delay = min(baseDelay * 2^attempt + jitter, maxDelay)
//
// Why exponential backoff + jitter?
//   - If a downstream service is overwhelmed, retrying immediately makes it worse.
//   - Exponential growth gives the system breathing room to recover.
//   - Jitter prevents "thundering herd": without it, all retries at attempt N
//     fire at exactly the same time, recreating the original spike.
//
// Why route to DLQ after maxRetries?
//   - Infinite retries risk queue starvation (one bad job blocks everything).
//   - DLQ preserves the failed job for manual inspection and replay.
type Manager struct {
	queue      *queue.Queue
	dlq        *dlq.DeadLetterQueue
	registry   *idempotency.Registry
	baseDelay  time.Duration
	maxDelay   time.Duration
}

// NewManager creates a retry manager.
func NewManager(q *queue.Queue, d *dlq.DeadLetterQueue, r *idempotency.Registry, baseDelay, maxDelay time.Duration) *Manager {
	return &Manager{
		queue:     q,
		dlq:       d,
		registry:  r,
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
	}
}

// HandleFailure decides whether to retry a job or dead-letter it.
// This is the NACK path — called when a worker reports a processing failure.
func (m *Manager) HandleFailure(job *models.Job, err error) {
	job.RetryCount++
	job.Error = err.Error()
	job.State = models.StateFailed

	if job.RetryCount >= job.MaxRetries {
		// Exhausted retries → move to DLQ
		m.dlq.Add(job)

		// Update idempotency registry with final failure
		if job.IdempotencyKey != "" {
			m.registry.Complete(job.IdempotencyKey, &models.JobResult{
				JobID:   job.ID,
				Success: false,
				State:   models.StateDeadLettered,
				Error:   job.Error,
			})
		}
		return
	}

	// Calculate backoff with jitter
	delay := m.calculateBackoff(job.RetryCount)
	log.Printf("[RETRY] Job %s failed (attempt %d/%d). Retrying in %v. Error: %v",
		job.ID, job.RetryCount, job.MaxRetries, delay, err)

	// Requeue with delay (non-blocking — runs in a goroutine)
	m.queue.RequeueWithDelay(job, delay)
}

// HandleSuccess is the ACK path — marks a job as completed.
func (m *Manager) HandleSuccess(job *models.Job) {
	job.State = models.StateCompleted
	log.Printf("[ACK] ✓ Job %s completed successfully", job.ID)

	// Update idempotency registry
	if job.IdempotencyKey != "" {
		m.registry.Complete(job.IdempotencyKey, &models.JobResult{
			JobID:   job.ID,
			Success: true,
			State:   models.StateCompleted,
		})
	}
}

// calculateBackoff returns the delay before the next retry.
func (m *Manager) calculateBackoff(attempt int) time.Duration {
	// Exponential: baseDelay * 2^attempt
	exp := math.Pow(2, float64(attempt))
	delay := time.Duration(float64(m.baseDelay) * exp)

	// Add jitter: ±25% randomization
	jitter := time.Duration(rand.Float64()*0.5*float64(delay)) - time.Duration(0.25*float64(delay))
	delay += jitter

	// Cap at maxDelay
	if delay > m.maxDelay {
		delay = m.maxDelay
	}

	return delay
}
