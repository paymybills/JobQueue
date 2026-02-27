package dlq

import (
	"log"
	"sync"

	"github.com/aniro/jobqueue/pkg/models"
)

// DeadLetterQueue holds jobs that have exhausted all retry attempts.
//
// Why a separate queue?
//   - Failed jobs shouldn't block the main queue or pollute it.
//   - Operators need visibility into persistent failures for debugging.
//   - Manual retry from DLQ is a common production pattern — you fix the
//     root cause, then replay the failed jobs.
type DeadLetterQueue struct {
	mu   sync.Mutex
	jobs []*models.Job
}

// New creates a new dead letter queue.
func New() *DeadLetterQueue {
	return &DeadLetterQueue{
		jobs: make([]*models.Job, 0),
	}
}

// Add moves a permanently failed job into the DLQ.
func (d *DeadLetterQueue) Add(job *models.Job) {
	d.mu.Lock()
	defer d.mu.Unlock()

	job.State = models.StateDeadLettered
	d.jobs = append(d.jobs, job)

	log.Printf("[DLQ] ☠ Job %s dead-lettered after %d attempts. Error: %s",
		job.ID, job.RetryCount, job.Error)
}

// List returns all jobs currently in the DLQ.
func (d *DeadLetterQueue) List() []*models.Job {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := make([]*models.Job, len(d.jobs))
	copy(result, d.jobs)
	return result
}

// Remove removes a job from the DLQ by ID (e.g., for manual retry).
// Returns the job if found, nil otherwise.
func (d *DeadLetterQueue) Remove(jobID string) *models.Job {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, job := range d.jobs {
		if job.ID == jobID {
			d.jobs = append(d.jobs[:i], d.jobs[i+1:]...)
			return job
		}
	}
	return nil
}

// Len returns the current size of the DLQ.
func (d *DeadLetterQueue) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.jobs)
}
