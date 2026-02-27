package queue

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/aniro/jobqueue/pkg/models"
)

// Queue is a thread-safe priority queue with blocking dequeue and backpressure.
//
// Design rationale:
//   - Mutex + sync.Cond chosen over channels because channels don't support
//     priority ordering or dynamic capacity-based backpressure.
//   - sync.Cond.Wait() blocks workers with zero CPU cost (parked goroutines),
//     and Signal() wakes exactly one, giving us an efficient blocking pop.
//   - The queue sorts by priority on each enqueue. In production you'd use
//     a heap for O(log n), but a sorted slice is clearer for demonstration.
type Queue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	jobs     []*models.Job
	jobIndex map[string]*models.Job // Fast lookup by ID
	maxSize  int
	closed   bool

	// Metrics
	totalEnqueued  int64
	totalDequeued  int64
	totalRejected  int64
}

// NewQueue creates a queue with a maximum capacity for backpressure.
func NewQueue(maxSize int) *Queue {
	q := &Queue{
		jobs:     make([]*models.Job, 0),
		jobIndex: make(map[string]*models.Job),
		maxSize:  maxSize,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Enqueue adds a job to the queue.
// Returns an error if the queue is full (backpressure) or closed.
func (q *Queue) Enqueue(job *models.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	if len(q.jobs) >= q.maxSize {
		q.totalRejected++
		return fmt.Errorf("queue is full (%d/%d) — backpressure active", len(q.jobs), q.maxSize)
	}

	job.State = models.StatePending
	q.jobs = append(q.jobs, job)
	q.jobIndex[job.ID] = job

	// Sort by priority descending (higher priority = dequeued first)
	sort.SliceStable(q.jobs, func(i, j int) bool {
		return q.jobs[i].Priority > q.jobs[j].Priority
	})

	q.totalEnqueued++
	log.Printf("[QUEUE] Enqueued job %s (priority=%d, queue_size=%d/%d)",
		job.ID, job.Priority, len(q.jobs), q.maxSize)

	// Wake one sleeping worker
	q.cond.Signal()
	return nil
}

// Dequeue blocks until a job is available, then removes and returns it.
// Returns nil if the queue is closed and empty.
func (q *Queue) Dequeue() *models.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Block while queue is empty and not closed
	for len(q.jobs) == 0 && !q.closed {
		q.cond.Wait() // Releases lock, sleeps, re-acquires on wake
	}

	if len(q.jobs) == 0 {
		return nil // Queue closed and drained
	}

	// Pop the highest priority job (index 0 after sort)
	job := q.jobs[0]
	q.jobs = q.jobs[1:]
	job.State = models.StateProcessing
	job.LastAttempt = time.Now()

	q.totalDequeued++
	return job
}

// RequeueWithDelay puts a job back in the queue after a retry delay.
// The job is held in a goroutine until the delay elapses.
func (q *Queue) RequeueWithDelay(job *models.Job, delay time.Duration) {
	log.Printf("[QUEUE] Requeuing job %s after %v (attempt %d/%d)",
		job.ID, delay, job.RetryCount, job.MaxRetries)

	go func() {
		time.Sleep(delay)
		job.State = models.StatePending
		if err := q.Enqueue(job); err != nil {
			log.Printf("[QUEUE] Failed to requeue job %s: %v", job.ID, err)
		}
	}()
}

// GetJob retrieves a job by ID for status queries.
func (q *Queue) GetJob(id string) (*models.Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobIndex[id]
	return job, ok
}

// Len returns the current number of jobs in the queue.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}

// IsFull returns true if the queue has reached its capacity.
func (q *Queue) IsFull() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs) >= q.maxSize
}

// Close signals all waiting workers to stop blocking.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast() // Wake all sleeping workers so they can exit
}

// Stats returns queue metrics.
type Stats struct {
	CurrentSize   int   `json:"current_size"`
	MaxSize       int   `json:"max_size"`
	TotalEnqueued int64 `json:"total_enqueued"`
	TotalDequeued int64 `json:"total_dequeued"`
	TotalRejected int64 `json:"total_rejected"`
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{
		CurrentSize:   len(q.jobs),
		MaxSize:       q.maxSize,
		TotalEnqueued: q.totalEnqueued,
		TotalDequeued: q.totalDequeued,
		TotalRejected: q.totalRejected,
	}
}
