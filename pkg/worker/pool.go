package worker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/aniro/jobqueue/pkg/models"
	"github.com/aniro/jobqueue/pkg/queue"
	"github.com/aniro/jobqueue/pkg/retry"
)

// HandlerFunc is the function signature for job processing logic.
// Return nil for ACK (success), non-nil error for NACK (failure).
type HandlerFunc func(job *models.Job) error

// Pool manages a fixed set of worker goroutines.
//
// Design:
//   - One goroutine per worker. Each goroutine runs an infinite loop:
//     Dequeue → Process → ACK/NACK.
//   - Workers block on queue.Dequeue() via sync.Cond — no busy-waiting,
//     no polling, zero CPU when idle.
//   - Graceful shutdown: context cancellation + WaitGroup ensures all
//     in-flight jobs complete before the pool exits.
//
// Backpressure interaction:
//   - With N workers and a bounded queue of size M, the system naturally
//     throttles: producers get 503s when the queue is full, and workers
//     drain it at their own pace.
type Pool struct {
	size         int
	queue        *queue.Queue
	retryManager *retry.Manager
	handler      HandlerFunc
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewPool creates a worker pool.
func NewPool(size int, q *queue.Queue, rm *retry.Manager, handler HandlerFunc) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		size:         size,
		queue:        q,
		retryManager: rm,
		handler:      handler,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start launches all worker goroutines.
func (p *Pool) Start() {
	log.Printf("[POOL] Starting %d workers...", p.size)
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// Stop signals all workers to shut down and waits for in-flight jobs to complete.
func (p *Pool) Stop() {
	log.Printf("[POOL] Shutting down workers...")
	p.cancel()
	p.queue.Close() // Unblock any workers waiting on Dequeue
	p.wg.Wait()
	log.Printf("[POOL] All workers stopped.")
}

// worker is the main loop for a single worker goroutine.
func (p *Pool) worker(id int) {
	defer p.wg.Done()
	log.Printf("[WORKER-%d] Started", id)

	for {
		// Check for shutdown
		select {
		case <-p.ctx.Done():
			log.Printf("[WORKER-%d] Shutting down", id)
			return
		default:
		}

		// Blocking dequeue — worker sleeps here until a job arrives
		job := p.queue.Dequeue()
		if job == nil {
			// Queue closed and empty
			log.Printf("[WORKER-%d] Queue closed, exiting", id)
			return
		}

		log.Printf("[WORKER-%d] Processing job %s (payload=%q, attempt=%d)",
			id, job.ID, job.Payload, job.RetryCount+1)

		// Execute the handler
		err := p.handler(job)

		if err != nil {
			// NACK — handler returned error
			log.Printf("[WORKER-%d] NACK job %s: %v", id, job.ID, err)
			p.retryManager.HandleFailure(job, err)
		} else {
			// ACK — success
			log.Printf("[WORKER-%d] ACK job %s", id, job.ID)
			p.retryManager.HandleSuccess(job)
		}
	}
}

// ExampleHandler returns a handler that randomly fails a percentage of jobs.
// Useful for demonstrating retry and DLQ behavior.
func ExampleHandler(failRate float64) HandlerFunc {
	var mu sync.Mutex
	processed := 0

	return func(job *models.Job) error {
		mu.Lock()
		processed++
		count := processed
		mu.Unlock()

		// Simulate work
		// time.Sleep(100 * time.Millisecond)

		// Simulate random failures
		if float64(count%10)/10.0 < failRate {
			return fmt.Errorf("simulated transient failure for job %s", job.ID)
		}
		return nil
	}
}
