package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aniro/jobqueue/pkg/dlq"
	"github.com/aniro/jobqueue/pkg/idempotency"
	"github.com/aniro/jobqueue/pkg/models"
	"github.com/aniro/jobqueue/pkg/producer"
	"github.com/aniro/jobqueue/pkg/queue"
	"github.com/aniro/jobqueue/pkg/retry"
	"github.com/aniro/jobqueue/pkg/worker"
)

const (
	queueCapacity = 100   // Max jobs before backpressure kicks in
	workerCount   = 5     // One goroutine per worker
	baseDelay     = 500 * time.Millisecond
	maxDelay      = 10 * time.Second
	httpAddr      = ":8080"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	fmt.Println(`
╔══════════════════════════════════════════════════════════════╗
║              DISTRIBUTED JOB QUEUE SYSTEM                   ║
║                                                              ║
║  Concurrency: Go goroutines + sync.Mutex + sync.Cond        ║
║  Guarantees:  At-least-once · Exactly-once · Idempotency    ║
║  Features:    Backpressure · Retry/Backoff · Dead Letter Q   ║
╚══════════════════════════════════════════════════════════════╝`)

	// ─── Initialize Components ───
	q := queue.NewQueue(queueCapacity)
	d := dlq.New()
	registry := idempotency.NewRegistry()
	retryMgr := retry.NewManager(q, d, registry, baseDelay, maxDelay)

	// ─── Worker Handler ───
	// This handler simulates real-world processing with random failures.
	// ~30% of jobs fail with transient errors to demonstrate retry + DLQ.
	handler := func(job *models.Job) error {
		// Simulate variable processing time (50-300ms)
		processingTime := time.Duration(50+rand.Intn(250)) * time.Millisecond
		time.Sleep(processingTime)

		// Simulate failures based on payload
		failRoll := rand.Float64()
		if failRoll < 0.3 {
			return fmt.Errorf("transient error: downstream service unavailable")
		}
		return nil
	}

	// ─── Start Worker Pool ───
	pool := worker.NewPool(workerCount, q, retryMgr, handler)
	pool.Start()

	// ─── Start HTTP Producer API ───
	srv := producer.NewServer(q, d, registry)
	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: srv.Handler(),
	}

	go func() {
		log.Printf("[MAIN] HTTP API listening on %s", httpAddr)
		log.Printf("[MAIN] Endpoints:")
		log.Printf("[MAIN]   POST /jobs          — Submit a job")
		log.Printf("[MAIN]   GET  /jobs/:id      — Query job status")
		log.Printf("[MAIN]   GET  /dlq           — List dead-lettered jobs")
		log.Printf("[MAIN]   POST /dlq/:id/retry — Retry a DLQ job")
		log.Printf("[MAIN]   GET  /metrics       — System metrics")
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("[MAIN] HTTP server error: %v", err)
		}
	}()

	// ─── Seed Demo Jobs ───
	// Submit a batch of jobs to demonstrate the system in action.
	go func() {
		time.Sleep(500 * time.Millisecond) // Let the server start
		log.Println("[DEMO] Seeding 15 demo jobs...")

		for i := 0; i < 15; i++ {
			priority := rand.Intn(10)
			job := models.NewJob(
				fmt.Sprintf("task-%d-data", i),
				fmt.Sprintf("idem-key-%d", i),
				priority,
				3, // max 3 retries before DLQ
			)
			if err := q.Enqueue(job); err != nil {
				log.Printf("[DEMO] Failed to enqueue: %v", err)
			}
			// Register idempotency key
			registry.Register(job.IdempotencyKey, job.ID)
		}

		// Demonstrate duplicate rejection
		time.Sleep(1 * time.Second)
		log.Println("[DEMO] Attempting duplicate idempotency key...")
		dupJob := models.NewJob("duplicate-task", "idem-key-0", 5, 3)
		if !registry.Register(dupJob.IdempotencyKey, dupJob.ID) {
			log.Println("[DEMO] ✓ Duplicate idempotency key correctly rejected!")
		}

		// Show backpressure by flooding the queue
		time.Sleep(2 * time.Second)
		log.Println("[DEMO] Testing backpressure (flooding queue)...")
		rejected := 0
		for i := 0; i < 200; i++ {
			job := models.NewJob(fmt.Sprintf("flood-%d", i), "", rand.Intn(5), 2)
			if err := q.Enqueue(job); err != nil {
				rejected++
			}
		}
		if rejected > 0 {
			log.Printf("[DEMO] ✓ Backpressure active! %d jobs rejected (queue full)", rejected)
		}

		// Print periodic stats
		for {
			time.Sleep(5 * time.Second)
			stats := q.Stats()
			log.Printf("[STATS] Queue: %d/%d | Enqueued: %d | Dequeued: %d | Rejected: %d | DLQ: %d",
				stats.CurrentSize, stats.MaxSize, stats.TotalEnqueued, stats.TotalDequeued, stats.TotalRejected, d.Len())
		}
	}()

	// ─── Graceful Shutdown ───
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[MAIN] Shutdown signal received...")
	pool.Stop()
	httpServer.Shutdown(context.Background())
	log.Println("[MAIN] Shutdown complete.")
}
