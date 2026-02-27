package producer

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/aniro/jobqueue/pkg/dlq"
	"github.com/aniro/jobqueue/pkg/idempotency"
	"github.com/aniro/jobqueue/pkg/models"
	"github.com/aniro/jobqueue/pkg/queue"
)

// Server is the HTTP producer API for submitting and querying jobs.
//
// Endpoints:
//   POST /jobs     — Submit a new job (with idempotency check + backpressure)
//   GET  /jobs/:id — Query job status
//   GET  /dlq      — List dead-lettered jobs
//   POST /dlq/:id/retry — Manually retry a dead-lettered job
//   GET  /metrics  — Queue and system stats
type Server struct {
	queue    *queue.Queue
	dlq      *dlq.DeadLetterQueue
	registry *idempotency.Registry
	mux      *http.ServeMux
}

// SubmitRequest is the JSON body for job submission.
type SubmitRequest struct {
	Payload        string `json:"payload"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	Priority       int    `json:"priority,omitempty"`
	MaxRetries     int    `json:"max_retries,omitempty"`
}

// SubmitResponse is returned on successful job submission.
type SubmitResponse struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// NewServer creates and configures the producer HTTP server.
func NewServer(q *queue.Queue, d *dlq.DeadLetterQueue, r *idempotency.Registry) *Server {
	s := &Server{
		queue:    q,
		dlq:      d,
		registry: r,
		mux:      http.NewServeMux(),
	}
	s.mux.HandleFunc("/jobs", s.handleJobs)
	s.mux.HandleFunc("/jobs/", s.handleJobByID)
	s.mux.HandleFunc("/dlq", s.handleDLQ)
	s.mux.HandleFunc("/dlq/", s.handleDLQRetry)
	s.mux.HandleFunc("/metrics", s.handleMetrics)
	return s
}

// Handler returns the HTTP handler for the server.
func (s *Server) Handler() http.Handler {
	return s.mux
}

// handleJobs handles POST /jobs
func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}

	if req.Payload == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "payload is required"})
		return
	}

	// Default max retries
	if req.MaxRetries == 0 {
		req.MaxRetries = 3
	}

	// ─── Idempotency Check ───
	// If the client provides an idempotency key, check if we've seen it before.
	// This is the core of exactly-once semantics from the client's perspective.
	if req.IdempotencyKey != "" {
		if entry, exists := s.registry.Check(req.IdempotencyKey); exists {
			log.Printf("[PRODUCER] Duplicate idempotency key: %s (job %s)", req.IdempotencyKey, entry.JobID)
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"error":  "duplicate idempotency key",
				"job_id": entry.JobID,
				"state":  entry.State.String(),
				"result": entry.Result,
			})
			return
		}
	}

	// Create the job
	job := models.NewJob(req.Payload, req.IdempotencyKey, req.Priority, req.MaxRetries)

	// Register idempotency key before enqueuing
	if req.IdempotencyKey != "" {
		if !s.registry.Register(req.IdempotencyKey, job.ID) {
			// Race condition — another request registered the key between Check and Register
			writeJSON(w, http.StatusConflict, map[string]string{"error": "duplicate idempotency key (race)"})
			return
		}
	}

	// ─── Backpressure ───
	// If the queue is full, reject with 503 so the client knows to slow down.
	if err := s.queue.Enqueue(job); err != nil {
		log.Printf("[PRODUCER] Backpressure! Rejecting job: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error":   "queue is full — try again later",
			"details": err.Error(),
		})
		return
	}

	log.Printf("[PRODUCER] Accepted job %s (key=%s, priority=%d)", job.ID, req.IdempotencyKey, req.Priority)

	writeJSON(w, http.StatusAccepted, SubmitResponse{
		JobID:  job.ID,
		Status: "accepted",
	})
}

// handleJobByID handles GET /jobs/:id
func (s *Server) handleJobByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job ID required"})
		return
	}

	job, found := s.queue.GetJob(id)
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// handleDLQ handles GET /dlq
func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"count": s.dlq.Len(),
		"jobs":  s.dlq.List(),
	})
}

// handleDLQRetry handles POST /dlq/:id/retry
func (s *Server) handleDLQRetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/dlq/")
	id := strings.TrimSuffix(path, "/retry")

	job := s.dlq.Remove(id)
	if job == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found in DLQ"})
		return
	}

	// Reset for retry
	job.RetryCount = 0
	job.State = models.StatePending
	job.Error = ""

	if err := s.queue.Enqueue(job); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}

	log.Printf("[PRODUCER] Retrying DLQ job %s", job.ID)
	writeJSON(w, http.StatusAccepted, map[string]string{
		"status": "requeued",
		"job_id": job.ID,
	})
}

// handleMetrics handles GET /metrics
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"queue":             s.queue.Stats(),
		"dlq_size":          s.dlq.Len(),
		"idempotency_keys":  s.registry.Size(),
	})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
