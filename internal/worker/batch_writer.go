package worker

import (
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"cargo-tracking-ingestion/internal/infrastructure/timescale"
	"cargo-tracking-ingestion/internal/resilience"
	"context"
	"log"
	"sync"
	"time"
)

type BatchWriter struct {
	repo           *timescale.Repository
	config         *config.WorkerConfig
	buffer         []*telemetry.Telemetry
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	flushSignal    chan struct{}
	circuitBreaker *resilience.CircuitBreaker
	retryConfig    resilience.RetryConfig
}

func NewBatchWriter(repo *timescale.Repository, config *config.WorkerConfig) *BatchWriter {
	ctx, cancel := context.WithCancel(context.Background())

	retryCfg := resilience.DefaultRetryConfig()
	retryCfg.MaxAttempts = 3
	retryCfg.InitialDelay = 500 * time.Millisecond
	retryCfg.MaxDelay = 5 * time.Second

	cbConfig := resilience.DefaultConfig()
	cbConfig.FailureThreshold = 5
	cbConfig.Timeout = 30 * time.Second

	return &BatchWriter{
		repo:           repo,
		config:         config,
		buffer:         make([]*telemetry.Telemetry, 0, config.BatchSize),
		ctx:            ctx,
		cancel:         cancel,
		flushSignal:    make(chan struct{}, 1),
		circuitBreaker: resilience.New(cbConfig),
		retryConfig:    retryCfg,
	}
}

func (bw *BatchWriter) Start() {
	bw.wg.Add(1)
	go bw.run()
	log.Println("Batch writer started")
}

func (bw *BatchWriter) Stop() {
	bw.cancel()
	bw.wg.Wait()

	// Flush remaining data
	bw.mu.Lock()
	remaining := bw.drainBuffer()
	bw.mu.Unlock()

	if len(remaining) > 0 {
		if err := bw.flushBatch(remaining); err != nil {
			log.Printf("Error flushing remaining data on shutdown: %v", err)
		}
	}

	log.Println("Batch writer stopped")
}

func (bw *BatchWriter) Add(t *telemetry.Telemetry) {
	bw.mu.Lock()
	bw.buffer = append(bw.buffer, t)
	shouldFlush := len(bw.buffer) >= bw.config.BatchSize
	bw.mu.Unlock()

	if shouldFlush {
		select {
		case bw.flushSignal <- struct{}{}:
		default:
		}
	}
}

func (bw *BatchWriter) run() {
	defer bw.wg.Done()

	ticker := time.NewTicker(bw.config.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-bw.ctx.Done():
			return
		case <-ticker.C:
			bw.tryFlush()
		case <-bw.flushSignal:
			bw.tryFlush()
		}
	}
}

// drainBuffer extracts and clears the buffer. Must be called with mu held.
func (bw *BatchWriter) drainBuffer() []*telemetry.Telemetry {
	if len(bw.buffer) == 0 {
		return nil
	}
	batch := make([]*telemetry.Telemetry, len(bw.buffer))
	copy(batch, bw.buffer)
	bw.buffer = bw.buffer[:0]
	return batch
}

// tryFlush attempts to flush the current buffer
func (bw *BatchWriter) tryFlush() {
	bw.mu.Lock()
	batch := bw.drainBuffer()
	bw.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	if err := bw.flushBatch(batch); err != nil {
		log.Printf("Error flushing batch: %v", err)
		// Re-queue the failed batch
		bw.requeueBatch(batch)
	}
}

// flushBatch writes a batch to the database. Does NOT hold the mutex.
func (bw *BatchWriter) flushBatch(batch []*telemetry.Telemetry) error {
	if len(batch) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(bw.ctx, 30*time.Second)
	defer cancel()

	start := time.Now()

	err := bw.circuitBreaker.Execute(ctx, func() error {
		return resilience.Do(ctx, bw.retryConfig, func() error {
			return bw.repo.BatchInsertTelemetry(ctx, batch)
		})
	})

	duration := time.Since(start)

	if err != nil {
		if bw.circuitBreaker.IsOpen() {
			log.Printf("Circuit breaker is open, dropping batch of %d records: %v", len(batch), err)
		} else {
			log.Printf("Failed to insert batch of %d records: %v (will retry)", len(batch), err)
		}
		return err
	}

	log.Printf("Successfully inserted batch of %d records in %v", len(batch), duration)
	return nil
}

// requeueBatch adds failed items back to the buffer for retry
func (bw *BatchWriter) requeueBatch(batch []*telemetry.Telemetry) {
	if bw.circuitBreaker.IsOpen() {
		// Don't requeue if circuit breaker is open
		return
	}

	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Prepend batch to buffer (failed items should be processed first)
	bw.buffer = append(batch, bw.buffer...)
}

func (bw *BatchWriter) BufferSize() int {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return len(bw.buffer)
}
