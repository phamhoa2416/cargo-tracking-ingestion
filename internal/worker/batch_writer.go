package worker

import (
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	repo "cargo-tracking-ingestion/internal/infrastructure/timescale/telemetry"
	"cargo-tracking-ingestion/internal/resilience"
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type BatchWriter struct {
	repo   *repo.Repository
	config *config.WorkerConfig

	buffer []*telemetry.Telemetry
	mu     sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	flushSignal chan struct{}

	circuitBreaker *resilience.CircuitBreaker
	retryConfig    resilience.RetryConfig

	stopped       atomic.Bool
	maxBufferSize int
}

func NewBatchWriter(repo *repo.Repository, config *config.WorkerConfig) *BatchWriter {
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
		maxBufferSize:  1000,
	}
}

func (bw *BatchWriter) Start() {
	bw.wg.Add(1)
	go bw.run()
	log.Println("[batch-writer] started")
}

func (bw *BatchWriter) Stop() {
	if bw.stopped.Swap(true) {
		return
	}

	bw.cancel()
	bw.wg.Wait()

	// Flush remaining data
	bw.mu.Lock()
	remaining := bw.drainBuffer()
	bw.mu.Unlock()

	if len(remaining) == 0 {
		return
	}

	log.Printf("Flushing %d remaining telemetry records on shutdown...", len(remaining))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := bw.circuitBreaker.Execute(ctx, func() error {
		return resilience.Do(ctx, bw.retryConfig, func() error {
			return bw.repo.BatchInsertTelemetry(ctx, remaining)
		})
	})

	if err != nil {
		log.Printf("Failed to flush %d remaining records on shutdown: %v", len(remaining), err)
	} else {
		log.Printf("Successfully flushed %d remaining records on shutdown", len(remaining))
	}

	log.Println("BatchWriter stopped gracefully")
}

func (bw *BatchWriter) Add(t *telemetry.Telemetry) {
	if bw.stopped.Load() {
		return
	}

	bw.mu.Lock()
	defer bw.mu.Unlock()

	if len(bw.buffer) >= bw.maxBufferSize {
		log.Printf("Buffer full (%d/%d), dropping new telemetry record", len(bw.buffer), bw.maxBufferSize)
		return
	}

	bw.buffer = append(bw.buffer, t)

	if len(bw.buffer) >= bw.config.BatchSize {
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

func (bw *BatchWriter) drainBuffer() []*telemetry.Telemetry {
	if len(bw.buffer) == 0 {
		return nil
	}
	batch := make([]*telemetry.Telemetry, len(bw.buffer))
	copy(batch, bw.buffer)
	bw.buffer = bw.buffer[:0]
	return batch
}

func (bw *BatchWriter) tryFlush() {
	bw.mu.Lock()
	batch := bw.drainBuffer()
	bw.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	if err := bw.flushBatch(batch); err != nil {
		log.Printf("Failed to flush batch of %d records: %v", len(batch), err)
		bw.requeueBatch(batch)
	}
}

func (bw *BatchWriter) flushBatch(batch []*telemetry.Telemetry) error {
	ctx, cancel := context.WithTimeout(bw.ctx, 30*time.Second)
	defer cancel()

	start := time.Now()
	err := bw.circuitBreaker.Execute(ctx, func() error {
		return resilience.Do(ctx, bw.retryConfig, func() error {
			return bw.repo.BatchInsertTelemetry(ctx, batch)
		})
	})

	if err != nil {
		if bw.circuitBreaker.IsOpen() {
			log.Printf("Circuit breaker OPEN - dropping batch of %d records", len(batch))
		}
		return err
	}

	log.Printf("Inserted batch of %d records in %v", len(batch), time.Since(start))
	return nil
}

func (bw *BatchWriter) requeueBatch(batch []*telemetry.Telemetry) {
	if bw.circuitBreaker.IsOpen() {
		log.Printf("Circuit breaker open - dropping failed batch of %d records", len(batch))
		return
	}

	bw.mu.Lock()
	defer bw.mu.Unlock()

	newBuffer := append(batch, bw.buffer...)
	if len(newBuffer) > bw.maxBufferSize {
		dropped := len(newBuffer) - bw.maxBufferSize
		newBuffer = newBuffer[dropped:]
		log.Printf("Buffer overflow during requeue - dropped %d oldest failed records", dropped)
	}
	bw.buffer = newBuffer
}

func (bw *BatchWriter) BufferSize() int {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return len(bw.buffer)
}
