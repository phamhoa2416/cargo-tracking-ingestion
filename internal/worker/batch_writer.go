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

	bw.mu.Lock()
	if len(bw.buffer) > 0 {
		if err := bw.flush(); err != nil {
			log.Printf("Error flushing remaining data on shutdown: %v", err)
		}
	}
	bw.mu.Unlock()

	log.Println("Batch writer stopped")
}

func (bw *BatchWriter) Add(t *telemetry.Telemetry) {
	bw.mu.Lock()
	bw.buffer = append(bw.buffer, t)
	shouldFlush := len(bw.buffer) >= bw.config.BatchSize
	bw.mu.Unlock()

	// Signal flush if batch size reached (non-blocking)
	if shouldFlush {
		select {
		case bw.flushSignal <- struct{}{}:
		default:
			// Signal already pending, skip
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
			bw.mu.Lock()
			if len(bw.buffer) > 0 {
				if err := bw.flush(); err != nil {
					log.Printf("Error flushing batch on timeout: %v", err)
				}
			}
			bw.mu.Unlock()

		case <-bw.flushSignal:
			bw.mu.Lock()
			if len(bw.buffer) > 0 {
				if err := bw.flush(); err != nil {
					log.Printf("Error flushing batch on size limit: %v", err)
				}
			}
			bw.mu.Unlock()
		}
	}
}

func (bw *BatchWriter) flush() error {
	if len(bw.buffer) == 0 {
		return nil
	}

	batch := make([]*telemetry.Telemetry, len(bw.buffer))
	copy(batch, bw.buffer)
	bw.buffer = bw.buffer[:0]

	bw.mu.Unlock()
	defer bw.mu.Lock()

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
		if !bw.circuitBreaker.IsOpen() {
			bw.buffer = append(batch, bw.buffer...)
			log.Printf(
				"Failed to insert batch of %d records: %v (will retry)",
				len(batch), err,
			)
		} else {
			log.Printf(
				"Circuit breaker is open, dropping batch of %d records: %v",
				len(batch), err,
			)
		}
		return err
	}

	log.Printf("Successfully inserted batch of %d records in %v", len(batch), duration)
	return nil
}
