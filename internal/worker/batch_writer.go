package worker

import (
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"cargo-tracking-ingestion/internal/infrastructure/timescale"
	"context"
	"log"
	"sync"
	"time"
)

type BatchWriter struct {
	repo       *timescale.Repository
	config     *config.WorkerConfig
	buffer     []*telemetry.Telemetry
	mu         sync.Mutex
	flushTimer *time.Timer
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewBatchWriter(repo *timescale.Repository, config *config.WorkerConfig) *BatchWriter {
	ctx, cancel := context.WithCancel(context.Background())

	return &BatchWriter{
		repo:   repo,
		config: config,
		buffer: make([]*telemetry.Telemetry, 0, config.BatchSize),
		ctx:    ctx,
		cancel: cancel,
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
	defer bw.mu.Unlock()

	bw.buffer = append(bw.buffer, t)

	if len(bw.buffer) == 1 {
		if bw.flushTimer != nil {
			bw.flushTimer.Stop()
		}
		bw.flushTimer = time.AfterFunc(bw.config.BatchTimeout, func() {
			bw.mu.Lock()
			defer bw.mu.Unlock()
			if len(bw.buffer) > 0 {
				if err := bw.flush(); err != nil {
					log.Printf("Error flushing batch on timeout: %v", err)
				}
			}
		})
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
					log.Printf("Error flushing batch on tick: %v", err)
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

	ctx, cancel := context.WithTimeout(bw.ctx, 10*time.Second)
	defer cancel()

	start := time.Now()
	err := bw.repo.BatchInsertTelemetry(ctx, batch)
	duration := time.Since(start)

	if err != nil {
		log.Printf("Failed to insert batch of %d record: %v", len(batch), err)
		return err
	}

	log.Printf("Successfully inserted batch of %d records in %v", len(batch), duration)
	return nil
}
