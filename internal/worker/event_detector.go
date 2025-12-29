package worker

import (
	"cargo-tracking-ingestion/internal/domain/event"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"cargo-tracking-ingestion/internal/infrastructure/timescale"
	"cargo-tracking-ingestion/internal/resilience"
	"context"
	"log"
	"sync"
	"time"
)

type EventDetector struct {
	repo           *timescale.Repository
	eventChan      chan *telemetry.Telemetry
	workers        int
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	circuitBreaker *resilience.CircuitBreaker
	retryConfig    resilience.RetryConfig
}

func NewEventDetector(repo *timescale.Repository, workers int) *EventDetector {
	ctx, cancel := context.WithCancel(context.Background())

	retryCfg := resilience.DefaultRetryConfig()
	retryCfg.MaxAttempts = 3
	retryCfg.InitialDelay = 500 * time.Millisecond
	retryCfg.MaxDelay = 5 * time.Second

	cbConfig := resilience.DefaultConfig()
	cbConfig.FailureThreshold = 5
	cbConfig.Timeout = 30 * time.Second

	return &EventDetector{
		repo:           repo,
		eventChan:      make(chan *telemetry.Telemetry, 1000),
		workers:        workers,
		ctx:            ctx,
		cancel:         cancel,
		circuitBreaker: resilience.New(cbConfig),
		retryConfig:    retryCfg,
	}
}

func (ed *EventDetector) Start() {
	for i := 0; i < ed.workers; i++ {
		ed.wg.Add(1)
		go ed.worker(i)
	}

	log.Printf("Event detector started with %d workers", ed.workers)
}

func (ed *EventDetector) Stop() {
	ed.cancel()
	close(ed.eventChan)
	ed.wg.Wait()
	log.Printf("Event detector stopped")
}

func (ed *EventDetector) Process(t *telemetry.Telemetry) {
	select {
	case ed.eventChan <- t:
	default:
		log.Printf("Event channel full, dropping telemetry for device %s", t.DeviceID)
	}
}

func (ed *EventDetector) worker(id int) {
	defer ed.wg.Done()

	for {
		select {
		case <-ed.ctx.Done():
			return
		case t, ok := <-ed.eventChan:
			if !ok {
				return
			}
			ed.detectAndSave(t)
		}
	}
}

func (ed *EventDetector) detectAndSave(t *telemetry.Telemetry) {
	events := ed.detectEvents(t)

	if len(events) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ed.ctx, 10*time.Second)
	defer cancel()

	// Use circuit breaker and retry
	err := ed.circuitBreaker.Execute(ctx, func() error {
		return resilience.Do(ctx, ed.retryConfig, func() error {
			return ed.repo.BatchInsertEvents(ctx, events)
		})
	})

	if err != nil {
		if ed.circuitBreaker.IsOpen() {
			log.Printf("Circuit breaker is open, dropping %d events for device %s: %v", len(events), t.DeviceID, err)
		} else {
			log.Printf("Failed to insert events for device %s after retries: %v", t.DeviceID, err)
		}
	} else {
		log.Printf("Detected and saved %d events for device %s", len(events), t.DeviceID)
	}
}

func (ed *EventDetector) detectEvents(t *telemetry.Telemetry) []*event.Event {
	var events []*event.Event

	// Low battery detection
	if t.BatteryLevel != nil && *t.BatteryLevel < 20 {
		e := event.NewEvent(t.DeviceID, event.LowBattery, event.SeverityWarning)
		e.WithHardwareUID(t.HardwareUID)
		e.WithDescription("Device battery is low")

		metadata := event.LowBatteryMetadata{
			BatteryLevel: *t.BatteryLevel,
			Threshold:    20,
		}
		if e, err := e.WithMetadata(metadata); err == nil {
			events = append(events, e)
		}
	}

	// Poor signal detection
	if t.SignalStrength != nil && *t.SignalStrength < -80 {
		e := event.NewEvent(t.DeviceID, event.PoorSignal, event.SeverityWarning)
		e.WithHardwareUID(t.HardwareUID)
		e.WithDescription("Device signal strength is low")
		metadata := event.SignalMetadata{
			SignalStrength: *t.SignalStrength,
			Threshold:      -80,
		}
		if e, err := e.WithMetadata(metadata); err == nil {
			events = append(events, e)
		}
	}

	// Temperature alerts
	if t.Temperature != nil {
		if *t.Temperature > 36.0 {
			e := event.NewEvent(t.DeviceID, event.TempHigh, event.SeverityCritical)
			e.WithHardwareUID(t.HardwareUID)
			e.WithDescription("Temperature exceeds safe threshold")

			metadata := event.TemperatureMetadata{
				Temperature: *t.Temperature,
				Threshold:   36.0,
				Unit:        "celsius",
			}
			if e, err := e.WithMetadata(metadata); err == nil {
				events = append(events, e)
			}
		} else if *t.Temperature < 0.0 {
			e := event.NewEvent(t.DeviceID, event.TempLow, event.SeverityCritical)
			e.WithHardwareUID(t.HardwareUID)
			e.WithDescription("Temperature below safe threshold")

			metadata := event.TemperatureMetadata{
				Temperature: *t.Temperature,
				Threshold:   0.0,
				Unit:        "celsius",
			}
			if e, err := e.WithMetadata(metadata); err == nil {
				events = append(events, e)
			}
		}
	}

	// Humidity alert
	if t.Humidity != nil && *t.Humidity > 80.0 {
		e := event.NewEvent(t.DeviceID, event.HumidityHigh, event.SeverityWarning)
		e.WithHardwareUID(t.HardwareUID)
		e.WithDescription("Humidity exceeds safe threshold")
		events = append(events, e)
	}

	if t.IsMoving != nil {
		if *t.IsMoving {
			e := event.NewEvent(t.DeviceID, event.Moving, event.SeverityInfo)
			e.WithHardwareUID(t.HardwareUID)
			e.WithDescription("Device started moving")
			events = append(events, e)
		} else {
			e := event.NewEvent(t.DeviceID, event.Stopped, event.SeverityInfo)
			e.WithHardwareUID(t.HardwareUID)
			e.WithDescription("Device stopped moving")
			events = append(events, e)
		}
	}

	for _, e := range events {
		e.Time = t.Time
	}

	return events
}
