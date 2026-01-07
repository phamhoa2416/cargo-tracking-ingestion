package worker

import (
	"cargo-tracking-ingestion/internal/domain/event"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"cargo-tracking-ingestion/internal/infrastructure/rabbitmq"
	"cargo-tracking-ingestion/internal/infrastructure/redis"
	repo "cargo-tracking-ingestion/internal/infrastructure/timescale/telemetry"
	"cargo-tracking-ingestion/internal/resilience"
	"context"
	"log"
	"sync"
	"time"
)

type EventDetector struct {
	repo           *repo.Repository
	eventChan      chan *telemetry.Telemetry
	workers        int
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	circuitBreaker *resilience.CircuitBreaker
	retryConfig    resilience.RetryConfig

	prevState   map[string]deviceState
	prevStateMu sync.RWMutex

	rmqPublisher *rabbitmq.Publisher
	redisPubsub  *redis.PubSub
}

type deviceState struct {
	BatteryLevel   *int
	SignalStrength *int
	Temperature    *float64
	Humidity       *float64
	IsMoving       *bool
}

func NewEventDetector(repo *repo.Repository, workers int, rmqPublisher *rabbitmq.Publisher, redisPubsub *redis.PubSub) *EventDetector {
	ctx, cancel := context.WithCancel(context.Background())

	retryCfg := resilience.DefaultRetryConfig()
	retryCfg.MaxAttempts = 3
	retryCfg.InitialDelay = 500 * time.Millisecond
	retryCfg.MaxDelay = 5 * time.Second

	cbCfg := resilience.DefaultConfig()
	cbCfg.FailureThreshold = 5
	cbCfg.Timeout = 60 * time.Second

	return &EventDetector{
		repo:           repo,
		eventChan:      make(chan *telemetry.Telemetry, 2000),
		workers:        workers,
		ctx:            ctx,
		cancel:         cancel,
		circuitBreaker: resilience.New(cbCfg),
		retryConfig:    retryCfg,
		prevState:      make(map[string]deviceState),
		rmqPublisher:   rmqPublisher,
		redisPubsub:    redisPubsub,
	}
}

func (ed *EventDetector) Start() {
	for i := 0; i < ed.workers; i++ {
		ed.wg.Add(1)
		go ed.worker(i)
	}
	log.Printf("EventDetector started with %d workers", ed.workers)
}

func (ed *EventDetector) Stop() {
	ed.cancel()
	close(ed.eventChan)
	ed.wg.Wait()
	log.Println("EventDetector stopped gracefully")
}

func (ed *EventDetector) Process(t *telemetry.Telemetry) {
	select {
	case ed.eventChan <- t:
	default:
		log.Printf("WARNING: EventDetector channel full, dropping telemetry from device %s", t.DeviceID)
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

	ctx, cancel := context.WithTimeout(ed.ctx, 15*time.Second)
	defer cancel()

	err := ed.circuitBreaker.Execute(ctx, func() error {
		return resilience.Do(ctx, ed.retryConfig, func() error {
			return ed.repo.BatchInsertEvents(ctx, events)
		})
	})

	if err != nil {
		if ed.circuitBreaker.IsOpen() {
			log.Printf("Circuit breaker OPEN - dropped %d events for device %s", len(events), t.DeviceID)
		} else {
			log.Printf("Failed to save %d events for device %s: %v", len(events), t.DeviceID, err)
		}
	} else {
		log.Printf("Saved %d events for device %s", len(events), t.DeviceID)
		publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
		go func() {
			defer publishCancel()
			ed.publishEvents(publishCtx, events)
		}()
	}
}

func (ed *EventDetector) publishEvents(ctx context.Context, events []*event.Event) {
	for _, e := range events {
		// Publish to RabbitMQ
		if ed.rmqPublisher != nil {
			if err := ed.rmqPublisher.PublishEvent(ctx, e); err != nil {
				log.Printf("Failed to publish event to RabbitMQ: %v", err)
			}
		}

		// Publish to Redis Pub/Sub
		if ed.redisPubsub != nil {
			alert := &redis.EventAlert{
				DeviceID:    e.DeviceID,
				EventType:   string(e.Type),
				Severity:    string(e.Severity),
				Description: "",
				Timestamp:   e.Time.Unix(),
			}
			if e.Description != nil {
				alert.Description = *e.Description
			}
			if err := ed.redisPubsub.PublishEventAlert(ctx, alert); err != nil {
				log.Printf("Failed to publish event alert to Redis: %v", err)
			}
		}
	}
}

func (ed *EventDetector) detectEvents(t *telemetry.Telemetry) []*event.Event {
	var events []*event.Event
	deviceKey := t.DeviceID.String()

	ed.prevStateMu.RLock()
	prev, exists := ed.prevState[deviceKey]
	ed.prevStateMu.RUnlock()

	// Low battery detection - create event if battery is low
	if t.BatteryLevel != nil && *t.BatteryLevel < 20 {
		if !exists || prev.BatteryLevel == nil || *prev.BatteryLevel >= 20 {
			e := event.NewEvent(t.DeviceID, event.LowBattery, event.SeverityWarning).
				WithHardwareUID(t.HardwareUID).
				WithDescription("Device battery low")
			if e, _ := e.WithMetadata(event.LowBatteryMetadata{
				BatteryLevel: *t.BatteryLevel,
				Threshold:    20,
			}); e != nil {
				e.Time = t.Time
				events = append(events, e)
			}
		}
	}

	// Poor signal detection - create event if signal is poor
	if t.SignalStrength != nil && *t.SignalStrength < -80 {
		if !exists || prev.SignalStrength == nil || *prev.SignalStrength >= -80 {
			e := event.NewEvent(t.DeviceID, event.PoorSignal, event.SeverityWarning).
				WithHardwareUID(t.HardwareUID).
				WithDescription("Poor signal strength")
			if e, _ := e.WithMetadata(event.SignalMetadata{
				SignalStrength: *t.SignalStrength,
				Threshold:      -80,
			}); e != nil {
				e.Time = t.Time
				events = append(events, e)
			}
		}
	}

	// Temperature alerts
	if t.Temperature != nil {
		if *t.Temperature > 36.0 {
			e := event.NewEvent(t.DeviceID, event.TempHigh, event.SeverityCritical).
				WithHardwareUID(t.HardwareUID).
				WithDescription("High temperature detected")
			if e, _ := e.WithMetadata(event.TemperatureMetadata{
				Temperature: *t.Temperature,
				Threshold:   36.0,
				Unit:        "celsius",
			}); e != nil {
				e.Time = t.Time
				events = append(events, e)
			}
		} else if *t.Temperature < 0.0 {
			e := event.NewEvent(t.DeviceID, event.TempLow, event.SeverityCritical).
				WithHardwareUID(t.HardwareUID).
				WithDescription("Low temperature detected")
			if e, _ := e.WithMetadata(event.TemperatureMetadata{
				Temperature: *t.Temperature,
				Threshold:   0.0,
				Unit:        "celsius",
			}); e != nil {
				e.Time = t.Time
				events = append(events, e)
			}
		}
	}

	// Humidity alert
	if t.Humidity != nil && *t.Humidity > 80.0 {
		e := event.NewEvent(t.DeviceID, event.HumidityHigh, event.SeverityWarning).
			WithHardwareUID(t.HardwareUID).
			WithDescription("High humidity detected")
		e.Time = t.Time
		events = append(events, e)
	}

	if t.IsMoving != nil {
		if !exists || prev.IsMoving == nil || *prev.IsMoving != *t.IsMoving {
			if *t.IsMoving {
				e := event.NewEvent(t.DeviceID, event.Moving, event.SeverityInfo).
					WithHardwareUID(t.HardwareUID).
					WithDescription("Device started moving")
				e.Time = t.Time
				events = append(events, e)
			} else {
				e := event.NewEvent(t.DeviceID, event.Stopped, event.SeverityInfo).
					WithHardwareUID(t.HardwareUID).
					WithDescription("Device stopped moving")
				e.Time = t.Time
				events = append(events, e)
			}
		}
	}

	ed.prevStateMu.Lock()
	ed.prevState[deviceKey] = deviceState{
		BatteryLevel:   t.BatteryLevel,
		SignalStrength: t.SignalStrength,
		Temperature:    t.Temperature,
		Humidity:       t.Humidity,
		IsMoving:       t.IsMoving,
	}
	ed.prevStateMu.Unlock()

	return events
}
