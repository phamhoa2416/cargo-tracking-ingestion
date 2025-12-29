package handler

import (
	telemetryService "cargo-tracking-ingestion/internal/application/telemetry"
	"cargo-tracking-ingestion/internal/domain/event"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type TelemetryHandler struct {
	telemetryService *telemetryService.Service
	eventRepository  interface {
		GetEvents(ctx context.Context, params *event.QueryParams) (*event.List, error)
	}
}

func NewTelemetryHandler(
	telemetryService *telemetryService.Service,
	eventRepo interface {
		GetEvents(ctx context.Context, params *event.QueryParams) (*event.List, error)
	},
) *TelemetryHandler {
	return &TelemetryHandler{
		telemetryService: telemetryService,
		eventRepository:  eventRepo,
	}
}

func (h *TelemetryHandler) IngestTelemetry(c *gin.Context) {
	var t telemetry.Telemetry
	if err := c.ShouldBindJSON(&t); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.telemetryService.IngestTelemetry(c.Request.Context(), &t); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message":   "Telemetry accepted",
		"device_id": t.DeviceID,
		"timestamp": t.Time,
	})
}

func (h *TelemetryHandler) IngestBatch(c *gin.Context) {
	var batch []telemetry.Telemetry
	if err := c.ShouldBindJSON(&batch); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.telemetryService.IngestTelemetryBatch(c.Request.Context(), &batch); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message": "Batch accepted",
		"count":   len(batch),
	})
}

func (h *TelemetryHandler) Heartbeat(c *gin.Context) {
	var hb telemetry.Heartbeat
	if err := c.ShouldBindJSON(&hb); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.telemetryService.RecordHeartbeat(c.Request.Context(), &hb); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":   "Heartbeat received",
		"device_id": hb.DeviceID,
		"timestamp": hb.Timestamp,
	})
}

func (h *TelemetryHandler) GetLatestLocation(c *gin.Context) {
	deviceId := c.Param("id")
	deviceID, err := uuid.Parse(deviceId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid device_id"})
		return
	}

	location, err := h.telemetryService.GetLatestLocation(c.Request.Context(), deviceID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if location == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "location not found"})
		return
	}

	c.JSON(http.StatusOK, location)
}

func (h *TelemetryHandler) GetLocationHistory(c *gin.Context) {
	deviceId := c.Param("id")
	deviceID, err := uuid.Parse(deviceId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid device_id"})
		return
	}

	var params telemetry.LocationQueryParams
	params.DeviceID = deviceID

	if startTime := c.Query("start_time"); startTime != "" {
		if t, err := time.Parse(time.RFC3339, startTime); err == nil {
			params.StartTime = t
		}
	}

	if endTime := c.Query("end_time"); endTime != "" {
		if t, err := time.Parse(time.RFC3339, endTime); err == nil {
			params.EndTime = t
		}
	}

	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	history, err := h.telemetryService.GetLocationHistory(c.Request.Context(), &params)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, history)
}

func (h *TelemetryHandler) GetDeviceEvents(c *gin.Context) {
	deviceId := c.Param("id")
	deviceID, err := uuid.Parse(deviceId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid device_id"})
		return
	}

	var params event.QueryParams
	params.DeviceID = deviceID
	params.Limit = 100

	// Parse query parameters
	if startTime := c.Query("start_time"); startTime != "" {
		if t, err := time.Parse(time.RFC3339, startTime); err == nil {
			params.StartTime = t
		} else {
			params.StartTime = time.Now().Add(-24 * time.Hour)
		}
	} else {
		params.StartTime = time.Now().Add(-24 * time.Hour)
	}

	if endTime := c.Query("end_time"); endTime != "" {
		if t, err := time.Parse(time.RFC3339, endTime); err == nil {
			params.EndTime = t
		} else {
			params.EndTime = time.Now()
		}
	} else {
		params.EndTime = time.Now()
	}

	if eventType := c.Query("event_type"); eventType != "" {
		et := event.Type(eventType)
		params.EventType = &et
	}

	if severity := c.Query("severity"); severity != "" {
		s := event.Severity(severity)
		params.Severity = &s
	}

	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	events, err := h.eventRepository.GetEvents(c.Request.Context(), &params)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, events)
}
