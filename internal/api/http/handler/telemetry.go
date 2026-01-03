package handler

import (
	service "cargo-tracking-ingestion/internal/application/telemetry"
	"cargo-tracking-ingestion/internal/domain/event"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"context"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http"
)

type TelemetryHandler struct {
	service         *service.Service
	eventRepository interface {
		GetEvents(ctx context.Context, params *event.QueryParams) (*event.List, error)
	}
}

func NewTelemetryHandler(
	service *service.Service,
	eventRepo interface {
		GetEvents(ctx context.Context, params *event.QueryParams) (*event.List, error)
	},
) *TelemetryHandler {
	return &TelemetryHandler{
		service:         service,
		eventRepository: eventRepo,
	}
}

func (h *TelemetryHandler) IngestTelemetry(c *gin.Context) {
	var t telemetry.Telemetry
	if err := c.ShouldBindJSON(&t); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.service.IngestTelemetry(c.Request.Context(), &t); err != nil {
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

	if err := h.service.IngestTelemetryBatch(c.Request.Context(), batch); err != nil {
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

	if err := h.service.RecordHeartbeat(c.Request.Context(), &hb); err != nil {
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
	deviceID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid device_id"})
		return
	}

	location, err := h.service.GetLatestLocation(c.Request.Context(), deviceID)
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
	deviceID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid device_id"})
		return
	}

	var params telemetry.LocationQueryParams
	params.DeviceID = deviceID

	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	history, err := h.service.GetLocationHistory(c.Request.Context(), &params)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, history)
}

func (h *TelemetryHandler) GetDeviceEvents(c *gin.Context) {
	deviceID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid device_id"})
		return
	}

	var params event.QueryParams
	params.DeviceID = deviceID

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
