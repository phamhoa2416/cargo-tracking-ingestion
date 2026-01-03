package handler

import (
	service "cargo-tracking-ingestion/internal/application/shipment"
	"cargo-tracking-ingestion/internal/domain/shipment"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http"
)

type ShipmentHandler struct {
	service *service.Service
}

func NewShipmentHandler(service *service.Service) *ShipmentHandler {
	return &ShipmentHandler{
		service: service,
	}
}

func (h *ShipmentHandler) CreateTrackingPoint(c *gin.Context) {
	var point shipment.TrackingPoint
	if err := c.ShouldBindJSON(&point); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.service.CreateTrackingPoint(c.Request.Context(), &point); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":     "Tracking point created",
		"shipment_id": point.ShipmentID,
		"device_id":   point.DeviceID,
		"timestamp":   point.Time,
	})
}

func (h *ShipmentHandler) GetTrackingHistory(c *gin.Context) {
	shipmentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid shipment_id"})
		return
	}

	var params shipment.TrackingQueryParams
	params.ShipmentID = shipmentID

	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	history, err := h.service.GetTrackingHistory(c.Request.Context(), &params)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, history)
}

func (h *ShipmentHandler) GetLiveTracking(c *gin.Context) {
	shipmentID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid shipment_id"})
		return
	}

	liveTracking, err := h.service.GetLiveTracking(c.Request.Context(), shipmentID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if liveTracking == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "shipment not found"})
		return
	}

	c.JSON(http.StatusOK, liveTracking)
}
