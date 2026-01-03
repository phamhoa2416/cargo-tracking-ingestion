package redis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	deviceInfoPrefix      = "device:info:"
	deviceLocationPrefix  = "device:location:"
	deviceStatusPrefix    = "device:status:"
	shipmentDevicesPrefix = "shipment:devices:"

	deviceCacheTTL     = 1 * time.Hour
	locationCacheTTL   = 5 * time.Minute
	statusCacheTTL     = 10 * time.Minute
	shipmentDevicesTTL = 24 * time.Hour
)

type Cache struct {
	client *Client
}

func NewCache(client *Client) *Cache {
	return &Cache{client: client}
}

type DeviceInfo struct {
	DeviceID    uuid.UUID  `json:"device_id"`
	HardwareUID uuid.UUID  `json:"hardware_uid"`
	Name        string     `json:"name"`
	Type        string     `json:"type"`
	ShipmentID  *uuid.UUID `json:"shipment_id,omitempty"`
	IsActive    bool       `json:"is_active"`
	LastSeen    time.Time  `json:"last_seen"`
	FirmwareVer string     `json:"firmware_version"`
	CachedAt    time.Time  `json:"cached_at"`
}

type DeviceLocation struct {
	DeviceID  uuid.UUID `json:"device_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Altitude  *float64  `json:"altitude"`
	Speed     *float64  `json:"speed"`
	Heading   *float64  `json:"heading"`
	Accuracy  *float64  `json:"accuracy"`
	Timestamp time.Time `json:"timestamp"`
	CachedAt  time.Time `json:"cached_at"`
}

type DeviceStatus struct {
	DeviceID       uuid.UUID `json:"device_id"`
	IsOnline       bool      `json:"is_online"`
	BatteryLevel   *int      `json:"battery_level"`
	SignalStrength *int      `json:"signal_strength"`
	IsMoving       *bool     `json:"is_moving"`
	LastHeartbeat  time.Time `json:"last_heartbeat"`
	CachedAt       time.Time `json:"cached_at"`
}

func (c *Cache) GetDeviceInfo(ctx context.Context, deviceID uuid.UUID) (*DeviceInfo, error) {
	key := deviceInfoPrefix + deviceID.String()
	data, err := c.client.Client().Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil // cache miss
	}

	if err != nil {
		return nil, err
	}

	var info DeviceInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

func (c *Cache) SetDeviceInfo(ctx context.Context, info *DeviceInfo) error {
	info.CachedAt = time.Now()

	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	key := deviceInfoPrefix + info.DeviceID.String()
	return c.client.Client().Set(ctx, key, data, deviceCacheTTL).Err()
}

func (c *Cache) DeleteDeviceInfo(ctx context.Context, deviceID uuid.UUID) error {
	key := deviceInfoPrefix + deviceID.String()
	return c.client.Client().Del(ctx, key).Err()
}

func (c *Cache) GetDeviceLocation(ctx context.Context, deviceID uuid.UUID) (*DeviceLocation, error) {
	key := deviceLocationPrefix + deviceID.String()

	data, err := c.client.Client().Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var location DeviceLocation
	if err := json.Unmarshal(data, &location); err != nil {
		return nil, err
	}

	return &location, nil
}

func (c *Cache) SetDeviceLocation(ctx context.Context, location *DeviceLocation) error {
	location.CachedAt = time.Now()

	data, err := json.Marshal(location)
	if err != nil {
		return err
	}

	key := deviceLocationPrefix + location.DeviceID.String()
	return c.client.Client().Set(ctx, key, data, locationCacheTTL).Err()
}

func (c *Cache) GetDeviceStatus(ctx context.Context, deviceID uuid.UUID) (*DeviceStatus, error) {
	key := deviceStatusPrefix + deviceID.String()

	data, err := c.client.Client().Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var status DeviceStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, err
	}

	return &status, nil
}

func (c *Cache) SetDeviceStatus(ctx context.Context, status *DeviceStatus) error {
	status.CachedAt = time.Now()

	data, err := json.Marshal(status)
	if err != nil {
		return err
	}

	key := deviceStatusPrefix + status.DeviceID.String()
	return c.client.Client().Set(ctx, key, data, statusCacheTTL).Err()
}

func (c *Cache) GetShipmentDevices(ctx context.Context, shipmentID uuid.UUID) ([]uuid.UUID, error) {
	key := shipmentDevicesPrefix + shipmentID.String()

	members, err := c.client.Client().SMembers(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return []uuid.UUID{}, nil
	}
	if err != nil {
		return nil, err
	}

	devices := make([]uuid.UUID, 0, len(members))
	for _, m := range members {
		if id, err := uuid.Parse(m); err == nil {
			devices = append(devices, id)
		}
	}

	return devices, nil
}

func (c *Cache) AddShipmentDevice(ctx context.Context, shipmentID, deviceID uuid.UUID) error {
	key := shipmentDevicesPrefix + shipmentID.String()

	if err := c.client.Client().SAdd(ctx, key, deviceID.String()).Err(); err != nil {
		return err
	}

	return c.client.Client().Expire(ctx, key, shipmentDevicesTTL).Err()
}

func (c *Cache) RemoveShipmentDevice(ctx context.Context, shipmentID, deviceID uuid.UUID) error {
	key := shipmentDevicesPrefix + shipmentID.String()
	return c.client.Client().SRem(ctx, key, deviceID.String()).Err()
}

func (c *Cache) FlushDeviceCache(ctx context.Context, deviceID uuid.UUID) error {
	keys := []string{
		deviceInfoPrefix + deviceID.String(),
		deviceLocationPrefix + deviceID.String(),
		deviceStatusPrefix + deviceID.String(),
	}

	return c.client.Client().Del(ctx, keys...).Err()
}
