package handler

import (
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type WebSocketHandler struct {
	clients    map[*Client]bool
	broadcast  chan *telemetry.Telemetry
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

type Client struct {
	conn      *websocket.Conn
	send      chan *telemetry.Telemetry
	deviceIds map[uuid.UUID]bool
}

func NewWebSocketHandler() *WebSocketHandler {
	h := &WebSocketHandler{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan *telemetry.Telemetry),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
	go h.run()
	return h
}

func (h *WebSocketHandler) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("WebSocket client connected: %v", client.conn.RemoteAddr())
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("WebSocket client disconnected: %v", client.conn.RemoteAddr())

		case t := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				if len(client.deviceIds) > 0 {
					if !client.deviceIds[t.DeviceID] {
						continue
					}
				}

				select {
				case client.send <- t:
				default:
					h.mu.RUnlock()
					h.mu.Lock()
					close(client.send)
					delete(h.clients, client)
					h.mu.Unlock()
					h.mu.RLock()
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *WebSocketHandler) HandleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	deviceIds := make(map[uuid.UUID]bool)
	if deviceIDsStr := c.QueryArray("device_id"); len(deviceIDsStr) > 0 {
		for _, idStr := range deviceIDsStr {
			if deviceID, err := uuid.Parse(idStr); err == nil {
				deviceIds[deviceID] = true
			}
		}
	}

	client := &Client{
		conn:      conn,
		send:      make(chan *telemetry.Telemetry, 256),
		deviceIds: deviceIds,
	}
	h.register <- client

	go client.writePump()
	go client.readPump(h)
}

func (h *WebSocketHandler) Broadcast(t *telemetry.Telemetry) {
	select {
	case h.broadcast <- t:
	default:
		log.Println("Broadcast channel full, dropping telemetry")
	}
}

func (c *Client) readPump(h *WebSocketHandler) {
	defer func() {
		h.unregister <- c
		_ = c.conn.Close()
	}()

	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}

		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err == nil {
			if action, ok := msg["action"].(string); ok {
				switch action {
				case "subscribe":
					if deviceId, ok := msg["device_id"].(string); ok {
						if deviceID, err := uuid.Parse(deviceId); err == nil {
							c.deviceIds[deviceID] = true
							log.Printf("Client subscribed to device: %s", deviceID)
						}
					}
				case "unsubscribe":
					if deviceId, ok := msg["device_id"].(string); ok {
						if deviceID, err := uuid.Parse(deviceId); err == nil {
							delete(c.deviceIds, deviceID)
							log.Printf("Client unsubscribed from device: %s", deviceID)
						}
					}
				}
			}
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case t, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}

			payload, err := json.Marshal(t)
			if err != nil {
				w.Close()
				continue
			}

			w.Write(payload)

			length := len(c.send)
			for i := 0; i < length; i++ {
				_, _ = w.Write([]byte{'\n'})
				t := <-c.send
				data, err := json.Marshal(t)
				if err != nil {
					continue
				}
				_, _ = w.Write(data)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
