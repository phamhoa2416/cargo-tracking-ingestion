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
	clients           map[*Client]bool
	broadcast         chan *telemetry.Telemetry
	register          chan *Client
	unregister        chan *Client
	mu                sync.RWMutex
	maxConnections    int
	connectionTimeout time.Duration
}

type Client struct {
	conn         *websocket.Conn
	send         chan *telemetry.Telemetry
	deviceIds    map[uuid.UUID]bool
	connectedAt  time.Time
	lastActivity time.Time
}

func NewWebSocketHandler() *WebSocketHandler {
	h := &WebSocketHandler{
		clients:           make(map[*Client]bool),
		broadcast:         make(chan *telemetry.Telemetry),
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		maxConnections:    1000,          // Max 1000 concurrent connections
		connectionTimeout: 1 * time.Hour, // 1 hour timeout
	}
	go h.run()
	go h.cleanupInactiveConnections()
	return h
}

func (h *WebSocketHandler) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			// Check connection limit
			if len(h.clients) >= h.maxConnections {
				h.mu.Unlock()
				log.Printf("WebSocket connection limit reached, rejecting: %v", client.conn.RemoteAddr())
				client.conn.Close()
				continue
			}
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("WebSocket client connected: %v (total: %d)", client.conn.RemoteAddr(), len(h.clients))
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

	now := time.Now()
	client := &Client{
		conn:         conn,
		send:         make(chan *telemetry.Telemetry, 256),
		deviceIds:    deviceIds,
		connectedAt:  now,
		lastActivity: now,
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
		c.lastActivity = time.Now()
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

		c.lastActivity = time.Now()

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

			c.lastActivity = time.Now()
			w.Write(payload)

			length := len(c.send)
			for i := 0; i < length; i++ {
				_, _ = w.Write([]byte{'\n'})
				t := <-c.send
				c.lastActivity = time.Now()
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
			c.lastActivity = time.Now()
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// cleanupInactiveConnections periodically closes connections that have been inactive
func (h *WebSocketHandler) cleanupInactiveConnections() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		h.mu.Lock()

		clientsToClose := make([]*Client, 0)
		for client := range h.clients {
			// Close if inactive for too long or total connection time exceeded
			if now.Sub(client.lastActivity) > h.connectionTimeout ||
				now.Sub(client.connectedAt) > h.connectionTimeout*2 {
				clientsToClose = append(clientsToClose, client)
			}
		}

		for _, client := range clientsToClose {
			delete(h.clients, client)
			close(client.send)
			client.conn.Close()
			log.Printf("Closed inactive WebSocket connection: %v", client.conn.RemoteAddr())
		}

		h.mu.Unlock()
	}
}
