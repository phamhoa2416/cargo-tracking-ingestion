package handler

import (
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// TODO: restrict origin in production
		return true
	},
}

type WebSocketHub struct {
	clients    map[*Client]struct{}
	broadcast  chan *telemetry.Telemetry
	register   chan *Client
	unregister chan *Client

	maxConnections int
	mu             sync.RWMutex
}

type Client struct {
	conn *websocket.Conn
	send chan *telemetry.Telemetry
}

func NewWebSocketHandler() *WebSocketHub {
	h := &WebSocketHub{
		clients:        make(map[*Client]struct{}),
		register:       make(chan *Client),
		unregister:     make(chan *Client),
		broadcast:      make(chan *telemetry.Telemetry, 1024),
		maxConnections: 1000,
	}

	go h.run()
	return h
}

func (h *WebSocketHub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()

			if len(h.clients) >= h.maxConnections {
				h.mu.Unlock()
				_ = client.conn.Close()
				log.Println("WebSocket connection rejected: limit reached")
				continue
			}
			h.clients[client] = struct{}{}
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				_ = client.conn.Close()
			}
			h.mu.Unlock()
			log.Printf("WebSocket client disconnected: %v", client.conn.RemoteAddr())

		case t := <-h.broadcast:
			h.mu.RLock()
			clientsToUnregister := make([]*Client, 0)
			for client := range h.clients {
				select {
				case client.send <- t:
				default:
					clientsToUnregister = append(clientsToUnregister, client)
				}
			}
			h.mu.RUnlock()

			for _, client := range clientsToUnregister {
				select {
				case h.unregister <- client:
				default:
					log.Printf("Unregister channel full, dropping client")
				}
			}
		}
	}
}

func (h *WebSocketHub) HandleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}

	client := &Client{
		conn: conn,
		send: make(chan *telemetry.Telemetry, 256),
	}

	h.register <- client

	go client.readPump(h)
	go client.writePump(h)
}

func (h *WebSocketHub) Broadcast(t *telemetry.Telemetry) {
	select {
	case h.broadcast <- t:
	default:
		log.Println("WebSocket broadcast dropped (buffer full)")
	}
}

func (c *Client) readPump(h *WebSocketHub) {
	defer func() {
		h.unregister <- c
	}()

	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket read error: %v", err)
			}
			break
		}
		// Just keep connection alive, no message processing needed
	}
}

func (c *Client) writePump(h *WebSocketHub) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case t, ok := <-c.send:
			if !ok {
				return
			}

			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteJSON(t); err != nil {
				h.unregister <- c
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				h.unregister <- c
				return
			}
		}
	}
}
