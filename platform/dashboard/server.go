// Package dashboard serves the platform's HTTP API and the live incident UI. It
// exposes the current service health, the reconstructed dependency graph, and
// the incident feed — the last of which is streamed to the browser over
// Server-Sent Events so an incident and its AI root cause appear in real time as
// the pipeline produces them.
package dashboard

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ajay-1134/dirp/platform/depgraph"
	"github.com/ajay-1134/dirp/platform/model"
	"github.com/ajay-1134/dirp/platform/store"
)

//go:embed dashboard.html
var dashboardHTML []byte

// Server holds the dependencies the HTTP layer reads from.
type Server struct {
	store store.Store
	hub   *hub
}

// NewServer builds the dashboard server over the given store.
func NewServer(s store.Store) *Server {
	return &Server{store: s, hub: newHub()}
}

// Broadcast pushes an incident update to all connected SSE clients. The
// orchestrator wires this to the incident manager's OnChange hook.
func (s *Server) Broadcast(inc model.Incident) {
	if b, err := json.Marshal(inc); err == nil {
		s.hub.broadcast(b)
	}
}

// Register mounts all routes on the given Gin engine.
func (s *Server) Register(r *gin.Engine) {
	r.GET("/", s.handleIndex)
	r.GET("/api/services", s.handleServices)
	r.GET("/api/graph", s.handleGraph)
	r.GET("/api/incidents", s.handleIncidents)
	r.GET("/api/incidents/:id", s.handleIncident)
	r.GET("/api/stream", s.handleStream)
}

func (s *Server) handleIndex(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", dashboardHTML)
}

// handleServices returns the latest health snapshot per service.
func (s *Server) handleServices(c *gin.Context) {
	out := make([]model.ServiceSnapshot, 0)
	for _, svc := range s.store.Services() {
		if snap, ok := s.store.LatestSnapshot(svc); ok {
			out = append(out, snap)
		}
	}
	c.JSON(http.StatusOK, out)
}

// handleGraph reconstructs the dependency graph from recent spans on demand.
func (s *Server) handleGraph(c *gin.Context) {
	spans := s.store.SpansSince(recentWindowStart())
	g := depgraph.Build(spans)
	c.JSON(http.StatusOK, gin.H{"edges": g.EdgesList()})
}

func (s *Server) handleIncidents(c *gin.Context) {
	c.JSON(http.StatusOK, s.store.Incidents())
}

func (s *Server) handleIncident(c *gin.Context) {
	inc, ok := s.store.Incident(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	c.JSON(http.StatusOK, inc)
}

// handleStream is the SSE endpoint. It registers a client channel and streams
// incident JSON until the client disconnects.
func (s *Server) handleStream(c *gin.Context) {
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")

	ch := s.hub.register()
	defer s.hub.unregister(ch)

	// Flush headers immediately so the browser opens the stream.
	c.Writer.Flush()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			c.Writer.Write([]byte("data: "))
			c.Writer.Write(msg)
			c.Writer.Write([]byte("\n\n"))
			c.Writer.Flush()
		}
	}
}

// hub is a minimal SSE fan-out: it holds a set of client channels and copies
// each broadcast to all of them without blocking on a slow client.
type hub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

// recentWindowStart is the lookback horizon for on-demand graph reconstruction.
func recentWindowStart() time.Time {
	return time.Now().Add(-2 * time.Minute)
}

func newHub() *hub {
	return &hub{clients: make(map[chan []byte]struct{})}
}

func (h *hub) register() chan []byte {
	ch := make(chan []byte, 16)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unregister(ch chan []byte) {
	h.mu.Lock()
	if _, ok := h.clients[ch]; ok {
		delete(h.clients, ch)
		close(ch)
	}
	h.mu.Unlock()
}

func (h *hub) broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
			// Drop for a slow client rather than blocking the pipeline; the
			// client can re-fetch /api/incidents to resync.
		}
	}
}
