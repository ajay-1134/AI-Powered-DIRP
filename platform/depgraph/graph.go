// Package depgraph reconstructs the service dependency graph from trace spans
// and uses it to turn a symptom into a root cause. The graph's edges come from
// parent/child span relationships that cross a service boundary: if a span in
// service A is the parent of a span in service B, then A calls B. Walking this
// graph is what lets the platform say the Order error spike was *caused by*
// Payment, rather than blaming Order itself.
package depgraph

import (
	"sort"

	"github.com/ajay-1134/dirp/platform/model"
)

// Graph is a directed service dependency graph aggregated over a set of spans.
type Graph struct {
	Nodes map[string]bool        // service -> present
	Edges map[string]*model.Edge // "from|to" -> aggregated edge
	out   map[string][]string    // service -> downstream services (callees)
}

// Build constructs the graph from spans. Spans are indexed by span id so each
// span can find its parent and, if the parent lives in a different service,
// record the cross-service call as an edge.
func Build(spans []model.Span) *Graph {
	g := &Graph{
		Nodes: map[string]bool{},
		Edges: map[string]*model.Edge{},
		out:   map[string][]string{},
	}
	index := make(map[string]model.Span, len(spans))
	for _, sp := range spans {
		index[sp.SpanID] = sp
		g.Nodes[sp.Service] = true
	}

	// Collect per-edge latencies to compute a p99 per edge.
	edgeLatencies := map[string][]float64{}
	for _, sp := range spans {
		if isEmptyID(sp.ParentSpanID) {
			continue
		}
		parent, ok := index[sp.ParentSpanID]
		if !ok || parent.Service == sp.Service {
			continue // same-service internal span, or parent not captured
		}
		key := parent.Service + "|" + sp.Service
		e, ok := g.Edges[key]
		if !ok {
			e = &model.Edge{From: parent.Service, To: sp.Service}
			g.Edges[key] = e
			g.out[parent.Service] = append(g.out[parent.Service], sp.Service)
		}
		e.CallCount++
		if sp.Error {
			e.ErrorCount++
		}
		edgeLatencies[key] = append(edgeLatencies[key], sp.Duration.Seconds())
	}

	for key, lats := range edgeLatencies {
		g.Edges[key].P99Latency = p99(lats)
	}
	return g
}

// Downstream returns the services directly called by the given service.
func (g *Graph) Downstream(service string) []string {
	ds := g.out[service]
	out := make([]string, len(ds))
	copy(out, ds)
	sort.Strings(out)
	return out
}

// EdgesList returns all edges in a stable order for display.
func (g *Graph) EdgesList() []model.Edge {
	out := make([]model.Edge, 0, len(g.Edges))
	for _, e := range g.Edges {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}

func p99(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	idx := int(0.99 * float64(len(s)-1))
	return s[idx]
}

// isEmptyID reports whether a hex span id is absent or all-zero (an OTLP root
// span encodes its parent as empty or zero bytes).
func isEmptyID(id string) bool {
	if id == "" {
		return true
	}
	for _, c := range id {
		if c != '0' {
			return false
		}
	}
	return true
}
