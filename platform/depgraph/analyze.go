package depgraph

import "github.com/ajay-1134/dirp/platform/model"

// Analysis is the graph-based root-cause conclusion: the origin service, the
// causal path from the user-visible symptom down to it, and the reasoning. This
// heuristic result is also what we hand to the AI layer as structured evidence.
type Analysis struct {
	RootService string   // service identified as the origin of the failure
	SymptomPath []string // symptom -> ... -> root, following dependency edges
	Reason      string   // why this service was chosen
	Confidence  float64  // 0..1 heuristic confidence
	Remediation []string // ranked suggested actions
}

// Signals summarizes, per service, what the anomaly detector flagged. It's the
// bridge between detection output and graph analysis.
type Signals struct {
	Anomalous   map[string]bool // service -> had any anomaly this incident
	DBSaturated map[string]bool // service -> DB pool saturation observed
	ErrorRate   map[string]float64
}

// FindRootCause walks the dependency graph to locate the origin of a cascade.
//
// The core rule: in a downstream-propagating failure, the true cause is the
// deepest anomalous service — one that is unhealthy but whose own dependencies
// are healthy. An anomalous service with an anomalous callee is just relaying a
// failure from below, so it can't be the origin. DB pool saturation is treated
// as a decisive origin marker because it is a local resource exhaustion, not an
// inherited symptom.
func (g *Graph) FindRootCause(symptom string, sig Signals) Analysis {
	// Candidates: anomalous services with no anomalous downstream dependency.
	var candidates []string
	for svc := range sig.Anomalous {
		if !g.Nodes[svc] {
			continue
		}
		hasAnomalousCallee := false
		for _, callee := range g.Downstream(svc) {
			if sig.Anomalous[callee] {
				hasAnomalousCallee = true
				break
			}
		}
		if !hasAnomalousCallee {
			candidates = append(candidates, svc)
		}
	}

	// Prefer a candidate exhibiting DB pool saturation — the strongest signal.
	root := ""
	for _, c := range candidates {
		if sig.DBSaturated[c] {
			root = c
			break
		}
	}
	if root == "" && len(candidates) > 0 {
		// Otherwise take the deepest candidate reachable from the symptom.
		root = deepestFrom(g, symptom, candidates, sig.Anomalous)
	}
	if root == "" {
		// No downstream cause found: the symptom service is itself the origin.
		root = symptom
	}

	path := pathBetween(g, symptom, root, sig.Anomalous)
	reason, conf := explain(root, symptom, sig)
	return Analysis{
		RootService: root,
		SymptomPath: path,
		Reason:      reason,
		Confidence:  conf,
		Remediation: remediationFor(root, sig),
	}
}

// remediationFor returns ranked, actionable steps for the identified cause. When
// DB pool saturation is the marker, the advice is specific to that failure mode;
// otherwise it's generic downstream-failure guidance.
func remediationFor(root string, sig Signals) []string {
	if sig.DBSaturated[root] {
		return []string{
			"Increase the DB connection pool capacity on " + root + " (raise SetMaxOpenConns).",
			"Inspect for long-running or stuck queries holding connections.",
			"Enable a circuit breaker on callers of " + root + " to fail fast instead of queuing.",
		}
	}
	return []string{
		"Inspect " + root + " for elevated latency or errors at its own dependencies.",
		"Add a timeout and circuit breaker on callers of " + root + ".",
		"Scale " + root + " or shed load until it recovers.",
	}
}

// deepestFrom returns the candidate at the greatest dependency depth from the
// symptom, restricting traversal to anomalous services so we follow the actual
// failure chain rather than unrelated branches.
func deepestFrom(g *Graph, symptom string, candidates []string, anomalous map[string]bool) string {
	candSet := map[string]bool{}
	for _, c := range candidates {
		candSet[c] = true
	}
	best, bestDepth := "", -1
	visited := map[string]bool{}
	type item struct {
		svc   string
		depth int
	}
	queue := []item{{symptom, 0}}
	visited[symptom] = true
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if candSet[cur.svc] && cur.depth > bestDepth {
			best, bestDepth = cur.svc, cur.depth
		}
		for _, next := range g.Downstream(cur.svc) {
			if visited[next] || !anomalous[next] {
				continue
			}
			visited[next] = true
			queue = append(queue, item{next, cur.depth + 1})
		}
	}
	if best == "" && len(candidates) > 0 {
		return candidates[0]
	}
	return best
}

// pathBetween finds a path from -> to through anomalous services via BFS.
func pathBetween(g *Graph, from, to string, anomalous map[string]bool) []string {
	if from == to {
		return []string{from}
	}
	prev := map[string]string{}
	visited := map[string]bool{from: true}
	queue := []string{from}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range g.Downstream(cur) {
			if visited[next] {
				continue
			}
			// Allow the destination even if not flagged anomalous; require
			// intermediate hops to be anomalous so we trace the real chain.
			if next != to && !anomalous[next] {
				continue
			}
			visited[next] = true
			prev[next] = cur
			if next == to {
				return reconstruct(prev, from, to)
			}
			queue = append(queue, next)
		}
	}
	// No connected path in the captured window: report endpoints only.
	return []string{from, to}
}

func reconstruct(prev map[string]string, from, to string) []string {
	var rev []string
	for cur := to; cur != ""; cur = prev[cur] {
		rev = append(rev, cur)
		if cur == from {
			break
		}
	}
	// reverse
	out := make([]string, len(rev))
	for i, v := range rev {
		out[len(rev)-1-i] = v
	}
	return out
}

func explain(root, symptom string, sig Signals) (string, float64) {
	switch {
	case sig.DBSaturated[root]:
		return "DB connection pool exhaustion at " + root +
			" is saturating downstream capacity, causing upstream timeouts.", 0.9
	case root != symptom:
		return root + " is the deepest unhealthy service with healthy dependencies, " +
			"so the failure originates there and propagates upstream to " + symptom + ".", 0.7
	default:
		return symptom + " shows anomalies with no unhealthy dependency, " +
			"so it appears to be the origin.", 0.5
	}
}

// ToRootCause converts a heuristic Analysis into a model.RootCause, giving the
// platform a usable conclusion even before (or without) the AI layer.
func (a Analysis) ToRootCause() model.RootCause {
	return model.RootCause{
		Service:     a.RootService,
		Summary:     a.Reason,
		Confidence:  a.Confidence,
		Remediation: a.Remediation,
		Source:      "heuristic",
	}
}
