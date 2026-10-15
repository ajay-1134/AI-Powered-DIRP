// Package ai turns the structured evidence the platform has already assembled
// into a plain-English root-cause explanation and ranked remediation, using
// Claude. The design point worth calling out: the model does not go hunting for
// data. It reasons over evidence we hand it — the incident's metrics, the
// dependency graph, and the heuristic conclusion — which keeps the analysis
// grounded, cheap, and fast. If no API key is configured, or the call fails, we
// fall back to the deterministic heuristic so the platform always produces an
// answer.
package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/ajay-1134/dirp/platform/depgraph"
	"github.com/ajay-1134/dirp/platform/model"
)

// Analyzer produces AI root-cause analyses. It is safe to use with no API key:
// Enabled() reports whether the Claude path is available.
type Analyzer struct {
	client  *anthropic.Client
	enabled bool
}

// NewAnalyzer builds an analyzer. It looks for ANTHROPIC_API_KEY; when absent,
// the analyzer is disabled and callers should use the heuristic result.
func NewAnalyzer() *Analyzer {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		return &Analyzer{enabled: false}
	}
	c := anthropic.NewClient() // reads ANTHROPIC_API_KEY
	return &Analyzer{client: &c, enabled: true}
}

// Enabled reports whether the Claude-backed path is available.
func (a *Analyzer) Enabled() bool { return a.enabled }

// aiResponse is the JSON shape we ask Claude to return.
type aiResponse struct {
	RootService string   `json:"root_service"`
	Summary     string   `json:"summary"`
	Confidence  float64  `json:"confidence"`
	Remediation []string `json:"remediation"`
}

// Analyze asks Claude to explain the incident given the assembled evidence and
// the heuristic graph analysis, returning a model.RootCause tagged source="ai".
// On any failure it returns the heuristic's RootCause so the caller still gets a
// usable answer.
func (a *Analyzer) Analyze(ctx context.Context, inc model.Incident, ga depgraph.Analysis, edges []model.Edge) model.RootCause {
	fallback := ga.ToRootCause()
	if !a.enabled {
		return fallback
	}

	prompt := buildPrompt(inc, ga, edges)
	msg, err := a.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.ModelClaudeOpus4_8,
		MaxTokens: 1024,
		// Adaptive thinking lets the model reason about the causal chain before
		// answering, which is exactly the kind of multi-step inference this
		// task needs.
		Thinking: anthropic.ThinkingConfigParamUnion{OfAdaptive: &anthropic.ThinkingConfigAdaptiveParam{}},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(prompt)),
		},
	})
	if err != nil {
		return fallback
	}

	parsed, ok := parseResponse(msg)
	if !ok {
		return fallback
	}

	rc := model.RootCause{
		Service:     parsed.RootService,
		Summary:     parsed.Summary,
		Confidence:  parsed.Confidence,
		Remediation: parsed.Remediation,
		Source:      "ai",
	}
	// Guard against an empty or nonsensical model answer.
	if rc.Service == "" {
		rc.Service = fallback.Service
	}
	if rc.Summary == "" {
		rc.Summary = fallback.Summary
	}
	return rc
}

// buildPrompt assembles the evidence into a single instruction. Everything the
// model needs is inlined; it is told explicitly to reason only over what's here.
func buildPrompt(inc model.Incident, ga depgraph.Analysis, edges []model.Edge) string {
	var b strings.Builder
	b.WriteString("You are an expert Site Reliability Engineer performing root-cause analysis on a live incident in a microservices system.\n")
	b.WriteString("Reason ONLY over the evidence provided below — do not invent metrics or services.\n\n")

	b.WriteString(fmt.Sprintf("INCIDENT %s (severity %s)\n", inc.ID, inc.Severity))
	b.WriteString(fmt.Sprintf("User-visible symptom appeared on: %s\n\n", inc.AffectedService))

	b.WriteString("SERVICE DEPENDENCY GRAPH (caller -> callee, with call/error counts and p99):\n")
	for _, e := range edges {
		b.WriteString(fmt.Sprintf("  %s -> %s | calls=%d errors=%d p99=%.0fms\n",
			e.From, e.To, e.CallCount, e.ErrorCount, e.P99Latency*1000))
	}
	b.WriteString("\n")

	b.WriteString("ANOMALY EVIDENCE:\n")
	for _, ev := range inc.Evidence {
		b.WriteString(fmt.Sprintf("  [%s] %s: %s\n", ev.Service, ev.Kind, ev.Detail))
	}
	b.WriteString("\n")

	b.WriteString("HEURISTIC GRAPH ANALYSIS (a deterministic first guess you may agree with or correct):\n")
	b.WriteString(fmt.Sprintf("  Suspected origin: %s\n", ga.RootService))
	b.WriteString(fmt.Sprintf("  Failure path (symptom -> cause): %s\n", strings.Join(ga.SymptomPath, " -> ")))
	b.WriteString(fmt.Sprintf("  Reasoning: %s\n\n", ga.Reason))

	b.WriteString("Respond with ONLY a JSON object (no markdown, no prose) of this exact shape:\n")
	b.WriteString(`{"root_service": "<service name>", "summary": "<one or two sentence explanation of the true root cause and how it propagated>", "confidence": <0.0-1.0>, "remediation": ["<action 1>", "<action 2>", "<action 3>"]}`)
	b.WriteString("\n")
	return b.String()
}

// parseResponse extracts the JSON object from Claude's text output. It tolerates
// the model wrapping the JSON in prose or code fences by scanning for the outer
// braces.
func parseResponse(msg *anthropic.Message) (aiResponse, bool) {
	var text strings.Builder
	for _, block := range msg.Content {
		if b, ok := block.AsAny().(anthropic.TextBlock); ok {
			text.WriteString(b.Text)
		}
	}
	raw := text.String()
	start := strings.IndexByte(raw, '{')
	end := strings.LastIndexByte(raw, '}')
	if start < 0 || end <= start {
		return aiResponse{}, false
	}
	var out aiResponse
	if err := json.Unmarshal([]byte(raw[start:end+1]), &out); err != nil {
		return aiResponse{}, false
	}
	return out, true
}
