package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Client is a small HTTP client for service-to-service calls. Its transport is
// wrapped with otelhttp so every outbound request injects the current span's
// W3C trace context (traceparent header) — this is what stitches User → Order →
// Payment → Ledger → Notification into a single trace the platform can walk.
type Client struct {
	name string // downstream service name, for error messages
	base string // base URL, e.g. "http://payment-service:8083"
	http *http.Client
}

// NewClient builds a downstream client for the named service at base URL.
func NewClient(name, base string) *Client {
	return &Client{
		name: name,
		base: base,
		http: &http.Client{
			// The timeout is intentionally short: when the Payment service
			// stalls on an exhausted DB pool, callers should time out and
			// surface errors quickly. That fast failure is what makes the
			// error-rate spike observable in the demo.
			Timeout:   3 * time.Second,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
}

// GetJSON issues a GET to path and decodes a JSON response into out. A non-2xx
// status is returned as an error so callers propagate downstream failures.
func (c *Client) GetJSON(ctx context.Context, path string, out any) error {
	return c.doJSON(ctx, http.MethodGet, path, nil, out)
}

// PostJSON issues a POST with a JSON body and decodes the JSON response.
func (c *Client) PostJSON(ctx context.Context, path string, body, out any) error {
	return c.doJSON(ctx, http.MethodPost, path, body, out)
}

func (c *Client) doJSON(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request to %s: %w", c.name, err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return fmt.Errorf("build request to %s: %w", c.name, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("call %s%s: %w", c.name, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s%s returned %d: %s", c.name, path, resp.StatusCode, msg)
	}

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode response from %s: %w", c.name, err)
		}
	}
	return nil
}
