// Package chaos provides a connection-pool model and a controller for injecting
// the demo's signature failure: database connection-pool exhaustion in the
// Payment service.
//
// The Pool here mirrors the semantics of database/sql's pool (a fixed maximum
// number of open connections, callers block when all are in use, and stats
// expose in-use/max/wait counts) but is self-contained so the demo runs with
// no external database. In a production build this type would wrap a real
// *sql.DB and read Stats() from it; the exhaustion mechanics — and every metric
// the platform observes — are identical either way.
package chaos

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Conn is a leased connection. Callers must Release it when done.
type Conn struct {
	pool     *Pool
	released atomic.Bool
}

// Release returns the connection to the pool. Safe to call once; extra calls
// are no-ops.
func (c *Conn) Release() {
	if c.released.CompareAndSwap(false, true) {
		c.pool.tokens <- struct{}{}
		c.pool.inUse.Add(-1)
	}
}

// Pool is a bounded connection pool. tokens is a buffered channel acting as the
// set of available connections: acquiring receives a token, releasing sends one
// back. This gives us exact, race-free control over utilization for the demo.
type Pool struct {
	tokens    chan struct{}
	max       int
	inUse     atomic.Int64
	waitCount atomic.Int64
	waitTime  atomic.Int64 // cumulative nanoseconds spent waiting

	mu     sync.Mutex
	closed bool
}

// NewPool creates a pool with the given maximum number of connections.
func NewPool(max int) *Pool {
	if max < 1 {
		max = 1
	}
	p := &Pool{
		tokens: make(chan struct{}, max),
		max:    max,
	}
	for i := 0; i < max; i++ {
		p.tokens <- struct{}{}
	}
	return p
}

// Acquire leases a connection, blocking until one is free or ctx is done. When
// the pool is exhausted (the chaos scenario), callers pile up here and their
// latency climbs — which is the mechanism behind the observed p99 spike and the
// resulting upstream timeouts.
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	// Fast path: a connection is immediately available.
	select {
	case <-p.tokens:
		p.inUse.Add(1)
		return &Conn{pool: p}, nil
	default:
	}

	// Slow path: record that we had to wait, then block.
	p.waitCount.Add(1)
	start := time.Now()
	select {
	case <-p.tokens:
		p.waitTime.Add(int64(time.Since(start)))
		p.inUse.Add(1)
		return &Conn{pool: p}, nil
	case <-ctx.Done():
		p.waitTime.Add(int64(time.Since(start)))
		return nil, ctx.Err()
	}
}

// Stats is a point-in-time snapshot of pool utilization, shaped like the subset
// of sql.DBStats the platform cares about.
type Stats struct {
	InUse        int
	Max          int
	WaitCount    int64
	WaitDuration time.Duration
}

// Stats returns the current pool statistics.
func (p *Pool) Stats() Stats {
	return Stats{
		InUse:        int(p.inUse.Load()),
		Max:          p.max,
		WaitCount:    p.waitCount.Load(),
		WaitDuration: time.Duration(p.waitTime.Load()),
	}
}
