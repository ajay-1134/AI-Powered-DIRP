package chaos

import (
	"context"
	"sync"
	"time"
)

// Controller injects and clears the DB pool exhaustion fault against a Pool. It
// works by leasing connections and holding them, leaving too few (or none) for
// real request traffic — reproducing the exact cascade in the demo: Payment
// stalls acquiring a connection, upstream Order calls time out, and the error
// rate climbs.
type Controller struct {
	pool *Pool

	mu       sync.Mutex
	active   bool
	held     []*Conn
	cancelFn context.CancelFunc
	until    time.Time
}

// NewController builds a Controller bound to the given pool.
func NewController(pool *Pool) *Controller {
	return &Controller{pool: pool}
}

// State describes the current fault-injection status, surfaced by the chaos
// endpoint and recorded as incident evidence.
type State struct {
	Active    bool      `json:"active"`
	HeldConns int       `json:"held_conns"`
	Until     time.Time `json:"until,omitempty"`
}

// Exhaust holds `holders` connections for `duration`, then automatically
// releases them. Calling it again while active tops up to the new holder count
// and extends the deadline. Any acquisition that itself has to wait is skipped
// so injection never deadlocks against real traffic already queued.
func (ctl *Controller) Exhaust(holders int, duration time.Duration) State {
	ctl.mu.Lock()
	defer ctl.mu.Unlock()

	if holders < 1 {
		holders = ctl.pool.max
	}
	// Cancel any prior auto-release timer; we'll set a fresh one.
	if ctl.cancelFn != nil {
		ctl.cancelFn()
		ctl.cancelFn = nil
	}

	// Grab connections without blocking: use a already-cancelled-on-timeout
	// context so we only take what's freely available and never wedge.
	for len(ctl.held) < holders {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		conn, err := ctl.pool.Acquire(ctx)
		cancel()
		if err != nil {
			break // pool already saturated; hold what we have
		}
		ctl.held = append(ctl.held, conn)
	}

	ctl.active = true
	ctl.until = time.Now().Add(duration)

	// Schedule automatic recovery so the demo self-heals after `duration`.
	ctx, cancel := context.WithCancel(context.Background())
	ctl.cancelFn = cancel
	go func() {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-timer.C:
			ctl.Reset()
		case <-ctx.Done():
		}
	}()

	return ctl.stateLocked()
}

// Reset releases all held connections and clears the fault.
func (ctl *Controller) Reset() State {
	ctl.mu.Lock()
	defer ctl.mu.Unlock()

	if ctl.cancelFn != nil {
		ctl.cancelFn()
		ctl.cancelFn = nil
	}
	for _, c := range ctl.held {
		c.Release()
	}
	ctl.held = nil
	ctl.active = false
	ctl.until = time.Time{}
	return ctl.stateLocked()
}

// Status returns the current injection state.
func (ctl *Controller) Status() State {
	ctl.mu.Lock()
	defer ctl.mu.Unlock()
	return ctl.stateLocked()
}

func (ctl *Controller) stateLocked() State {
	return State{
		Active:    ctl.active,
		HeldConns: len(ctl.held),
		Until:     ctl.until,
	}
}
