package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ajay-1134/dirp/platform/model"
)

// Timescale is a TimescaleDB/Postgres-backed Store. It satisfies the same
// interface as Memory, so selecting it is a one-line swap at startup driven by
// the STORE_DSN environment variable — nothing in ingestion, detection, or the
// API changes. Snapshots and spans are stored in hypertables (when the
// TimescaleDB extension is available) for efficient time-range queries;
// incidents are stored as JSONB keyed by id.
//
// The in-memory store remains the default and the runtime-verified path; this
// implementation is exercised via docker-compose, where a TimescaleDB container
// is provisioned.
type Timescale struct {
	pool *pgxpool.Pool
}

// NewTimescale connects to the database at dsn and ensures the schema exists.
func NewTimescale(ctx context.Context, dsn string) (*Timescale, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect timescale: %w", err)
	}
	ts := &Timescale{pool: pool}
	if err := ts.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return ts, nil
}

// Close releases the connection pool.
func (t *Timescale) Close() { t.pool.Close() }

func (t *Timescale) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS snapshots (
			service       TEXT        NOT NULL,
			ts            TIMESTAMPTZ NOT NULL,
			request_rate  DOUBLE PRECISION NOT NULL,
			error_rate    DOUBLE PRECISION NOT NULL,
			p99_latency   DOUBLE PRECISION NOT NULL,
			db_in_use     INTEGER     NOT NULL,
			db_max        INTEGER     NOT NULL,
			db_wait_total BIGINT      NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS snapshots_service_ts ON snapshots (service, ts DESC)`,
		`CREATE TABLE IF NOT EXISTS spans (
			trace_id       TEXT        NOT NULL,
			span_id        TEXT        NOT NULL,
			parent_span_id TEXT        NOT NULL,
			service        TEXT        NOT NULL,
			name           TEXT        NOT NULL,
			start_ts       TIMESTAMPTZ NOT NULL,
			duration_ns    BIGINT      NOT NULL,
			error          BOOLEAN     NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS spans_start ON spans (start_ts DESC)`,
		`CREATE TABLE IF NOT EXISTS incidents (
			id        TEXT PRIMARY KEY,
			opened_at TIMESTAMPTZ NOT NULL,
			data      JSONB       NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := t.pool.Exec(ctx, s); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	// Convert the time-series tables to hypertables when TimescaleDB is present.
	// Best-effort: on plain Postgres these fail harmlessly and we ignore them.
	_, _ = t.pool.Exec(ctx, `SELECT create_hypertable('snapshots','ts',if_not_exists=>TRUE)`)
	_, _ = t.pool.Exec(ctx, `SELECT create_hypertable('spans','start_ts',if_not_exists=>TRUE)`)
	return nil
}

func bg() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}

func (t *Timescale) AddSnapshot(s model.ServiceSnapshot) {
	ctx, cancel := bg()
	defer cancel()
	_, _ = t.pool.Exec(ctx,
		`INSERT INTO snapshots (service,ts,request_rate,error_rate,p99_latency,db_in_use,db_max,db_wait_total)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		s.Service, s.Timestamp, s.RequestRate, s.ErrorRate, s.P99Latency, s.DBInUse, s.DBMax, s.DBWaitTotal)
}

func (t *Timescale) SnapshotsSince(service string, since time.Time) []model.ServiceSnapshot {
	ctx, cancel := bg()
	defer cancel()
	rows, err := t.pool.Query(ctx,
		`SELECT service,ts,request_rate,error_rate,p99_latency,db_in_use,db_max,db_wait_total
		 FROM snapshots WHERE service=$1 AND ts>=$2 ORDER BY ts ASC`, service, since)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []model.ServiceSnapshot
	for rows.Next() {
		var s model.ServiceSnapshot
		if err := rows.Scan(&s.Service, &s.Timestamp, &s.RequestRate, &s.ErrorRate,
			&s.P99Latency, &s.DBInUse, &s.DBMax, &s.DBWaitTotal); err == nil {
			out = append(out, s)
		}
	}
	return out
}

func (t *Timescale) LatestSnapshot(service string) (model.ServiceSnapshot, bool) {
	ctx, cancel := bg()
	defer cancel()
	var s model.ServiceSnapshot
	err := t.pool.QueryRow(ctx,
		`SELECT service,ts,request_rate,error_rate,p99_latency,db_in_use,db_max,db_wait_total
		 FROM snapshots WHERE service=$1 ORDER BY ts DESC LIMIT 1`, service).
		Scan(&s.Service, &s.Timestamp, &s.RequestRate, &s.ErrorRate,
			&s.P99Latency, &s.DBInUse, &s.DBMax, &s.DBWaitTotal)
	if err != nil {
		return model.ServiceSnapshot{}, false
	}
	return s, true
}

func (t *Timescale) Services() []string {
	ctx, cancel := bg()
	defer cancel()
	rows, err := t.pool.Query(ctx, `SELECT DISTINCT service FROM snapshots ORDER BY service`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err == nil {
			out = append(out, s)
		}
	}
	return out
}

func (t *Timescale) AddSpan(sp model.Span) {
	ctx, cancel := bg()
	defer cancel()
	_, _ = t.pool.Exec(ctx,
		`INSERT INTO spans (trace_id,span_id,parent_span_id,service,name,start_ts,duration_ns,error)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		sp.TraceID, sp.SpanID, sp.ParentSpanID, sp.Service, sp.Name, sp.Start, sp.Duration.Nanoseconds(), sp.Error)
}

func (t *Timescale) SpansSince(since time.Time) []model.Span {
	ctx, cancel := bg()
	defer cancel()
	rows, err := t.pool.Query(ctx,
		`SELECT trace_id,span_id,parent_span_id,service,name,start_ts,duration_ns,error
		 FROM spans WHERE start_ts>=$1`, since)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []model.Span
	for rows.Next() {
		var sp model.Span
		var durNs int64
		if err := rows.Scan(&sp.TraceID, &sp.SpanID, &sp.ParentSpanID, &sp.Service,
			&sp.Name, &sp.Start, &durNs, &sp.Error); err == nil {
			sp.Duration = time.Duration(durNs)
			out = append(out, sp)
		}
	}
	return out
}

func (t *Timescale) SaveIncident(inc model.Incident) {
	ctx, cancel := bg()
	defer cancel()
	data, err := json.Marshal(inc)
	if err != nil {
		return
	}
	_, _ = t.pool.Exec(ctx,
		`INSERT INTO incidents (id,opened_at,data) VALUES ($1,$2,$3)
		 ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data`,
		inc.ID, inc.OpenedAt, data)
}

func (t *Timescale) Incident(id string) (model.Incident, bool) {
	ctx, cancel := bg()
	defer cancel()
	var data []byte
	if err := t.pool.QueryRow(ctx, `SELECT data FROM incidents WHERE id=$1`, id).Scan(&data); err != nil {
		return model.Incident{}, false
	}
	var inc model.Incident
	if err := json.Unmarshal(data, &inc); err != nil {
		return model.Incident{}, false
	}
	return inc, true
}

func (t *Timescale) Incidents() []model.Incident {
	ctx, cancel := bg()
	defer cancel()
	rows, err := t.pool.Query(ctx, `SELECT data FROM incidents ORDER BY opened_at DESC`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []model.Incident
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			continue
		}
		var inc model.Incident
		if err := json.Unmarshal(data, &inc); err == nil {
			out = append(out, inc)
		}
	}
	return out
}

// compile-time check
var _ Store = (*Timescale)(nil)
