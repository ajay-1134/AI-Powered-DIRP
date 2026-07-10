package ingest

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/ajay-1134/dirp/platform/model"
	"github.com/ajay-1134/dirp/platform/store"
)

// OTLPReceiver is an embedded OpenTelemetry trace receiver. The sample services
// export their spans here over gRPC (OTEL_EXPORTER_OTLP protocol); we translate
// each span into a model.Span and persist it. Those spans are the raw material
// for the dependency graph — parent/child span relationships across services
// are precisely the "who calls whom" edges root-cause analysis walks.
//
// Embedding the receiver (rather than running a standalone collector) keeps the
// demo to a single platform binary while remaining genuine OTLP on the wire.
type OTLPReceiver struct {
	coltracepb.UnimplementedTraceServiceServer
	store store.Store
	grpc  *grpc.Server
	addr  string
}

// NewOTLPReceiver builds a receiver that writes spans to the store and listens
// on addr (e.g. ":4317").
func NewOTLPReceiver(addr string, s store.Store) *OTLPReceiver {
	return &OTLPReceiver{store: s, addr: addr}
}

// Run starts the gRPC server and blocks until ctx is cancelled.
func (r *OTLPReceiver) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", r.addr)
	if err != nil {
		return fmt.Errorf("otlp listen %s: %w", r.addr, err)
	}
	r.grpc = grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(r.grpc, r)

	go func() {
		<-ctx.Done()
		r.grpc.GracefulStop()
	}()
	return r.grpc.Serve(lis)
}

// Export implements the OTLP TraceService. It flattens the nested
// Resource/Scope/Span structure into individual model.Span records.
func (r *OTLPReceiver) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	for _, rs := range req.GetResourceSpans() {
		service := serviceName(rs.GetResource().GetAttributes())
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				r.store.AddSpan(toModelSpan(service, sp))
			}
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func toModelSpan(service string, sp *tracepb.Span) model.Span {
	start := time.Unix(0, int64(sp.GetStartTimeUnixNano()))
	end := time.Unix(0, int64(sp.GetEndTimeUnixNano()))
	return model.Span{
		TraceID:      hex.EncodeToString(sp.GetTraceId()),
		SpanID:       hex.EncodeToString(sp.GetSpanId()),
		ParentSpanID: hex.EncodeToString(sp.GetParentSpanId()),
		Service:      service,
		Name:         sp.GetName(),
		Start:        start,
		Duration:     end.Sub(start),
		Error:        sp.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR,
	}
}

// serviceName pulls the OTel "service.name" resource attribute, which the
// telemetry package sets to each service's logical name.
func serviceName(attrs []*commonpb.KeyValue) string {
	for _, kv := range attrs {
		if kv.GetKey() == "service.name" {
			return kv.GetValue().GetStringValue()
		}
	}
	return "unknown"
}
