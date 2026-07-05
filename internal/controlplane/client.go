package controlplane

import (
	"context"
	"fmt"
	"strings"
	"time"

	mdv1 "github.com/hushine-tech/control-panel-service/gen/marketdatav1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client interface {
	ListStreams(ctx context.Context) ([]Stream, error)
	ListHistoricalRequests(ctx context.Context, includeTerminal bool) ([]HistoricalRequest, error)
	ReportState(ctx context.Context, report StreamStateReport) error
	ReportHistoricalState(ctx context.Context, report HistoricalRequestStateReport) error
	ReportCoverageSegments(ctx context.Context, segments []CoverageSegmentReport) error
	Close() error
}

type grpcClient struct {
	conn    *grpc.ClientConn
	client  mdv1.MarketDataControlPlaneServiceClient
	timeout time.Duration
}

func NewGRPCClient(addr string, timeout time.Duration) (Client, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, fmt.Errorf("control-plane grpc target is required")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial control-plane grpc target %q: %w", addr, err)
	}

	return &grpcClient{
		conn:    conn,
		client:  mdv1.NewMarketDataControlPlaneServiceClient(conn),
		timeout: timeout,
	}, nil
}

func (c *grpcClient) ListStreams(ctx context.Context) ([]Stream, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.client.ListMarketDataStreams(ctx, &mdv1.ListMarketDataStreamsRequest{})
	if err != nil {
		return nil, fmt.Errorf("list market-data streams: %w", err)
	}

	out := make([]Stream, 0, len(resp.GetStreams()))
	for _, stream := range resp.GetStreams() {
		out = append(out, fromProtoStream(stream))
	}
	return out, nil
}

func (c *grpcClient) ListHistoricalRequests(ctx context.Context, includeTerminal bool) ([]HistoricalRequest, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.client.ListMarketDataHistoryRequests(ctx, &mdv1.ListMarketDataHistoryRequestsRequest{
		IncludeTerminal: includeTerminal,
	})
	if err != nil {
		return nil, fmt.Errorf("list market-data history requests: %w", err)
	}

	out := make([]HistoricalRequest, 0, len(resp.GetRequests()))
	for _, req := range resp.GetRequests() {
		out = append(out, fromProtoHistoricalRequest(req))
	}
	return out, nil
}

func (c *grpcClient) ReportState(ctx context.Context, report StreamStateReport) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	req := &mdv1.ReportMarketDataStreamStateRequest{
		StreamId:    report.StreamID,
		ActualState: report.ActualState,
		LastError:   report.LastError,
	}
	if report.LastDataAt != nil {
		req.LastDataAt = timestamppb.New(report.LastDataAt.UTC())
	}

	if _, err := c.client.ReportMarketDataStreamState(ctx, req); err != nil {
		return fmt.Errorf("report stream %d state %q: %w", report.StreamID, report.ActualState, err)
	}
	return nil
}

func (c *grpcClient) ReportHistoricalState(ctx context.Context, report HistoricalRequestStateReport) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	req := &mdv1.ReportMarketDataHistoryRequestStateRequest{
		RequestId: report.RequestID,
		Status:    report.Status,
		LastError: report.LastError,
	}
	if report.CoveredStartAt != nil {
		req.CoveredStartAt = timestamppb.New(report.CoveredStartAt.UTC())
	}
	if report.CoveredEndAt != nil {
		req.CoveredEndAt = timestamppb.New(report.CoveredEndAt.UTC())
	}
	if _, err := c.client.ReportMarketDataHistoryRequestState(ctx, req); err != nil {
		return fmt.Errorf("report history request %d state %q: %w", report.RequestID, report.Status, err)
	}
	return nil
}

func (c *grpcClient) ReportCoverageSegments(ctx context.Context, reports []CoverageSegmentReport) error {
	if len(reports) == 0 {
		return nil
	}
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	segments := make([]*mdv1.MarketDataCoverageSegment, 0, len(reports))
	for _, report := range reports {
		segments = append(segments, &mdv1.MarketDataCoverageSegment{
			Key: &mdv1.StreamKey{
				Exchange: report.Key.Exchange,
				Market:   report.Key.Market,
				Kind:     report.Key.Kind,
				Symbol:   report.Key.Symbol,
				Interval: report.Key.Interval,
			},
			Year:     report.Year,
			StartAt:  timestamppb.New(report.StartAt.UTC()),
			EndAt:    timestamppb.New(report.EndAt.UTC()),
			RowCount: report.RowCount,
			Source:   report.Source,
		})
	}

	if _, err := c.client.ReportMarketDataCoverageSegments(ctx, &mdv1.ReportMarketDataCoverageSegmentsRequest{
		Segments: segments,
	}); err != nil {
		return fmt.Errorf("report market-data coverage segments: %w", err)
	}
	return nil
}

func (c *grpcClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *grpcClient) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.timeout)
}

func fromProtoStream(stream *mdv1.MarketDataStream) Stream {
	if stream == nil {
		return Stream{}
	}
	out := Stream{
		StreamID:              stream.GetStreamId(),
		DesiredState:          strings.ToLower(strings.TrimSpace(stream.GetDesiredState())),
		ActualState:           strings.ToLower(strings.TrimSpace(stream.GetActualState())),
		EffectiveLiveDelivery: stream.GetEffectiveLiveDelivery(),
		ActiveLeaseCount:      int(stream.GetActiveLeaseCount()),
		LastError:             strings.TrimSpace(stream.GetLastError()),
	}
	if key := stream.GetKey(); key != nil {
		out.Key = StreamKey{
			Exchange: strings.ToLower(strings.TrimSpace(key.GetExchange())),
			Market:   strings.ToLower(strings.TrimSpace(key.GetMarket())),
			Kind:     strings.ToLower(strings.TrimSpace(key.GetKind())),
			Symbol:   strings.ToUpper(strings.TrimSpace(key.GetSymbol())),
			Interval: strings.TrimSpace(key.GetInterval()),
		}
	}
	if ts := stream.GetLastDataAt(); ts != nil {
		t := ts.AsTime().UTC()
		out.LastDataAt = &t
	}
	return out
}

func fromProtoHistoricalRequest(req *mdv1.MarketDataRequest) HistoricalRequest {
	if req == nil {
		return HistoricalRequest{}
	}
	out := HistoricalRequest{
		RequestID: req.GetRequestId(),
		UserID:    req.GetUserId(),
		PortfolioID: req.GetPortfolioId(),
		Status:    strings.ToLower(strings.TrimSpace(req.GetStatus())),
		LastError: strings.TrimSpace(req.GetLastError()),
		Key: StreamKey{
			Exchange: strings.ToLower(strings.TrimSpace(req.GetKey().GetExchange())),
			Market:   strings.ToLower(strings.TrimSpace(req.GetKey().GetMarket())),
			Kind:     strings.ToLower(strings.TrimSpace(req.GetKey().GetKind())),
			Symbol:   strings.ToUpper(strings.TrimSpace(req.GetKey().GetSymbol())),
			Interval: strings.TrimSpace(req.GetKey().GetInterval()),
		},
	}
	if ts := req.GetRequestedStartAt(); ts != nil {
		t := ts.AsTime().UTC()
		out.RequestedStartAt = &t
	}
	if ts := req.GetRequestedEndAt(); ts != nil {
		t := ts.AsTime().UTC()
		out.RequestedEndAt = &t
	}
	if ts := req.GetCoveredStartAt(); ts != nil {
		t := ts.AsTime().UTC()
		out.CoveredStartAt = &t
	}
	if ts := req.GetCoveredEndAt(); ts != nil {
		t := ts.AsTime().UTC()
		out.CoveredEndAt = &t
	}
	return out
}
