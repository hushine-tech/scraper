package controlplane

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hushine-tech/scraper/internal/storage"

	mdv1 "github.com/hushine-tech/control-panel-service/gen/marketdatav1"
)

type WriterLeaseClient interface {
	CreateOrRenewWriterLease(
		ctx context.Context,
		domain storage.WriterLeaseDomain,
		ownerInstanceID string,
		scraperInstanceID string,
		collectorID string,
		ttl time.Duration,
	) (storage.WriterLease, error)
}

type WriterLeaseManagerConfig struct {
	Client            WriterLeaseClient
	OwnerInstanceID   string
	ScraperInstanceID string
	TTL               time.Duration
}

type WriterLeaseManager struct {
	client            WriterLeaseClient
	ownerInstanceID   string
	scraperInstanceID string
	ttl               time.Duration
}

func NewWriterLeaseManager(cfg WriterLeaseManagerConfig) *WriterLeaseManager {
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = 90 * time.Second
	}
	owner := strings.TrimSpace(cfg.OwnerInstanceID)
	scraperInstanceID := strings.TrimSpace(cfg.ScraperInstanceID)
	if owner == "" {
		owner = scraperInstanceID
	}
	return &WriterLeaseManager{
		client:            cfg.Client,
		ownerInstanceID:   owner,
		scraperInstanceID: scraperInstanceID,
		ttl:               ttl,
	}
}

func (m *WriterLeaseManager) Acquire(ctx context.Context, domain storage.WriterLeaseDomain, collectorID string) (storage.WriterLease, error) {
	if m == nil || m.client == nil {
		return storage.WriterLease{}, fmt.Errorf("writer lease client is not configured")
	}
	if m.ownerInstanceID == "" {
		return storage.WriterLease{}, fmt.Errorf("writer lease owner_instance_id is not configured")
	}
	if m.scraperInstanceID == "" {
		return storage.WriterLease{}, fmt.Errorf("writer lease scraper_instance_id is not configured")
	}
	collectorID = strings.TrimSpace(collectorID)
	if collectorID == "" {
		return storage.WriterLease{}, fmt.Errorf("writer lease collector_id is required")
	}
	lease, err := m.client.CreateOrRenewWriterLease(ctx, domain, m.ownerInstanceID, m.scraperInstanceID, collectorID, m.ttl)
	if err != nil {
		return storage.WriterLease{}, err
	}
	if strings.ToLower(strings.TrimSpace(lease.Status)) != "active" {
		return storage.WriterLease{}, fmt.Errorf("writer lease %s is not active: %s", lease.LeaseID, lease.Status)
	}
	if !lease.ExpiresAt.IsZero() && !lease.ExpiresAt.After(time.Now().UTC()) {
		return storage.WriterLease{}, fmt.Errorf("writer lease %s expired at %s", lease.LeaseID, lease.ExpiresAt.Format(time.RFC3339))
	}
	return lease, nil
}

func (c *grpcClient) CreateOrRenewWriterLease(
	ctx context.Context,
	domain storage.WriterLeaseDomain,
	ownerInstanceID string,
	scraperInstanceID string,
	collectorID string,
	ttl time.Duration,
) (storage.WriterLease, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.client.CreateOrRenewMarketDataWriterLease(ctx, &mdv1.CreateOrRenewMarketDataWriterLeaseRequest{
		Key: &mdv1.StreamKey{
			Exchange: domain.Exchange,
			Market:   domain.Market,
			Kind:     domain.Kind,
			Symbol:   domain.Symbol,
			Interval: domain.Interval,
		},
		Year:              int32(domain.Year),
		OwnerInstanceId:   ownerInstanceID,
		ScraperInstanceId: scraperInstanceID,
		CollectorId:       collectorID,
		TtlSeconds:        int64(ttl.Seconds()),
	})
	if err != nil {
		return storage.WriterLease{}, fmt.Errorf("create/renew writer lease: %w", err)
	}
	return writerLeaseFromProto(resp.GetLease()), nil
}

func writerLeaseFromProto(lease *mdv1.MarketDataWriterLease) storage.WriterLease {
	if lease == nil {
		return storage.WriterLease{}
	}
	out := storage.WriterLease{
		LeaseID:           strings.TrimSpace(lease.GetLeaseId()),
		OwnerInstanceID:   strings.TrimSpace(lease.GetOwnerInstanceId()),
		ScraperInstanceID: strings.TrimSpace(lease.GetScraperInstanceId()),
		CollectorID:       strings.TrimSpace(lease.GetCollectorId()),
		Status:            strings.TrimSpace(lease.GetStatus()),
	}
	if ts := lease.GetExpiresAt(); ts != nil {
		out.ExpiresAt = ts.AsTime().UTC()
	}
	return out
}
