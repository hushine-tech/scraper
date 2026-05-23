package controlplane

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/storage"
)

type fakeWriterLeaseClient struct {
	lease storage.WriterLease
	err   error
}

func (c *fakeWriterLeaseClient) CreateOrRenewWriterLease(
	_ context.Context,
	_ storage.WriterLeaseDomain,
	_ string,
	_ string,
	_ string,
	_ time.Duration,
) (storage.WriterLease, error) {
	if c.err != nil {
		return storage.WriterLease{}, c.err
	}
	return c.lease, nil
}

func TestWriterLeaseManagerAcquireRejectsExpiredLease(t *testing.T) {
	manager := NewWriterLeaseManager(WriterLeaseManagerConfig{
		Client: &fakeWriterLeaseClient{lease: storage.WriterLease{
			LeaseID:           "lease-1",
			ScraperInstanceID: "scraper-1",
			CollectorID:       "collector-1",
			Status:            "active",
			ExpiresAt:         time.Now().Add(-time.Second),
		}},
		OwnerInstanceID:   "owner-1",
		ScraperInstanceID: "scraper-1",
	})

	_, err := manager.Acquire(context.Background(), storage.WriterLeaseDomain{
		Exchange: "binance",
		Market:   "futures",
		Kind:     "kline",
		Symbol:   "BTCUSDT",
		Interval: "1m",
		Year:     2026,
	}, "collector-1")
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired lease error, got %v", err)
	}
}

func TestWriterLeaseManagerAcquirePropagatesLeaseDenial(t *testing.T) {
	manager := NewWriterLeaseManager(WriterLeaseManagerConfig{
		Client:            &fakeWriterLeaseClient{err: errors.New("owned by another scraper")},
		OwnerInstanceID:   "owner-1",
		ScraperInstanceID: "scraper-1",
	})

	_, err := manager.Acquire(context.Background(), storage.WriterLeaseDomain{
		Exchange: "binance",
		Market:   "spot",
		Kind:     "orderbook",
		Symbol:   "ETHUSDT",
		Year:     2026,
	}, "collector-1")
	if err == nil || err.Error() != "owned by another scraper" {
		t.Fatalf("expected lease denial, got %v", err)
	}
}
