package common

import (
	"context"
	"io"
	"net/http"
	"time"
)

type RetryConfig struct {
	MaxRetries    int
	RetryInterval time.Duration
}

var DefaultRetryConfig = RetryConfig{
	MaxRetries:    3,
	RetryInterval: 1 * time.Second,
}

func DoWithRetry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt < cfg.MaxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.RetryInterval * time.Duration(attempt+1)):
			}
		}
	}
	return lastErr
}

type RateLimiter struct {
	interval time.Duration
	lastCall time.Time
}

func NewRateLimiter(interval time.Duration) *RateLimiter {
	return &RateLimiter{interval: interval}
}

func (r *RateLimiter) Wait(ctx context.Context) error {
	if r.lastCall.IsZero() {
		r.lastCall = time.Now()
		return nil
	}

	elapsed := time.Since(r.lastCall)
	if elapsed < r.interval {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval - elapsed):
		}
	}
	r.lastCall = time.Now()
	return nil
}

type HTTPClient struct {
	Client  *http.Client
	Limiter *RateLimiter
}

func NewHTTPClient(timeout, rateLimitInterval time.Duration) *HTTPClient {
	return &HTTPClient{
		Client: &http.Client{
			Timeout: timeout,
		},
		Limiter: NewRateLimiter(rateLimitInterval),
	}
}

func (c *HTTPClient) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if c.Limiter != nil {
		if err := c.Limiter.Wait(ctx); err != nil {
			return nil, err
		}
	}
	return c.Client.Do(req)
}

func ReadAllBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func IsRateLimited(resp *http.Response) bool {
	return resp.StatusCode == http.StatusTooManyRequests
}
