package controlplane

import "time"

type HistoricalRequest struct {
	RequestID        int64
	UserID           int64
	AccountID        int64
	Key              StreamKey
	Status           string
	RequestedStartAt *time.Time
	RequestedEndAt   *time.Time
	CoveredStartAt   *time.Time
	CoveredEndAt     *time.Time
	LastError        string
}

type HistoricalRequestStateReport struct {
	RequestID      int64
	Status         string
	CoveredStartAt *time.Time
	CoveredEndAt   *time.Time
	LastError      string
}

type CoverageSegmentReport struct {
	Key      StreamKey
	Year     int32
	StartAt  time.Time
	EndAt    time.Time
	RowCount int64
	Source   string
}
