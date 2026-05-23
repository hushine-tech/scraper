package backfill

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
)

const coverageSegmentSource = "historical_backfill"

type CoverageSegment struct {
	Exchange string
	Market   string
	Symbol   string
	Interval string
	Year     int
	StartAt  time.Time
	EndAt    time.Time
	RowCount int64
	Source   string
}

func BuildCoverageSegments(exchange, market, symbol, interval string, rows []models.Kline) ([]CoverageSegment, error) {
	step, err := coverageIntervalDuration(interval)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}

	openTimes := make([]time.Time, 0, len(rows))
	seen := make(map[int64]struct{}, len(rows))
	for _, row := range rows {
		openTime := row.OpenTime.UTC()
		if openTime.IsZero() {
			continue
		}
		key := openTime.UnixNano()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		openTimes = append(openTimes, openTime)
	}
	if len(openTimes) == 0 {
		return nil, nil
	}
	sort.Slice(openTimes, func(i, j int) bool {
		return openTimes[i].Before(openTimes[j])
	})

	normalized := CoverageSegment{
		Exchange: strings.ToLower(strings.TrimSpace(exchange)),
		Market:   strings.ToLower(strings.TrimSpace(market)),
		Symbol:   strings.ToUpper(strings.TrimSpace(symbol)),
		Interval: strings.TrimSpace(interval),
		Source:   coverageSegmentSource,
	}

	segments := make([]CoverageSegment, 0, len(openTimes))
	start := openTimes[0]
	prev := openTimes[0]
	count := int64(1)
	for _, current := range openTimes[1:] {
		if current.Equal(prev.Add(step)) && current.UTC().Year() == prev.UTC().Year() {
			prev = current
			count++
			continue
		}
		segments = append(segments, newCoverageSegment(normalized, start, prev.Add(step), count))
		start = current
		prev = current
		count = 1
	}
	segments = append(segments, newCoverageSegment(normalized, start, prev.Add(step), count))
	return segments, nil
}

func newCoverageSegment(base CoverageSegment, startAt, endAt time.Time, rowCount int64) CoverageSegment {
	seg := base
	seg.Year = startAt.UTC().Year()
	seg.StartAt = startAt.UTC()
	seg.EndAt = endAt.UTC()
	seg.RowCount = rowCount
	return seg
}

func coverageIntervalDuration(interval string) (time.Duration, error) {
	interval = strings.TrimSpace(interval)
	if len(interval) < 2 {
		return 0, fmt.Errorf("invalid interval %q", interval)
	}
	value, err := strconv.Atoi(interval[:len(interval)-1])
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid interval %q", interval)
	}
	switch interval[len(interval)-1] {
	case 's':
		return time.Duration(value) * time.Second, nil
	case 'm':
		return time.Duration(value) * time.Minute, nil
	case 'h':
		return time.Duration(value) * time.Hour, nil
	case 'd':
		return time.Duration(value) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid interval unit %q", interval)
	}
}
