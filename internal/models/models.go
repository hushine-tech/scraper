package models

import (
	"encoding/json"
	"time"
)

type Kline struct {
	Time        time.Time `json:"time"`
	Symbol      string    `json:"symbol"`
	Market      string    `json:"market"`      // "spot" 或 "futures"
	Exchange    string    `json:"exchange"`    // "binance", "okx"
	Interval    string    `json:"interval"`    // "1m", "5m", "15m" 等
	OpenTime    time.Time `json:"open_time"`
	CloseTime   time.Time `json:"close_time"`
	Open        float64   `json:"open"`
	High        float64   `json:"high"`
	Low         float64   `json:"low"`
	Close       float64   `json:"close"`
	Volume      float64   `json:"volume"`
	QuoteVolume float64   `json:"quote_volume"`
	NumTrades   int64     `json:"num_trades"`
}

type OrderBook struct {
	Time     time.Time       `json:"time"`
	Symbol   string          `json:"symbol"`
	Market   string          `json:"market"`    // "spot" 或 "futures"
	Exchange string          `json:"exchange"`   // "binance", "okx"
	Bids     json.RawMessage `json:"bids"`
	Asks     json.RawMessage `json:"asks"`
}

type FundingRate struct {
	Time            time.Time `json:"time"`
	Symbol          string    `json:"symbol"`
	Market          string    `json:"market"`    // 固定 "futures"
	Exchange        string    `json:"exchange"`   // "binance", "okx"
	FundingRate     float64   `json:"funding_rate"`
	MarkPrice       float64   `json:"mark_price"`
	NextFundingTime time.Time `json:"next_funding_time"`
}

type OpenInterest struct {
	Time         time.Time `json:"time"`
	Symbol       string    `json:"symbol"`
	OpenInterest float64   `json:"open_interest"`
	Period       string    `json:"period"`
	Market       string    `json:"market"`   // 固定 "futures"
	Exchange     string    `json:"exchange"` // "binance", "okx"
}