package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type esProducer struct {
	cfg     ElasticsearchConfig
	buffer  chan []byte
	done    chan struct{}
	wg      sync.WaitGroup
	client  *http.Client
}

func newESProducer(cfg ElasticsearchConfig) *esProducer {
	e := &esProducer{
		cfg:     cfg,
		buffer:  make(chan []byte, 10000),
		done:    make(chan struct{}),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	e.wg.Add(1)
	go e.worker()

	return e
}

func (e *esProducer) worker() {
	defer e.wg.Done()

	for {
		select {
		case <-e.done:
			e.flush()
			return
		case msg := <-e.buffer:
			e.sendWithRetry(msg)
		}
	}
}

func (e *esProducer) flush() {
	for {
		select {
		case msg := <-e.buffer:
			e.sendWithRetry(msg)
		default:
			return
		}
	}
}

func (e *esProducer) sendWithRetry(msg []byte) {
	if len(e.cfg.Addresses) == 0 {
		return
	}

	address := e.cfg.Addresses[0]
	indexName := fmt.Sprintf("%s-%s", e.cfg.IndexPrefix, time.Now().Format("2006.01.02"))

	url := fmt.Sprintf("%s/%s/_doc", address, indexName)

	for i := 0; i < 3; i++ {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(msg))
		if err != nil {
			return
		}

		req.Header.Set("Content-Type", "application/json")
		if e.cfg.Username != "" {
			req.SetBasicAuth(e.cfg.Username, e.cfg.Password)
		}

		resp, err := e.client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 300 {
				return
			}
		}

		time.Sleep(time.Duration(1<<uint(i)) * time.Second)
	}
}

func (e *esProducer) send(entry map[string]interface{}) {
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}

	select {
	case e.buffer <- data:
	default:
		// Buffer full, drop
	}
}

func (e *esProducer) Close() error {
	close(e.done)
	e.wg.Wait()
	return nil
}
