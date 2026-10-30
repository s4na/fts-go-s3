package s3lite

import (
	"io"
	"net/http"
	"sync"
)

type Counts struct {
	Requests      map[string]int64 `json:"requests"`
	SentBytes     int64            `json:"sent_body_bytes"`
	ReceivedBytes int64            `json:"received_body_bytes"`
}

type Meter struct {
	Base   http.RoundTripper
	mu     sync.Mutex
	counts Counts
}

func (m *Meter) Snapshot() Counts {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := Counts{Requests: make(map[string]int64), SentBytes: m.counts.SentBytes, ReceivedBytes: m.counts.ReceivedBytes}
	for key, value := range m.counts.Requests {
		out.Requests[key] = value
	}
	return out
}

type countingBody struct {
	io.ReadCloser
	add func(int64)
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.add(int64(n))
	return n, err
}

func (m *Meter) RoundTrip(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	if m.counts.Requests == nil {
		m.counts.Requests = make(map[string]int64)
	}
	m.counts.Requests[req.Method]++
	m.mu.Unlock()
	if req.Body != nil {
		req.Body = &countingBody{ReadCloser: req.Body, add: func(n int64) {
			m.mu.Lock()
			m.counts.SentBytes += n
			m.mu.Unlock()
		}}
	}
	base := m.Base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &countingBody{ReadCloser: resp.Body, add: func(n int64) {
		m.mu.Lock()
		m.counts.ReceivedBytes += n
		m.mu.Unlock()
	}}
	return resp, nil
}
