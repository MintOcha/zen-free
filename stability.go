package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	dialTimeout             = 15 * time.Second
	tlsHandshakeTimeout     = 10 * time.Second
	responseHeaderTimeout   = 300 * time.Second
	idleConnTimeout         = 50 * time.Second
	expectContinueTimeout   = 1 * time.Second
	maxIdleConns            = 32
	maxIdleConnsPerHost     = 16
	upstreamIdleTimeout     = 300 * time.Second
	clientKeepAliveInterval = 15 * time.Second
	maxAttempts             = 8
	retryBackoffs           = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	retryAfterCap           = 30 * time.Second
	retryLoopBudget         = 300 * time.Second
	maxSSELineBytes         = 16 * 1024 * 1024
	sseErrorScanWindow      = 64 * 1024
)

const interruptedStreamPair = "data: {\"error\":{\"message\":\"oc-zen: upstream stream interrupted\",\"type\":\"upstream_interrupted\"}}\n\ndata: [DONE]\n\n"

func newUpstreamClient() *http.Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
		IdleConnTimeout:       idleConnTimeout,
		ExpectContinueTimeout: expectContinueTimeout,
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{Transport: transport}
}

type idleWatchBody struct {
	body          io.ReadCloser
	timeout       time.Duration
	timer         *time.Timer
	done          chan struct{}
	once          sync.Once
	bodyCloseOnce sync.Once
	expired       chan struct{}
}

func watchUpstreamBody(body io.ReadCloser, timeout time.Duration, session string) *idleWatchBody {
	if timeout <= 0 {
		timeout = time.Nanosecond
	}
	w := &idleWatchBody{body: body, timeout: timeout, done: make(chan struct{}), expired: make(chan struct{})}
	w.timer = time.AfterFunc(timeout, func() {
		select {
		case <-w.done:
			return
		default:
		}
		close(w.expired)
		log.Printf("idle_watchdog after=%s ses=%s", timeout, session)
		w.closeBody()
	})
	return w
}

func (w *idleWatchBody) Read(p []byte) (int, error) {
	n, err := w.body.Read(p)
	if n > 0 && w.timer != nil {
		w.timer.Reset(w.timeout)
	}
	select {
	case <-w.expired:
		if err == nil {
			err = context.DeadlineExceeded
		}
	default:
	}
	return n, err
}

func (w *idleWatchBody) closeBody() { w.bodyCloseOnce.Do(func() { _ = w.body.Close() }) }

func (w *idleWatchBody) Close() error {
	w.once.Do(func() {
		close(w.done)
		if w.timer != nil {
			w.timer.Stop()
		}
	})
	w.closeBody()
	return nil
}

type serializedStreamWriter struct {
	w           http.ResponseWriter
	mu          sync.Mutex
	reset       chan struct{}
	stop        chan struct{}
	done        chan struct{}
	interval    time.Duration
	atBoundary  bool
	doneTracker sseDoneTracker
}

func newSerializedStreamWriter(w http.ResponseWriter, interval time.Duration) *serializedStreamWriter {
	s := &serializedStreamWriter{w: w, reset: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), interval: interval, atBoundary: true}
	go s.heartbeat()
	return s
}

func (s *serializedStreamWriter) Header() http.Header { return s.w.Header() }

func (s *serializedStreamWriter) WriteHeader(status int) {
	s.mu.Lock()
	s.w.WriteHeader(status)
	s.mu.Unlock()
}
func (s *serializedStreamWriter) heartbeat() {
	defer close(s.done)
	if s.interval <= 0 {
		<-s.stop
		return
	}
	timer := time.NewTimer(s.interval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			s.mu.Lock()
			if s.atBoundary && !s.doneTracker.sawDone {
				_, _ = io.WriteString(s.w, ": keep-alive\n\n")
				if f, ok := s.w.(http.Flusher); ok {
					f.Flush()
				}
			}
			s.mu.Unlock()
			timer.Reset(s.interval)
		case <-s.reset:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(s.interval)
		case <-s.stop:
			return
		}
	}
}

func (s *serializedStreamWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	n, err := s.w.Write(p)
	if n > 0 {
		s.doneTracker.observe(p[:n])
		s.atBoundary = bytes.HasSuffix(p[:n], []byte("\n\n"))
	}
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	s.mu.Unlock()
	select {
	case s.reset <- struct{}{}:
	default:
	}
	return n, err
}

func (s *serializedStreamWriter) Flush() {
	s.mu.Lock()
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	s.mu.Unlock()
	select {
	case s.reset <- struct{}{}:
	default:
	}
}

func (s *serializedStreamWriter) Close() {
	close(s.stop)
	<-s.done
}

type sseDoneTracker struct {
	sawDone bool
	tail    []byte
}

func (t *sseDoneTracker) observe(b []byte) {
	if t.sawDone || len(b) == 0 {
		return
	}
	if isSSEDone(b) || (len(t.tail) > 0 && isSSEDone(append(t.tail, b...))) {
		t.sawDone = true
		return
	}
	if len(b) >= 64 {
		t.tail = append(t.tail[:0], b[len(b)-64:]...)
	} else {
		t.tail = append(t.tail, b...)
		if len(t.tail) > 64 {
			t.tail = t.tail[len(t.tail)-64:]
		}
	}
}

type sseDoneWriter struct {
	w       io.Writer
	tracker sseDoneTracker
}

func (s *sseDoneWriter) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	if n > 0 {
		s.tracker.observe(p[:n])
	}
	return n, err
}

func isSSEDone(b []byte) bool {
	return bytes.Contains(b, []byte("data: [DONE]")) ||
		bytes.Contains(b, []byte("data:[DONE]")) ||
		bytes.Contains(b, []byte(`"finish_reason":"stop"`)) ||
		bytes.Contains(b, []byte(`"finish_reason": "stop"`)) ||
		bytes.Contains(b, []byte(`"finish_reason":"length"`)) ||
		bytes.Contains(b, []byte(`"finish_reason": "length"`)) ||
		bytes.Contains(b, []byte(`"finish_reason":"tool_calls"`)) ||
		bytes.Contains(b, []byte(`"finish_reason": "tool_calls"`)) ||
		bytes.Contains(b, []byte(`"finish_reason":"content_filter"`)) ||
		bytes.Contains(b, []byte(`"finish_reason": "content_filter"`)) ||
		bytes.Contains(b, []byte(`"response.completed"`))
}

func isSSEError(b []byte) bool {
	for _, line := range bytes.Split(b, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("event:")) {
			if strings.TrimSpace(string(line[len("event:"):])) == "error" {
				return true
			}
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			if bytes.HasPrefix(payload, []byte(`{"error":`)) ||
				bytes.HasPrefix(payload, []byte(`{"type":"response.failed"`)) ||
				bytes.HasPrefix(payload, []byte(`{"type": "response.failed"`)) {
				return true
			}
		}
	}
	return false
}

func hasSSEData(b []byte) bool {
	for _, line := range bytes.Split(b, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("data:")) {
			val := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			if len(val) > 0 {
				return true
			}
		}
	}
	return false
}

var errSSELineTooLong = errors.New("upstream SSE line exceeds configured limit")

func readSSELine(reader *bufio.Reader) (string, error) {
	limit := maxSSELineBytes
	var line []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > limit {
			return "", fmt.Errorf("oc-zen: upstream SSE line exceeds %d bytes: %w", limit, errSSELineTooLong)
		}
		if err == nil {
			return string(line), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return string(line), err
		}
	}
}

type clientWriteTracker struct {
	w        io.Writer
	writeErr error
}

func (c *clientWriteTracker) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if err != nil && c.writeErr == nil {
		c.writeErr = err
	}
	return n, err
}

func isClientDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, net.ErrClosed) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "broken pipe") || strings.Contains(s, "connection reset") || strings.Contains(s, "closed network connection")
}

func relayPassThroughStream(w http.ResponseWriter, r *http.Request, resp *http.Response, session string) (int, bool) {
	resp.Body = watchUpstreamBody(resp.Body, upstreamIdleTimeout, session)
	defer resp.Body.Close()

	prelude := make([]byte, 0, 4096)
	chunk := make([]byte, 4096)
	var readErr error
	for {
		n, err := resp.Body.Read(chunk)
		if n > 0 {
			prelude = append(prelude, chunk[:n]...)
		}
		if isSSEError(prelude) {
			return http.StatusBadGateway, true
		}
		if len(prelude) >= sseErrorScanWindow {
			break
		}
		if err != nil {
			readErr = err
			break
		}
	}
	if len(prelude) == 0 && readErr != nil {
		return http.StatusBadGateway, true
	}
	if readErr != nil && !hasSSEData(prelude) {
		return http.StatusBadGateway, true
	}

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	writer := newSerializedStreamWriter(w, clientKeepAliveInterval)
	defer writer.Close()

	w.WriteHeader(resp.StatusCode)

	tracker := &clientWriteTracker{w: writer}
	doneWriter := &sseDoneWriter{w: tracker}

	written, writeErr := 0, error(nil)
	if len(prelude) > 0 {
		count, err := doneWriter.Write(prelude)
		written += count
		writeErr = err
	}

	if readErr == nil && writeErr == nil {
		count, err := io.Copy(doneWriter, resp.Body)
		written += int(count)
		writeErr = err
	} else if readErr != io.EOF && writeErr == nil {
		writeErr = readErr
	}

	if writeErr != nil && !doneWriter.tracker.sawDone {
		if tracker.writeErr != nil || isClientDisconnect(writeErr) {
			log.Printf("client_disconnected bytes_written=%d err=%v ses=%s", written, writeErr, session)
			return resp.StatusCode, false
		}
		if r.Context().Err() == nil {
			log.Printf("stream_interrupted bytes_written=%d err=%v ses=%s", written, writeErr, session)
			_, _ = io.WriteString(w, interruptedStreamPair)
		}
	}
	return resp.StatusCode, false
}
