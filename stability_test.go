package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const stabilityInterruptedPair = "data: {\"error\":{\"message\":\"oc-zen: upstream stream interrupted\",\"type\":\"upstream_interrupted\"}}\n\ndata: [DONE]\n\n"

const stabilityCompletedResponsesSSE = "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\",\"id\":\"msg_1\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"created_at\":123,\"usage\":{}}}\n\n"

func setStabilityTestKnobs(t *testing.T) {
	t.Helper()
	oldAttempts := maxAttempts
	oldBackoffs := retryBackoffs
	oldRetryAfterCap := retryAfterCap
	oldRetryLoopBudget := retryLoopBudget
	oldUpstreamIdleTimeout := upstreamIdleTimeout
	oldClientKeepAliveInterval := clientKeepAliveInterval
	t.Cleanup(func() {
		maxAttempts = oldAttempts
		retryBackoffs = oldBackoffs
		retryAfterCap = oldRetryAfterCap
		retryLoopBudget = oldRetryLoopBudget
		upstreamIdleTimeout = oldUpstreamIdleTimeout
		clientKeepAliveInterval = oldClientKeepAliveInterval
	})
	maxAttempts = 3
	retryBackoffs = []time.Duration{time.Millisecond, 2 * time.Millisecond}
	retryAfterCap = 50 * time.Millisecond
	retryLoopBudget = 3 * time.Second
	upstreamIdleTimeout = 80 * time.Millisecond
	clientKeepAliveInterval = 20 * time.Millisecond
}

func stabilityRequest(body string, accept string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	return req.WithContext(context.WithValue(req.Context(), contextKey{}, "session-stability-test"))
}

func serveStabilityRequest(upstream *httptest.Server, body string, accept string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	proxyHandler(upstream.URL, upstream.Client(), &idMint{}).ServeHTTP(recorder, stabilityRequest(body, accept))
	return recorder
}

func assertInterruptedPair(t *testing.T, body string) {
	t.Helper()
	if !strings.HasSuffix(body, stabilityInterruptedPair) {
		t.Fatalf("stream does not end with the interruption error and DONE pair: %q", body)
	}
}

func writeCompletedResponses(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, stabilityCompletedResponsesSSE)
}

func TestProxyHandlerRetriesBeforeMuseContent(t *testing.T) {
	setStabilityTestKnobs(t)
	const requestBody = `{"model":"muse-spark-1.3","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	var hits atomic.Int32
	var sessions, requestIDs, bodies []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read upstream request body: %v", err)
			return
		}
		sessions = append(sessions, r.Header.Get("x-opencode-session"))
		requestIDs = append(requestIDs, r.Header.Get("x-opencode-request"))
		bodies = append(bodies, string(body))
		if len(sessions) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		writeCompletedResponses(w)
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, requestBody, "text/event-stream")
	if hits.Load() != 2 {
		t.Fatalf("upstream attempts = %d, want 2", hits.Load())
	}
	if len(sessions) != 2 || sessions[0] != "session-stability-test" || sessions[1] != sessions[0] {
		t.Fatalf("upstream session IDs = %q, want the same session on both attempts", sessions)
	}
	if len(requestIDs) != 2 || requestIDs[0] == "" || requestIDs[1] != requestIDs[0] {
		t.Fatalf("upstream request IDs = %q, want one stable non-empty ID", requestIDs)
	}
	if len(bodies) != 2 || bodies[0] == "" || bodies[1] != bodies[0] {
		t.Fatalf("upstream request bodies differ across retries: %q", bodies)
	}
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"content":"hello"`) || !strings.HasSuffix(recorder.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("downstream stream is not a complete streamed answer: status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestProxyHandlerStopsAfterThreeFailedAttempts(t *testing.T) {
	setStabilityTestKnobs(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"muse-spark-1.3","stream":true,"messages":[]}`, "text/event-stream")
	if hits.Load() != 3 {
		t.Fatalf("upstream attempts = %d, want exactly 3", hits.Load())
	}
	assertInterruptedPair(t, recorder.Body.String())
	if strings.Count(recorder.Body.String(), "data: [DONE]\n\n") != 1 {
		t.Fatalf("downstream stream has other than one DONE frame: %q", recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "finish_reason") {
		t.Fatalf("failed upstream fabricated a finish reason: %q", recorder.Body.String())
	}
}

func TestProxyHandlerRetryAfterDeltaAndCap(t *testing.T) {
	t.Run("one second delta is honored", func(t *testing.T) {
		setStabilityTestKnobs(t)
		retryAfterCap = 2 * time.Second
		retryLoopBudget = 3 * time.Second
		var hits atomic.Int32
		var firstAttempt time.Time
		var retryAttempt time.Time
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) == 1 {
				firstAttempt = time.Now()
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			retryAttempt = time.Now()
			writeCompletedResponses(w)
		}))
		defer upstream.Close()

		recorder := serveStabilityRequest(upstream, `{"model":"muse-spark-1.3","stream":true,"messages":[]}`, "text/event-stream")
		if hits.Load() != 2 {
			t.Fatalf("upstream attempts = %d, want 2", hits.Load())
		}
		waited := retryAttempt.Sub(firstAttempt)
		if waited < 900*time.Millisecond || waited > 2500*time.Millisecond {
			t.Fatalf("Retry-After: 1 delay = %s, want approximately one second", waited)
		}
		if !strings.Contains(recorder.Body.String(), `"content":"hello"`) {
			t.Fatalf("retry did not return the streamed answer: %q", recorder.Body.String())
		}
	})

	t.Run("sixty second delta is capped", func(t *testing.T) {
		setStabilityTestKnobs(t)
		const shortCap = 60 * time.Millisecond
		retryAfterCap = shortCap
		var hits atomic.Int32
		var firstAttempt time.Time
		var retryAttempt time.Time
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) == 1 {
				firstAttempt = time.Now()
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			retryAttempt = time.Now()
			writeCompletedResponses(w)
		}))
		defer upstream.Close()

		serveStabilityRequest(upstream, `{"model":"muse-spark-1.3","stream":true,"messages":[]}`, "text/event-stream")
		if hits.Load() != 2 {
			t.Fatalf("upstream attempts = %d, want 2", hits.Load())
		}
		waited := retryAttempt.Sub(firstAttempt)
		if waited < 40*time.Millisecond || waited > 500*time.Millisecond {
			t.Fatalf("Retry-After: 60 delay = %s, want the shortened cap near %s", waited, shortCap)
		}
	})
}

func TestProxyHandlerDoesNotRetryAfterMuseContent(t *testing.T) {
	setStabilityTestKnobs(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		payload := "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\",\"id\":\"msg_1\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)+100))
		_, _ = io.WriteString(w, payload)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"muse-spark-1.3","stream":true,"messages":[]}`, "text/event-stream")
	if hits.Load() != 1 {
		t.Fatalf("upstream attempts = %d, want one after content started", hits.Load())
	}
	if !strings.Contains(recorder.Body.String(), `"content":"partial"`) {
		t.Fatalf("downstream did not receive content before interruption: %q", recorder.Body.String())
	}
	assertInterruptedPair(t, recorder.Body.String())
	if strings.Contains(recorder.Body.String(), "finish_reason") {
		t.Fatalf("interrupted stream fabricated a finish reason: %q", recorder.Body.String())
	}
}

func TestProxyHandlerPassThroughRetriesBeforeResponseHeaders(t *testing.T) {
	setStabilityTestKnobs(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("upstream response writer cannot close the connection")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack upstream connection: %v", err)
				return
			}
			_ = conn.Close()
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: recovered\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	if hits.Load() != 2 {
		t.Fatalf("upstream attempts = %d, want retry after header-level disconnect", hits.Load())
	}
	if recorder.Code != http.StatusOK || recorder.Body.String() != "data: recovered\n\ndata: [DONE]\n\n" {
		t.Fatalf("pass-through retry response = %d %q", recorder.Code, recorder.Body.String())
	}
}

func TestProxyHandlerPassThroughDoesNotRetryAfterBytes(t *testing.T) {
	setStabilityTestKnobs(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		payload := "data: partial\n\n"
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)+100))
		_, _ = io.WriteString(w, payload)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	if hits.Load() != 1 {
		t.Fatalf("upstream attempts = %d, want one after downstream bytes", hits.Load())
	}
	if !strings.HasPrefix(recorder.Body.String(), "data: partial\n\n") {
		t.Fatalf("downstream missed upstream bytes: %q", recorder.Body.String())
	}
	assertInterruptedPair(t, recorder.Body.String())
}

func TestProxyHandlerIdleWatchdogRetriesStalledMuseStream(t *testing.T) {
	setStabilityTestKnobs(t)
	upstreamIdleTimeout = 35 * time.Millisecond
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-time.After(120 * time.Millisecond)
			return
		}
		writeCompletedResponses(w)
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"muse-spark-1.3","stream":true,"messages":[]}`, "text/event-stream")
	if hits.Load() != 2 {
		t.Fatalf("upstream attempts = %d, want pre-content retry after idle watchdog", hits.Load())
	}
	if !strings.Contains(recorder.Body.String(), `"content":"hello"`) || !strings.HasSuffix(recorder.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("watchdog retry did not produce a complete stream: %q", recorder.Body.String())
	}
}

func TestProxyHandlerKeepsMuseStreamAliveUntilContent(t *testing.T) {
	setStabilityTestKnobs(t)
	upstreamIdleTimeout = 2 * time.Second
	clientKeepAliveInterval = 30 * time.Millisecond
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(140 * time.Millisecond)
		writeCompletedResponses(w)
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"muse-spark-1.3","stream":true,"messages":[]}`, "text/event-stream")
	if !strings.Contains(recorder.Body.String(), ": keep-alive\n\n") {
		t.Fatalf("downstream did not receive an SSE keep-alive: %q", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"content":"hello"`) || !strings.HasSuffix(recorder.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("stream did not continue to a complete answer after keep-alive: %q", recorder.Body.String())
	}
}

func TestProxyHandlerStreamExceedsRetryBudget(t *testing.T) {
	setStabilityTestKnobs(t)
	retryLoopBudget = 40 * time.Millisecond
	upstreamIdleTimeout = 2 * time.Second
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(100 * time.Millisecond)
		_, _ = io.WriteString(w, "data: long-output\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "data: long-output\n\n") || !strings.HasSuffix(recorder.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("stream truncated by retry budget: %q", recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "upstream_interrupted") {
		t.Fatalf("stream contains upstream_interrupted error: %q", recorder.Body.String())
	}
}

func TestProxyHandlerPassThroughNoInterruptionAfterDone(t *testing.T) {
	setStabilityTestKnobs(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: final\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "upstream_interrupted") {
		t.Fatalf("stream unexpectedly received interruption error after DONE: %q", recorder.Body.String())
	}
	if strings.Count(recorder.Body.String(), "data: [DONE]\n\n") != 1 {
		t.Fatalf("stream DONE frame count = %d, want exactly 1: %q", strings.Count(recorder.Body.String(), "data: [DONE]\n\n"), recorder.Body.String())
	}
}
func TestRetryableStatusExpanded(t *testing.T) {
	retryable := []int{408, 429, 500, 502, 503, 504, 520, 521, 522, 524, 599}
	for _, code := range retryable {
		if !retryableStatus(code) {
			t.Errorf("status %d should be retryable", code)
		}
	}
	notRetryable := []int{200, 201, 400, 401, 403, 404, 422}
	for _, code := range notRetryable {
		if retryableStatus(code) {
			t.Errorf("status %d should not be retryable", code)
		}
	}
}

func TestExponentialRetryBackoff(t *testing.T) {
	setStabilityTestKnobs(t)
	retryBackoffs = []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	retryAfterCap = 500 * time.Millisecond

	d1 := retryDelay(1, nil, 0)
	d2 := retryDelay(2, nil, 0)
	d3 := retryDelay(3, nil, 0)
	d4 := retryDelay(4, nil, 0)

	if d1 != 10*time.Millisecond || d2 != 20*time.Millisecond {
		t.Fatalf("unexpected initial backoffs: d1=%v d2=%v", d1, d2)
	}
	if d3 != 40*time.Millisecond || d4 != 80*time.Millisecond {
		t.Fatalf("unexpected exponential backoffs: d3=%v d4=%v", d3, d4)
	}
}

func TestIsSSEDoneCompletionMarkers(t *testing.T) {
	markers := [][]byte{
		[]byte("data: [DONE]"),
		[]byte("data:[DONE]"),
		[]byte(`{"choices":[{"delta":{"content":""},"finish_reason":"stop"}]}`),
		[]byte(`{"choices":[{"delta":{"content":""},"finish_reason":"length"}]}`),
		[]byte(`{"choices":[{"delta":{"content":""},"finish_reason":"tool_calls"}]}`),
		[]byte(`{"type":"response.completed"}`),
	}
	for _, m := range markers {
		if !isSSEDone(m) {
			t.Errorf("expected marker to be detected as done: %s", string(m))
		}
	}
	if isSSEDone([]byte(`{"choices":[{"delta":{"content":"thinking..."}}]}`)) {
		t.Error("in-flight chunk should not be detected as done")
	}
}

func TestProxyHandlerPassThroughRetriesEmptyStream(t *testing.T) {
	setStabilityTestKnobs(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if hits.Add(1) == 1 {
			// Closed immediately with empty body
			return
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"success\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	if hits.Load() != 2 {
		t.Fatalf("upstream hits = %d, want 2 (retried empty stream)", hits.Load())
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("client status = %d body = %q", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"content":"success"`) {
		t.Fatalf("expected success payload, got: %s", recorder.Body.String())
	}
}

func TestProxyHandlerPassThroughRetriesErrorEventBeforeContent(t *testing.T) {
	setStabilityTestKnobs(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if hits.Add(1) == 1 {
			// Emits an upstream error event before any completion content
			_, _ = io.WriteString(w, "data: {\"error\":{\"message\":\"upstream overloaded\",\"type\":\"server_error\"}}\n\n")
			return
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"recovered\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	if hits.Load() != 2 {
		t.Fatalf("upstream hits = %d, want 2 (retried error event)", hits.Load())
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("client status = %d body = %q", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"content":"recovered"`) {
		t.Fatalf("expected recovered payload, got: %s", recorder.Body.String())
	}
}

func TestUncappedLineReaderLargeLines(t *testing.T) {
	largeContent := strings.Repeat("x", 2*1024*1024) // 2MB string
	rawChunk := fmt.Sprintf("data: {\"id\":\"large\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"%s\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", largeContent)
	parsed, err := aggregateChatStream(strings.NewReader(rawChunk), "test-model")
	if err != nil {
		t.Fatalf("aggregateChatStream failed on 2MB line: %v", err)
	}
	choices := array(t, parsed["choices"])
	choice := object(t, choices[0])
	message := object(t, choice["message"])
	content := stringValue(message["content"])
	if len(content) != len(largeContent) {
		t.Fatalf("content len = %d, want %d", len(content), len(largeContent))
	}
}

func TestIsSSEErrorAndHasSSEData(t *testing.T) {
	if !isSSEError([]byte("data: {\"error\":{\"message\":\"fail\"}}\n\n")) {
		t.Error("expected error event to be detected")
	}
	if !isSSEError([]byte("event: error\ndata: fail\n\n")) {
		t.Error("expected event: error to be detected")
	}
	if isSSEError([]byte("data: {\"type\":\"response.created\",\"error\":null}\n\n")) {
		t.Error("error: null must not be treated as error")
	}

	if hasSSEData([]byte(": keep-alive\n\n: ping\n\n")) {
		t.Error("comments must not be treated as data")
	}
	if !hasSSEData([]byte("data: {\"id\":\"1\"}\n\n")) {
		t.Error("data line must be detected as data")
	}
}

func TestInitKnobsHeaderTimeoutNeverBelowBudget(t *testing.T) {
	origBudget := retryLoopBudget
	origHeader := responseHeaderTimeout
	t.Cleanup(func() {
		retryLoopBudget = origBudget
		responseHeaderTimeout = origHeader
	})
	t.Setenv("ZEN_RETRY_BUDGET", "300")
	t.Setenv("ZEN_RESPONSE_HEADER_TIMEOUT", "10")
	initStabilityKnobs()
	if responseHeaderTimeout < retryLoopBudget {
		t.Fatalf("response header timeout %s below retry budget %s after init", responseHeaderTimeout, retryLoopBudget)
	}
}

func TestRetryDelayNeverCollapses(t *testing.T) {
	for _, attempt := range []int{64, 70, 100} {
		d := retryDelay(attempt, nil, 0)
		if d < 15*time.Second || d > 20*time.Second {
			t.Errorf("attempt %d: delay %s outside [15s, 20s]", attempt, d)
		}
	}
}

func TestRetryableStatusExcludesPermanentErrors(t *testing.T) {
	for _, status := range []int{501, 505, 506} {
		if retryableStatus(status) {
			t.Errorf("status %d must not be retryable", status)
		}
	}
	for status := 520; status <= 527; status++ {
		if !retryableStatus(status) {
			t.Errorf("status %d must be retryable", status)
		}
	}
	for _, status := range []int{408, 429, 500, 502, 503, 504, 599} {
		if !retryableStatus(status) {
			t.Errorf("status %d must be retryable", status)
		}
	}
}

func TestPassThroughFlagsTruncationAfterItemDoneEvent(t *testing.T) {
	setStabilityTestKnobs(t)
	payload := "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"id\":\"msg_1\",\"status\":\"completed\"}}\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)+100))
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, payload)
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	body := recorder.Body.String()
	if !strings.Contains(body, "response.output_item.done") {
		t.Errorf("body missing item done event: %q", body)
	}
	if !strings.HasSuffix(body, stabilityInterruptedPair) {
		t.Errorf("truncated stream must end with interrupted pair: %q", body)
	}
}

func TestPassThroughNoFalseErrorOnEmbeddedErrorField(t *testing.T) {
	setStabilityTestKnobs(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call_output\",\"output\":{\"error\":{\"message\":\"nonfatal\"},\"ok\":true}}}\n\n")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	body := recorder.Body.String()
	if got := hits.Load(); got != 1 {
		t.Errorf("upstream hits = %d, want 1 (no retry for embedded error field)", got)
	}
	if recorder.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", recorder.Code)
	}
	if !strings.Contains(body, `"content":"hello"`) {
		t.Errorf("body missing streamed content: %q", body)
	}
	if got := strings.Count(body, "data: [DONE]\n\n"); got != 1 {
		t.Errorf("DONE count = %d, want 1", got)
	}
}

func TestAggregateChatStreamRejectsOversizedLine(t *testing.T) {
	oldLimit := maxSSELineBytes
	t.Cleanup(func() { maxSSELineBytes = oldLimit })
	maxSSELineBytes = 1024
	stream := "data: " + strings.Repeat("x", 4096) + "\n\n"
	_, err := aggregateChatStream(strings.NewReader(stream), "test-model")
	if err == nil {
		t.Fatal("expected oversized SSE line to fail, got nil error")
	}
	if !strings.Contains(err.Error(), "SSE line exceeds") {
		t.Fatalf("error = %v, want clear oversized-line error", err)
	}
}

func TestInitKnobsMaxSSELineBytes(t *testing.T) {
	old := maxSSELineBytes
	t.Cleanup(func() { maxSSELineBytes = old })
	t.Setenv("ZEN_MAX_SSE_LINE_BYTES", "4096")
	initStabilityKnobs()
	if maxSSELineBytes != 4096 {
		t.Fatalf("maxSSELineBytes = %d, want 4096", maxSSELineBytes)
	}
}

func TestProxyHandlerRetriesErrorEventBeyondFirstRead(t *testing.T) {
	setStabilityTestKnobs(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if hits.Add(1) == 1 {
			padding := strings.Repeat("data: "+strings.Repeat("p", 96)+"\n\n", 64) // >4096 bytes
			_, _ = io.WriteString(w, padding)
			_, _ = io.WriteString(w, "event: error\ndata: {\"error\":{\"message\":\"late failure\",\"type\":\"server_error\"}}\n\n")
			return
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"recovered\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	if hits.Load() != 2 {
		t.Fatalf("upstream hits = %d, want 2 (error event beyond first 4096-byte read must be caught)", hits.Load())
	}
	if !strings.Contains(recorder.Body.String(), `"content":"recovered"`) {
		t.Fatalf("expected recovered payload, got: %s", recorder.Body.String())
	}
}

func TestRelayPassThroughStreamReturnsUpstreamStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	resp := &http.Response{
		StatusCode: http.StatusUnauthorized,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("data: {\"id\":\"x\"}\n\n")),
	}
	status, retryable := relayPassThroughStream(rec, req, resp, "ses-status-test")
	if retryable {
		t.Fatal("401 pass-through must not be flagged retryable")
	}
	if status != http.StatusUnauthorized {
		t.Fatalf("returned status = %d, want upstream 401 (log propagation)", status)
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("client status = %d, want 401 (client must still receive upstream status)", rec.Code)
	}
}

func TestUpstreamClientNegotiatesHTTP2(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	client := newUpstreamClient()
	transport := client.Transport.(*http.Transport)
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	transport.TLSClientConfig = &tls.Config{RootCAs: pool}

	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("negotiated %s, want HTTP/2 (ForceAttemptHTTP2 must keep h2 despite custom DialContext)", resp.Proto)
	}
}
