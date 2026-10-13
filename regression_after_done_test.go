package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// hardCloseAfterDone serves a complete SSE stream ending in [DONE], then severs
// the connection with RST instead of a clean EOF. That leaves the proxy with a
// non-nil read error AFTER the stream was already complete — the exact shape of
// the production false alarms (13x in 24h, always followed by "-> 200").
// A complete stream must never gain a second [DONE] or an interruption frame.
func hardCloseAfterDone(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
		w.(http.Flusher).Flush()
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("upstream does not support hijack")
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = conn.Close()
	}))
}

func TestPassThroughNoInterruptionAfterDoneHardClose(t *testing.T) {
	setStabilityTestKnobs(t)
	upstream := hardCloseAfterDone(t)
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "text/event-stream")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("missing DONE frame: %q", body)
	}
	if got := strings.Count(body, "data: [DONE]"); got != 1 {
		t.Fatalf("DONE count = %d, want 1 (a spurious frame was appended): %q", got, body)
	}
	if strings.Contains(body, "upstream_interrupted") {
		t.Fatalf("SPURIOUS interruption after a complete stream: %q", body)
	}
}

func TestAggregateNoInterruptionAfterDoneHardClose(t *testing.T) {
	setStabilityTestKnobs(t)
	upstream := hardCloseAfterDone(t)
	defer upstream.Close()

	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":false,"messages":[]}`, "application/json")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (complete upstream reply must not become an error): %q",
			recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "hello") {
		t.Fatalf("aggregated reply lost the content: %q", body)
	}
	if strings.Contains(body, "interrupted") {
		t.Fatalf("SPURIOUS interruption after complete DONE: %q", body)
	}
}
