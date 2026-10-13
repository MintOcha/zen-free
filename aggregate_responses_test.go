package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func sampleRealResponsesSSE(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("testdata/responses-sse-real.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func assertBENCHOutput(t *testing.T, completion map[string]any) {
	t.Helper()
	output := array(t, completion["output"])
	found := false
	for _, rawItem := range output {
		item := object(t, rawItem)
		if item["type"] != "message" {
			continue
		}
		for _, rawPart := range array(t, item["content"]) {
			part := object(t, rawPart)
			if part["text"] == "BENCH" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("output has no BENCH text part: %#v", output)
	}
}

func TestAggregateResponsesStream(t *testing.T) {
	fixture := sampleRealResponsesSSE(t)
	tests := []struct {
		name    string
		stream  string
		wantErr error
	}{
		{name: "real captured completed stream", stream: fixture},
		{name: "empty stream", stream: "", wantErr: errEmptyResponsesStream},
		{name: "interrupted before completed", stream: "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\ndata: [DONE]\n\n", wantErr: errResponsesStreamInterrupted},
		{name: "completed without done marker", stream: "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n"},
		{name: "completed then done stops early", stream: "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\ndata: [DONE]\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_2\"}}\n\n"},
		{name: "top-level error event surfaces message", stream: "data: {\"error\":{\"message\":\"boom\"}}\n\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := aggregateResponsesStream(strings.NewReader(tt.stream))
			if tt.name == "top-level error event surfaces message" {
				if err == nil || !strings.Contains(err.Error(), "boom") {
					t.Fatalf("err = %v, want message containing boom", err)
				}
				return
			}
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if tt.name == "real captured completed stream" {
				if got["id"] != "resp_6ace467f4298c874d5c1476f" || got["object"] != "response" || got["status"] != "completed" {
					t.Fatalf("completion = %#v", got)
				}
				if got["model"] != "muse-spark-1.3-contributor-free" || got["created_at"] != float64(1791903359) {
					t.Fatalf("completion = %#v", got)
				}
				usage := object(t, got["usage"])
				if usage["input_tokens"] != float64(653) || usage["output_tokens"] != float64(204) || usage["total_tokens"] != float64(857) {
					t.Fatalf("usage = %#v", usage)
				}
				assertBENCHOutput(t, got)
				return
			}
			if got["id"] != "resp_1" {
				t.Fatalf("completion = %#v", got)
			}
		})
	}
}

func TestProxyHandlerResponsesForcesStreamAndAggregates(t *testing.T) {
	fixture := sampleRealResponsesSSE(t)
	var captured map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		captured = decodeJSON(t, body)
		if captured["stream"] != true {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"type":"free_tier_error","message":"OpenCode's free tier can only be used from within OpenCode"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, fixture)
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"muse-spark-1.3-contributor-free","stream":false,"input":"Reply with exactly: BENCH"}`))
	request.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	proxyHandler(upstream.URL, upstream.Client(), &idMint{}).ServeHTTP(w, request)
	if captured["stream"] != true {
		t.Fatalf("upstream stream = %#v, want true", captured["stream"])
	}
	if w.Code != http.StatusOK {
		t.Fatalf("client status = %d body %q", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("client content type = %q", got)
	}
	completion := decodeJSON(t, w.Body.Bytes())
	if completion["object"] != "response" || completion["status"] != "completed" {
		t.Fatalf("completion = %#v", completion)
	}
	if completion["id"] != "resp_6ace467f4298c874d5c1476f" {
		t.Fatalf("completion id = %#v", completion["id"])
	}
	assertBENCHOutput(t, completion)
}

func TestProxyHandlerResponsesStreamTruePassThrough(t *testing.T) {
	fixture := sampleRealResponsesSSE(t)
	var upstreamBytes atomic.Int64
	var captured map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		captured = decodeJSON(t, body)
		w.Header().Set("Content-Type", "text/event-stream")
		n, _ := io.WriteString(w, fixture)
		upstreamBytes.Store(int64(n))
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"muse-spark-1.3-contributor-free","stream":true,"input":"Reply with exactly: BENCH"}`))
	request.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	proxyHandler(upstream.URL, upstream.Client(), &idMint{}).ServeHTTP(w, request)
	if captured["stream"] != true {
		t.Fatalf("upstream stream = %#v, want true", captured["stream"])
	}
	if w.Code != http.StatusOK {
		t.Fatalf("client status = %d body %q", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("client content type = %q", got)
	}
	if w.Body.String() != fixture || w.Body.Len() != int(upstreamBytes.Load()) {
		t.Fatalf("pass-through bytes differ: got %d want %d", w.Body.Len(), upstreamBytes.Load())
	}
}

func TestProxyHandlerResponsesEmptyStreamRetries(t *testing.T) {
	setStabilityTestKnobs(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"muse-spark-1.3-contributor-free","stream":false,"input":"hi"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	proxyHandler(upstream.URL, upstream.Client(), &idMint{}).ServeHTTP(recorder, request)
	if hits.Load() != 3 {
		t.Fatalf("upstream hits = %d, want 3", hits.Load())
	}
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("client status = %d body %q", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "upstream responses stream was empty") {
		t.Fatalf("client body = %q", recorder.Body.String())
	}
}
