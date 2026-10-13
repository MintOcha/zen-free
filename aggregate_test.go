package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func chatChunk(choices string) string {
	return `data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1791883811,"model":"space-bunny-free","choices":` + choices + `,"usage":null}` + "\n\n"
}

const chatDoneLine = "data: [DONE]\n\n"

var aggregateContentStream = chatChunk(`[{"index":0,"delta":{"role":"assistant"}}]`) +
	chatChunk(`[{"index":0,"delta":{"content":"Hello world"}}]`) +
	chatChunk(`[{"index":0,"delta":{},"finish_reason":"stop"}]`) +
	`data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1791883811,"model":"space-bunny-free","choices":[],"usage":{"prompt_tokens":161,"completion_tokens":3,"total_tokens":164,"prompt_tokens_details":{"cached_tokens":145}}}` + "\n\n" +
	chatDoneLine

var aggregateToolCallStream = chatChunk(`[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":""}}]}}]`) +
	chatChunk(`[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"q\""}}]}}]`) +
	chatChunk(`[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":":\"x\"}"}}]}}]`) +
	chatChunk(`[{"index":0,"delta":{},"finish_reason":"tool_calls"}]`) +
	chatDoneLine

var aggregateReasoningStream = chatChunk(`[{"index":0,"delta":{"reasoning_content":"step one "}}]`) +
	chatChunk(`[{"index":0,"delta":{"reasoning":"step two"}}]`) +
	chatChunk(`[{"index":0,"delta":{"content":"answer"}}]`) +
	chatChunk(`[{"index":0,"delta":{},"finish_reason":"stop"}]`) +
	chatDoneLine

var aggregateFinishLengthStream = chatChunk(`[{"index":0,"delta":{"role":"assistant"}}]`) +
	chatChunk(`[{"index":0,"delta":{"content":"truncated"}}]`) +
	chatChunk(`[{"index":0,"delta":{},"finish_reason":"length"}]`) +
	chatDoneLine

var aggregateDoneWithoutFinishStream = chatChunk(`[{"index":0,"delta":{"role":"assistant","content":"partial"}}]`) +
	chatDoneLine

var aggregateInterruptedStream = chatChunk(`[{"index":0,"delta":{"role":"assistant","content":"partial"}}]`)

const aggregateErrorStream = `data: {"error":{"message":"boom"}}` + "\n\n"

var aggregatePassThroughStream = chatChunk(`[{"index":0,"delta":{"content":"hi"}}]`) +
	chatDoneLine

func TestAggregateChatStream(t *testing.T) {
	cases := []struct {
		name        string
		stream      string
		model       string
		wantErr     error
		wantErrText string
		check       func(t *testing.T, completion map[string]any)
	}{
		{
			name:    "empty stream",
			stream:  "",
			wantErr: errEmptyChatStream,
		},
		{
			name:   "content only",
			stream: aggregateContentStream,
			check: func(t *testing.T, completion map[string]any) {
				if completion["object"] != "chat.completion" {
					t.Fatalf("object = %#v", completion["object"])
				}
				if completion["id"] != "chatcmpl-abc123" {
					t.Fatalf("id = %#v", completion["id"])
				}
				if completion["created"] != int64(1791883811) {
					t.Fatalf("created = %#v", completion["created"])
				}
				if completion["model"] != "space-bunny-free" {
					t.Fatalf("model = %#v", completion["model"])
				}
				choices := array(t, completion["choices"])
				if len(choices) != 1 {
					t.Fatalf("choices = %#v", completion["choices"])
				}
				choice := object(t, choices[0])
				if choice["index"] != 0 {
					t.Fatalf("choice index = %#v", choice["index"])
				}
				if choice["finish_reason"] != "stop" {
					t.Fatalf("finish_reason = %#v", choice["finish_reason"])
				}
				message := object(t, choice["message"])
				if message["role"] != "assistant" || message["content"] != "Hello world" {
					t.Fatalf("message = %#v", message)
				}
				usage := object(t, completion["usage"])
				if usage["prompt_tokens"] != float64(161) || usage["completion_tokens"] != float64(3) || usage["total_tokens"] != float64(164) {
					t.Fatalf("usage = %#v", usage)
				}
			},
		},
		{
			name:   "tool_calls",
			stream: aggregateToolCallStream,
			model:  "gpt-4o",
			check: func(t *testing.T, completion map[string]any) {
				choices := array(t, completion["choices"])
				choice := object(t, choices[0])
				if choice["finish_reason"] != "tool_calls" {
					t.Fatalf("finish_reason = %#v", choice["finish_reason"])
				}
				message := object(t, choice["message"])
				calls, ok := message["tool_calls"].([]*responseToolCall)
				if !ok || len(calls) != 1 {
					t.Fatalf("tool_calls = %#v", message["tool_calls"])
				}
				call := calls[0]
				if call.ID != "call_1" || call.Type != "function" || call.Function.Name != "lookup" || call.Function.Arguments != `{"q":"x"}` {
					t.Fatalf("tool call = %#v", call)
				}
				if completion["model"] != "gpt-4o" {
					t.Fatalf("model = %#v", completion["model"])
				}
			},
		},
		{
			name:   "reasoning",
			stream: aggregateReasoningStream,
			check: func(t *testing.T, completion map[string]any) {
				choices := array(t, completion["choices"])
				message := object(t, object(t, choices[0])["message"])
				if message["reasoning_content"] != "step one step two" {
					t.Fatalf("reasoning_content = %#v", message["reasoning_content"])
				}
				if message["content"] != "answer" {
					t.Fatalf("content = %#v", message["content"])
				}
			},
		},
		{
			name:   "finish_reason length",
			stream: aggregateFinishLengthStream,
			check: func(t *testing.T, completion map[string]any) {
				choices := array(t, completion["choices"])
				if choice := object(t, choices[0]); choice["finish_reason"] != "length" {
					t.Fatalf("finish_reason = %#v", choice["finish_reason"])
				}
			},
		},
		{
			name:   "done without finish_reason",
			stream: aggregateDoneWithoutFinishStream,
			check: func(t *testing.T, completion map[string]any) {
				choices := array(t, completion["choices"])
				choice := object(t, choices[0])
				if choice["finish_reason"] != "stop" {
					t.Fatalf("finish_reason = %#v", choice["finish_reason"])
				}
				usage := object(t, completion["usage"])
				if usage["prompt_tokens"] != 0 || usage["completion_tokens"] != 0 || usage["total_tokens"] != 0 {
					t.Fatalf("usage = %#v", usage)
				}
			},
		},
		{
			name:    "interrupted partial",
			stream:  aggregateInterruptedStream,
			wantErr: errChatStreamInterrupted,
		},
		{
			name:        "upstream error event",
			stream:      aggregateErrorStream,
			wantErrText: "boom",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			completion, err := aggregateChatStream(strings.NewReader(tc.stream), tc.model)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("aggregate error = %v, want %v", err, tc.wantErr)
				}
				if tc.wantErr == errChatStreamInterrupted && errors.Is(err, errEmptyChatStream) {
					t.Fatalf("interrupted stream misclassified as empty: %v", err)
				}
				return
			}
			if tc.wantErrText != "" {
				if err == nil {
					t.Fatalf("aggregate error = nil, want text %q", tc.wantErrText)
				}
				if errors.Is(err, errEmptyChatStream) || errors.Is(err, errChatStreamInterrupted) {
					t.Fatalf("aggregate error = %v, want upstream error message", err)
				}
				if !strings.Contains(err.Error(), tc.wantErrText) {
					t.Fatalf("aggregate error = %v, want substring %q", err, tc.wantErrText)
				}
				return
			}
			if err != nil {
				t.Fatalf("aggregate error = %v", err)
			}
			tc.check(t, completion)
		})
	}
}

func TestProxyHandlerForcesStreamAndAggregates(t *testing.T) {
	var captured map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		captured = decodeJSON(t, body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, aggregateContentStream)
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-4o","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
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
	if completion["object"] != "chat.completion" || completion["model"] != "gpt-4o" {
		t.Fatalf("completion = %#v", completion)
	}
	choices := array(t, completion["choices"])
	choice := object(t, choices[0])
	message := object(t, choice["message"])
	if message["content"] != "Hello world" || choice["finish_reason"] != "stop" {
		t.Fatalf("choice = %#v", choice)
	}
	usage := object(t, completion["usage"])
	if usage["prompt_tokens"] != float64(161) {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestProxyHandlerRetriesEmptyAggregateStream(t *testing.T) {
	setStabilityTestKnobs(t)
	t.Run("retries empty then succeeds", func(t *testing.T) {
		var hits atomic.Int32
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			if hits.Add(1) == 1 {
				return
			}
			_, _ = io.WriteString(w, aggregateContentStream)
		}))
		defer upstream.Close()
		recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":false,"messages":[]}`, "")
		if hits.Load() != 2 {
			t.Fatalf("upstream hits = %d, want 2", hits.Load())
		}
		if recorder.Code != http.StatusOK {
			t.Fatalf("client status = %d body %q", recorder.Code, recorder.Body.String())
		}
		if got := recorder.Header().Get("Content-Type"); got != "application/json" {
			t.Fatalf("client content type = %q", got)
		}
		completion := decodeJSON(t, recorder.Body.Bytes())
		choices := array(t, completion["choices"])
		if choice := object(t, choices[0]); choice["finish_reason"] != "stop" {
			t.Fatalf("finish_reason = %#v", choice["finish_reason"])
		}
	})
	t.Run("gives up after three attempts", func(t *testing.T) {
		var hits atomic.Int32
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
		}))
		defer upstream.Close()
		recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":false,"messages":[]}`, "")
		if hits.Load() != 3 {
			t.Fatalf("upstream hits = %d, want 3", hits.Load())
		}
		if recorder.Code != http.StatusBadGateway {
			t.Fatalf("client status = %d body %q", recorder.Code, recorder.Body.String())
		}
		if !strings.Contains(recorder.Body.String(), "upstream stream was empty") {
			t.Fatalf("client body = %q", recorder.Body.String())
		}
	})
}

func TestProxyHandlerAggregateInterruptedReturns502(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, aggregateInterruptedStream)
	}))
	defer upstream.Close()
	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":false,"messages":[]}`, "")
	if hits.Load() != 1 {
		t.Fatalf("upstream hits = %d, want no retry", hits.Load())
	}
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("client status = %d body %q", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "upstream response ended before completion") {
		t.Fatalf("client body = %q", recorder.Body.String())
	}
}

func TestProxyHandlerStreamTruePassThroughBytes(t *testing.T) {
	var upstreamBytes atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		n, _ := io.WriteString(w, aggregatePassThroughStream)
		upstreamBytes.Store(int64(n))
	}))
	defer upstream.Close()
	recorder := serveStabilityRequest(upstream, `{"model":"gpt-4o","stream":true,"messages":[]}`, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("client status = %d body %q", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("client content type = %q", got)
	}
	if recorder.Body.String() != aggregatePassThroughStream || recorder.Body.Len() != int(upstreamBytes.Load()) {
		t.Fatalf("pass-through bytes differ: got %q", recorder.Body.String())
	}
}
