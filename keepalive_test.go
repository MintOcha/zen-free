package main

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSerializedStreamWriterKeepAliveOnlyAtFrameBoundary(t *testing.T) {
	rec := httptest.NewRecorder()
	w := newSerializedStreamWriter(rec, 10*time.Millisecond)
	if _, err := w.Write([]byte("data: long-")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := w.Write([]byte("output\n\n")); err != nil {
		t.Fatalf("second write: %v", err)
	}
	w.Close()
	body := rec.Body.String()
	marker := ": keep-alive\n\n"
	searched := 0
	for {
		i := strings.Index(body[searched:], marker)
		if i < 0 {
			break
		}
		pos := searched + i
		if pos > 0 && !strings.HasSuffix(body[:pos], "\n\n") {
			t.Fatalf("keep-alive emitted mid-frame at byte %d: %q", pos, body)
		}
		searched = pos + len(marker)
	}
	if stripped := strings.ReplaceAll(body, marker, ""); stripped != "data: long-output\n\n" {
		t.Fatalf("stream payload corrupted: %q", body)
	}
}

func TestSerializedStreamWriterNoKeepAliveAfterDone(t *testing.T) {
	rec := httptest.NewRecorder()
	w := newSerializedStreamWriter(rec, 10*time.Millisecond)
	if _, err := w.Write([]byte("data: [DONE]\n\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	w.Close()
	if body := rec.Body.String(); !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("keep-alive emitted after [DONE]: %q", body)
	}
}
