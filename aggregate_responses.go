package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

var errEmptyResponsesStream = errors.New("upstream responses stream was empty")
var errResponsesStreamInterrupted = errors.New("upstream responses stream ended before completion")

func aggregateResponsesStream(body io.Reader) (map[string]any, error) {
	var final map[string]any
	sawPayload := 0
	done := false
	process := func(payload []byte) (bool, error) {
		if string(payload) == "[DONE]" {
			return true, nil
		}
		sawPayload++
		var event map[string]any
		if err := json.Unmarshal(payload, &event); err != nil {
			return false, nil
		}
		if eventErr, exists := event["error"]; exists && eventErr != nil {
			return false, errors.New(upstreamErrorMessage(eventErr))
		}
		if stringValue(event["type"]) != "response.completed" {
			return false, nil
		}
		if response, ok := event["response"].(map[string]any); ok {
			final = response
			return true, nil
		}
		return false, nil
	}
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var dataLines []string
	processEvent := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		payload := []byte(strings.Join(dataLines, "\n"))
		dataLines = dataLines[:0]
		finished, err := process(payload)
		if err != nil {
			return err
		}
		if finished {
			done = true
		}
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := processEvent(); err != nil {
				return nil, err
			}
			if done {
				break
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			value := strings.TrimPrefix(line, "data:")
			dataLines = append(dataLines, strings.TrimPrefix(value, " "))
		}
	}
	if !done {
		if err := processEvent(); err != nil {
			return nil, err
		}
	}
	if sawPayload == 0 {
		return nil, errEmptyResponsesStream
	}
	if scanner.Err() != nil || final == nil {
		return nil, errResponsesStreamInterrupted
	}
	return final, nil
}
