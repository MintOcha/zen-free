package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

var errEmptyChatStream = errors.New("upstream stream was empty")
var errChatStreamInterrupted = errors.New("upstream response ended before completion")

func aggregateChatStream(body io.Reader, model string) (map[string]any, error) {
	state, err := newResponseSSEState()
	if err != nil {
		return nil, err
	}
	var (
		id         string
		created    int64
		createdSet bool
		chunkModel string
		role       string
		content    strings.Builder
		reasoning  strings.Builder
		toolCalls  []*responseToolCall
		finish     string
		usage      map[string]any
		sawPayload int
	)
	process := func(payload []byte) (bool, error) {
		if string(payload) == "[DONE]" {
			return true, nil
		}
		sawPayload++
		var chunk map[string]any
		if err := json.Unmarshal(payload, &chunk); err != nil {
			return false, nil
		}
		if eventErr, exists := chunk["error"]; exists && eventErr != nil {
			return false, errors.New(upstreamErrorMessage(eventErr))
		}
		if id == "" {
			id = stringValue(chunk["id"])
		}
		if !createdSet {
			if value, ok := chunk["created"].(float64); ok && value > 0 {
				created, createdSet = int64(value), true
			}
		}
		if value := stringValue(chunk["model"]); value != "" {
			chunkModel = value
		}
		if rawUsage, exists := chunk["usage"]; exists && rawUsage != nil {
			if usageMap, ok := rawUsage.(map[string]any); ok {
				usage = usageMap
			}
		}
		choices, _ := chunk["choices"].([]any)
		for i, rawChoice := range choices {
			choice, ok := rawChoice.(map[string]any)
			if !ok {
				continue
			}
			if index, hasIndex := choice["index"].(float64); hasIndex {
				if int(index) != 0 {
					continue
				}
			} else if i != 0 {
				continue
			}
			delta, ok := choice["delta"].(map[string]any)
			if !ok {
				continue
			}
			if role == "" {
				role = stringValue(delta["role"])
			}
			content.WriteString(stringValue(delta["content"]))
			reasoning.WriteString(stringValue(delta["reasoning_content"]))
			reasoning.WriteString(stringValue(delta["reasoning"]))
			if calls, ok := delta["tool_calls"].([]any); ok {
				for _, rawCall := range calls {
					call, ok := rawCall.(map[string]any)
					if !ok {
						continue
					}
					target := len(toolCalls)
					if rawIndex, ok := call["index"].(float64); ok {
						callIndex := int(rawIndex)
						if callIndex < 0 || callIndex >= 4096 {
							continue
						}
						target = callIndex
					}
					for len(toolCalls) <= target {
						toolCalls = append(toolCalls, &responseToolCall{Type: "function"})
					}
					merged := toolCalls[target]
					if merged.ID == "" {
						merged.ID = stringValue(call["id"])
					}
					if fn, ok := call["function"].(map[string]any); ok {
						if name := stringValue(fn["name"]); name != "" {
							merged.Function.Name = name
						}
						merged.Function.Arguments += stringValue(fn["arguments"])
					}
				}
			}
			if finish == "" {
				finish = stringValue(choice["finish_reason"])
			}
		}
		return false, nil
	}
	reader := bufio.NewReaderSize(body, 64*1024)
	var dataLines []string
	done := false
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
	var scanErr error
	for {
		line, readErr := reader.ReadString('\n')
		if len(line) > 0 {
			trimmed := strings.TrimRight(line, "\r\n")
			if trimmed == "" {
				if err := processEvent(); err != nil {
					return nil, err
				}
				if done {
					break
				}
			} else if strings.HasPrefix(trimmed, "data:") {
				value := strings.TrimPrefix(trimmed, "data:")
				dataLines = append(dataLines, strings.TrimPrefix(value, " "))
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				scanErr = readErr
			}
			break
		}
	}
	if !done {
		if err := processEvent(); err != nil {
			return nil, err
		}
	}
	if sawPayload == 0 {
		return nil, errEmptyChatStream
	}
	if scanErr != nil || (finish == "" && !done) {
		return nil, errChatStreamInterrupted
	}
	if role == "" {
		role = "assistant"
	}
	if id == "" {
		id = state.id
	}
	if !createdSet {
		created = state.created
	}
	outModel := model
	if outModel == "" {
		outModel = chunkModel
	}
	if finish == "" {
		if len(toolCalls) > 0 {
			finish = "tool_calls"
		} else {
			finish = "stop"
		}
	}
	usageOut := usage
	if usageOut == nil {
		usageOut = map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
	}
	message := map[string]any{"role": role, "content": content.String()}
	if reasoning.Len() > 0 {
		message["reasoning_content"] = reasoning.String()
	}
	message["tool_calls"] = toolCalls
	completion := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": created,
		"model":   outModel,
		"choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}},
		"usage":   usageOut,
	}
	return completion, nil
}
