package asyncapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	openbindings "github.com/openbindings/openbindings-go"
)

const defaultTimeout = 30 * time.Second

func executeBindingWithDoc(ctx context.Context, input *openbindings.BindingExecutionInput, doc *Document) *openbindings.ExecuteOutput {
	start := time.Now()

	opID, err := parseRef(input.Ref)
	if err != nil {
		return openbindings.FailedOutput(start, "invalid_ref", err.Error())
	}

	asyncOp, ok := doc.Operations[opID]
	if !ok {
		return openbindings.FailedOutput(start, "operation_not_found", fmt.Sprintf("operation %q not in AsyncAPI doc", opID))
	}

	serverURL, protocol, err := resolveServer(doc, input.Options)
	if err != nil {
		return openbindings.FailedOutput(start, "no_server", err.Error())
	}

	channelName := extractRefName(asyncOp.Channel.Ref)
	channel, hasChannel := doc.Channels[channelName]

	address := channelName
	if hasChannel && channel.Address != "" {
		address = channel.Address
	}

	switch asyncOp.Action {
	case "receive":
		return executeReceive(ctx, serverURL, protocol, address, input, start)
	case "send":
		return executeSend(ctx, serverURL, protocol, address, input, start)
	default:
		return openbindings.FailedOutput(start, "unsupported_action", fmt.Sprintf("unknown action %q", asyncOp.Action))
	}
}

func subscribeBindingWithDoc(ctx context.Context, input *openbindings.BindingExecutionInput, doc *Document) (<-chan openbindings.StreamEvent, error) {
	opID, err := parseRef(input.Ref)
	if err != nil {
		return nil, fmt.Errorf("parse ref: %w", err)
	}

	asyncOp, ok := doc.Operations[opID]
	if !ok {
		return nil, fmt.Errorf("operation %q not in AsyncAPI doc", opID)
	}

	if asyncOp.Action != "receive" {
		return nil, fmt.Errorf("streaming not supported for action %q (only receive)", asyncOp.Action)
	}

	serverURL, protocol, err := resolveServer(doc, input.Options)
	if err != nil {
		return nil, fmt.Errorf("resolve server: %w", err)
	}

	channelName := extractRefName(asyncOp.Channel.Ref)
	channel, hasChannel := doc.Channels[channelName]
	address := channelName
	if hasChannel && channel.Address != "" {
		address = channel.Address
	}

	switch protocol {
	case "http", "https":
		return subscribeSSE(ctx, serverURL, address, input)
	default:
		return nil, fmt.Errorf("streaming not supported for protocol %q (supported: http, https)", protocol)
	}
}

func parseRef(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", fmt.Errorf("empty ref")
	}

	const prefix = "#/operations/"
	if strings.HasPrefix(ref, prefix) {
		opID := strings.TrimPrefix(ref, prefix)
		if opID == "" {
			return "", fmt.Errorf("empty operation ID in ref %q", ref)
		}
		return opID, nil
	}

	return ref, nil
}

func resolveServer(doc *Document, opts *openbindings.ExecutionOptions) (url string, protocol string, err error) {
	if opts != nil && opts.Metadata != nil {
		if base, ok := opts.Metadata["baseURL"].(string); ok && base != "" {
			proto := "http"
			if strings.HasPrefix(base, "https://") {
				proto = "https"
			} else if strings.HasPrefix(base, "wss://") {
				proto = "wss"
			} else if strings.HasPrefix(base, "ws://") {
				proto = "ws"
			}
			return strings.TrimRight(base, "/"), proto, nil
		}
	}

	serverNames := make([]string, 0, len(doc.Servers))
	for name := range doc.Servers {
		serverNames = append(serverNames, name)
	}
	sort.Strings(serverNames)

	for _, name := range serverNames {
		server := doc.Servers[name]
		proto := strings.ToLower(server.Protocol)
		host := server.Host
		pathname := server.PathName

		switch proto {
		case "http", "https", "ws", "wss":
			url := proto + "://" + host
			if pathname != "" {
				url += pathname
			}
			return strings.TrimRight(url, "/"), proto, nil
		}
	}

	return "", "", fmt.Errorf("no supported server found (need http, https, ws, or wss protocol)")
}

func executeReceive(ctx context.Context, serverURL, protocol, address string, input *openbindings.BindingExecutionInput, start time.Time) *openbindings.ExecuteOutput {
	maxEvents := 1
	if input.Input != nil {
		if m, ok := input.Input.(map[string]any); ok {
			if n, ok := m["maxEvents"].(float64); ok && n > 0 {
				maxEvents = int(n)
			}
		}
	}

	switch protocol {
	case "http", "https":
		return executeSSESubscribe(ctx, serverURL, address, maxEvents, input, start)
	default:
		return openbindings.FailedOutput(start, "unsupported_protocol",
			fmt.Sprintf("receive not supported for protocol %q (supported: http, https)", protocol))
	}
}

func executeSend(ctx context.Context, serverURL, protocol, address string, input *openbindings.BindingExecutionInput, start time.Time) *openbindings.ExecuteOutput {
	switch protocol {
	case "http", "https":
		return executeHTTPSend(ctx, serverURL, address, input, start)
	default:
		return openbindings.FailedOutput(start, "unsupported_protocol",
			fmt.Sprintf("send not supported for protocol %q (supported: http, https)", protocol))
	}
}

func executeSSESubscribe(ctx context.Context, serverURL, address string, maxEvents int, input *openbindings.BindingExecutionInput, start time.Time) *openbindings.ExecuteOutput {
	result := doSSESubscribe(ctx, serverURL, address, maxEvents, input, start)
	if result.Status == http.StatusUnauthorized && result.Error != nil {
		if resolveAsyncAPIContext(ctx, input, serverURL) {
			result = doSSESubscribe(ctx, serverURL, address, maxEvents, input, start)
		}
	}
	return result
}

func doSSESubscribe(ctx context.Context, serverURL, address string, maxEvents int, input *openbindings.BindingExecutionInput, start time.Time) *openbindings.ExecuteOutput {
	url := serverURL + "/" + strings.TrimLeft(address, "/")

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return openbindings.FailedOutput(start, "request_build_failed", err.Error())
	}
	req.Header.Set("Accept", "text/event-stream")
	applyHTTPContext(req, input.Context, input.Options)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return openbindings.FailedOutput(start, "sse_connect_failed", err.Error())
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return openbindings.HTTPErrorOutput(start, resp.StatusCode, resp.Status)
	}

	var events []any
	scanner := bufio.NewScanner(resp.Body)
	var dataLines []string

	for scanner.Scan() && len(events) < maxEvents {
		line := scanner.Text()

		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			continue
		}

		if line == "" && len(dataLines) > 0 {
			events = append(events, parseSSEPayload(dataLines))
			dataLines = dataLines[:0]
		}
	}

	if len(dataLines) > 0 {
		events = append(events, parseSSEPayload(dataLines))
	}

	var output any
	if len(events) == 1 {
		output = events[0]
	} else {
		output = events
	}

	return &openbindings.ExecuteOutput{
		Output:     output,
		Status:     0,
		DurationMs: time.Since(start).Milliseconds(),
	}
}

func subscribeSSE(ctx context.Context, serverURL, address string, input *openbindings.BindingExecutionInput) (<-chan openbindings.StreamEvent, error) {
	sseURL := serverURL + "/" + strings.TrimLeft(address, "/")

	req, err := http.NewRequestWithContext(ctx, "GET", sseURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	applyHTTPContext(req, input.Context, input.Options)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SSE connect: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("SSE endpoint returned HTTP %d", resp.StatusCode)
	}

	ch := make(chan openbindings.StreamEvent)
	go func() {
		defer func() { _ = resp.Body.Close() }()
		defer close(ch)

		scanner := bufio.NewScanner(resp.Body)
		var dataLines []string

		for scanner.Scan() {
			if ctx.Err() != nil {
				return
			}

			line := scanner.Text()

			if strings.HasPrefix(line, "data:") {
				dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
				continue
			}

			if line == "" && len(dataLines) > 0 {
				ev := parseSSEPayload(dataLines)
				dataLines = dataLines[:0]
				select {
				case ch <- openbindings.StreamEvent{Data: ev}:
				case <-ctx.Done():
					return
				}
			}
		}

		if len(dataLines) > 0 {
			select {
			case ch <- openbindings.StreamEvent{Data: parseSSEPayload(dataLines)}:
			case <-ctx.Done():
			}
		}

		if err := scanner.Err(); err != nil && ctx.Err() == nil {
			select {
			case ch <- openbindings.StreamEvent{Error: &openbindings.ExecuteError{Code: "stream_error", Message: err.Error()}}:
			case <-ctx.Done():
			}
		}
	}()

	return ch, nil
}

func executeHTTPSend(ctx context.Context, serverURL, address string, input *openbindings.BindingExecutionInput, start time.Time) *openbindings.ExecuteOutput {
	result := doHTTPSend(ctx, serverURL, address, input, start)
	if result.Status == http.StatusUnauthorized && result.Error != nil {
		if resolveAsyncAPIContext(ctx, input, serverURL) {
			result = doHTTPSend(ctx, serverURL, address, input, start)
		}
	}
	return result
}

func doHTTPSend(ctx context.Context, serverURL, address string, input *openbindings.BindingExecutionInput, start time.Time) *openbindings.ExecuteOutput {
	url := serverURL + "/" + strings.TrimLeft(address, "/")

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	var bodyData []byte
	if input.Input != nil {
		var marshalErr error
		bodyData, marshalErr = json.Marshal(input.Input)
		if marshalErr != nil {
			return openbindings.FailedOutput(start, "body_marshal_failed", marshalErr.Error())
		}
	} else {
		bodyData = []byte("{}")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyData))
	if err != nil {
		return openbindings.FailedOutput(start, "request_build_failed", err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	applyHTTPContext(req, input.Context, input.Options)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return openbindings.FailedOutput(start, "request_failed", err.Error())
	}
	defer func() { _ = resp.Body.Close() }()

	duration := time.Since(start).Milliseconds()

	if resp.StatusCode >= 400 {
		errOutput := openbindings.HTTPErrorOutput(start, resp.StatusCode, resp.Status)
		if body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20)); readErr == nil && len(body) > 0 {
			var parsed any
			if json.Unmarshal(body, &parsed) == nil {
				errOutput.Output = parsed
			} else {
				errOutput.Output = string(body)
			}
		}
		return errOutput
	}

	if resp.StatusCode == 202 || resp.StatusCode == 204 {
		return &openbindings.ExecuteOutput{
			Status:     0,
			DurationMs: duration,
		}
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	var output any
	if len(body) == 0 || json.Unmarshal(body, &output) != nil {
		return &openbindings.ExecuteOutput{
			Status:     0,
			DurationMs: duration,
		}
	}

	return &openbindings.ExecuteOutput{
		Output:     output,
		Status:     0,
		DurationMs: duration,
	}
}

// resolveAsyncAPIContext prompts for a bearer token on 401 and updates context.
func resolveAsyncAPIContext(ctx context.Context, input *openbindings.BindingExecutionInput, serverURL string) bool {
	if input.Callbacks == nil || input.Callbacks.Prompt == nil {
		return false
	}

	value, err := input.Callbacks.Prompt(ctx, fmt.Sprintf("Enter bearer token for %s", serverURL), &openbindings.PromptOptions{
		Label:  "bearerToken",
		Secret: true,
	})
	if err != nil || value == "" {
		return false
	}

	if input.Context == nil {
		input.Context = make(map[string]any)
	}
	input.Context["bearerToken"] = value

	if input.Store != nil {
		_ = input.Store.Set(ctx, openbindings.NormalizeContextKey(serverURL), input.Context)
	}

	return true
}

func parseSSEPayload(dataLines []string) any {
	raw := strings.Join(dataLines, "\n")
	var parsed any
	if json.Unmarshal([]byte(raw), &parsed) == nil {
		return parsed
	}
	return raw
}

// applyHTTPContext applies opaque binding context (credentials via well-known
// fields) and execution options (headers, cookies) to an HTTP request.
func applyHTTPContext(req *http.Request, bindCtx map[string]any, opts *openbindings.ExecutionOptions) {
	if token := openbindings.ContextBearerToken(bindCtx); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if u, p, ok := openbindings.ContextBasicAuth(bindCtx); ok {
		req.SetBasicAuth(u, p)
	} else if key := openbindings.ContextAPIKey(bindCtx); key != "" {
		req.Header.Set("Authorization", "ApiKey "+key)
	}

	if opts != nil {
		for k, v := range opts.Headers {
			req.Header.Set(k, v)
		}
		for k, v := range opts.Cookies {
			req.AddCookie(&http.Cookie{Name: k, Value: v})
		}
	}
}
