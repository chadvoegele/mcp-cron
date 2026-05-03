// SPDX-License-Identifier: AGPL-3.0-only

// Package http implements the HTTP/webhook task executor.
//
// HTTP tasks issue an HTTP request and capture the response. The result
// shape reuses model.Result without new fields: ExitCode holds the HTTP
// status code (0 if no response), Output holds a body preview, Error
// holds the transport error or a "non-2xx status N" message, Duration
// captures the round-trip latency.
package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	netHTTP "net/http"
	"strings"
	"time"

	"github.com/jolks/mcp-cron/internal/logging"
	"github.com/jolks/mcp-cron/internal/model"
)

// MaxBodyPreview caps the response body captured into Result.Output. Larger
// responses are truncated with a "... (truncated)" suffix so the row stays
// query-friendly.
const MaxBodyPreview = 8 * 1024

// HTTPExecutor handles executing HTTP tasks.
type HTTPExecutor struct {
	resultStore model.ResultStore
	logger      *logging.Logger
	client      *netHTTP.Client
}

// NewHTTPExecutor creates a new HTTP executor. The HTTP client uses the
// per-request timeout passed to Execute, not a client-level one, so each
// task can have its own deadline.
func NewHTTPExecutor(store model.ResultStore, logger *logging.Logger) *HTTPExecutor {
	return &HTTPExecutor{
		resultStore: store,
		logger:      logger,
		client:      &netHTTP.Client{},
	}
}

// Execute implements model.Executor.
func (e *HTTPExecutor) Execute(ctx context.Context, task *model.Task, timeout time.Duration) error {
	if task.ID == "" || task.URL == "" {
		return fmt.Errorf("invalid task: missing ID or URL")
	}

	result := e.ExecuteHTTP(ctx, task, timeout)
	if result.Error != "" {
		return errors.New(result.Error)
	}
	return nil
}

// ExecuteHTTP performs the HTTP request and returns the resulting Result.
func (e *HTTPExecutor) ExecuteHTTP(ctx context.Context, task *model.Task, timeout time.Duration) *model.Result {
	method := strings.ToUpper(strings.TrimSpace(task.Method))
	if method == "" {
		method = netHTTP.MethodPost
	}

	result := &model.Result{
		TaskID:    task.ID,
		URL:       task.URL,
		StartTime: time.Now(),
	}

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var bodyReader io.Reader
	if task.Body != "" {
		bodyReader = strings.NewReader(task.Body)
	}

	req, err := netHTTP.NewRequestWithContext(execCtx, method, task.URL, bodyReader)
	if err != nil {
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime).String()
		result.Error = fmt.Sprintf("build request: %v", err)
		model.PersistAndLogResult(e.resultStore, result, e.logger)
		return result
	}
	for k, v := range task.Headers {
		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime).String()

	if err != nil {
		result.Error = err.Error()
		model.PersistAndLogResult(e.resultStore, result, e.logger)
		return result
	}
	defer func() { _ = resp.Body.Close() }()

	result.ExitCode = resp.StatusCode

	bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, MaxBodyPreview+1))
	if readErr != nil {
		result.Error = fmt.Sprintf("read response body: %v", readErr)
		model.PersistAndLogResult(e.resultStore, result, e.logger)
		return result
	}
	if len(bodyBytes) > MaxBodyPreview {
		result.Output = string(bodyBytes[:MaxBodyPreview]) + "... (truncated)"
	} else {
		result.Output = string(bodyBytes)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = fmt.Sprintf("non-2xx status: %d %s", resp.StatusCode, resp.Status)
	}

	model.PersistAndLogResult(e.resultStore, result, e.logger)
	return result
}
