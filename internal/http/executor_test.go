// SPDX-License-Identifier: AGPL-3.0-only
package http

import (
	"context"
	"io"
	netHTTP "net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jolks/mcp-cron/internal/logging"
	"github.com/jolks/mcp-cron/internal/model"
)

func testLogger() *logging.Logger {
	return logging.New(logging.Options{Output: io.Discard, Level: logging.Fatal})
}

func TestNewHTTPExecutor(t *testing.T) {
	if NewHTTPExecutor(nil, testLogger()) == nil {
		t.Fatal("NewHTTPExecutor returned nil")
	}
}

func TestExecuteHTTP_Success(t *testing.T) {
	var gotMethod, gotPath, gotAuth, gotBody string
	srv := httptest.NewServer(netHTTP.HandlerFunc(func(w netHTTP.ResponseWriter, r *netHTTP.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(netHTTP.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	exec := NewHTTPExecutor(nil, testLogger())
	task := &model.Task{
		ID:      "task_http_1",
		URL:     srv.URL + "/hook",
		Method:  "POST",
		Headers: map[string]string{"Authorization": "Bearer t"},
		Body:    `{"hello":"world"}`,
	}

	result := exec.ExecuteHTTP(context.Background(), task, 5*time.Second)
	if result.ExitCode != 200 {
		t.Errorf("ExitCode = %d, want 200", result.ExitCode)
	}
	if result.Error != "" {
		t.Errorf("Error = %q, want empty", result.Error)
	}
	if !strings.Contains(result.Output, `"ok":true`) {
		t.Errorf("Output = %q, missing body", result.Output)
	}
	if result.URL != task.URL {
		t.Errorf("URL = %q, want %q", result.URL, task.URL)
	}
	if gotMethod != "POST" || gotPath != "/hook" || gotAuth != "Bearer t" || gotBody != `{"hello":"world"}` {
		t.Errorf("server saw method=%s path=%s auth=%s body=%s", gotMethod, gotPath, gotAuth, gotBody)
	}
}

func TestExecuteHTTP_DefaultsToPOST(t *testing.T) {
	var gotMethod string
	srv := httptest.NewServer(netHTTP.HandlerFunc(func(w netHTTP.ResponseWriter, r *netHTTP.Request) {
		gotMethod = r.Method
		w.WriteHeader(netHTTP.StatusNoContent)
	}))
	defer srv.Close()

	exec := NewHTTPExecutor(nil, testLogger())
	result := exec.ExecuteHTTP(context.Background(), &model.Task{
		ID:  "task_http_default",
		URL: srv.URL,
	}, 5*time.Second)

	if result.ExitCode != 204 {
		t.Errorf("ExitCode = %d, want 204", result.ExitCode)
	}
	if gotMethod != "POST" {
		t.Errorf("server saw method=%s, want POST", gotMethod)
	}
}

func TestExecuteHTTP_Non2xxBecomesError(t *testing.T) {
	srv := httptest.NewServer(netHTTP.HandlerFunc(func(w netHTTP.ResponseWriter, _ *netHTTP.Request) {
		w.WriteHeader(netHTTP.StatusInternalServerError)
		_, _ = io.WriteString(w, "boom")
	}))
	defer srv.Close()

	exec := NewHTTPExecutor(nil, testLogger())
	result := exec.ExecuteHTTP(context.Background(), &model.Task{
		ID: "task_http_500", URL: srv.URL, Method: "POST",
	}, 5*time.Second)

	if result.ExitCode != 500 {
		t.Errorf("ExitCode = %d, want 500", result.ExitCode)
	}
	if !strings.Contains(result.Error, "non-2xx") {
		t.Errorf("Error = %q, want 'non-2xx'", result.Error)
	}
	if result.Output != "boom" {
		t.Errorf("Output = %q, want 'boom'", result.Output)
	}
}

func TestExecuteHTTP_TransportErrorPopulatesError(t *testing.T) {
	exec := NewHTTPExecutor(nil, testLogger())
	// Bind to an invalid URL host so net/http fails fast at the transport layer.
	result := exec.ExecuteHTTP(context.Background(), &model.Task{
		ID: "task_http_bad", URL: "http://127.0.0.1:1/", Method: "POST",
	}, 1*time.Second)

	if result.Error == "" {
		t.Error("expected transport error, got none")
	}
	if result.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0 for transport error", result.ExitCode)
	}
}

func TestExecuteHTTP_Timeout(t *testing.T) {
	srv := httptest.NewServer(netHTTP.HandlerFunc(func(w netHTTP.ResponseWriter, _ *netHTTP.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(netHTTP.StatusOK)
	}))
	defer srv.Close()

	exec := NewHTTPExecutor(nil, testLogger())
	result := exec.ExecuteHTTP(context.Background(), &model.Task{
		ID: "task_http_timeout", URL: srv.URL, Method: "GET",
	}, 100*time.Millisecond)

	if result.Error == "" {
		t.Error("expected timeout error, got none")
	}
}

func TestExecuteHTTP_BodyTruncation(t *testing.T) {
	srv := httptest.NewServer(netHTTP.HandlerFunc(func(w netHTTP.ResponseWriter, _ *netHTTP.Request) {
		// Write 2x MaxBodyPreview.
		big := strings.Repeat("x", MaxBodyPreview*2)
		_, _ = io.WriteString(w, big)
	}))
	defer srv.Close()

	exec := NewHTTPExecutor(nil, testLogger())
	result := exec.ExecuteHTTP(context.Background(), &model.Task{
		ID: "task_http_big", URL: srv.URL, Method: "GET",
	}, 5*time.Second)

	if result.ExitCode != 200 {
		t.Fatalf("ExitCode = %d, want 200", result.ExitCode)
	}
	if !strings.HasSuffix(result.Output, "... (truncated)") {
		end := len(result.Output)
		start := end - 30
		if start < 0 {
			start = 0
		}
		t.Errorf("Output should be truncated, got len=%d suffix=%q", end, result.Output[start:])
	}
}

func TestExecute_ValidatesRequiredFields(t *testing.T) {
	exec := NewHTTPExecutor(nil, testLogger())
	if err := exec.Execute(context.Background(), &model.Task{ID: "x"}, time.Second); err == nil {
		t.Error("expected error when URL missing")
	}
	if err := exec.Execute(context.Background(), &model.Task{URL: "http://example"}, time.Second); err == nil {
		t.Error("expected error when ID missing")
	}
}

func TestExecute_PersistsOnSuccessAndFailure(t *testing.T) {
	store := &countingStore{}
	srv := httptest.NewServer(netHTTP.HandlerFunc(func(w netHTTP.ResponseWriter, _ *netHTTP.Request) {
		w.WriteHeader(netHTTP.StatusOK)
	}))
	defer srv.Close()

	exec := NewHTTPExecutor(store, testLogger())
	_ = exec.Execute(context.Background(), &model.Task{ID: "ok", URL: srv.URL}, 5*time.Second)
	_ = exec.Execute(context.Background(), &model.Task{ID: "bad", URL: "http://127.0.0.1:1/"}, 500*time.Millisecond)

	if got := atomic.LoadInt32(&store.saves); got != 2 {
		t.Errorf("SaveResult called %d times, want 2", got)
	}
}

// countingStore is a minimal model.ResultStore that counts SaveResult calls.
type countingStore struct {
	saves int32
}

func (c *countingStore) SaveResult(_ *model.Result) error {
	atomic.AddInt32(&c.saves, 1)
	return nil
}
func (c *countingStore) GetLatestResult(_ string) (*model.Result, error)         { return nil, nil }
func (c *countingStore) GetResults(_ string, _ int) ([]*model.Result, error)     { return nil, nil }
func (c *countingStore) QueryDB(_ context.Context, _ string) ([]map[string]interface{}, error) {
	return nil, nil
}
func (c *countingStore) GetSchema() (string, error) { return "", nil }
func (c *countingStore) Close() error               { return nil }
