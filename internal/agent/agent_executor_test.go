// SPDX-License-Identifier: AGPL-3.0-only
package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jolks/mcp-cron/internal/config"
	"github.com/jolks/mcp-cron/internal/logging"
	"github.com/jolks/mcp-cron/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func testLogger() *logging.Logger {
	return logging.New(logging.Options{Output: io.Discard, Level: logging.Fatal})
}

// TestAgentExecutor is a test structure with a mockable RunTask function
type TestAgentExecutor struct {
	*AgentExecutor
	mockRunTask func(ctx context.Context, t *model.Task, cfg *config.Config) (string, error)
}

// NewTestAgentExecutor creates a test executor with a mockable RunTask function
func NewTestAgentExecutor(mockFunc func(ctx context.Context, t *model.Task, cfg *config.Config) (string, error)) *TestAgentExecutor {
	// Create a default config for testing
	cfg := config.DefaultConfig()

	return &TestAgentExecutor{
		AgentExecutor: NewAgentExecutor(cfg, nil, testLogger()),
		mockRunTask:   mockFunc,
	}
}

// ExecuteAgentTask overrides the base implementation to use the mock function
func (tae *TestAgentExecutor) ExecuteAgentTask(
	ctx context.Context,
	taskID string,
	prompt string,
	timeout time.Duration,
) *model.Result {
	result := &model.Result{
		StartTime: time.Now(),
		TaskID:    taskID,
		Prompt:    prompt,
	}

	// Create a context with timeout
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Create a task structure for RunTask
	task := &model.Task{
		ID:     taskID,
		Prompt: prompt,
	}

	// Execute the task using the mock function
	output, err := tae.mockRunTask(execCtx, task, tae.config)

	// Update result fields
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime).String()

	if err != nil {
		result.Error = err.Error()
		result.ExitCode = 1
		result.Output = fmt.Sprintf("Error executing AI task: %v", err)
	} else {
		result.Output = output
		result.ExitCode = 0
	}

	return result
}

func TestExecuteAgentTask(t *testing.T) {
	// Create mock output
	mockOutput := "This is a test result from the AI agent"

	// Create test executor with mock function
	executor := NewTestAgentExecutor(func(ctx context.Context, task *model.Task, cfg *config.Config) (string, error) {
		// Verify that the task has the expected fields
		if task.ID != "test-task-id" {
			t.Errorf("Expected task ID 'test-task-id', got '%s'", task.ID)
		}
		// Return mock output
		return mockOutput, nil
	})

	// Set up test parameters
	ctx := context.Background()
	taskID := "test-task-id"
	prompt := "Test prompt for the AI agent"
	timeout := 5 * time.Second

	// Execute the agent task
	result := executor.ExecuteAgentTask(ctx, taskID, prompt, timeout)

	// Verify result
	if result.TaskID != taskID {
		t.Errorf("Expected TaskID %s, got %s", taskID, result.TaskID)
	}
	if result.ExitCode != 0 {
		t.Errorf("Expected ExitCode 0, got %d", result.ExitCode)
	}
	if result.Error != "" {
		t.Errorf("Expected empty Error, got %s", result.Error)
	}
	if result.Output != mockOutput {
		t.Errorf("Expected Output '%s', got '%s'", mockOutput, result.Output)
	}
}

func TestExecuteAgentTaskWithError(t *testing.T) {
	// Create a mock error
	mockError := "test error from AI agent"

	// Create test executor with mock function that returns an error
	executor := NewTestAgentExecutor(func(ctx context.Context, task *model.Task, cfg *config.Config) (string, error) {
		return "", errors.New(mockError)
	})

	// Set up test parameters
	ctx := context.Background()
	taskID := "test-task-error"
	prompt := "Test prompt for the AI agent"
	timeout := 5 * time.Second

	// Execute the agent task
	result := executor.ExecuteAgentTask(ctx, taskID, prompt, timeout)

	// Verify result shows the error
	if result.ExitCode != 1 {
		t.Errorf("Expected ExitCode 1, got %d", result.ExitCode)
	}
	if result.Error != mockError {
		t.Errorf("Expected Error '%s', got '%s'", mockError, result.Error)
	}
}

func TestExecuteAgentTaskACPResultLifecycle(t *testing.T) {
	store := newMockResultStore()
	cfg := config.DefaultConfig()
	cfg.AI.Provider = config.ProviderACP
	cfg.AI.ACPSocket = startACPResultServer(t, "completed output", "end_turn")
	cfg.AI.ACPCWD = "/workspace"
	executor := NewAgentExecutor(cfg, store, testLogger())

	result := executor.ExecuteAgentTask(context.Background(), "acp-success", "complete this", time.Second)
	if result.Error != "" {
		t.Fatalf("Error = %q, want empty", result.Error)
	}
	if result.Output != "completed output" {
		t.Fatalf("Output = %q, want %q", result.Output, "completed output")
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	if result.StartTime.IsZero() || result.EndTime.IsZero() || !result.EndTime.After(result.StartTime) || result.Duration == "" {
		t.Fatalf("invalid timing fields: start=%v end=%v duration=%q", result.StartTime, result.EndTime, result.Duration)
	}

	persisted, err := store.GetLatestResult("acp-success")
	if err != nil {
		t.Fatalf("GetLatestResult returned error: %v", err)
	}
	if persisted != result {
		t.Fatalf("persisted result = %#v, want the executed result %#v", persisted, result)
	}
}

func TestExecuteAgentTaskACPFailurePreservesPartialOutput(t *testing.T) {
	store := newMockResultStore()
	cfg := config.DefaultConfig()
	cfg.AI.Provider = config.ProviderACP
	cfg.AI.ACPSocket = startACPResultServer(t, "partial output", "max_tokens")
	cfg.AI.ACPCWD = "/workspace"
	executor := NewAgentExecutor(cfg, store, testLogger())

	result := executor.ExecuteAgentTask(context.Background(), "acp-failure", "stop after a partial response", time.Second)
	if !strings.Contains(result.Error, "max_tokens") {
		t.Fatalf("Error = %q, want max_tokens diagnostic", result.Error)
	}
	if result.Output != "partial output" {
		t.Fatalf("Output = %q, want partial ACP output", result.Output)
	}
	if result.ExitCode != 1 {
		t.Fatalf("ExitCode = %d, want 1", result.ExitCode)
	}

	persisted, err := store.GetLatestResult("acp-failure")
	if err != nil {
		t.Fatalf("GetLatestResult returned error: %v", err)
	}
	if persisted == nil || persisted.Output != "partial output" || persisted.ExitCode != 1 {
		t.Fatalf("persisted result = %#v, want failed result with partial output", persisted)
	}
}

func TestExecuteAgentTaskACPFailureUsesErrorOutputFallback(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = config.ProviderACP
	cfg.AI.ACPSocket = startACPResultServer(t, "", "refusal")
	cfg.AI.ACPCWD = "/workspace"
	executor := NewAgentExecutor(cfg, nil, testLogger())

	result := executor.ExecuteAgentTask(context.Background(), "acp-empty-failure", "fail without output", time.Second)
	if result.Output == "" || !strings.Contains(result.Output, "Error executing AI task") {
		t.Fatalf("Output = %q, want error-output fallback", result.Output)
	}
	if result.ExitCode != 1 || result.Error == "" {
		t.Fatalf("failed result = %#v, want exit code 1 and an error", result)
	}
}

func TestExecuteAgentTaskACPTimeoutPersistsPartialOutput(t *testing.T) {
	promptReceived := make(chan struct{})
	cancelReceived := make(chan struct{}, 1)
	socketPath := startACPServer(t, func(conn net.Conn, scanner *bufio.Scanner) error {
		request, err := readACPRequest(scanner, nil)
		if err != nil {
			return err
		}
		if err := writeACPResponse(conn, request.ID, `{"protocolVersion":1,"authMethods":[]}`); err != nil {
			return err
		}
		request, err = readACPRequest(scanner, nil)
		if err != nil {
			return err
		}
		if err := writeACPResponse(conn, request.ID, `{"sessionId":"timeout-test"}`); err != nil {
			return err
		}
		request, err = readACPRequest(scanner, nil)
		if err != nil {
			return err
		}
		if request.Method != "session/prompt" {
			return fmt.Errorf("request method = %q, want session/prompt", request.Method)
		}
		if _, err := fmt.Fprintln(conn, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"timeout-test","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"before timeout"}}}}`); err != nil {
			return err
		}
		close(promptReceived)

		for scanner.Scan() {
			var notification struct {
				Method string `json:"method"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &notification); err != nil {
				return err
			}
			if notification.Method == "session/cancel" {
				cancelReceived <- struct{}{}
				return nil
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		return errors.New("client closed before sending session/cancel")
	})

	store := newMockResultStore()
	cfg := config.DefaultConfig()
	cfg.AI.Provider = config.ProviderACP
	cfg.AI.ACPSocket = socketPath
	cfg.AI.ACPCWD = "/workspace"
	executor := NewAgentExecutor(cfg, store, testLogger())

	result := executor.ExecuteAgentTask(context.Background(), "acp-timeout", "wait for timeout", 100*time.Millisecond)
	if result.Error == "" {
		t.Fatal("Error is empty, want timeout/cancellation diagnostic")
	}
	if result.Output != "before timeout" {
		t.Fatalf("Output = %q, want partial output", result.Output)
	}
	if result.ExitCode != 1 {
		t.Fatalf("ExitCode = %d, want 1", result.ExitCode)
	}
	select {
	case <-cancelReceived:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for session/cancel")
	}

	persisted, err := store.GetLatestResult("acp-timeout")
	if err != nil {
		t.Fatalf("GetLatestResult returned error: %v", err)
	}
	if persisted == nil || persisted.Output != "before timeout" || persisted.ExitCode != 1 || persisted.Error == "" {
		t.Fatalf("persisted result = %#v, want failed result with partial output", persisted)
	}
}

func TestExecuteAgentTaskACPBlockedPromptWritePersistsFailure(t *testing.T) {
	const timeout = 100 * time.Millisecond

	promptReady := make(chan struct{})
	releaseServer := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseServer) }) }

	server := newFakeACPAgent(t, func(conn net.Conn, scanner *bufio.Scanner) error {
		request, err := readACPRequest(scanner, nil)
		if err != nil {
			return err
		}
		if request.Method != "initialize" {
			return fmt.Errorf("first request method = %q, want initialize", request.Method)
		}
		if err := writeACPResponse(conn, request.ID, `{"protocolVersion":1,"authMethods":[]}`); err != nil {
			return err
		}

		request, err = readACPRequest(scanner, nil)
		if err != nil {
			return err
		}
		if request.Method != "session/new" {
			return fmt.Errorf("second request method = %q, want session/new", request.Method)
		}
		if err := writeACPResponse(conn, request.ID, `{"sessionId":"blocked-write"}`); err != nil {
			return err
		}

		// Do not read session/prompt. A sufficiently large request must block in
		// the client's socket write until the task context expires.
		close(promptReady)
		<-releaseServer
		return nil
	})
	t.Cleanup(release)

	store := newMockResultStore()
	cfg := config.DefaultConfig()
	cfg.AI.Provider = config.ProviderACP
	cfg.AI.ACPSocket = server.socketPath()
	cfg.AI.ACPCWD = "/workspace"
	executor := NewAgentExecutor(cfg, store, testLogger())

	prompt := strings.Repeat("x", 1<<20)
	resultCh := make(chan *model.Result, 1)
	go func() {
		resultCh <- executor.ExecuteAgentTask(context.Background(), "acp-blocked-write", prompt, timeout)
	}()

	select {
	case <-promptReady:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for session/new")
	}
	promptStarted := time.Now()

	var result *model.Result
	select {
	case result = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("ExecuteAgentTask blocked on a full ACP socket write")
	}
	release()
	server.waitForDisconnect(t)

	maxDuration := timeout + acpCancelWriteGrace + 500*time.Millisecond
	if elapsed := time.Since(promptStarted); elapsed > maxDuration {
		t.Fatalf("ExecuteAgentTask took %s after the prompt write started, want at most %s", elapsed, maxDuration)
	}
	if result.Error == "" {
		t.Fatal("Error is empty, want timeout/cancellation diagnostic")
	}
	if result.ExitCode != 1 {
		t.Fatalf("ExitCode = %d, want 1", result.ExitCode)
	}

	persisted, err := store.GetLatestResult("acp-blocked-write")
	if err != nil {
		t.Fatalf("GetLatestResult returned error: %v", err)
	}
	if persisted == nil || persisted.ExitCode != 1 || persisted.Error == "" {
		t.Fatalf("persisted result = %#v, want failed result", persisted)
	}
}

func startACPResultServer(t *testing.T, output, stopReason string) string {
	t.Helper()

	return startACPServer(t, func(conn net.Conn, scanner *bufio.Scanner) error {
		request, err := readACPRequest(scanner, nil)
		if err != nil {
			return err
		}
		if request.Method != "initialize" {
			return fmt.Errorf("first request method = %q, want initialize", request.Method)
		}
		if err := writeACPResponse(conn, request.ID, `{"protocolVersion":1,"authMethods":[]}`); err != nil {
			return err
		}

		request, err = readACPRequest(scanner, nil)
		if err != nil {
			return err
		}
		if request.Method != "session/new" {
			return fmt.Errorf("second request method = %q, want session/new", request.Method)
		}
		if err := writeACPResponse(conn, request.ID, `{"sessionId":"result-test"}`); err != nil {
			return err
		}

		request, err = readACPRequest(scanner, nil)
		if err != nil {
			return err
		}
		if request.Method != "session/prompt" {
			return fmt.Errorf("third request method = %q, want session/prompt", request.Method)
		}
		if output != "" {
			if _, err := fmt.Fprintf(conn, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"result-test","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":%q}}}}`+"\n", output); err != nil {
				return err
			}
		}
		return writeACPResponse(conn, request.ID, fmt.Sprintf(`{"stopReason":%q}`, stopReason))
	})
}

// Execute overrides the base implementation for testing
func (tae *TestAgentExecutor) Execute(ctx context.Context, task *model.Task, timeout time.Duration) error {
	if task.ID == "" || task.Prompt == "" {
		return fmt.Errorf("invalid task: missing ID or Prompt")
	}

	// Use our overridden ExecuteAgentTask
	result := tae.ExecuteAgentTask(ctx, task.ID, task.Prompt, timeout)
	if result.Error != "" {
		return fmt.Errorf("%s", result.Error)
	}

	return nil
}

func TestExecute(t *testing.T) {
	// Create mock output
	mockOutput := "Test output from Execute"

	// Create test executor with mock function
	testExecutor := NewTestAgentExecutor(func(ctx context.Context, task *model.Task, cfg *config.Config) (string, error) {
		return mockOutput, nil
	})

	// Set up test parameters
	ctx := context.Background()
	task := &model.Task{
		ID:     "test-execute-task",
		Type:   model.TypeAI,
		Prompt: "Test prompt for execution",
	}
	timeout := 5 * time.Second

	// Execute the task using our test executor's Execute method
	err := testExecutor.Execute(ctx, task, timeout)

	// Verify execution was successful
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}
}

// TestRunTaskIntegration updates the integration test to use config
func TestRunTaskIntegration(t *testing.T) {
	// Skip by default to avoid making actual API calls during routine testing
	if os.Getenv("MCP_CRON_ENABLE_OPENAI_TESTS") != "true" {
		t.Skip("Skipping OpenAI integration test. Set MCP_CRON_ENABLE_OPENAI_TESTS=true to run.")
	}

	cfg := config.DefaultConfig()
	cfg.AI.MCPConfigFilePath = filepath.Join(t.TempDir(), "mcp.json") // no real MCP servers

	cfg.AI.OpenAIAPIKey = os.Getenv("OPENAI_API_KEY")
	if cfg.AI.OpenAIAPIKey == "" {
		t.Skip("OPENAI_API_KEY environment variable not set")
	}

	// Create a simple task
	task := &model.Task{
		ID:     "integration-test",
		Prompt: "What is 2+2? Answer with just the number",
	}

	// Run the task
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	output, err := RunTask(ctx, task, cfg, nil)
	if err != nil {
		t.Fatalf("RunTask failed: %v", err)
	}

	// Verify we got some kind of response
	if output == "" {
		t.Error("Expected non-empty output from OpenAI API")
	}

	t.Logf("OpenAI API response: %s", output)
}

func TestRunTaskIntegrationAnthropic(t *testing.T) {
	// Skip by default to avoid making actual API calls during routine testing
	if os.Getenv("MCP_CRON_ENABLE_ANTHROPIC_TESTS") != "true" {
		t.Skip("Skipping Anthropic integration test. Set MCP_CRON_ENABLE_ANTHROPIC_TESTS=true to run.")
	}

	cfg := config.DefaultConfig()
	cfg.AI.MCPConfigFilePath = filepath.Join(t.TempDir(), "mcp.json") // no real MCP servers
	cfg.AI.Provider = "anthropic"
	cfg.AI.Model = "claude-haiku-4-5-20251001"

	cfg.AI.AnthropicAPIKey = os.Getenv("ANTHROPIC_API_KEY")
	if cfg.AI.AnthropicAPIKey == "" {
		t.Skip("ANTHROPIC_API_KEY environment variable not set")
	}

	// Create a simple task
	task := &model.Task{
		ID:     "integration-test-anthropic",
		Prompt: "What is 2+2? Answer with just the number",
	}

	// Run the task
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	output, err := RunTask(ctx, task, cfg, nil)
	if err != nil {
		t.Fatalf("RunTask failed: %v", err)
	}

	// Verify we got some kind of response
	if output == "" {
		t.Error("Expected non-empty output from Anthropic API")
	}

	t.Logf("Anthropic API response: %s", output)
}

// mockResultStore is a simple in-memory ResultStore for testing.
type mockResultStore struct {
	results map[string][]*model.Result
}

func newMockResultStore() *mockResultStore {
	return &mockResultStore{results: make(map[string][]*model.Result)}
}

func (m *mockResultStore) SaveResult(result *model.Result) error {
	m.results[result.TaskID] = append(m.results[result.TaskID], result)
	return nil
}

func (m *mockResultStore) GetLatestResult(taskID string) (*model.Result, error) {
	rs := m.results[taskID]
	if len(rs) == 0 {
		return nil, nil
	}
	return rs[len(rs)-1], nil
}

func (m *mockResultStore) GetResults(taskID string, limit int) ([]*model.Result, error) {
	rs := m.results[taskID]
	if limit > 0 && limit < len(rs) {
		return rs[len(rs)-limit:], nil
	}
	return rs, nil
}

func (m *mockResultStore) QueryDB(_ context.Context, _ string) ([]map[string]interface{}, error) {
	return nil, nil
}

func (m *mockResultStore) GetSchema() (string, error) { return "", nil }

func (m *mockResultStore) Close() error { return nil }

// TestRunTaskIntegration_InternalGetTaskResult_MCPNamespace verifies that
// the AI model can discover and use the internal get_task_result tool via
// the system message alone — the prompt does NOT mention the tool name or
// the task ID. The model must learn both from the system message.
func TestRunTaskIntegration_InternalGetTaskResult_MCPNamespace(t *testing.T) {
	if os.Getenv("MCP_CRON_ENABLE_OPENAI_TESTS") != "true" {
		t.Skip("Skipping OpenAI integration test. Set MCP_CRON_ENABLE_OPENAI_TESTS=true to run.")
	}

	cfg := config.DefaultConfig()
	cfg.AI.MCPConfigFilePath = filepath.Join(t.TempDir(), "mcp.json") // no real MCP servers
	cfg.AI.OpenAIAPIKey = os.Getenv("OPENAI_API_KEY")
	if cfg.AI.OpenAIAPIKey == "" {
		t.Skip("OPENAI_API_KEY environment variable not set")
	}

	// Use realistic opaque task IDs (production IDs are snowflake-like).
	const taskID = "task_1771138119398992000"

	// Seed the result store with multiple tasks so the model must pick
	// the right one using its own task ID from the system message.
	store := newMockResultStore()
	_ = store.SaveResult(&model.Result{
		TaskID:   "task_1771000000000000001",
		Output:   "Disk usage: 45% used, 120GB free",
		ExitCode: 0,
	})
	_ = store.SaveResult(&model.Result{
		TaskID:   taskID,
		Output:   "Temperature: 72F, Humidity: 45%, Condition: Sunny",
		ExitCode: 0,
	})
	_ = store.SaveResult(&model.Result{
		TaskID:   "task_1771000000000000003",
		Output:   "CPU load average: 0.42, Memory: 8.2GB/16GB",
		ExitCode: 0,
	})

	// Prompt does NOT mention get_task_result or the task ID.
	// The model must discover both from the system message.
	// Uses the real MCP config so external tools (browser, etc.) are loaded
	// alongside the internal get_task_result — matching production (48+ tools).
	task := &model.Task{
		ID:     taskID,
		Prompt: `Retrieve the results from your previous execution and summarize them.`,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	output, err := RunTask(ctx, task, cfg, store)
	if err != nil {
		t.Fatalf("RunTask failed: %v", err)
	}

	t.Logf("Model output:\n%s", output)

	// The model should have called get_task_result with its own task ID
	// and returned the weather data — not disk or CPU data from other tasks.
	if !strings.Contains(output, "72") && !strings.Contains(output, "Sunny") {
		t.Errorf("Expected output to contain data from the stored result (72 or Sunny), got: %s", output)
	}
}

func TestRunTaskIntegration_InternalGetTaskResult_MCPNamespaceAnthropic(t *testing.T) {
	if os.Getenv("MCP_CRON_ENABLE_ANTHROPIC_TESTS") != "true" {
		t.Skip("Skipping Anthropic integration test. Set MCP_CRON_ENABLE_ANTHROPIC_TESTS=true to run.")
	}

	cfg := config.DefaultConfig()
	cfg.AI.MCPConfigFilePath = filepath.Join(t.TempDir(), "mcp.json") // no real MCP servers
	cfg.AI.Provider = "anthropic"
	cfg.AI.Model = "claude-haiku-4-5-20251001"
	cfg.AI.AnthropicAPIKey = os.Getenv("ANTHROPIC_API_KEY")
	if cfg.AI.AnthropicAPIKey == "" {
		t.Skip("ANTHROPIC_API_KEY environment variable not set")
	}

	const taskID = "task_1771138119398992000"

	store := newMockResultStore()
	_ = store.SaveResult(&model.Result{
		TaskID:   "task_1771000000000000001",
		Output:   "Disk usage: 45% used, 120GB free",
		ExitCode: 0,
	})
	_ = store.SaveResult(&model.Result{
		TaskID:   taskID,
		Output:   "Temperature: 72F, Humidity: 45%, Condition: Sunny",
		ExitCode: 0,
	})
	_ = store.SaveResult(&model.Result{
		TaskID:   "task_1771000000000000003",
		Output:   "CPU load average: 0.42, Memory: 8.2GB/16GB",
		ExitCode: 0,
	})

	// Prompt does NOT mention get_task_result or the task ID.
	task := &model.Task{
		ID:     taskID,
		Prompt: `Retrieve the results from your previous execution and summarize them.`,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	output, err := RunTask(ctx, task, cfg, store)
	if err != nil {
		t.Fatalf("RunTask failed: %v", err)
	}

	t.Logf("Model output:\n%s", output)

	// The model should have called get_task_result and included the weather data
	if !strings.Contains(output, "72") && !strings.Contains(output, "Sunny") {
		t.Errorf("Expected output to contain data from the stored result (72 or Sunny), got: %s", output)
	}
}

// startTestMCPServer creates a test MCP server with known tools and returns
// the httptest server. Cleanup is registered via t.Cleanup.
func startTestMCPServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "test-tools-server", Version: "1.0.0"}, nil)
	srv.AddTool(&mcp.Tool{
		Name:        "get_weather",
		Description: "Get current weather for a location",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"location": map[string]interface{}{
					"type":        "string",
					"description": "City name",
				},
			},
			"required": []string{"location"},
		},
	}, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Sunny, 72F"}},
		}, nil
	})
	srv.AddTool(&mcp.Tool{
		Name:        "calculate",
		Description: "Perform a calculation",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"expression": map[string]interface{}{
					"type":        "string",
					"description": "Math expression",
				},
			},
			"required": []string{"expression"},
		},
	}, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "42"}},
		}, nil
	})

	handler := mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server {
		return srv
	}, nil)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	return ts
}

// writeTestMCPConfig writes a temporary MCP config JSON pointing to the given
// server URL and returns the config file path.
func writeTestMCPConfig(t *testing.T, serverURL string) string {
	t.Helper()
	cfg := map[string]interface{}{
		"mcpServers": map[string]interface{}{
			"test-server": map[string]interface{}{
				"url": serverURL,
			},
		},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Failed to marshal test MCP config: %v", err)
	}
	cfgPath := filepath.Join(t.TempDir(), "mcp.json")
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		t.Fatalf("Failed to write test MCP config: %v", err)
	}
	return cfgPath
}

func TestRunTaskIntegrationListTools(t *testing.T) {
	cases := []struct {
		name    string
		envFlag string
		setup   func(t *testing.T, cfg *config.Config)
	}{
		{
			name:    "OpenAI",
			envFlag: "MCP_CRON_ENABLE_OPENAI_TESTS",
			setup: func(t *testing.T, cfg *config.Config) {
				t.Helper()
				cfg.AI.OpenAIAPIKey = os.Getenv("OPENAI_API_KEY")
				if cfg.AI.OpenAIAPIKey == "" {
					t.Skip("OPENAI_API_KEY environment variable not set")
				}
			},
		},
		{
			name:    "Anthropic",
			envFlag: "MCP_CRON_ENABLE_ANTHROPIC_TESTS",
			setup: func(t *testing.T, cfg *config.Config) {
				t.Helper()
				cfg.AI.Provider = "anthropic"
				cfg.AI.Model = "claude-haiku-4-5-20251001"
				cfg.AI.AnthropicAPIKey = os.Getenv("ANTHROPIC_API_KEY")
				if cfg.AI.AnthropicAPIKey == "" {
					t.Skip("ANTHROPIC_API_KEY environment variable not set")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if os.Getenv(tc.envFlag) != "true" {
				t.Skipf("Skipping %s list-tools integration test. Set %s=true to run.", tc.name, tc.envFlag)
			}

			cfg := config.DefaultConfig()
			tc.setup(t, cfg)

			// Use a local test MCP server instead of real ~/.cursor/mcp.json
			ts := startTestMCPServer(t)
			cfg.AI.MCPConfigFilePath = writeTestMCPConfig(t, ts.URL)

			// Verify tools load from the test server
			tools, _, closeFn, err := buildToolsFromConfig(cfg)
			if closeFn != nil {
				defer closeFn()
			}
			if err != nil {
				t.Fatalf("Failed to build tools: %v", err)
			}
			if len(tools) == 0 {
				t.Fatal("Expected tools from test MCP server, got 0")
			}
			t.Logf("Loaded %d MCP tools from test server", len(tools))

			task := &model.Task{
				ID:     fmt.Sprintf("integration-test-%s-list-tools", strings.ToLower(tc.name)),
				Prompt: "List all the tools you have available. Just list their names, one per line. Do not call any tool.",
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			output, err := RunTask(ctx, task, cfg, nil)
			if err != nil {
				t.Fatalf("RunTask failed: %v", err)
			}
			if output == "" {
				t.Fatal("Expected non-empty output")
			}

			matched := 0
			for _, td := range tools {
				if strings.Contains(output, td.Name) {
					matched++
				}
			}
			if matched == 0 {
				t.Errorf("Expected output to mention at least one loaded tool, but none matched")
			}
			t.Logf("AI mentioned %d/%d loaded tools", matched, len(tools))
			t.Logf("%s listed tools:\n%s", tc.name, output)
		})
	}
}
