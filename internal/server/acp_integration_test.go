// SPDX-License-Identifier: AGPL-3.0-only
package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/jolks/mcp-cron/internal/acptest"
	"github.com/jolks/mcp-cron/internal/agent"
	"github.com/jolks/mcp-cron/internal/config"
	"github.com/jolks/mcp-cron/internal/model"
	"github.com/jolks/mcp-cron/internal/scheduler"
	"github.com/jolks/mcp-cron/internal/store"
)

func acpTestConfig(socketPath, cwd, mcpConfigPath string) func(*config.Config) {
	return func(cfg *config.Config) {
		cfg.AI.Provider = config.ProviderACP
		cfg.AI.ACPSocket = socketPath
		cfg.AI.ACPCWD = cwd
		cfg.AI.APIKey = ""
		cfg.AI.OpenAIAPIKey = ""
		cfg.AI.AnthropicAPIKey = ""
		cfg.AI.Model = "ignored-model"
		cfg.AI.BaseURL = "not-a-provider-url"
		cfg.AI.MCPConfigFilePath = mcpConfigPath
	}
}

func acpTaskConversation(cwd, wantPrompt, output string) acptest.Handler {
	return func(conn net.Conn, scanner *bufio.Scanner) error {
		var initialize acp.InitializeRequest
		request, err := acptest.ReadRequest(scanner, &initialize)
		if err != nil {
			return err
		}
		if request.Method != "initialize" || initialize.ProtocolVersion != acp.ProtocolVersionNumber {
			return fmt.Errorf("unexpected initialize request: method=%q protocol=%d", request.Method, initialize.ProtocolVersion)
		}
		if err := acptest.WriteResponse(conn, request.ID, `{"protocolVersion":1,"authMethods":[]}`); err != nil {
			return err
		}

		var session acp.NewSessionRequest
		request, err = acptest.ReadRequest(scanner, &session)
		if err != nil {
			return err
		}
		if request.Method != "session/new" || session.Cwd != cwd || session.McpServers == nil || len(session.McpServers) != 0 {
			return fmt.Errorf("unexpected session/new request: method=%q cwd=%q mcpServers=%#v", request.Method, session.Cwd, session.McpServers)
		}
		if err := acptest.WriteResponse(conn, request.ID, `{"sessionId":"server-api-session"}`); err != nil {
			return err
		}

		var prompt acp.PromptRequest
		request, err = acptest.ReadRequest(scanner, &prompt)
		if err != nil {
			return err
		}
		if request.Method != "session/prompt" || prompt.SessionId != "server-api-session" || len(prompt.Prompt) != 1 || prompt.Prompt[0].Text == nil || prompt.Prompt[0].Text.Text != wantPrompt {
			return fmt.Errorf("unexpected session/prompt request: method=%q session=%q prompt=%#v", request.Method, prompt.SessionId, prompt.Prompt)
		}

		if err := acptest.WriteUpdate(conn, prompt.SessionId, acp.UpdateAgentThoughtText("ignored thought")); err != nil {
			return err
		}
		if err := acptest.WriteUpdate(conn, prompt.SessionId, acp.UpdateAgentMessageText(output)); err != nil {
			return err
		}
		return acptest.WriteResponse(conn, request.ID, `{"stopReason":"end_turn"}`)
	}
}

func waitForStoredResult(t *testing.T, srv *MCPServer, taskID string, timeout time.Duration) *model.Result {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if result, found := srv.GetTaskResult(taskID); found {
			return result
		}
		select {
		case <-deadline.C:
			t.Fatalf("did not get a result for task %s within %s", taskID, timeout)
		case <-ticker.C:
		}
	}
}

func TestIntegration_ACPTaskAPIAndSQLiteReload(t *testing.T) {
	cwd := t.TempDir()
	mcpConfigPath := filepath.Join(t.TempDir(), "ignored-mcp.json")
	fake := acptest.New(t, acpTaskConversation(cwd, "run through the task API", "ACP API output"))
	dbPath := filepath.Join(t.TempDir(), "acp-api.db")

	srv := createIntegrationTestServer(t, integrationOpts{
		withStore:    true,
		pollInterval: 10 * time.Millisecond,
		dbPath:       dbPath,
		configure:    acpTestConfig(fake.SocketPath(), cwd, mcpConfigPath),
	})

	task := mustAddAITask(t, srv, AITaskParams{
		TaskParams: TaskParams{
			Name:    "ACP task API",
			Enabled: true,
		},
		Prompt: "run through the task API",
	})
	if task.Type != model.TypeAI {
		t.Fatalf("task type = %q, want %q", task.Type, model.TypeAI)
	}
	if task.Prompt != "run through the task API" || task.Command != "" || task.Schedule != "" {
		t.Fatalf("unexpected task payload: %+v", task)
	}

	gotTask := mustGetTask(t, srv, task.ID)
	if gotTask.Type != model.TypeAI || gotTask.Prompt != task.Prompt {
		t.Fatalf("get_task changed the AI task shape: %+v", gotTask)
	}

	response, err := srv.handleRunTask(context.Background(), makeRequest(t, TaskIDParams{ID: task.ID}))
	if err != nil {
		t.Fatalf("run_task failed in ACP mode without API keys: %v", err)
	}
	var runResult model.Result
	parseResponse(t, response, &runResult)
	if runResult.Output != "ACP API output" || runResult.ExitCode != 0 || runResult.Error != "" {
		t.Fatalf("unexpected run_task result: %+v", runResult)
	}

	persisted := mustGetResult(t, srv, task.ID)
	if persisted.Output != runResult.Output || persisted.ExitCode != 0 || persisted.Prompt != task.Prompt {
		t.Fatalf("unexpected get_task_result result: %+v", persisted)
	}
	fake.WaitForDisconnect(t)

	// A fresh scheduler and server can reload the unchanged AI task and its
	// result from SQLite while retaining ACP's provider-independent task shape.
	reloadedStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore for reload: %v", err)
	}
	t.Cleanup(func() { _ = reloadedStore.Close() })
	reloadedScheduler := scheduler.NewScheduler(&srv.config.Scheduler, testLogger())
	reloadedScheduler.SetTaskStore(reloadedStore)
	if err := reloadedScheduler.LoadTasks(); err != nil {
		t.Fatalf("LoadTasks: %v", err)
	}
	reloadedServer := &MCPServer{
		scheduler:     reloadedScheduler,
		agentExecutor: agent.NewAgentExecutor(srv.config, reloadedStore, testLogger()),
		resultStore:   reloadedStore,
		logger:        testLogger(),
		config:        srv.config,
	}
	reloadedScheduler.SetTaskExecutor(reloadedServer)

	reloadedTask, err := reloadedScheduler.GetTask(task.ID)
	if err != nil {
		t.Fatalf("GetTask after reload: %v", err)
	}
	if reloadedTask.Type != model.TypeAI || reloadedTask.Prompt != task.Prompt || reloadedTask.Schedule != "" {
		t.Fatalf("task did not retain its ACP-compatible shape after reload: %+v", reloadedTask)
	}
	reloadedResult := mustGetResult(t, reloadedServer, task.ID)
	if reloadedResult.Output != "ACP API output" || reloadedResult.ExitCode != 0 {
		t.Fatalf("result did not persist through reload: %+v", reloadedResult)
	}
}

func TestIntegration_ACPScheduledExecutionPersistsResult(t *testing.T) {
	cwd := t.TempDir()
	mcpConfigPath := filepath.Join(t.TempDir(), "ignored-mcp.json")
	fake := acptest.New(t, acpTaskConversation(cwd, "run on schedule", "ACP scheduled output"))
	srv := createIntegrationTestServer(t, integrationOpts{
		withStore:    true,
		pollInterval: 10 * time.Millisecond,
		configure:    acpTestConfig(fake.SocketPath(), cwd, mcpConfigPath),
	})

	task := mustAddAITask(t, srv, AITaskParams{
		TaskParams: TaskParams{
			Name:     "ACP scheduled task",
			Schedule: "* * * * * *",
			Enabled:  true,
		},
		Prompt: "run on schedule",
	})
	if task.NextRun.IsZero() {
		t.Fatal("scheduled ACP task has no next run")
	}

	result := waitForStoredResult(t, srv, task.ID, 3*time.Second)
	if result.Output != "ACP scheduled output" || result.ExitCode != 0 || result.Error != "" {
		t.Fatalf("unexpected scheduled ACP result: %+v", result)
	}
	// Stop before the next cron tick so this one-connection fake remains
	// deterministic while still proving the recurring schedule was advanced.
	if err := srv.scheduler.Stop(); err != nil {
		t.Fatalf("stop scheduler: %v", err)
	}
	fake.WaitForDisconnect(t)

	after := mustGetTask(t, srv, task.ID)
	if after.Schedule != task.Schedule || !after.Enabled || after.NextRun.IsZero() || !after.NextRun.After(time.Now()) {
		t.Fatalf("recurring ACP task did not retain its schedule: %+v", after)
	}
	got := mustGetResult(t, srv, task.ID)
	if got.Output != result.Output || got.ExitCode != result.ExitCode {
		t.Fatalf("get_task_result did not return the scheduled result: %+v", got)
	}
}

func TestIntegration_ACPConcurrentRunsUseIsolatedSessions(t *testing.T) {
	cwd := t.TempDir()
	mcpConfigPath := filepath.Join(t.TempDir(), "ignored-mcp.json")
	var promptCount int32
	allPrompts := make(chan struct{})
	outputs := make(chan string, 2)
	fake := acptest.NewMulti(t, 2, func(index int, conn net.Conn, scanner *bufio.Scanner) error {
		var initialize acp.InitializeRequest
		request, err := acptest.ReadRequest(scanner, &initialize)
		if err != nil {
			return err
		}
		if request.Method != "initialize" || initialize.ProtocolVersion != acp.ProtocolVersionNumber {
			return fmt.Errorf("unexpected initialize request on connection %d", index)
		}
		if err := acptest.WriteResponse(conn, request.ID, `{"protocolVersion":1,"authMethods":[]}`); err != nil {
			return err
		}

		var session acp.NewSessionRequest
		request, err = acptest.ReadRequest(scanner, &session)
		if err != nil {
			return err
		}
		if request.Method != "session/new" || session.Cwd != cwd || session.McpServers == nil || len(session.McpServers) != 0 {
			return fmt.Errorf("unexpected session/new request on connection %d", index)
		}
		sessionID := acp.SessionId(fmt.Sprintf("isolated-session-%d", index))
		if err := acptest.WriteResponse(conn, request.ID, fmt.Sprintf(`{"sessionId":%q}`, sessionID)); err != nil {
			return err
		}

		var prompt acp.PromptRequest
		request, err = acptest.ReadRequest(scanner, &prompt)
		if err != nil {
			return err
		}
		if request.Method != "session/prompt" || prompt.SessionId != sessionID || len(prompt.Prompt) != 1 || prompt.Prompt[0].Text == nil {
			return fmt.Errorf("unexpected session/prompt request on connection %d", index)
		}
		if atomic.AddInt32(&promptCount, 1) == 2 {
			close(allPrompts)
		}
		select {
		case <-allPrompts:
		case <-time.After(time.Second):
			return fmt.Errorf("timed out waiting for the other ACP prompt on connection %d", index)
		}

		if err := acptest.WriteUpdate(conn, sessionID, acp.UpdateAgentMessageText(string(sessionID))); err != nil {
			return err
		}
		outputs <- string(sessionID)
		return acptest.WriteResponse(conn, request.ID, `{"stopReason":"end_turn"}`)
	})
	srv := createIntegrationTestServer(t, integrationOpts{
		pollInterval: 10 * time.Millisecond,
		configure:    acpTestConfig(fake.SocketPath(), cwd, mcpConfigPath),
	})

	tasks := []model.Task{
		mustAddAITask(t, srv, AITaskParams{
			TaskParams: TaskParams{Name: "ACP concurrent one", Schedule: "0 0 1 1 *", Enabled: true},
			Prompt:     "first concurrent prompt",
		}),
		mustAddAITask(t, srv, AITaskParams{
			TaskParams: TaskParams{Name: "ACP concurrent two", Schedule: "0 0 1 1 *", Enabled: true},
			Prompt:     "second concurrent prompt",
		}),
	}

	errs := make(chan error, len(tasks))
	for _, task := range tasks {
		task := task
		go func() {
			errs <- srv.Execute(context.Background(), &task, 5*time.Second)
		}()
	}

	seenSessions := map[string]bool{}
	for range tasks {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent ACP run failed: %v", err)
		}
		seenSessions[<-outputs] = true
	}
	if len(seenSessions) != 2 {
		t.Fatalf("expected two isolated ACP session outputs, got %v", seenSessions)
	}
	fake.WaitForDisconnect(t)
}
