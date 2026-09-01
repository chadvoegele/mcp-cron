// SPDX-License-Identifier: AGPL-3.0-only
package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jolks/mcp-cron/internal/agent"
	"github.com/jolks/mcp-cron/internal/command"
	"github.com/jolks/mcp-cron/internal/config"
	httpexec "github.com/jolks/mcp-cron/internal/http"
	"github.com/jolks/mcp-cron/internal/model"
	"github.com/jolks/mcp-cron/internal/scheduler"
	"github.com/jolks/mcp-cron/internal/store"
)

const oneShotTestPollInterval = 10 * time.Millisecond

func newOneShotIntegrationStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	return newOneShotIntegrationStoreAt(t, filepath.Join(t.TempDir(), "one-shot.db"))
}

func newOneShotIntegrationStoreAt(t *testing.T, dbPath string) *store.SQLiteStore {
	t.Helper()
	sqlStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore(%s): %v", dbPath, err)
	}
	t.Cleanup(func() { _ = sqlStore.Close() })
	return sqlStore
}

// newOneShotIntegrationServer assembles the production scheduler and executor
// routing without opening a network transport. Tests start the scheduler only
// after their task definitions have been persisted and loaded as needed.
func newOneShotIntegrationServer(t *testing.T, sqlStore *store.SQLiteStore) *MCPServer {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Scheduler.PollInterval = oneShotTestPollInterval
	cfg.AI.MCPConfigFilePath = filepath.Join(t.TempDir(), "mcp.json")

	logger := testLogger()
	sched := scheduler.NewScheduler(&cfg.Scheduler, logger)
	sched.SetTaskStore(sqlStore)

	srv := &MCPServer{
		scheduler:     sched,
		cmdExecutor:   command.NewCommandExecutor(sqlStore, logger),
		agentExecutor: agent.NewAgentExecutor(cfg, sqlStore, logger),
		httpExecutor:  httpexec.NewHTTPExecutor(sqlStore, logger),
		resultStore:   sqlStore,
		logger:        logger,
		config:        cfg,
	}
	sched.SetTaskExecutor(srv)
	return srv
}

func startOneShotIntegrationScheduler(t *testing.T, srv *MCPServer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv.scheduler.Start(ctx)
	t.Cleanup(func() {
		cancel()
		if err := srv.scheduler.Stop(); err != nil {
			t.Errorf("stop one-shot scheduler: %v", err)
		}
	})
}

func mustAddHTTPTask(t *testing.T, srv *MCPServer, params TaskParams) model.Task {
	t.Helper()
	result, err := srv.handleAddHTTPTask(context.Background(), makeRequest(t, params))
	if err != nil {
		t.Fatalf("handleAddHTTPTask failed: %v", err)
	}
	var task model.Task
	parseResponse(t, result, &task)
	return task
}

func waitForOneShotResult(t *testing.T, sqlStore *store.SQLiteStore, taskID string) *model.Result {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		result, err := sqlStore.GetLatestResult(taskID)
		if err != nil {
			t.Fatalf("GetLatestResult(%s): %v", taskID, err)
		}
		if result != nil {
			return result
		}
		time.Sleep(oneShotTestPollInterval)
	}
	t.Fatalf("one-shot task %s did not produce a result", taskID)
	return nil
}

func loadPersistedTask(t *testing.T, sqlStore *store.SQLiteStore, taskID string) *model.Task {
	t.Helper()
	tasks, err := sqlStore.LoadTasks()
	if err != nil {
		t.Fatalf("LoadTasks: %v", err)
	}
	for _, task := range tasks {
		if task.ID == taskID {
			return task
		}
	}
	t.Fatalf("task %s was not persisted", taskID)
	return nil
}

func assertConsumedOneShot(t *testing.T, sqlStore *store.SQLiteStore, taskID string) {
	t.Helper()
	task := loadPersistedTask(t, sqlStore, taskID)
	if task.Enabled {
		t.Errorf("consumed task %s is enabled", taskID)
	}
	if task.RunAt != nil {
		t.Errorf("consumed task %s retains run_at %v", taskID, task.RunAt)
	}
	if !task.NextRun.IsZero() {
		t.Errorf("consumed task %s retains next_run %v", taskID, task.NextRun)
	}
}

func assertFutureOneShot(t *testing.T, task model.Task, wantRunAt time.Time) {
	t.Helper()
	if task.RunAt == nil {
		t.Fatalf("task %s has no run_at", task.ID)
	}
	if !task.RunAt.Equal(wantRunAt) {
		t.Errorf("task %s run_at = %v, want %v", task.ID, task.RunAt, wantRunAt)
	}
	if task.RunAt.Location() != time.UTC {
		t.Errorf("task %s run_at location = %v, want UTC", task.ID, task.RunAt.Location())
	}
	if !task.NextRun.Equal(wantRunAt) {
		t.Errorf("task %s next_run = %v, want %v", task.ID, task.NextRun, wantRunAt)
	}
}

func TestIntegration_OneShotPersistsShellAIAndHTTPTasks(t *testing.T) {
	sqlStore := newOneShotIntegrationStore(t)
	srv := newOneShotIntegrationServer(t, sqlStore)
	runAtText := "2030-01-02T03:04:05.123456789+02:00"
	wantRunAt, err := time.Parse(time.RFC3339, runAtText)
	if err != nil {
		t.Fatalf("parse run_at: %v", err)
	}
	wantRunAt = wantRunAt.UTC()

	shellTask := mustAddShellTask(t, srv, TaskParams{
		Name: "future shell one-shot", Command: "echo shell", RunAt: &runAtText, Enabled: true,
	})
	aiTask := mustAddAITask(t, srv, AITaskParams{
		TaskParams: TaskParams{Name: "future AI one-shot", RunAt: &runAtText, Enabled: true},
		Prompt:     "summarize this",
	})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "future HTTP")
	}))
	t.Cleanup(httpServer.Close)
	httpTask := mustAddHTTPTask(t, srv, TaskParams{
		Name: "future HTTP one-shot", URL: httpServer.URL, RunAt: &runAtText, Enabled: true,
	})

	for _, task := range []model.Task{shellTask, aiTask, httpTask} {
		assertFutureOneShot(t, task, wantRunAt)
	}

	persisted, err := sqlStore.LoadTasks()
	if err != nil {
		t.Fatalf("LoadTasks: %v", err)
	}
	if len(persisted) != 3 {
		t.Fatalf("persisted task count = %d, want 3", len(persisted))
	}
	for _, taskID := range []string{shellTask.ID, aiTask.ID, httpTask.ID} {
		assertFutureOneShot(t, *loadPersistedTask(t, sqlStore, taskID), wantRunAt)
	}
}

func TestIntegration_OneShotShellExecutesPersistsResultAndConsumes(t *testing.T) {
	sqlStore := newOneShotIntegrationStore(t)
	srv := newOneShotIntegrationServer(t, sqlStore)
	runAt := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	task := mustAddShellTask(t, srv, TaskParams{
		Name: "automatic shell one-shot", Command: "printf shell-one-shot", RunAt: &runAt, Enabled: true,
	})
	startOneShotIntegrationScheduler(t, srv)

	result := waitForOneShotResult(t, sqlStore, task.ID)
	if result.ExitCode != 0 {
		t.Fatalf("exit_code = %d, want 0 (error: %s)", result.ExitCode, result.Error)
	}
	if result.Output != "shell-one-shot" {
		t.Fatalf("output = %q, want shell-one-shot", result.Output)
	}
	results, err := sqlStore.GetResults(task.ID, 10)
	if err != nil {
		t.Fatalf("GetResults: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("result count = %d, want 1", len(results))
	}
	time.Sleep(3 * oneShotTestPollInterval)
	results, err = sqlStore.GetResults(task.ID, 10)
	if err != nil {
		t.Fatalf("GetResults after poll settle: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("result count after poll settle = %d, want 1", len(results))
	}
	assertConsumedOneShot(t, sqlStore, task.ID)
}

func TestIntegration_OneShotHTTPExecutesAgainstLocalServer(t *testing.T) {
	sqlStore := newOneShotIntegrationStore(t)
	var requestCount atomic.Int32
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		if string(body) != `{"source":"one-shot"}` {
			t.Errorf("request body = %q, want one-shot payload", body)
		}
		if r.Header.Get("X-Test-Task") != "one-shot" {
			t.Errorf("X-Test-Task = %q, want one-shot", r.Header.Get("X-Test-Task"))
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "http-one-shot")
	}))
	t.Cleanup(httpServer.Close)

	srv := newOneShotIntegrationServer(t, sqlStore)
	runAt := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	task := mustAddHTTPTask(t, srv, TaskParams{
		Name: "automatic HTTP one-shot", URL: httpServer.URL, Method: http.MethodPost,
		Headers: map[string]string{"X-Test-Task": "one-shot"}, Body: `{"source":"one-shot"}`,
		RunAt: &runAt, Enabled: true,
	})
	startOneShotIntegrationScheduler(t, srv)

	result := waitForOneShotResult(t, sqlStore, task.ID)
	if result.ExitCode != http.StatusCreated {
		t.Fatalf("exit_code = %d, want %d (error: %s)", result.ExitCode, http.StatusCreated, result.Error)
	}
	if result.Output != "http-one-shot" {
		t.Fatalf("output = %q, want http-one-shot", result.Output)
	}
	if result.URL != httpServer.URL {
		t.Errorf("result URL = %q, want %q", result.URL, httpServer.URL)
	}
	if got := requestCount.Load(); got != 1 {
		t.Fatalf("HTTP request count = %d, want 1", got)
	}
	assertConsumedOneShot(t, sqlStore, task.ID)
}

func TestIntegration_OneShotPastTimestampWaitsUntilEnabled(t *testing.T) {
	sqlStore := newOneShotIntegrationStore(t)
	srv := newOneShotIntegrationServer(t, sqlStore)
	runAt := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	task := mustAddShellTask(t, srv, TaskParams{
		Name: "disabled past one-shot", Command: "echo enabled-later", RunAt: &runAt,
	})
	pending := loadPersistedTask(t, sqlStore, task.ID)
	if pending.Enabled || pending.RunAt == nil || !pending.NextRun.IsZero() {
		t.Fatalf("disabled past task = %+v, want run_at only", pending)
	}

	startOneShotIntegrationScheduler(t, srv)
	time.Sleep(3 * oneShotTestPollInterval)
	if result, err := sqlStore.GetLatestResult(task.ID); err != nil {
		t.Fatalf("GetLatestResult before enable: %v", err)
	} else if result != nil {
		t.Fatal("disabled past one-shot executed before enable")
	}

	enabled := mustEnableTask(t, srv, task.ID)
	if !enabled.Enabled || enabled.RunAt == nil || !enabled.NextRun.Equal(*enabled.RunAt) {
		t.Fatalf("enabled past task = %+v, want armed at run_at", enabled)
	}
	result := waitForOneShotResult(t, sqlStore, task.ID)
	if !strings.Contains(result.Output, "enabled-later") {
		t.Fatalf("output = %q, want enabled-later", result.Output)
	}
	assertConsumedOneShot(t, sqlStore, task.ID)
}

func TestIntegration_RunTaskConsumesArmedOneShot(t *testing.T) {
	sqlStore := newOneShotIntegrationStore(t)
	srv := newOneShotIntegrationServer(t, sqlStore)
	runAt := time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	task := mustAddShellTask(t, srv, TaskParams{
		Name: "manual one-shot", Command: "printf manual-one-shot", RunAt: &runAt, Enabled: true,
	})
	startOneShotIntegrationScheduler(t, srv)

	response := mustRunTask(t, srv, task.ID)
	if response["output"] != "manual-one-shot" {
		t.Fatalf("run_task response = %#v, want output", response)
	}
	assertConsumedOneShot(t, sqlStore, task.ID)
	if _, err := srv.handleRunTask(context.Background(), makeRequest(t, TaskIDParams{ID: task.ID})); err == nil {
		t.Fatal("run_task on consumed one-shot succeeded, want disabled-task error")
	}
}

func TestIntegration_FailedOneShotIsConsumedAcrossRestart(t *testing.T) {
	sqlStore := newOneShotIntegrationStore(t)
	srv := newOneShotIntegrationServer(t, sqlStore)
	runAt := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	task := mustAddShellTask(t, srv, TaskParams{
		Name: "failed one-shot", Command: "exit 23", RunAt: &runAt, Enabled: true,
	})
	startOneShotIntegrationScheduler(t, srv)

	result := waitForOneShotResult(t, sqlStore, task.ID)
	if result.ExitCode == 0 {
		t.Fatal("failed one-shot returned exit_code 0")
	}
	assertConsumedOneShot(t, sqlStore, task.ID)
	results, err := sqlStore.GetResults(task.ID, 10)
	if err != nil {
		t.Fatalf("GetResults before restart: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("result count before restart = %d, want 1", len(results))
	}

	if err := srv.scheduler.Stop(); err != nil {
		t.Fatalf("stop first scheduler: %v", err)
	}
	restarted := newOneShotIntegrationServer(t, sqlStore)
	if err := restarted.scheduler.LoadTasks(); err != nil {
		t.Fatalf("LoadTasks after restart: %v", err)
	}
	startOneShotIntegrationScheduler(t, restarted)
	time.Sleep(3 * oneShotTestPollInterval)
	results, err = sqlStore.GetResults(task.ID, 10)
	if err != nil {
		t.Fatalf("GetResults after restart: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("result count after restart = %d, want 1", len(results))
	}
	assertConsumedOneShot(t, sqlStore, task.ID)
}

func TestIntegration_OneShotRestartRepairsMissingNextRun(t *testing.T) {
	sqlStore := newOneShotIntegrationStore(t)
	runAt := time.Now().UTC().Add(-time.Second)
	if err := sqlStore.SaveTask(&model.Task{
		ID: "restart-repair-one-shot", Name: "restart repair", Type: model.TypeShellCommand,
		Command: "printf repaired", RunAt: &runAt, Enabled: true,
		CreatedAt: runAt, UpdatedAt: runAt,
	}); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}

	srv := newOneShotIntegrationServer(t, sqlStore)
	if err := srv.scheduler.LoadTasks(); err != nil {
		t.Fatalf("LoadTasks: %v", err)
	}
	loaded, err := srv.scheduler.GetTask("restart-repair-one-shot")
	if err != nil {
		t.Fatalf("GetTask after restart: %v", err)
	}
	if !loaded.NextRun.Equal(runAt) {
		t.Fatalf("repaired next_run = %v, want %v", loaded.NextRun, runAt)
	}
	startOneShotIntegrationScheduler(t, srv)
	result := waitForOneShotResult(t, sqlStore, "restart-repair-one-shot")
	if result.Output != "repaired" {
		t.Fatalf("output = %q, want repaired", result.Output)
	}
	assertConsumedOneShot(t, sqlStore, "restart-repair-one-shot")
}

func TestIntegration_OneShotSharedDatabaseExecutesFailedTaskOnce(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared-one-shot.db")
	store1 := newOneShotIntegrationStoreAt(t, dbPath)
	store2 := newOneShotIntegrationStoreAt(t, dbPath)
	srv1 := newOneShotIntegrationServer(t, store1)
	srv2 := newOneShotIntegrationServer(t, store2)
	runAt := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	task := mustAddShellTask(t, srv1, TaskParams{
		Name: "shared failed one-shot", Command: "exit 29", RunAt: &runAt, Enabled: true,
	})
	if err := srv2.scheduler.LoadTasks(); err != nil {
		t.Fatalf("second scheduler LoadTasks: %v", err)
	}
	startOneShotIntegrationScheduler(t, srv1)
	startOneShotIntegrationScheduler(t, srv2)

	result := waitForOneShotResult(t, store1, task.ID)
	if result.ExitCode == 0 {
		t.Fatal("shared failed one-shot returned exit_code 0")
	}
	results, err := store1.GetResults(task.ID, 10)
	if err != nil {
		t.Fatalf("GetResults: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("shared result count = %d, want 1", len(results))
	}
	time.Sleep(3 * oneShotTestPollInterval)
	results, err = store1.GetResults(task.ID, 10)
	if err != nil {
		t.Fatalf("GetResults after poll settle: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("shared result count after poll settle = %d, want 1", len(results))
	}
	assertConsumedOneShot(t, store1, task.ID)
}
