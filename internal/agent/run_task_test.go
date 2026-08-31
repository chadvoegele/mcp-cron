// SPDX-License-Identifier: AGPL-3.0-only
package agent

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jolks/mcp-cron/internal/config"
	"github.com/jolks/mcp-cron/internal/model"
)

func TestNewChatProvider_DefaultIsResponsesAPI(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.OpenAIAPIKey = "sk-test"

	provider, err := newChatProvider(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := provider.(*OpenAIResponsesProvider); !ok {
		t.Errorf("Expected *OpenAIResponsesProvider, got %T", provider)
	}
}

func TestNewChatProvider_ExplicitOpenAI(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "openai"
	cfg.AI.OpenAIAPIKey = "sk-test"

	provider, err := newChatProvider(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := provider.(*OpenAIResponsesProvider); !ok {
		t.Errorf("Expected *OpenAIResponsesProvider, got %T", provider)
	}
}

func TestNewChatProvider_CustomBaseURLUsesChatCompletions(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "openai"
	cfg.AI.OpenAIAPIKey = "sk-test"
	cfg.AI.BaseURL = "https://litellm.example.com"

	provider, err := newChatProvider(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := provider.(*OpenAIProvider); !ok {
		t.Errorf("Expected *OpenAIProvider for custom base URL, got %T", provider)
	}
}

func TestNewChatProvider_OpenAIDirectURLUsesResponsesAPI(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "openai"
	cfg.AI.OpenAIAPIKey = "sk-test"
	cfg.AI.BaseURL = "https://api.openai.com/v1"

	provider, err := newChatProvider(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := provider.(*OpenAIResponsesProvider); !ok {
		t.Errorf("Expected *OpenAIResponsesProvider for api.openai.com URL, got %T", provider)
	}
}

func TestNewChatProvider_Anthropic(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "anthropic"
	cfg.AI.AnthropicAPIKey = "sk-ant-test"

	provider, err := newChatProvider(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := provider.(*AnthropicProvider); !ok {
		t.Errorf("Expected *AnthropicProvider, got %T", provider)
	}
}

func TestNewChatProvider_AnthropicCaseInsensitive(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "Anthropic"
	cfg.AI.AnthropicAPIKey = "sk-ant-test"

	provider, err := newChatProvider(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := provider.(*AnthropicProvider); !ok {
		t.Errorf("Expected *AnthropicProvider, got %T", provider)
	}
}

func TestNewChatProvider_OpenAIFallbackToGenericKey(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "openai"
	cfg.AI.OpenAIAPIKey = ""
	cfg.AI.APIKey = "generic-key"

	provider, err := newChatProvider(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := provider.(*OpenAIResponsesProvider); !ok {
		t.Errorf("Expected *OpenAIResponsesProvider, got %T", provider)
	}
}

func TestNewChatProvider_AnthropicFallbackToGenericKey(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "anthropic"
	cfg.AI.AnthropicAPIKey = ""
	cfg.AI.APIKey = "generic-key"

	provider, err := newChatProvider(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := provider.(*AnthropicProvider); !ok {
		t.Errorf("Expected *AnthropicProvider, got %T", provider)
	}
}

func TestNewChatProvider_OpenAIMissingKey(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "openai"
	cfg.AI.OpenAIAPIKey = ""
	cfg.AI.APIKey = ""

	_, err := newChatProvider(cfg)
	if err == nil {
		t.Fatal("Expected error for missing OpenAI API key, got nil")
	}
}

func TestNewChatProvider_AnthropicMissingKey(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "anthropic"
	cfg.AI.AnthropicAPIKey = ""
	cfg.AI.APIKey = ""

	_, err := newChatProvider(cfg)
	if err == nil {
		t.Fatal("Expected error for missing Anthropic API key, got nil")
	}
}

func TestNewChatProvider_OpenAIKeyTakesPrecedence(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.Provider = "openai"
	cfg.AI.OpenAIAPIKey = "specific-key"
	cfg.AI.APIKey = "generic-key"

	// Should succeed using the specific key, not fall through to generic
	provider, err := newChatProvider(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := provider.(*OpenAIResponsesProvider); !ok {
		t.Errorf("Expected *OpenAIResponsesProvider, got %T", provider)
	}
}

func TestRunTaskACPBypassesMCPAndAPIProvider(t *testing.T) {
	const prompt = "run through the ACP agent"
	socketPath := startACPServer(t, func(conn net.Conn, scanner *bufio.Scanner) error {
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
		if err := writeACPResponse(conn, request.ID, `{"sessionId":"dispatch-test"}`); err != nil {
			return err
		}

		request, err = readACPRequest(scanner, nil)
		if err != nil {
			return err
		}
		if request.Method != "session/prompt" {
			return fmt.Errorf("third request method = %q, want session/prompt", request.Method)
		}
		if _, err := fmt.Fprintln(conn, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"dispatch-test","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"ACP output"}}}}`); err != nil {
			return err
		}
		return writeACPResponse(conn, request.ID, `{"stopReason":"end_turn"}`)
	})

	cfg := config.DefaultConfig()
	cfg.AI.Provider = config.ProviderACP
	cfg.AI.ACPSocket = socketPath
	cfg.AI.ACPCWD = "/workspace"
	cfg.AI.MCPConfigFilePath = filepath.Join(t.TempDir(), "missing-mcp.json")
	cfg.AI.OpenAIAPIKey = ""
	cfg.AI.AnthropicAPIKey = ""
	cfg.AI.APIKey = ""

	output, err := RunTask(context.Background(), &model.Task{ID: "dispatch-test", Prompt: prompt}, cfg, newMockResultStore())
	if err != nil {
		t.Fatalf("RunTask returned error: %v", err)
	}
	if output != "ACP output" {
		t.Fatalf("output = %q, want %q", output, "ACP output")
	}
}

func TestBuildSystemMessage_Empty(t *testing.T) {
	msg := buildSystemMessage("task-1", nil, false)
	if msg != "" {
		t.Errorf("Expected empty system message for nil tools, got %q", msg)
	}
}

func TestBuildSystemMessage_WithResultStore(t *testing.T) {
	tools := []ToolDefinition{
		{Name: "get_task_result", Description: "Gets execution results for a task."},
		{Name: "list_tasks", Description: "Lists all tasks"},
	}
	msg := buildSystemMessage("my-task", tools, true)
	if !strings.Contains(msg, "get_task_result") {
		t.Error("Expected system message to mention get_task_result")
	}
	if !strings.Contains(msg, "mcp__") {
		t.Error("Expected system message to contain MCP namespace guidance")
	}
	if !strings.Contains(msg, "my-task") {
		t.Error("Expected system message to contain task ID")
	}
}

func TestBuildSystemMessage_IncludesTaskID(t *testing.T) {
	tools := []ToolDefinition{
		{Name: "get_task_result", Description: "Gets execution results for a task."},
	}
	msg := buildSystemMessage("hn-top-5", tools, true)
	if !strings.Contains(msg, "hn-top-5") {
		t.Error("Expected system message to contain the task ID")
	}
	if !strings.Contains(msg, `id="hn-top-5"`) {
		t.Error("Expected explicit instruction to call get_task_result with the task ID")
	}
}

func TestBuildSystemMessage_NoResultStore(t *testing.T) {
	tools := []ToolDefinition{
		{Name: "get_task_result", Description: "Gets execution results for a task."},
	}
	msg := buildSystemMessage("task-1", tools, false)
	if strings.Contains(msg, "previous") {
		t.Error("Expected no previous-results guidance when result store is unavailable")
	}
}

func TestBuildSystemMessage_NoToolList(t *testing.T) {
	// System message should NOT include a full tool listing (models get
	// tool definitions via the API already).
	tools := []ToolDefinition{
		{Name: "some_tool", Description: "Does something"},
		{Name: "another_tool", Description: "Does another thing"},
	}
	msg := buildSystemMessage("task-1", tools, false)
	if strings.Contains(msg, "some_tool") {
		t.Error("System message should not list individual tool names (API provides them)")
	}
}
