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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/jolks/mcp-cron/internal/config"
)

func TestACPClientCollectsOnlyTextAgentMessageChunks(t *testing.T) {
	client := &acpClient{}

	updates := []acp.SessionUpdate{
		acp.UpdateAgentMessageText("first "),
		acp.UpdateAgentThoughtText("secret thought"),
		acp.UpdateAgentMessage(acp.ImageBlock("aW1hZ2U=", "image/png")),
		acp.UpdateAgentMessageText("second"),
	}
	for _, update := range updates {
		if err := client.SessionUpdate(context.Background(), acp.SessionNotification{Update: update}); err != nil {
			t.Fatalf("SessionUpdate returned error: %v", err)
		}
	}

	if got, want := client.output(), "first second"; got != want {
		t.Fatalf("collected output = %q, want %q", got, want)
	}
}

func TestACPClientRejectsUnsupportedCallbacks(t *testing.T) {
	client := &acpClient{}

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "filesystem read",
			call: func() error {
				_, err := client.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: "/tmp/file"})
				return err
			},
		},
		{
			name: "filesystem write",
			call: func() error {
				_, err := client.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: "/tmp/file"})
				return err
			},
		},
		{
			name: "permission",
			call: func() error {
				_, err := client.RequestPermission(context.Background(), acp.RequestPermissionRequest{})
				return err
			},
		},
		{
			name: "terminal create",
			call: func() error {
				_, err := client.CreateTerminal(context.Background(), acp.CreateTerminalRequest{})
				return err
			},
		},
		{
			name: "terminal kill",
			call: func() error {
				_, err := client.KillTerminal(context.Background(), acp.KillTerminalRequest{})
				return err
			},
		},
		{
			name: "terminal output",
			call: func() error {
				_, err := client.TerminalOutput(context.Background(), acp.TerminalOutputRequest{})
				return err
			},
		},
		{
			name: "terminal release",
			call: func() error {
				_, err := client.ReleaseTerminal(context.Background(), acp.ReleaseTerminalRequest{})
				return err
			},
		},
		{
			name: "terminal wait",
			call: func() error {
				_, err := client.WaitForTerminalExit(context.Background(), acp.WaitForTerminalExitRequest{})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if !errors.Is(err, ErrACPUnsupportedOperation) {
				t.Fatalf("error = %v, want ErrACPUnsupportedOperation", err)
			}
			if !strings.Contains(strings.ToLower(err.Error()), "unsupported") {
				t.Fatalf("error = %q, want an unsupported-operation diagnostic", err)
			}
		})
	}
}

func TestACPStopReasonError(t *testing.T) {
	tests := []struct {
		reason  acp.StopReason
		wantErr bool
		wantMsg string
	}{
		{reason: acp.StopReasonEndTurn},
		{reason: acp.StopReasonMaxTokens, wantErr: true, wantMsg: "max_tokens"},
		{reason: acp.StopReasonMaxTurnRequests, wantErr: true, wantMsg: "max_turn_requests"},
		{reason: acp.StopReasonRefusal, wantErr: true, wantMsg: "refusal"},
		{reason: acp.StopReasonCancelled, wantErr: true, wantMsg: "cancelled"},
		{reason: acp.StopReason("future_reason"), wantErr: true, wantMsg: "unknown stop reason"},
	}

	for _, tt := range tests {
		t.Run(string(tt.reason), func(t *testing.T) {
			err := acpStopReasonError(tt.reason)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %t", err, tt.wantErr)
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.wantMsg)
			}
		})
	}
}

func TestRunACPTaskLifecycleAndOutput(t *testing.T) {
	cwd := filepath.Join(t.TempDir(), "workspace")
	socketPath := startACPServer(t, func(conn net.Conn, scanner *bufio.Scanner) error {
		var initialize acp.InitializeRequest
		request, err := readACPRequest(scanner, &initialize)
		if err != nil {
			return err
		}
		if request.Method != "initialize" {
			return fmt.Errorf("first request method = %q, want initialize", request.Method)
		}
		if initialize.ProtocolVersion != acp.ProtocolVersionNumber {
			return fmt.Errorf("protocol version = %d, want %d", initialize.ProtocolVersion, acp.ProtocolVersionNumber)
		}
		if initialize.ClientCapabilities.Auth.Terminal ||
			initialize.ClientCapabilities.Fs.ReadTextFile ||
			initialize.ClientCapabilities.Fs.WriteTextFile ||
			initialize.ClientCapabilities.Terminal {
			return errors.New("ACP client advertised an unsupported capability")
		}
		if err := writeACPResponse(conn, request.ID, `{"protocolVersion":1,"authMethods":[]}`); err != nil {
			return err
		}

		var sessionRequest acp.NewSessionRequest
		request, err = readACPRequest(scanner, &sessionRequest)
		if err != nil {
			return err
		}
		if request.Method != "session/new" {
			return fmt.Errorf("second request method = %q, want session/new", request.Method)
		}
		if sessionRequest.Cwd != cwd {
			return fmt.Errorf("session cwd = %q, want %q", sessionRequest.Cwd, cwd)
		}
		if sessionRequest.McpServers == nil || len(sessionRequest.McpServers) != 0 {
			return fmt.Errorf("mcpServers = %#v, want non-nil empty list", sessionRequest.McpServers)
		}
		if err := writeACPResponse(conn, request.ID, `{"sessionId":"session-1"}`); err != nil {
			return err
		}

		var promptRequest acp.PromptRequest
		request, err = readACPRequest(scanner, &promptRequest)
		if err != nil {
			return err
		}
		if request.Method != "session/prompt" {
			return fmt.Errorf("third request method = %q, want session/prompt", request.Method)
		}
		if promptRequest.SessionId != "session-1" || len(promptRequest.Prompt) != 1 || promptRequest.Prompt[0].Text == nil || promptRequest.Prompt[0].Text.Text != "do the thing" {
			return fmt.Errorf("unexpected prompt request: %#v", promptRequest)
		}

		if err := writeACPUpdate(conn, "session-1", acp.UpdateAgentMessageText("hello ")); err != nil {
			return err
		}
		if err := writeACPUpdate(conn, "session-1", acp.UpdateAgentThoughtText("not output")); err != nil {
			return err
		}
		if err := writeACPUpdate(conn, "session-1", acp.UpdateAgentMessage(acp.ImageBlock("aW1hZ2U=", "image/png"))); err != nil {
			return err
		}
		if err := writeACPUpdate(conn, "session-1", acp.UpdatePlan(acp.PlanEntry{
			Content:  "not output",
			Priority: acp.PlanEntryPriorityLow,
			Status:   acp.PlanEntryStatusCompleted,
		})); err != nil {
			return err
		}
		if err := writeACPUpdate(conn, "session-1", acp.StartToolCall("tool-1", "not output", acp.WithStartKind(acp.ToolKindExecute))); err != nil {
			return err
		}
		if err := writeACPUpdate(conn, "session-1", acp.UpdateToolCall("tool-1", acp.WithUpdateTitle("not output"))); err != nil {
			return err
		}
		if err := writeACPUpdate(conn, "session-1", acp.UpdateAgentMessageText("world")); err != nil {
			return err
		}
		return writeACPResponse(conn, request.ID, `{"stopReason":"end_turn"}`)
	})

	cfg := config.DefaultConfig()
	cfg.AI.ACPSocket = socketPath
	cfg.AI.ACPCWD = cwd

	output, err := RunACPTask(context.Background(), "do the thing", cfg)
	if err != nil {
		t.Fatalf("RunACPTask returned error: %v", err)
	}
	if output != "hello world" {
		t.Fatalf("output = %q, want %q", output, "hello world")
	}
}

func TestRunACPTaskRejectsInitializeResponses(t *testing.T) {
	tests := []struct {
		name       string
		result     string
		want       error
		wantInText string
	}{
		{
			name:       "protocol mismatch",
			result:     `{"protocolVersion":2,"authMethods":[]}`,
			want:       ErrACPProtocolVersion,
			wantInText: "version 2",
		},
		{
			name:       "authentication methods",
			result:     `{"protocolVersion":1,"authMethods":[{}]}`,
			want:       ErrACPAuthenticationUnsupported,
			wantInText: "authentication method",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newFakeACPAgent(t, func(conn net.Conn, scanner *bufio.Scanner) error {
				request, err := readACPRequest(scanner, nil)
				if err != nil {
					return err
				}
				if request.Method != "initialize" {
					return fmt.Errorf("request method = %q, want initialize", request.Method)
				}
				if err := writeACPResponse(conn, request.ID, tt.result); err != nil {
					return err
				}
				for scanner.Scan() {
					var notification struct {
						Method string `json:"method"`
					}
					if err := json.Unmarshal(scanner.Bytes(), &notification); err != nil {
						return err
					}
					if notification.Method == "authenticate" {
						return errors.New("client called authenticate after rejecting advertised auth methods")
					}
				}
				return scanner.Err()
			})

			cfg := config.DefaultConfig()
			cfg.AI.ACPSocket = server.socketPath()
			cfg.AI.ACPCWD = "/workspace"
			_, err := RunACPTask(context.Background(), "prompt", cfg)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if !strings.Contains(err.Error(), tt.wantInText) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.wantInText)
			}
			server.waitForDisconnect(t)
		})
	}
}

func TestRunACPTaskStopReasonsOverUnixSocket(t *testing.T) {
	tests := []struct {
		name       string
		reason     acp.StopReason
		wantErr    bool
		wantInText string
	}{
		{name: "end turn", reason: acp.StopReasonEndTurn},
		{name: "max tokens", reason: acp.StopReasonMaxTokens, wantErr: true, wantInText: "max_tokens"},
		{name: "max turn requests", reason: acp.StopReasonMaxTurnRequests, wantErr: true, wantInText: "max_turn_requests"},
		{name: "refusal", reason: acp.StopReasonRefusal, wantErr: true, wantInText: "refusal"},
		{name: "cancelled", reason: acp.StopReasonCancelled, wantErr: true, wantInText: "cancelled"},
		{name: "unknown", reason: acp.StopReason("future_reason"), wantErr: true, wantInText: "unknown stop reason"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newFakeACPAgent(t, func(conn net.Conn, scanner *bufio.Scanner) error {
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
				if err := writeACPResponse(conn, request.ID, `{"sessionId":"stop-reason"}`); err != nil {
					return err
				}
				request, err = readACPRequest(scanner, nil)
				if err != nil {
					return err
				}
				if err := writeACPUpdate(conn, "stop-reason", acp.UpdateAgentMessageText("partial output")); err != nil {
					return err
				}
				return writeACPResponse(conn, request.ID, fmt.Sprintf(`{"stopReason":%q}`, tt.reason))
			})

			cfg := config.DefaultConfig()
			cfg.AI.ACPSocket = server.socketPath()
			cfg.AI.ACPCWD = "/workspace"
			output, err := RunACPTask(context.Background(), "prompt", cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %t", err, tt.wantErr)
			}
			if output != "partial output" {
				t.Fatalf("output = %q, want partial output", output)
			}
			if tt.wantInText != "" && !strings.Contains(err.Error(), tt.wantInText) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.wantInText)
			}
			server.waitForDisconnect(t)
		})
	}
}

func TestRunACPTaskRequestFailuresCloseUnixSocket(t *testing.T) {
	tests := []struct {
		name  string
		stage string
	}{
		{name: "initialize", stage: "initialize"},
		{name: "session new", stage: "session/new"},
		{name: "prompt", stage: "session/prompt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newFakeACPAgent(t, func(conn net.Conn, scanner *bufio.Scanner) error {
				request, err := readACPRequest(scanner, nil)
				if err != nil {
					return err
				}
				if request.Method != "initialize" {
					return fmt.Errorf("first request method = %q, want initialize", request.Method)
				}
				if tt.stage == "initialize" {
					if err := writeACPError(conn, request.ID, -32001, "fake initialize failure"); err != nil {
						return err
					}
					return waitForClientClose(scanner)
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
				if tt.stage == "session/new" {
					if err := writeACPError(conn, request.ID, -32002, "fake session failure"); err != nil {
						return err
					}
					return waitForClientClose(scanner)
				}
				if err := writeACPResponse(conn, request.ID, `{"sessionId":"request-failure"}`); err != nil {
					return err
				}

				request, err = readACPRequest(scanner, nil)
				if err != nil {
					return err
				}
				if request.Method != "session/prompt" {
					return fmt.Errorf("third request method = %q, want session/prompt", request.Method)
				}
				if err := writeACPError(conn, request.ID, -32003, "fake prompt failure"); err != nil {
					return err
				}
				return waitForClientClose(scanner)
			})

			cfg := config.DefaultConfig()
			cfg.AI.ACPSocket = server.socketPath()
			cfg.AI.ACPCWD = "/workspace"
			output, err := RunACPTask(context.Background(), "prompt", cfg)
			if err == nil {
				t.Fatal("RunACPTask returned nil error for a failed ACP request")
			}
			if output != "" {
				t.Fatalf("output = %q, want empty output", output)
			}
			if !strings.Contains(err.Error(), "fake ") {
				t.Fatalf("error = %q, want fake request diagnostic", err)
			}
			server.waitForDisconnect(t)
		})
	}
}

func TestRunACPTaskCancellationBeforeSessionClosesUnixSocket(t *testing.T) {
	tests := []struct {
		name  string
		stage string
	}{
		{name: "initialize", stage: "initialize"},
		{name: "session new", stage: "session/new"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestReceived := make(chan struct{})
			server := newFakeACPAgent(t, func(conn net.Conn, scanner *bufio.Scanner) error {
				request, err := readACPRequest(scanner, nil)
				if err != nil {
					return err
				}
				if request.Method != "initialize" {
					return fmt.Errorf("first request method = %q, want initialize", request.Method)
				}
				if tt.stage == "initialize" {
					close(requestReceived)
					return waitForClientCloseWithoutSessionCancel(scanner)
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
				close(requestReceived)
				return waitForClientCloseWithoutSessionCancel(scanner)
			})

			cfg := config.DefaultConfig()
			cfg.AI.ACPSocket = server.socketPath()
			cfg.AI.ACPCWD = "/workspace"
			ctx, cancel := context.WithCancel(context.Background())
			resultCh := make(chan error, 1)
			go func() {
				_, err := RunACPTask(ctx, "cancel before session", cfg)
				resultCh <- err
			}()

			select {
			case <-requestReceived:
				cancel()
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for ACP request")
			}
			select {
			case err := <-resultCh:
				if err == nil {
					t.Fatal("RunACPTask returned nil error after cancellation")
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for RunACPTask after cancellation")
			}
			server.waitForDisconnect(t)
		})
	}
}

func TestRunACPTaskConnectionFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.AI.ACPSocket = filepath.Join(t.TempDir(), "missing.sock")
	cfg.AI.ACPCWD = "/workspace"

	output, err := RunACPTask(context.Background(), "prompt", cfg)
	if err == nil {
		t.Fatal("RunACPTask returned nil error for a missing socket")
	}
	if output != "" {
		t.Fatalf("output = %q, want empty output", output)
	}
}

func TestRunACPTaskCancellationSendsSessionCancel(t *testing.T) {
	promptReceived := make(chan struct{})
	cancelReceived := make(chan string, 1)
	socketPath := startACPServer(t, func(conn net.Conn, scanner *bufio.Scanner) error {
		var initialize acp.InitializeRequest
		request, err := readACPRequest(scanner, &initialize)
		if err != nil {
			return err
		}
		if err := writeACPResponse(conn, request.ID, `{"protocolVersion":1,"authMethods":[]}`); err != nil {
			return err
		}

		var sessionRequest acp.NewSessionRequest
		request, err = readACPRequest(scanner, &sessionRequest)
		if err != nil {
			return err
		}
		if err := writeACPResponse(conn, request.ID, `{"sessionId":"session-cancel"}`); err != nil {
			return err
		}

		var promptRequest acp.PromptRequest
		request, err = readACPRequest(scanner, &promptRequest)
		if err != nil {
			return err
		}
		if request.Method != "session/prompt" {
			return fmt.Errorf("request method = %q, want session/prompt", request.Method)
		}
		close(promptReceived)

		var sawSessionCancel bool
		for !sawSessionCancel {
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					return err
				}
				return errors.New("client closed before sending session/cancel")
			}
			var notification struct {
				Method string          `json:"method"`
				Params json.RawMessage `json:"params"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &notification); err != nil {
				return err
			}
			switch notification.Method {
			case "$/cancel_request":
				// The SDK may race this best-effort JSON-RPC cancellation
				// with the required ACP session cancellation.
			case "session/cancel":
				var params acp.CancelNotification
				if err := json.Unmarshal(notification.Params, &params); err != nil {
					return err
				}
				if params.SessionId != "session-cancel" {
					return fmt.Errorf("session/cancel ID = %q, want session-cancel", params.SessionId)
				}
				sawSessionCancel = true
				cancelReceived <- string(params.SessionId)
			}
		}
		return nil
	})

	cfg := config.DefaultConfig()
	cfg.AI.ACPSocket = socketPath
	cfg.AI.ACPCWD = "/workspace"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := make(chan error, 1)
	go func() {
		_, err := RunACPTask(ctx, "cancel me", cfg)
		resultCh <- err
	}()

	select {
	case <-promptReceived:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for prompt")
	}

	select {
	case sessionID := <-cancelReceived:
		if sessionID != "session-cancel" {
			t.Fatalf("session/cancel ID = %q, want session-cancel", sessionID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for session/cancel")
	}

	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("RunACPTask returned nil error after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for RunACPTask after cancellation")
	}
}

type acpRPCRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

func readACPRequest(scanner *bufio.Scanner, params any) (acpRPCRequest, error) {
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return acpRPCRequest{}, err
		}
		return acpRPCRequest{}, io.EOF
	}

	var request acpRPCRequest
	if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
		return acpRPCRequest{}, err
	}
	if params != nil {
		var envelope struct {
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil {
			return acpRPCRequest{}, err
		}
		if err := json.Unmarshal(envelope.Params, params); err != nil {
			return acpRPCRequest{}, err
		}
	}
	return request, nil
}
