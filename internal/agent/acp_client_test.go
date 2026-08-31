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
	"sync"
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

		if _, err := fmt.Fprintln(conn, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello "}}}}`); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(conn, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"not output"}}}}`); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(conn, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"world"}}}}`); err != nil {
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
			socketPath := startACPServer(t, func(conn net.Conn, scanner *bufio.Scanner) error {
				request, err := readACPRequest(scanner, nil)
				if err != nil {
					return err
				}
				if request.Method != "initialize" {
					return fmt.Errorf("request method = %q, want initialize", request.Method)
				}
				return writeACPResponse(conn, request.ID, tt.result)
			})

			cfg := config.DefaultConfig()
			cfg.AI.ACPSocket = socketPath
			cfg.AI.ACPCWD = "/workspace"
			_, err := RunACPTask(context.Background(), "prompt", cfg)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if !strings.Contains(err.Error(), tt.wantInText) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.wantInText)
			}
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

func writeACPResponse(conn net.Conn, id json.RawMessage, result string) error {
	if len(id) == 0 {
		return errors.New("ACP request has no ID")
	}
	_, err := fmt.Fprintf(conn, `{"jsonrpc":"2.0","id":%s,"result":%s}`+"\n", id, result)
	return err
}

func startACPServer(t *testing.T, handler func(net.Conn, *bufio.Scanner) error) string {
	t.Helper()

	socketPath := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen on ACP socket: %v", err)
	}

	var mu sync.Mutex
	var active net.Conn
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		mu.Lock()
		active = conn
		mu.Unlock()
		defer func() {
			_ = conn.Close()
			mu.Lock()
			active = nil
			mu.Unlock()
		}()
		handlerErr := handler(conn, bufio.NewScanner(conn))
		if handlerErr == nil {
			// Keep the peer open until the client closes its socket. The ACP SDK
			// waits for notifications sent before a response to be processed.
			_, _ = io.Copy(io.Discard, conn)
		}
		done <- handlerErr
	}()

	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		if active != nil {
			_ = active.Close()
		}
		mu.Unlock()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
				t.Errorf("ACP test server: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("timed out waiting for ACP test server")
		}
	})

	return socketPath
}
