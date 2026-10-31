// SPDX-License-Identifier: AGPL-3.0-only
package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/google/shlex"
	"github.com/jolks/mcp-cron/internal/config"
)

const (
	acpShutdownTimeout  = time.Second
	acpCancelWriteGrace = 250 * time.Millisecond
)

var (
	// ErrACPUnsupportedOperation identifies a client callback that mcp-cron does
	// not implement for ACP sessions.
	ErrACPUnsupportedOperation = errors.New("ACP operation is unsupported")
	// ErrACPProtocolVersion identifies an agent that did not negotiate ACP v1.
	ErrACPProtocolVersion = errors.New("ACP protocol version 1 was not negotiated")
)

// RunACPTask executes one prompt in an isolated ACP session.
//
// The caller owns the context and its deadline. This function does not add a
// task timeout of its own; the same context covers every ACP
// request in the lifecycle.
func RunACPTask(ctx context.Context, prompt string, cfg *config.Config) (string, error) {
	if cfg == nil {
		return "", errors.New("ACP configuration is nil")
	}

	args, err := shlex.Split(cfg.AI.ACPCommand)
	if err != nil {
		return "", fmt.Errorf("parse ACP command: %w", err)
	}
	if len(args) == 0 || args[0] == "" {
		return "", errors.New("ACP command is required")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	// Do not use CommandContext: the SDK must send session/cancel before
	// shutdown closes stdin or kills an unresponsive agent.
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", fmt.Errorf("ACP stdin: %w", err)
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("ACP stdout: %w", err)
	}
	defer stdout.Close()
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start ACP command: %w", err)
	}
	defer func() {
		_ = stdin.Close()
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		timer := time.NewTimer(acpShutdownTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			_ = cmd.Process.Kill()
			<-done
		}
	}()

	stopPipeWatcher, err := watchACPContext(ctx, stdin.(*os.File), stdout.(*os.File))
	if err != nil {
		return "", fmt.Errorf("configure ACP pipe deadlines: %w", err)
	}
	defer stopPipeWatcher()

	client := &acpClient{}
	connection := acp.NewClientSideConnection(client, stdin, stdout)

	initResponse, err := connection.Initialize(ctx, acp.InitializeRequest{
		ProtocolVersion:    acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{},
	})
	if err != nil {
		return client.output(), fmt.Errorf("ACP initialize: %w", err)
	}
	if initResponse.ProtocolVersion != acp.ProtocolVersionNumber {
		return client.output(), fmt.Errorf("%w: agent negotiated version %d", ErrACPProtocolVersion, initResponse.ProtocolVersion)
	}

	session, err := connection.NewSession(ctx, acp.NewSessionRequest{
		Cwd:        cfg.AI.ACPCWD,
		McpServers: []acp.McpServer{},
	})
	if err != nil {
		return client.output(), fmt.Errorf("ACP session/new: %w", err)
	}
	if session.SessionId == "" {
		return client.output(), errors.New("ACP session/new returned an empty session ID")
	}

	client.setSessionID(session.SessionId)
	client.setPromptActive(true)
	response, err := connection.Prompt(ctx, acp.PromptRequest{
		SessionId: session.SessionId,
		Prompt:    []acp.ContentBlock{acp.TextBlock(prompt)},
	})
	client.setPromptActive(false)
	if err != nil && acpContextExpired(ctx) {
		cancelACPSession(stopPipeWatcher, stdin.(*os.File), connection, session.SessionId)
	}
	output := client.output()
	if err != nil {
		return output, fmt.Errorf("ACP session/prompt: %w", err)
	}
	if err := acpStopReasonError(response.StopReason); err != nil {
		return output, err
	}

	return output, nil
}

// watchACPContext bounds pipe I/O even when an agent stops reading stdin.
// The returned function joins the watcher before cancellation gets a fresh
// write deadline.
func watchACPContext(ctx context.Context, stdin, stdout *os.File) (func(), error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := stdin.SetWriteDeadline(deadline); err != nil {
			return nil, err
		}
		if err := stdout.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			_ = stdin.SetWriteDeadline(time.Now())
			_ = stdout.SetReadDeadline(time.Now())
		case <-stop:
		}
	}()

	return func() {
		stopOnce.Do(func() { close(stop) })
		<-done
	}, nil
}

func acpContextExpired(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	deadline, ok := ctx.Deadline()
	return ok && !time.Now().Before(deadline)
}

// cancelACPSession allows a bounded attempt to send session/cancel after
// the task deadline expires.
func cancelACPSession(
	stopPipeWatcher func(),
	stdin *os.File,
	connection *acp.ClientSideConnection,
	sessionID acp.SessionId,
) {
	stopPipeWatcher()

	if err := stdin.SetWriteDeadline(time.Now().Add(acpCancelWriteGrace)); err != nil {
		return
	}

	cancelCtx, cancel := context.WithTimeout(context.Background(), acpCancelWriteGrace)
	defer cancel()
	_ = connection.Cancel(cancelCtx, acp.CancelNotification{SessionId: sessionID})
}

// acpStopReasonError maps the ACP prompt stop reason to task success or
// failure. ACP may add stop reasons over time, so unknown values fail closed.
func acpStopReasonError(reason acp.StopReason) error {
	if reason == acp.StopReasonEndTurn {
		return nil
	}

	switch reason {
	case acp.StopReasonMaxTokens,
		acp.StopReasonMaxTurnRequests,
		acp.StopReasonRefusal,
		acp.StopReasonCancelled:
		return fmt.Errorf("ACP agent stopped with reason %q", reason)
	default:
		return fmt.Errorf("ACP agent returned unknown stop reason %q", reason)
	}
}

// acpClient implements all callbacks in acp.Client. ACP client capabilities
// are intentionally not advertised, so these callbacks are defensive
// rejection paths for agents that request unsupported operations anyway.
type acpClient struct {
	mu           sync.Mutex
	sessionID    acp.SessionId
	activePrompt bool
	text         strings.Builder
}

var _ acp.Client = (*acpClient)(nil)

func (c *acpClient) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	if params.Update.AgentMessageChunk == nil || params.Update.AgentMessageChunk.Content.Text == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.activePrompt || params.SessionId != c.sessionID {
		return nil
	}
	c.text.WriteString(params.Update.AgentMessageChunk.Content.Text.Text)
	return nil
}

func (c *acpClient) setSessionID(sessionID acp.SessionId) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionID = sessionID
}

func (c *acpClient) setPromptActive(active bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activePrompt = active
}

func (c *acpClient) outputTextFileOperation(operation string) error {
	return fmt.Errorf("%w: filesystem %s", ErrACPUnsupportedOperation, operation)
}

func (c *acpClient) terminalOperation(operation string) error {
	return fmt.Errorf("%w: terminal %s", ErrACPUnsupportedOperation, operation)
}

func (c *acpClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, c.outputTextFileOperation("read")
}

func (c *acpClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, c.outputTextFileOperation("write")
}

func (c *acpClient) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return acp.RequestPermissionResponse{}, fmt.Errorf("%w: permission requests are rejected", ErrACPUnsupportedOperation)
}

func (c *acpClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, c.terminalOperation("create")
}

func (c *acpClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, c.terminalOperation("kill")
}

func (c *acpClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, c.terminalOperation("output")
}

func (c *acpClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, c.terminalOperation("release")
}

func (c *acpClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, c.terminalOperation("wait")
}

func (c *acpClient) output() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.text.String()
}
