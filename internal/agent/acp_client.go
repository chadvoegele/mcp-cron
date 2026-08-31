// SPDX-License-Identifier: AGPL-3.0-only
package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/jolks/mcp-cron/internal/config"
)

const (
	acpConnectionTimeout = 10 * time.Second
	acpCancelWriteGrace  = 250 * time.Millisecond
)

var (
	// ErrACPUnsupportedOperation identifies a client callback that mcp-cron does
	// not implement for ACP sessions.
	ErrACPUnsupportedOperation = errors.New("ACP operation is unsupported")
	// ErrACPAuthenticationUnsupported identifies an agent that requires an
	// authentication flow mcp-cron intentionally does not provide.
	ErrACPAuthenticationUnsupported = errors.New("ACP authentication is unsupported")
	// ErrACPProtocolVersion identifies an agent that did not negotiate ACP v1.
	ErrACPProtocolVersion = errors.New("ACP protocol version 1 was not negotiated")
)

// RunACPTask executes one prompt in an isolated ACP session.
//
// The caller owns the context and its deadline. This function does not add a
// task timeout of its own; the same context covers dialing and every ACP
// request in the lifecycle.
func RunACPTask(ctx context.Context, prompt string, cfg *config.Config) (string, error) {
	if cfg == nil {
		return "", errors.New("ACP configuration is nil")
	}

	dialer := net.Dialer{Timeout: acpConnectionTimeout}
	socket, err := dialer.DialContext(ctx, "unix", cfg.AI.ACPSocket)
	if err != nil {
		return "", fmt.Errorf("connect to ACP socket %q: %w", cfg.AI.ACPSocket, err)
	}

	stopSocketWatcher, err := watchACPContext(ctx, socket)
	if err != nil {
		_ = socket.Close()
		return "", fmt.Errorf("configure ACP socket deadlines: %w", err)
	}
	defer func() {
		stopSocketWatcher()
		_ = socket.Close()
	}()

	client := &acpClient{}
	connection := acp.NewClientSideConnection(client, socket, socket)

	initResponse, err := connection.Initialize(ctx, acp.InitializeRequest{
		ProtocolVersion:    acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{},
	})
	if err != nil {
		return client.output(), fmt.Errorf("ACP initialize: %w", err)
	}
	if len(initResponse.AuthMethods) != 0 {
		return client.output(), fmt.Errorf("%w: agent advertised %d authentication method(s)", ErrACPAuthenticationUnsupported, len(initResponse.AuthMethods))
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
		cancelACPSession(stopSocketWatcher, socket, connection, session.SessionId)
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

// watchACPContext applies the context deadline to socket I/O and makes
// cancellation wake both reads and writes. The returned function stops and
// joins the watcher so it cannot overwrite a later deadline during cleanup.
func watchACPContext(ctx context.Context, socket net.Conn) (func(), error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := socket.SetDeadline(deadline); err != nil {
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
			_ = socket.SetDeadline(time.Now())
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

// cancelACPSession preserves the expired read deadline while allowing one
// short, bounded attempt to send the required session/cancel notification.
func cancelACPSession(
	stopSocketWatcher func(),
	socket net.Conn,
	connection *acp.ClientSideConnection,
	sessionID acp.SessionId,
) {
	stopSocketWatcher()

	now := time.Now()
	_ = socket.SetReadDeadline(now)
	if err := socket.SetWriteDeadline(now.Add(acpCancelWriteGrace)); err != nil {
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
