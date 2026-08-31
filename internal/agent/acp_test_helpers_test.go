// SPDX-License-Identifier: AGPL-3.0-only
package agent

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

// fakeACPAgent is a small, deterministic ACP peer for integration tests. It
// speaks the line-delimited JSON-RPC transport directly so tests exercise the
// real Unix-socket client without starting an external process.
type fakeACPAgent struct {
	listener net.Listener
	path     string

	mu           sync.Mutex
	active       net.Conn
	serveErr     error
	disconnected chan struct{}
}

func newFakeACPAgent(t *testing.T, handler func(net.Conn, *bufio.Scanner) error) *fakeACPAgent {
	t.Helper()

	path := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on ACP socket: %v", err)
	}

	server := &fakeACPAgent{
		listener:     listener,
		path:         path,
		disconnected: make(chan struct{}),
	}
	go server.serve(handler)

	t.Cleanup(func() {
		_ = listener.Close()
		server.mu.Lock()
		if server.active != nil {
			_ = server.active.Close()
		}
		server.mu.Unlock()

		select {
		case <-server.disconnected:
		case <-time.After(time.Second):
			t.Error("timed out waiting for ACP test server")
		}

		server.mu.Lock()
		serveErr := server.serveErr
		server.mu.Unlock()
		if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) && !errors.Is(serveErr, io.EOF) {
			t.Errorf("ACP test server: %v", serveErr)
		}
	})

	return server
}

func (s *fakeACPAgent) serve(handler func(net.Conn, *bufio.Scanner) error) {
	conn, err := s.listener.Accept()
	if err != nil {
		s.finish(err)
		return
	}

	s.mu.Lock()
	s.active = conn
	s.mu.Unlock()

	defer func() {
		_ = conn.Close()
		s.finish(err)
	}()

	err = handler(conn, bufio.NewScanner(conn))
	if err == nil {
		// A successful handler normally sends a final response and returns. Keep
		// reading until the client closes the connection so tests can assert that
		// every client exit path releases its socket.
		_, err = io.Copy(io.Discard, conn)
	}
}

func (s *fakeACPAgent) finish(err error) {
	s.mu.Lock()
	s.serveErr = err
	close(s.disconnected)
	s.mu.Unlock()
}

func (s *fakeACPAgent) socketPath() string {
	return s.path
}

func (s *fakeACPAgent) waitForDisconnect(t *testing.T) {
	t.Helper()

	select {
	case <-s.disconnected:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ACP client to close the socket")
	}
}

func writeACPResponse(conn net.Conn, id json.RawMessage, result string) error {
	if len(id) == 0 {
		return errors.New("ACP request has no ID")
	}
	_, err := fmt.Fprintf(conn, `{"jsonrpc":"2.0","id":%s,"result":%s}`+"\n", id, result)
	return err
}

func writeACPError(conn net.Conn, id json.RawMessage, code int, message string) error {
	if len(id) == 0 {
		return errors.New("ACP request has no ID")
	}

	payload := struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{
		JSONRPC: "2.0",
		ID:      id,
	}
	payload.Error.Code = code
	payload.Error.Message = message
	return json.NewEncoder(conn).Encode(payload)
}

func writeACPUpdate(conn net.Conn, sessionID acp.SessionId, update acp.SessionUpdate) error {
	payload := struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  struct {
			SessionID acp.SessionId     `json:"sessionId"`
			Update    acp.SessionUpdate `json:"update"`
		} `json:"params"`
	}{
		JSONRPC: "2.0",
		Method:  "session/update",
	}
	payload.Params.SessionID = sessionID
	payload.Params.Update = update
	return json.NewEncoder(conn).Encode(payload)
}

// startACPServer retains the original helper's compact API for tests that do
// not need to inspect the fake peer after RunACPTask returns.
func startACPServer(t *testing.T, handler func(net.Conn, *bufio.Scanner) error) string {
	t.Helper()
	return newFakeACPAgent(t, handler).socketPath()
}

func waitForClientClose(scanner *bufio.Scanner) error {
	for scanner.Scan() {
	}
	return scanner.Err()
}

func waitForClientCloseWithoutSessionCancel(scanner *bufio.Scanner) error {
	for scanner.Scan() {
		var notification struct {
			Method string `json:"method"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &notification); err != nil {
			return err
		}
		if notification.Method == "session/cancel" {
			return errors.New("session/cancel sent before a session existed")
		}
	}
	return scanner.Err()
}
