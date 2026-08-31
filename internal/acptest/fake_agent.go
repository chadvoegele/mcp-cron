// SPDX-License-Identifier: AGPL-3.0-only
// Package acptest contains deterministic ACP peers for integration tests.
package acptest

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

// Handler handles one ACP connection. Returning nil makes the fake peer keep
// reading until the client closes the socket.
type Handler func(net.Conn, *bufio.Scanner) error

// MultiHandler handles one connection in a multi-connection fake peer. index
// is assigned in accept order and is unique for the lifetime of the peer.
type MultiHandler func(index int, conn net.Conn, scanner *bufio.Scanner) error

// FakeAgent is a small, deterministic ACP peer that speaks the line-delimited
// JSON-RPC transport directly.
type FakeAgent struct {
	listener net.Listener
	path     string
	expected int

	mu           sync.Mutex
	active       map[net.Conn]struct{}
	accepted     int
	acceptDone   bool
	serveErrs    []error
	disconnected chan struct{}
	doneOnce     sync.Once
}

// New creates a fake ACP peer that accepts exactly one client connection.
func New(t testing.TB, handler Handler) *FakeAgent {
	t.Helper()
	return NewMulti(t, 1, func(_ int, conn net.Conn, scanner *bufio.Scanner) error {
		return handler(conn, scanner)
	})
}

// NewMulti creates a fake ACP peer that accepts exactly expected client
// connections. Each connection is handled in its own goroutine.
func NewMulti(t testing.TB, expected int, handler MultiHandler) *FakeAgent {
	t.Helper()
	if expected < 1 {
		t.Fatalf("expected connection count must be positive, got %d", expected)
	}

	path := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on ACP socket: %v", err)
	}

	server := &FakeAgent{
		listener:     listener,
		path:         path,
		expected:     expected,
		active:       make(map[net.Conn]struct{}),
		disconnected: make(chan struct{}),
	}
	go server.acceptLoop(handler)

	t.Cleanup(func() { server.cleanup(t) })
	return server
}

func (s *FakeAgent) acceptLoop(handler MultiHandler) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			s.acceptDone = true
			s.finishIfDoneLocked()
			s.mu.Unlock()
			return
		}

		s.mu.Lock()
		index := s.accepted
		s.accepted++
		s.active[conn] = struct{}{}
		acceptDone := s.accepted == s.expected
		if acceptDone {
			s.acceptDone = true
		}
		s.mu.Unlock()

		go s.serve(index, conn, handler)
		if acceptDone {
			return
		}
	}
}

func (s *FakeAgent) serve(index int, conn net.Conn, handler MultiHandler) {
	var serveErr error
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.active, conn)
		if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) && !errors.Is(serveErr, io.EOF) {
			s.serveErrs = append(s.serveErrs, serveErr)
		}
		s.finishIfDoneLocked()
		s.mu.Unlock()
	}()

	serveErr = handler(index, conn, bufio.NewScanner(conn))
	if serveErr == nil {
		_, serveErr = io.Copy(io.Discard, conn)
	}
}

func (s *FakeAgent) finishIfDoneLocked() {
	if s.acceptDone && len(s.active) == 0 {
		s.doneOnce.Do(func() { close(s.disconnected) })
	}
}

func (s *FakeAgent) cleanup(t testing.TB) {
	t.Helper()
	_ = s.listener.Close()

	s.mu.Lock()
	for conn := range s.active {
		_ = conn.Close()
	}
	s.acceptDone = true
	s.finishIfDoneLocked()
	s.mu.Unlock()

	select {
	case <-s.disconnected:
	case <-time.After(time.Second):
		t.Errorf("timed out waiting for ACP test server")
	}

	s.mu.Lock()
	serveErrs := append([]error(nil), s.serveErrs...)
	s.mu.Unlock()
	for _, err := range serveErrs {
		t.Errorf("ACP test server: %v", err)
	}
}

// SocketPath returns the Unix socket path served by the fake peer.
func (s *FakeAgent) SocketPath() string {
	return s.path
}

// WaitForDisconnect waits until all expected client connections have closed.
func (s *FakeAgent) WaitForDisconnect(t testing.TB) {
	t.Helper()
	select {
	case <-s.disconnected:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for ACP client to close the socket")
	}
}

// RPCRequest is the common JSON-RPC request envelope used by ACP requests.
type RPCRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

// ReadRequest reads one ACP JSON-RPC request and optionally decodes params.
func ReadRequest(scanner *bufio.Scanner, params any) (RPCRequest, error) {
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return RPCRequest{}, err
		}
		return RPCRequest{}, io.EOF
	}

	var request RPCRequest
	if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
		return RPCRequest{}, err
	}
	if params != nil {
		var envelope struct {
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil {
			return RPCRequest{}, err
		}
		if err := json.Unmarshal(envelope.Params, params); err != nil {
			return RPCRequest{}, err
		}
	}
	return request, nil
}

// WriteResponse writes a successful ACP JSON-RPC response.
func WriteResponse(conn net.Conn, id json.RawMessage, result string) error {
	if len(id) == 0 {
		return errors.New("ACP request has no ID")
	}
	_, err := fmt.Fprintf(conn, `{"jsonrpc":"2.0","id":%s,"result":%s}`+"\n", id, result)
	return err
}

// WriteError writes an ACP JSON-RPC error response.
func WriteError(conn net.Conn, id json.RawMessage, code int, message string) error {
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

// WriteUpdate writes an ACP session/update notification.
func WriteUpdate(conn net.Conn, sessionID acp.SessionId, update acp.SessionUpdate) error {
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

// WaitForClientClose waits until the client closes the connection.
func WaitForClientClose(scanner *bufio.Scanner) error {
	for scanner.Scan() {
	}
	return scanner.Err()
}

// WaitForClientCloseWithoutSessionCancel waits for the client to close and
// fails if it sends session/cancel before a session exists.
func WaitForClientCloseWithoutSessionCancel(scanner *bufio.Scanner) error {
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
