// SPDX-License-Identifier: AGPL-3.0-only
package agent

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/jolks/mcp-cron/internal/acptest"
)

type fakeACPAgent struct{ *acptest.FakeAgent }

func newFakeACPAgent(t *testing.T, handler func(net.Conn, *bufio.Scanner) error) *fakeACPAgent {
	t.Helper()
	return &fakeACPAgent{acptest.New(t, handler)}
}

func (s *fakeACPAgent) socketPath() string {
	return s.SocketPath()
}

func (s *fakeACPAgent) waitForDisconnect(t *testing.T) {
	s.WaitForDisconnect(t)
}

func writeACPResponse(conn net.Conn, id json.RawMessage, result string) error {
	return acptest.WriteResponse(conn, id, result)
}

func writeACPError(conn net.Conn, id json.RawMessage, code int, message string) error {
	return acptest.WriteError(conn, id, code, message)
}

func writeACPUpdate(conn net.Conn, sessionID acp.SessionId, update acp.SessionUpdate) error {
	return acptest.WriteUpdate(conn, sessionID, update)
}

// startACPServer retains the original helper's compact API for tests that do
// not need to inspect the fake peer after RunACPTask returns.
func startACPServer(t *testing.T, handler func(net.Conn, *bufio.Scanner) error) string {
	t.Helper()
	return newFakeACPAgent(t, handler).socketPath()
}

func waitForClientClose(scanner *bufio.Scanner) error {
	return acptest.WaitForClientClose(scanner)
}

func waitForClientCloseWithoutSessionCancel(scanner *bufio.Scanner) error {
	return acptest.WaitForClientCloseWithoutSessionCancel(scanner)
}
