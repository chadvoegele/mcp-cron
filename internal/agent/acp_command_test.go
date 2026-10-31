// SPDX-License-Identifier: AGPL-3.0-only
package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jolks/mcp-cron/internal/config"
)

// TestACPCommandProcess runs as a real stdio agent in a child test process.
func TestACPCommandProcess(t *testing.T) {
	mode := os.Getenv("MCP_CRON_TEST_ACP_PROCESS")
	if mode == "" {
		return
	}
	if mode == "exit" {
		os.Exit(1)
	}
	if mode == "hang" {
		time.Sleep(time.Hour)
		os.Exit(1)
	}
	if os.Args[len(os.Args)-1] != "argument with spaces" {
		os.Exit(2)
	}
	fmt.Fprintln(os.Stderr, "agent diagnostic, not protocol output")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var request acpRPCRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			os.Exit(3)
		}
		var result string
		switch request.Method {
		case "initialize":
			result = `{"protocolVersion":1,"authMethods":[]}`
		case "session/new":
			result = `{"sessionId":"stdio-process"}`
		case "session/prompt":
			fmt.Fprintln(os.Stdout, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"stdio-process","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"stdio output"}}}}`)
			result = `{"stopReason":"end_turn"}`
		default:
			os.Exit(4)
		}
		fmt.Fprintf(os.Stdout, "{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":%s}\n", request.ID, result)
	}
	os.Exit(0)
}

func TestRunACPCommandProcess(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"success", "exit", "hang"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MCP_CRON_TEST_ACP_PROCESS", mode)
			cfg := config.DefaultConfig()
			cfg.AI.ACPCommand = strconv.Quote(executable) + ` -test.run=^TestACPCommandProcess$ -- "argument with spaces"`
			cfg.AI.ACPCWD = t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			output, err := RunACPTask(ctx, "prompt", cfg)
			if mode == "success" {
				if err != nil || output != "stdio output" {
					t.Fatalf("output = %q, error = %v", output, err)
				}
			} else if err == nil {
				t.Fatal("expected an error")
			}
			if time.Since(start) > 4*time.Second {
				t.Fatal("command was not cleaned up promptly")
			}
		})
	}
}

func TestRunACPCommandInvalid(t *testing.T) {
	for _, command := range []string{"", "  ", `""`, `agent "unterminated`} {
		t.Run(command, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.AI.ACPCommand = command
			if _, err := RunACPTask(context.Background(), "prompt", cfg); err == nil || !strings.Contains(err.Error(), "ACP command") {
				t.Fatalf("expected command validation error, got %v", err)
			}
		})
	}
}
