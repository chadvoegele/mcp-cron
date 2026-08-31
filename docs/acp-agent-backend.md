+++
status = "final"
created = 2026-08-31
last_update = 2026-08-31
+++

# ACP-backed AI tasks

## Purpose

Add the Agent Client Protocol (ACP) as an alternative backend for mcp-cron AI tasks. This lets operators run scheduled and on-demand prompts through an existing local ACP agent while retaining mcp-cron’s task, scheduling, timeout, and result APIs.

## Context

mcp-cron currently executes AI tasks through OpenAI, Anthropic, or OpenAI-compatible APIs. An ACP agent can instead own the model, agent loop, and MCP servers. In this integration, mcp-cron is an ACP client and the already-running local agent is the ACP server.

## Goals

- Execute scheduled and on-demand AI tasks through an existing ACP agent.
- Connect through a Unix-domain socket without launching or supervising the agent.
- Preserve the existing AI task and result APIs and database schema.
- Use MCP servers configured by the ACP agent.
- Persist the agent’s final textual response in the existing result store.
- Leave non-ACP provider behavior unchanged.

## Non-goals

- Launching, supervising, or authenticating an ACP agent.
- Persistent ACP connections or session reuse.
- Passing mcp-cron’s MCP server or MCP configuration to the ACP agent.
- Per-task ACP server selection or ACP-specific task fields.
- The internal `get_task_result` tool and previous-result context in ACP mode.
- Client-side filesystem, terminal, permission, or interactive user support.
- ACP transports other than a Unix-domain socket.

## Specification

### Provider and configuration

`MCP_CRON_AI_PROVIDER` already selects the global AI backend. Its accepted values must be `openai`, `anthropic`, and `acp`; an empty value means `openai`. Unknown values must fail configuration validation rather than silently selecting OpenAI.

ACP adds these options:

```text
--ai-provider acp
--acp-socket /run/my-agent/acp.sock
--acp-cwd /home/chad/project
```

```text
MCP_CRON_AI_PROVIDER=acp
MCP_CRON_ACP_SOCKET=/run/my-agent/acp.sock
MCP_CRON_ACP_CWD=/home/chad/project
```

When the provider is `acp`:

- The socket path is required and must be absolute.
- The working directory is required and must be absolute.
- OpenAI and Anthropic credentials are not required.
- `--ai-model` and `MCP_CRON_AI_MODEL` are ignored.
- `--ai-base-url` and `MCP_CRON_AI_BASE_URL` are ignored.
- `--mcp-config-path` and `MCP_CRON_MCP_CONFIG_FILE_PATH` are ignored.

The paths are application configuration, not persisted task fields.

### Task and execution integration

`add_ai_task` must not gain ACP-specific fields. Tasks continue to have `type: "AI"`, and `run_task`, `get_task_result`, and scheduled execution retain their existing behavior.

`MCPServer.Execute` continues routing AI tasks to `AgentExecutor`. ACP selection occurs in the AI execution path before the existing MCP configuration loading and OpenAI/Anthropic provider loop. The existing paths remain unchanged.

`AgentExecutor` continues to create the execution context, persist `model.Result`, and report errors. ACP execution must return the collected output and an error through that existing path. Failed ACP executions must have `ExitCode = 1`; successful executions must have `ExitCode = 0`.

### Connection and ACP lifecycle

The configured ACP agent must already be listening on the Unix socket. mcp-cron must not start it or use `socat`.

Each task execution must create one isolated connection and session:

1. Dial the socket with a 10-second connection deadline. The dial must also respect the task execution context.
2. Construct an ACP client-side connection over the bidirectional Unix socket using the selected, pinned Go ACP SDK.
3. Call `initialize` with protocol version 1 and no filesystem, terminal, or authentication client capabilities.
4. Require the initialize response to negotiate protocol version 1. Treat advertised authentication methods as optional; mcp-cron does not call `authenticate`. If a later request returns an authentication-required error, fail the task.
5. Call `session/new` with the configured working directory and a non-nil empty `mcpServers` array.
6. Call `session/prompt` with the task prompt as one text content block.
7. Receive `session/update` notifications while the prompt runs.
8. Map the prompt response and collected output to `model.Result`.
9. Close the socket, including after every failure.

The Unix socket is a local trust boundary. mcp-cron must rely on operating-system socket permissions and must not bypass them.

There must be no concurrent prompts on one ACP connection. Separate task executions use separate connections and sessions.

### Timeout and cancellation

The existing scheduler task timeout is the ACP execution timeout. It is currently 10 minutes by default and is configured through `MCP_CRON_SCHEDULER_DEFAULT_TIMEOUT`.

`AgentExecutor` must create the timeout context before the ACP lifecycle begins. The deadline covers dialing, initialization, session creation, and prompting. ACP does not add a separate task timeout.

When the prompt context is cancelled, the Go ACP SDK must cause a `session/cancel` notification for the active session. The SDK used by this integration performs that notification when its `Prompt` call observes context cancellation. mcp-cron must then close the socket even if the agent does not honor cancellation.

If cancellation occurs before a session exists, no `session/cancel` is sent; the pending request fails and the socket is closed.

The scheduler runs executions asynchronously. The existing `run_task` handler triggers execution and waits for a persisted result for at most the configured task timeout. Cancelling the `run_task` request stops that wait but does not cancel the already-scheduled execution.

### Output and stop reasons

The ACP client must append text from `agent_message_chunk` updates in arrival order. Only chunks whose content is text are included in `Result.Output`. Thought chunks, plans, tool-call updates, and non-text content are excluded initially.

The prompt response does not contain the final text; the collected agent message chunks are the output. The ACP SDK’s notification ordering must be relied on so updates sent before the prompt response are processed before `Prompt` returns.

Stop reasons map as follows:

- `end_turn`: success, exit code 0.
- `max_tokens`, `max_turn_requests`, `refusal`, and `cancelled`: failure, exit code 1.
- An unknown stop reason: failure, exit code 1.
- ACP initialization, request, connection, transport, timeout, and cancellation errors: failure, exit code 1.

`Result.Error` must contain a diagnostic for failures. Collected agent text must be retained in `Result.Output` when available, including for failed stop reasons.

### Client callbacks and MCP behavior

The client must implement `SessionUpdate`. Required filesystem and terminal callbacks must return unsupported-operation errors. Permission requests must be rejected with a clear error. These capabilities must not be advertised during initialization.

mcp-cron must not load its MCP configuration or perform MCP tool discovery in ACP mode. The ACP agent’s own MCP configuration is authoritative. The empty `mcpServers` request means the agent must provide any self-configured MCP servers; agents that require servers to be supplied by the client are not supported by this integration.

### Compatibility and migration

No task-table or result-table migration is required. Existing task definitions remain valid. No change is required to the MCP tool names or their request and response shapes.

The implementation must add and pin the Go ACP SDK dependency. Existing OpenAI, Anthropic, and OpenAI-compatible provider behavior must continue to pass its current tests.

## Verification

Use a fake ACP agent over a temporary Unix socket. Tests must not require an external agent, model provider, credentials, or network access.

Verify:

- Existing and ACP provider configuration loads from flags and environment variables.
- Missing or non-absolute ACP paths and unknown providers are rejected.
- ACP mode does not require API keys and ignores model, base URL, and MCP configuration settings.
- Socket connection failures produce failed results and close resources.
- `initialize`, `session/new`, and `session/prompt` receive the expected requests, including protocol version 1, working directory, empty MCP servers, and prompt text.
- Advertised authentication methods, including `pi_terminal_login`, are accepted without calling `authenticate`.
- Later authentication-required responses persist failed results.
- Multiple text message chunks are aggregated in order and non-text updates are ignored.
- Every defined stop reason and unknown stop reasons map correctly.
- Task timeout causes `session/cancel`, closes the socket, and persists a failed result.
- Cancellation during initialization or session creation closes the socket.
- Successful and failed executions persist results through `get_task_result`.
- Existing `add_ai_task`, `run_task`, scheduled execution, and non-ACP provider behavior remain unchanged.
