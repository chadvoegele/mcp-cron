+++
status = "draft"
created = 2026-09-01
last_update = 2026-09-01
+++

# One-shot scheduled tasks

> Prepared by Chad's Agent.

## Purpose

Allow an agent to schedule one shell, HTTP, or AI task for one absolute time without encoding a reminder as a recurring cron expression or depending on the task itself to disable. The scheduled definition must be durable across restarts and safe across multiple mcp-cron instances.

## Context

mcp-cron already persists task definitions and `next_run` in SQLite. Its poll loop finds due tasks and uses an atomic optimistic-lock update before dispatching them. One-shot execution should use that existing path rather than add another scheduler.

## Goals

- Accept an optional absolute `run_at` timestamp on shell, HTTP, and AI task creation and update operations.
- Persist the timestamp and restore it across restarts.
- Execute an enabled one-shot task once when its time is due, then durably disable and consume it.
- Preserve the existing behavior of recurring and on-demand tasks.
- Keep the existing executor routing, result persistence, and `run_task` behavior.
- Deduplicate a one-shot claim when multiple server instances share a database.

## Non-goals

- Natural-language durations or relative scheduling. The caller must calculate an absolute timestamp.
- Recurrence, retries, delivery guarantees, or a new executor type.
- A separate scheduler or background worker model.
- Changing the semantics of existing `schedule` or on-demand tasks.

## Specification

### Task modes and API

The task request fields are mutually exclusive:

- `schedule` only: recurring task; existing behavior is unchanged.
- `run_at` only: one-shot task.
- Neither: on-demand task; it runs only through `run_task`.
- Both: reject the request with an invalid-input error.

`run_at` must be an RFC 3339 timestamp containing an absolute instant. The server must normalize it to UTC for storage and comparison. A timestamp in the past is valid and becomes due on the next poll. A malformed timestamp must be rejected.

`run_at` must be available on `add_task`, `add_ai_task`, `add_http_task`, and `update_task`. The task response must expose the configured value using the existing task JSON naming convention (`runAt`).

When a task is created or updated with `enabled: true` and `run_at`, its `next_run` must be set to that instant. A disabled task retains its `run_at` but has no `next_run` until enabled. `enable_task` arms a pending one-shot task for its configured time. `disable_task` clears `next_run` without clearing a pending `run_at`.

An update that includes `run_at: null` clears the one-shot time. An omitted `run_at` leaves it unchanged. Supplying a new `run_at` explicitly re-arms a task when it is enabled. Updates must validate the final combination of `schedule` and `run_at`, not only the fields present in the request.

### Persistence and state

Migration 5 must add an optional `run_at` text column to `tasks`, using an empty string for an absent value to match existing task columns. Existing rows must remain valid and behave as they do today.

The task model must represent the optional value distinctly from a zero timestamp. The following states are required:

| Mode | `schedule` | `run_at` | Enabled | `next_run` |
| --- | --- | --- | --- | --- |
| Recurring | non-empty | absent | true | next cron instant |
| Armed one-shot | empty | present | true | `run_at` or an immediate manual trigger |
| Disarmed one-shot | empty | present | false | empty |
| On-demand | empty | absent | either | empty |
| Consumed one-shot | empty | absent | false | empty |

The server must not persist a spurious `next_run` for disabled or on-demand tasks.

### Claiming and execution

On each poll, a due task with `run_at` must be claimed with one atomic database update that verifies its current `next_run` and enabled state, then clears `run_at` and `next_run` and sets `enabled` to false. Only the instance whose update affects one row may dispatch the task. Clearing `run_at` on claim prevents `enable_task` from silently re-arming a consumed task; a caller may explicitly set a new `run_at` to schedule it again.

The task snapshot obtained before the claim must be dispatched through the existing shell, HTTP, or AI executor. Claiming occurs before execution, so a failed execution or process termination after the claim does not automatically retry the task. The result must still be persisted through the existing result path when execution starts normally. The task is disabled regardless of executor success.

`run_task` on an armed one-shot task must trigger it immediately and consume it through the same claim path. `run_task` on a consumed or disabled task follows the existing disabled-task error behavior. Recurring tasks triggered by `run_task` must continue to resume their cron schedule.

After a successful claim, the in-memory task must reflect `enabled: false` and an empty `next_run` before execution starts. Its runtime status may transition through `running` to `completed` or `failed`, as it does for other tasks.

### Compatibility and migration

The existing `next_run` optimistic-lock path remains the recurring-task path. The store must add a distinct atomic consume operation rather than overload recurring advancement with one-shot behavior. No result schema or executor interface change is required.

Documentation for task creation, task modes, and the database schema must describe `run_at`, including that it is an absolute UTC-normalized timestamp and that a claimed task is consumed before execution.

## Verification

- Create each task type with a future `run_at`, reload it from SQLite, and verify the timestamp round-trips as the same instant and becomes `next_run` when enabled.
- Verify malformed, relative, and `schedule` plus `run_at` requests are rejected; verify past timestamps are accepted and become due.
- Verify disabled one-shot tasks have no `next_run`, can be armed with `enable_task`, and can be disarmed without losing their pending `run_at`.
- Verify `run_at: null` clears the field and that omitted update fields remain unchanged.
- Verify a due one-shot executes once, persists its result, and is disabled with empty `run_at` and `next_run` after the atomic claim.
- Run two schedulers against one database and verify a due one-shot is claimed and executed once.
- Verify executor failure still leaves the one-shot consumed and disabled; verify a restart after claim does not execute it again.
- Verify `run_task` immediately consumes an armed one-shot and that recurring and on-demand lifecycle tests remain unchanged.
- Verify migration from a pre-migration database and confirm the existing recurring and on-demand task behavior passes unchanged.
