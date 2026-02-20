from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import os
import shlex
import signal
from collections import deque
from typing import Any, Literal

# The decky plugin module is located at decky-loader/plugin
# For easy intellisense checkout the decky-loader code repo
# and add the `decky-loader/plugin/imports` path to `python.analysis.extraPaths` in `.vscode/settings.json`
import decky

LifecycleState = Literal["stopped", "starting", "running", "stopping", "error"]

ALLOWED_TRANSITIONS: dict[LifecycleState, set[LifecycleState]] = {
    "stopped": {"starting", "error"},
    "starting": {"running", "stopping", "error"},
    "running": {"stopping", "error"},
    "stopping": {"stopped", "error"},
    "error": {"starting", "stopping", "stopped", "error"},
}

DEFAULT_SUPERVISED_COMMAND = ["sleep", "2147483647"]
LOG_BUFFER_MAX_LINES = 2000


class Plugin:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self._lifecycle_lock = asyncio.Lock()

        self._state: LifecycleState = "stopped"
        self._last_error: dict[str, Any] | None = None
        self._started_at: str | None = None
        self._last_transition_at = self._utc_now()

        self._process: asyncio.subprocess.Process | None = None
        self._owned_pgid: int | None = None
        self._current_command: list[str] | None = None

        self._monitor_task: asyncio.Task[None] | None = None
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None

        self._logs: deque[dict[str, Any]] = deque(maxlen=LOG_BUFFER_MAX_LINES)
        self._next_log_seq = 1

    # Diagnostic method for frontend/backend connectivity checks.
    async def add(self, left: int, right: int) -> int:
        return left + right

    async def long_running(self) -> None:
        await asyncio.sleep(15)
        await decky.emit("timer_event", "Hello from pw-ac3-live backend", True, 2)

    async def start_timer(self) -> None:
        if self.loop is None:
            self.loop = asyncio.get_running_loop()
        self.loop.create_task(self.long_running())

    async def start_encoder(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        async with self._lifecycle_lock:
            if self._state not in {"stopped", "error"} or self._is_process_alive_locked():
                error = self._make_error(
                    code="START_REJECTED_STATE",
                    message="Cannot start encoder from current state.",
                    detail=f"state={self._state}",
                    hint="Stop the current session before starting again.",
                )
                self._append_log("WARN", error["detail"])
                await self._emit_error(error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": error,
                }

            command, cwd, env_override, config_error = self._resolve_start_command(config)
            if config_error is not None:
                self._last_error = config_error
                self._append_log(
                    "ERROR",
                    f"{config_error['code']}: {config_error['detail']}",
                )
                self._transition_locked("error")
                await self._emit_state_changed_locked()
                await self._emit_error(config_error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": config_error,
                }

            self._transition_locked("starting")
            self._last_error = None
            self._append_log(
                "INFO",
                f"Starting supervised command: {' '.join(shlex.quote(arg) for arg in command)}",
            )
            await self._emit_state_changed_locked()

            try:
                await self._spawn_supervised_process_locked(
                    command=command,
                    cwd=cwd,
                    env_override=env_override,
                )
            except Exception as exc:
                error = self._make_error(
                    code="START_PROCESS_FAILED",
                    message="Failed to spawn encoder process.",
                    detail=str(exc),
                    hint="Inspect logs and verify executable/runtime environment.",
                )
                self._last_error = error
                self._append_log("ERROR", f"{error['code']}: {error['detail']}")
                self._transition_locked("error")
                await self._emit_state_changed_locked()
                await self._emit_error(error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": error,
                }

            self._transition_locked("running")
            await self._emit_state_changed_locked()
            return {
                "ok": True,
                "status": self._build_status_payload_locked(),
                "error": None,
            }

    async def stop_encoder(
        self,
        force: bool = False,
        timeout_ms: int = 5000,
    ) -> dict[str, Any]:
        start = asyncio.get_running_loop().time()
        normalized_timeout_ms = self._normalize_timeout_ms(timeout_ms)

        async with self._lifecycle_lock:
            if self._state == "stopped" and not self._is_process_alive_locked():
                error = self._make_error(
                    code="STOP_REJECTED_STATE",
                    message="Encoder is already stopped.",
                    detail=f"state={self._state}",
                    hint="Start the encoder before issuing stop.",
                )
                self._append_log("WARN", error["detail"])
                await self._emit_error(error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": error,
                    "stopped_pid": None,
                    "duration_ms": self._duration_ms(start),
                }

            if self._state == "stopping":
                error = self._make_error(
                    code="STOP_REJECTED_STATE",
                    message="Encoder stop is already in progress.",
                    detail=f"state={self._state}",
                    hint="Wait for the current stop transition to complete.",
                )
                self._append_log("WARN", error["detail"])
                await self._emit_error(error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": error,
                    "stopped_pid": None,
                    "duration_ms": self._duration_ms(start),
                }

            stopped_pid = self._process.pid if self._process is not None else None
            if self._state != "stopped":
                self._transition_locked("stopping")
                await self._emit_state_changed_locked()

            stop_error = await self._terminate_process_group_locked(
                force=force,
                timeout_ms=normalized_timeout_ms,
            )

            if stop_error is not None:
                self._last_error = stop_error
                self._append_log("ERROR", f"{stop_error['code']}: {stop_error['detail']}")
                self._transition_locked("error")
                await self._emit_state_changed_locked()
                await self._emit_error(stop_error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": stop_error,
                    "stopped_pid": stopped_pid,
                    "duration_ms": self._duration_ms(start),
                }

            self._last_error = None
            self._transition_locked("stopped")
            await self._emit_state_changed_locked()
            self._append_log("INFO", "Supervised process stopped cleanly.")
            return {
                "ok": True,
                "status": self._build_status_payload_locked(),
                "error": None,
                "stopped_pid": stopped_pid,
                "duration_ms": self._duration_ms(start),
            }

    async def get_status(self) -> dict[str, Any]:
        async with self._lifecycle_lock:
            return {
                "status": self._build_status_payload_locked(),
                "pipeline": {
                    "producer_pid": self._active_pid_locked(),
                    "consumer_pid": None,
                },
                "last_transition_at": self._last_transition_at,
            }

    async def get_recent_logs(
        self,
        limit: int = 200,
        since_seq: int | None = None,
    ) -> dict[str, Any]:
        async with self._lifecycle_lock:
            normalized_limit = self._normalize_log_limit(limit)
            normalized_since = self._normalize_since_seq(since_seq)

            entries = list(self._logs)
            if normalized_since is not None:
                entries = [entry for entry in entries if entry["seq"] > normalized_since]

            truncated = len(entries) > normalized_limit
            if truncated:
                entries = entries[-normalized_limit:]

            return {
                "entries": entries,
                "next_seq": self._next_log_seq,
                "truncated": truncated,
            }

    # Asyncio-compatible long-running code, executed in a task when the plugin is loaded
    async def _main(self) -> None:
        self.loop = asyncio.get_event_loop()
        self._append_log("INFO", "pw-ac3-live backend loaded")
        async with self._lifecycle_lock:
            await self._emit_state_changed_locked()

    # Function called first during the unload process, utilize this to handle your plugin being stopped, but not
    # completely removed
    async def _unload(self) -> None:
        self._append_log("INFO", "pw-ac3-live backend unloading")
        await self._cleanup_for_lifecycle("unload")

    # Function called after `_unload` during uninstall, utilize this to clean up processes and other remnants of your
    # plugin that may remain on the system
    async def _uninstall(self) -> None:
        self._append_log("INFO", "pw-ac3-live backend uninstall")
        await self._cleanup_for_lifecycle("uninstall")

    # Migrations that should be performed before entering `_main()`.
    async def _migration(self) -> None:
        self._append_log("INFO", "Running migrations for decky-pw-ac3-live")

        decky.migrate_logs(
            os.path.join(
                decky.DECKY_USER_HOME,
                ".config",
                "decky-pw-ac3-live",
                "plugin.log",
            )
        )
        decky.migrate_settings(
            os.path.join(decky.DECKY_HOME, "settings", "decky-pw-ac3-live.json"),
            os.path.join(decky.DECKY_USER_HOME, ".config", "decky-pw-ac3-live"),
        )
        decky.migrate_runtime(
            os.path.join(decky.DECKY_HOME, "decky-pw-ac3-live"),
            os.path.join(decky.DECKY_USER_HOME, ".local", "share", "decky-pw-ac3-live"),
        )

    async def _spawn_supervised_process_locked(
        self,
        command: list[str],
        cwd: str | None,
        env_override: dict[str, str] | None,
    ) -> None:
        env = os.environ.copy()
        if env_override:
            env.update(env_override)

        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            env=env,
            preexec_fn=os.setsid,
        )

        self._process = process
        self._owned_pgid = os.getpgid(process.pid)
        self._started_at = self._utc_now()
        self._current_command = list(command)

        self._monitor_task = asyncio.create_task(
            self._watch_process_exit(process.pid),
            name=f"pw-ac3-live-monitor-{process.pid}",
        )
        if process.stdout is not None:
            self._stdout_task = asyncio.create_task(
                self._consume_stream(process.stdout, "stdout", process.pid),
                name=f"pw-ac3-live-stdout-{process.pid}",
            )
        if process.stderr is not None:
            self._stderr_task = asyncio.create_task(
                self._consume_stream(process.stderr, "stderr", process.pid),
                name=f"pw-ac3-live-stderr-{process.pid}",
            )

        self._append_log(
            "INFO",
            f"Supervised process started pid={process.pid} pgid={self._owned_pgid}",
        )

    async def _terminate_process_group_locked(
        self,
        force: bool,
        timeout_ms: int,
    ) -> dict[str, Any] | None:
        if self._process is None:
            await self._clear_process_handles_locked()
            return None

        process = self._process
        pid = process.pid
        pgid, ownership_error = self._resolve_owned_pgid_locked(pid)
        if ownership_error is not None:
            return ownership_error
        if pgid is None:
            await self._clear_process_handles_locked()
            return None

        timeout_s = timeout_ms / 1000.0
        if force:
            self._append_log("INFO", f"Force-stopping process group pgid={pgid} with SIGKILL")
            signal_error = self._send_group_signal(pgid, signal.SIGKILL)
            if signal_error is not None:
                return signal_error
            exited = await self._wait_for_process_exit_locked(timeout_s if timeout_s > 0 else 2.0)
            if not exited:
                return self._make_error(
                    code="STOP_TIMEOUT",
                    message="Encoder did not terminate after SIGKILL.",
                    detail=f"pid={pid} pgid={pgid} timeout_ms={timeout_ms}",
                    hint="Retry stop or restart Decky.",
                )
            await self._clear_process_handles_locked()
            return None

        grace_s = min(timeout_s, 3.0) if timeout_s > 0 else 3.0
        hard_s = max(timeout_s - grace_s, 0.0)
        if hard_s <= 0:
            hard_s = 2.0

        self._append_log("INFO", f"Stopping process group pgid={pgid} with SIGTERM")
        signal_error = self._send_group_signal(pgid, signal.SIGTERM)
        if signal_error is not None:
            return signal_error

        exited_gracefully = await self._wait_for_process_exit_locked(grace_s)
        if exited_gracefully:
            await self._clear_process_handles_locked()
            return None

        self._append_log("WARN", f"Process group pgid={pgid} still alive, escalating to SIGKILL")
        signal_error = self._send_group_signal(pgid, signal.SIGKILL)
        if signal_error is not None:
            return signal_error

        exited_after_kill = await self._wait_for_process_exit_locked(hard_s)
        if not exited_after_kill:
            return self._make_error(
                code="STOP_TIMEOUT",
                message="Encoder process group did not terminate in time.",
                detail=f"pid={pid} pgid={pgid} timeout_ms={timeout_ms}",
                hint="Retry stop or restart Decky.",
            )

        await self._clear_process_handles_locked()
        return None

    async def _clear_process_handles_locked(self) -> None:
        await self._cancel_task_locked(self._stdout_task)
        await self._cancel_task_locked(self._stderr_task)
        await self._cancel_task_locked(self._monitor_task)

        self._stdout_task = None
        self._stderr_task = None
        self._monitor_task = None
        self._process = None
        self._owned_pgid = None
        self._current_command = None
        self._started_at = None

    async def _cancel_task_locked(self, task: asyncio.Task[None] | None) -> None:
        if task is None or task.done():
            return
        if task is asyncio.current_task():
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _watch_process_exit(self, pid: int) -> None:
        process = self._process
        if process is None or process.pid != pid:
            return

        returncode = await process.wait()
        async with self._lifecycle_lock:
            if self._process is None or self._process.pid != pid:
                return

            self._append_log(
                "INFO",
                f"Supervised process pid={pid} exited with code {returncode}",
            )
            await self._clear_process_handles_locked()

            if self._state == "stopping":
                self._transition_locked("stopped")
                await self._emit_state_changed_locked()
                return

            if self._state in {"running", "starting"}:
                error = self._make_error(
                    code="PIPELINE_BROKEN",
                    message="Encoder process exited unexpectedly.",
                    detail=f"pid={pid} returncode={returncode}",
                    hint="Inspect logs and restart encoder.",
                )
                self._last_error = error
                self._transition_locked("error")
                await self._emit_state_changed_locked()
                await self._emit_error(error)
                return

            if self._state != "stopped":
                self._transition_locked("stopped")
                await self._emit_state_changed_locked()

    async def _consume_stream(
        self,
        stream: asyncio.StreamReader,
        stream_name: str,
        pid: int,
    ) -> None:
        while True:
            try:
                raw = await stream.readline()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._append_log(
                    "ERROR",
                    f"{stream_name} reader failed for pid={pid}: {exc}",
                )
                return

            if not raw:
                return

            message = raw.decode(errors="replace").rstrip()
            if message:
                self._append_log("INFO", f"{stream_name}[{pid}]: {message}")

    def _resolve_start_command(
        self,
        config: dict[str, Any] | None,
    ) -> tuple[list[str], str | None, dict[str, str] | None, dict[str, Any] | None]:
        if config is None:
            config = {}
        if not isinstance(config, dict):
            return [], None, None, self._make_error(
                code="CONFIG_INVALID",
                message="start_encoder config must be an object.",
                detail=f"received_type={type(config).__name__}",
                hint="Pass a JSON object to start_encoder.",
            )

        command: list[str]
        command_override = config.get("command")
        if command_override is not None:
            if not isinstance(command_override, list) or not command_override:
                return [], None, None, self._make_error(
                    code="CONFIG_INVALID",
                    message="command must be a non-empty string array.",
                    detail=f"command={command_override!r}",
                    hint="Provide command like ['sleep', '2147483647'].",
                )
            if any(not isinstance(arg, str) or not arg.strip() for arg in command_override):
                return [], None, None, self._make_error(
                    code="CONFIG_INVALID",
                    message="command array must only contain non-empty strings.",
                    detail=f"command={command_override!r}",
                    hint="Check command entries and retry.",
                )
            command = [arg.strip() for arg in command_override]
        else:
            binary_path = config.get("binary_path")
            extra_args = config.get("extra_args", [])

            if binary_path is None:
                command = list(DEFAULT_SUPERVISED_COMMAND)
            else:
                if not isinstance(binary_path, str) or not binary_path.strip():
                    return [], None, None, self._make_error(
                        code="CONFIG_INVALID",
                        message="binary_path must be a non-empty string when provided.",
                        detail=f"binary_path={binary_path!r}",
                        hint="Set binary_path to an executable path.",
                    )
                if not isinstance(extra_args, list) or any(
                    not isinstance(arg, str) for arg in extra_args
                ):
                    return [], None, None, self._make_error(
                        code="CONFIG_INVALID",
                        message="extra_args must be a string array.",
                        detail=f"extra_args={extra_args!r}",
                        hint="Set extra_args as a list of CLI arguments.",
                    )
                command = [binary_path.strip(), *extra_args]

        cwd: str | None = None
        cwd_value = config.get("cwd")
        if cwd_value is not None:
            if not isinstance(cwd_value, str) or not cwd_value.strip():
                return [], None, None, self._make_error(
                    code="CONFIG_INVALID",
                    message="cwd must be a non-empty string when provided.",
                    detail=f"cwd={cwd_value!r}",
                    hint="Provide a valid working directory path.",
                )
            cwd = cwd_value.strip()

        env_override: dict[str, str] | None = None
        env_value = config.get("env")
        if env_value is not None:
            if not isinstance(env_value, dict):
                return [], None, None, self._make_error(
                    code="CONFIG_INVALID",
                    message="env must be an object of string pairs.",
                    detail=f"env_type={type(env_value).__name__}",
                    hint="Provide env as {\"KEY\": \"value\"}.",
                )
            env_override = {}
            for key, value in env_value.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    return [], None, None, self._make_error(
                        code="CONFIG_INVALID",
                        message="env keys and values must be strings.",
                        detail=f"env_entry={key!r}:{value!r}",
                        hint="Cast env keys and values to strings.",
                    )
                env_override[key] = value

        return command, cwd, env_override, None

    def _resolve_owned_pgid_locked(self, pid: int) -> tuple[int | None, dict[str, Any] | None]:
        observed_pgid: int | None = None
        with contextlib.suppress(ProcessLookupError):
            observed_pgid = os.getpgid(pid)

        if observed_pgid is None:
            return None, None

        if self._owned_pgid is None:
            self._owned_pgid = observed_pgid
            return observed_pgid, None

        if observed_pgid != self._owned_pgid:
            return None, self._make_error(
                code="PIPELINE_BROKEN",
                message="Managed process ownership changed unexpectedly.",
                detail=f"pid={pid} expected_pgid={self._owned_pgid} actual_pgid={observed_pgid}",
                hint="Do not reuse external processes; restart encoder.",
            )

        return observed_pgid, None

    def _send_group_signal(
        self,
        pgid: int,
        signum: signal.Signals,
    ) -> dict[str, Any] | None:
        try:
            os.killpg(pgid, signum)
            return None
        except ProcessLookupError:
            return None
        except PermissionError as exc:
            return self._make_error(
                code="PERMISSION_DENIED",
                message="Insufficient permission to signal encoder process group.",
                detail=f"pgid={pgid} signal={signum.name} error={exc}",
                hint="Verify plugin runtime permissions.",
            )
        except Exception as exc:
            return self._make_error(
                code="PIPELINE_BROKEN",
                message="Failed to signal encoder process group.",
                detail=f"pgid={pgid} signal={signum.name} error={exc}",
                hint="Inspect logs and retry stop.",
            )

    async def _wait_for_process_exit_locked(self, timeout_s: float) -> bool:
        if self._process is None:
            return True
        if self._process.returncode is not None:
            return True
        try:
            await asyncio.wait_for(self._process.wait(), timeout=timeout_s)
            return True
        except asyncio.TimeoutError:
            return False

    async def _cleanup_for_lifecycle(self, source: str) -> None:
        async with self._lifecycle_lock:
            if self._state == "stopped" and not self._is_process_alive_locked():
                self._append_log("INFO", f"No active process to clean during {source}.")
                return

        result = await self.stop_encoder(force=True, timeout_ms=5000)
        if result["ok"]:
            self._append_log("INFO", f"Lifecycle cleanup completed for {source}.")
            return

        error = result.get("error")
        if error is not None and error.get("code") == "STOP_REJECTED_STATE":
            self._append_log("INFO", f"No active process to clean during {source}.")
            return

        self._append_log("ERROR", f"Lifecycle cleanup failed for {source}: {error}")

    async def _emit_state_changed_locked(self) -> None:
        await self._emit_safe("encoder_state_changed", self._build_status_payload_locked())

    async def _emit_error(self, error: dict[str, Any]) -> None:
        await self._emit_safe("encoder_error", error)

    async def _emit_safe(self, event: str, *args: Any) -> None:
        try:
            await decky.emit(event, *args)
        except Exception as exc:
            self._append_log("WARN", f"Failed to emit event {event}: {exc}")

    def _transition_locked(self, next_state: LifecycleState) -> None:
        if next_state == self._state:
            return
        if next_state not in ALLOWED_TRANSITIONS[self._state]:
            raise RuntimeError(f"illegal transition {self._state} -> {next_state}")
        self._state = next_state
        self._last_transition_at = self._utc_now()

    def _build_status_payload_locked(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "pid": self._active_pid_locked(),
            "started_at": self._started_at,
            "last_error": self._last_error,
        }

    def _active_pid_locked(self) -> int | None:
        if self._process is None:
            return None
        if self._process.returncode is not None:
            return None
        return self._process.pid

    def _is_process_alive_locked(self) -> bool:
        return self._active_pid_locked() is not None

    def _append_log(self, level: str, message: str) -> None:
        normalized_level = level.upper()
        entry = {
            "seq": self._next_log_seq,
            "ts": self._utc_now(),
            "level": normalized_level,
            "message": message,
        }
        self._next_log_seq += 1
        self._logs.append(entry)

        logger_method_name = normalized_level.lower()
        if logger_method_name == "warn":
            logger_method_name = "warning"
        logger_method = getattr(decky.logger, logger_method_name, decky.logger.info)
        logger_method(message)

    def _make_error(
        self,
        code: str,
        message: str,
        detail: str,
        hint: str,
    ) -> dict[str, Any]:
        return {
            "code": code,
            "message": message,
            "detail": detail,
            "hint": hint,
            "timestamp": self._utc_now(),
        }

    def _normalize_timeout_ms(self, timeout_ms: int) -> int:
        if not isinstance(timeout_ms, int):
            return 5000
        return max(timeout_ms, 0)

    def _normalize_log_limit(self, limit: int) -> int:
        if not isinstance(limit, int):
            return 200
        return max(1, min(limit, LOG_BUFFER_MAX_LINES))

    def _normalize_since_seq(self, since_seq: int | None) -> int | None:
        if since_seq is None:
            return None
        if not isinstance(since_seq, int):
            return None
        return max(since_seq, 0)

    def _duration_ms(self, start: float) -> int:
        now = asyncio.get_running_loop().time()
        return max(0, int((now - start) * 1000))

    @staticmethod
    def _utc_now() -> str:
        return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
