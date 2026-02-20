# IMPLEMENTATION PLAN - decky-pw-ac3-live

## Mission
Deliver a production-ready Decky Loader plugin that controls `pw-ac3-live` on Steam Deck, with reliable start/stop, clear status reporting, safe recovery behavior for HDMI/ALSA edge cases, and a self-contained bundled encoder binary (no external `pw-ac3-live` install required).

## Scope
- In scope:
  - Decky frontend controls for runtime management
  - Python backend process supervision
  - Integration with `pw-ac3-live` launcher flow for Steam Deck (Direct ALSA path only)
  - Build, stage, and ship `pw-ac3-live` binary with plugin package
  - Version pinning and provenance tracking of bundled encoder build
  - User-visible diagnostics and recovery actions
- Out of scope:
  - Rewriting `pw-ac3-live` DSP/encoder internals
  - Replacing PipeWire/ALSA system services
  - PipeWire native playback mode in this plugin release
  - Support for non-Steam-Deck platforms

## Supported Platform
- Steam Deck OLED only.
- SteamOS stable channel only.
- Direct ALSA output path only (`--stdout | aplay`).

## Current State
- Frontend and backend template plumbing is present.
- Metadata is configured for `pw-ac3-live`.
- Core process control APIs are not implemented yet.
- Bundled `pw-ac3-live` build/staging pipeline is not implemented yet.

## Execution Tracking
Use this file as the execution source of truth until a separate tracker is added.

Status values:
- `not_started`
- `in_progress`
- `blocked`
- `done`

Required tracking fields for each milestone:
- owner
- status
- target_start
- target_finish
- actual_finish
- blockers
- evidence (commit/PR + verification notes)

Milestone tracker:

| Milestone | Status | Owner | Target Finish | Evidence |
| --- | --- | --- | --- | --- |
| M1 Backend process supervisor | done | Codex | TBD | `main.py` lifecycle supervisor + APIs implemented; local gates passed (`pnpm run build`, `python -m py_compile`, JSON validation, mocked backend smoke for start/stop + duplicate-start + `START_PROCESS_FAILED` + `PIPELINE_BROKEN`) |
| M2 Steam Deck runtime integration | in_progress | Codex | TBD | `main.py` now resolves bundled binary, applies Deck pre-start actions, spawns explicit `pw-ac3-live -> aplay` pipeline, routes sink, runs shared cleanup/restore, rejects unsafe `extra_args`, and normalizes unexpected startup exceptions; local compile/build + mocked config/hardening smoke passed; Steam Deck hardware validation pending |
| M3 Frontend controls and status UX | not_started | TBD | TBD | TBD |
| M4 Diagnostics and recovery | not_started | TBD | TBD | TBD |
| M5 Release hardening | not_started | TBD | TBD | TBD |

## Milestones

### M1 - Backend process supervisor
Implementation tasks:
- Implement backend APIs:
  - `start_encoder(config)`
  - `stop_encoder()`
  - `get_status()`
  - `get_recent_logs(limit=200)`
- Introduce a guarded state machine:
  - states: `stopped | starting | running | stopping | error`
  - legal transitions only; reject invalid transitions with structured errors
- Enforce single active instance with lifecycle lock and PID ownership checks.
- Start subprocesses in a dedicated process group; terminate group on stop.
- Add bounded log buffering (ring buffer in memory with max lines).

Exit criteria:
- Starting twice does not duplicate processes.
- Stop always terminates child process tree.
- Status is correct across startup, running, stopping, failed.

### M2 - Steam Deck runtime integration
Implementation tasks:
- Resolve `pw-ac3-live` binary using deterministic order:
  - `decky.DECKY_PLUGIN_RUNTIME_DIR/bin/pw-ac3-live` if bundled there
  - `decky.DECKY_PLUGIN_DIR/bin/pw-ac3-live` if bundled at plugin root
  - `config.binary_path` only when `allow_external_binary_override=true` (development mode)
  - fail with `BUNDLED_BINARY_MISSING` if no bundled binary is present
- Use Steam Deck OLED hardcoded runtime values (intentional for v1):
  - HDMI card: `alsa_card.pci-0000_04_00.1`
  - Internal speaker card: `alsa_card.pci-0000_04_00.5-platform-nau8821-max`
  - ALSA device: `hw:0,8`
- Implement Deck launch pipeline with explicit subprocesses:
  - producer: `pw-ac3-live --stdout --latency 1536/48000 --ffmpeg-thread-queue-size 4 --ffmpeg-chunk-frames 1536`
  - consumer: `aplay -D hw:0,8 -t raw -f S16_LE -r 48000 -c 2 --buffer-time=60000 --period-time=15000`
- Snapshot audio state before start (default sink/profile + IEC958 relevant bits).
- Apply Steam Deck pre-start actions before pipeline launch:
  - `pactl set-card-profile alsa_card.pci-0000_04_00.1 off`
  - `iecset -c 0 -n 2 audio off rate 48000`
  - `amixer -c 0 set Master/PCM unmute 100%` and `IEC958,2` unmute
- After startup, route playback into encoder input:
  - `pactl set-default-sink pw-ac3-live-input`
  - move active sink inputs to `pw-ac3-live-input`
  - force `pw-ac3-live-input` volume 100% and unmuted
- Idempotent cleanup sequence shared by stop/unload/uninstall:
  - stop process tree
  - `iecset -c 0 -n 2 audio on`
  - restore captured audio state
  - clear runtime state and temporary handles

Exit criteria:
- Start -> audio works.
- Stop/unload -> system returns to expected sink/profile state.
- Failures surface actionable error messages.
- Runtime does not depend on host-installed `pw-ac3-live` command.

### M3 - Frontend controls and status UX
Implementation tasks:
- Replace diagnostic UI with:
  - Start/Stop button
  - Running/Stopped/Error state
  - Last error details
  - Compact runtime config panel (optional)
- Disable conflicting actions during `starting` and `stopping`.
- Subscribe to backend events and refresh status on mount/resume.
- Add user-facing toasts for major state transitions and recoverable failures.

Exit criteria:
- No hidden background state changes.
- UI state always reflects backend status after refresh/reopen.

### M4 - Diagnostics and recovery
Implementation tasks:
- Add backend health checks:
  - required dependency availability (`ffmpeg`, `aplay`, `pactl`, `pw-cli`, `pw-link`, `iecset`, `amixer`, `wpctl`, `pw-metadata`)
  - binary executable validation
  - output path/device sanity checks
- Add one-click recovery callables:
  - `recover_audio_profile()`
  - `recover_default_sink()`
  - `recover_hdmi_unmute()`
- Add suspend/resume lifecycle handling:
  - subscribe to system sleep notifications (`PrepareForSleep` on logind)
  - stop encoder before suspend when running (best-effort with existing stop timeout)
  - restart encoder after resume only if health checks pass
  - if health checks fail after resume, keep stopped and surface actionable error
  - run restore/recovery sequence before restart on resume
  - expose suspend/resume actions and failures in logs and UI status
- Expose recent backend log slice in UI for troubleshooting.

Exit criteria:
- Common failure modes have direct recovery path in UI.
- Logs provide enough context to troubleshoot without shell access.
- Suspend while running triggers clean stop before sleep and restart after resume only when health checks pass.

### M5 - Release hardening
Implementation tasks:
- Versioning and changelog process for each release.
- Packaging verification (`dist`, metadata, license, README).
- Implement bundled encoder delivery pipeline:
  - pin `pw-ac3-live` release version and download GitHub artifact for Steam Deck target (`x86_64-unknown-linux-gnu`)
  - stage binary into plugin package (`bin/pw-ac3-live`)
  - verify artifact checksum/integrity before staging
  - emit build manifest (`pw_ac3_live_build.json`) with source release/tag, asset URL, target triple, build timestamp, and checksum
- Define development override policy:
  - external binary override allowed only when explicitly enabled in settings for local debugging
  - release defaults use bundled binary only
- Validate Decky privilege requirements and set minimal necessary `plugin.json:flags` for runtime commands.
- Decky install/uninstall validation on target Steam Deck hardware.
- Confirm uninstall leaves no orphan runtime process or altered default audio state.

Exit criteria:
- Clean install, upgrade, uninstall on Steam Deck.
- No orphan processes after uninstall.
- `pw-ac3-live` runs from bundled plugin path on clean system with no external installation.
- Ready for repository release and optional Decky Store submission.

## Technical Plan

### Backend responsibilities (`main.py`)
- Own lifecycle and process supervision.
- Serialize start/stop operations.
- Emit events for state transitions and failures.
- Keep logs bounded (ring buffer or capped file reads).

### Frontend responsibilities (`src/index.tsx`)
- Render control surface and current state.
- Invoke backend callables only for user actions.
- Display concise diagnostics and recovery actions.

### API contract (initial)
`start_encoder(config)` config shape:
- `binary_path`: `string | null` (debug override only, default `null`)
- `alsa_device`: `string` (default `"hw:0,8"` for Steam Deck direct ALSA path)
- `latency`: `string` (default `"1536/48000"` on Steam Deck path)
- `buffer_size`: `number | null` (maps to `--buffer-size`)
- `output_buffer_size`: `number | null` (maps to `--output-buffer-size`)
- `ffmpeg_thread_queue_size`: `number` (default `4` on Steam Deck path)
- `ffmpeg_chunk_frames`: `number` (default `1536` on Steam Deck path)
- `aplay_buffer_time_us`: `number` (default `60000`)
- `aplay_period_time_us`: `number` (default `15000`)
- `extra_args`: `string[]` (default `[]`, optional pass-through for known-safe flags only)

`start_encoder` response:
- `ok`: `boolean`
- `status`: status payload
- `error`: structured error or `null`

`stop_encoder(force=false, timeout_ms=5000)` response:
- `ok`: `boolean`
- `status`: status payload
- `error`: structured error or `null`
- `stopped_pid`: `number | null`
- `duration_ms`: `number`

`get_status()` response:
- `status`: status payload
- `pipeline`: `{ producer_pid: number | null, consumer_pid: number | null }`
- `last_transition_at`: ISO timestamp

`get_recent_logs(limit=200, since_seq=null)` response:
- `entries`: `Array<{ seq: number, ts: string, level: string, message: string }>`
- `next_seq`: `number`
- `truncated`: `boolean`

Recovery callable responses:
- `recover_audio_profile()`: `{ ok, error }`
- `recover_default_sink()`: `{ ok, error }`
- `recover_hdmi_unmute()`: `{ ok, error }`

Status payload:
- `state`: `stopped | starting | running | stopping | error`
- `pid`: number | null
- `started_at`: ISO timestamp | null
- `last_error`: error payload | null

Error payload:
- `code`: machine-readable string
- `message`: user-facing summary
- `detail`: short technical context (command/device/exit code)
- `hint`: recovery hint for UI
- `timestamp`: ISO timestamp

Events:
- `encoder_state_changed` (status payload)
- `encoder_error` (error payload)

### Error code catalog (stable strings)
- `CONFIG_INVALID`: invalid startup config or unsupported value range.
- `BUNDLED_BINARY_MISSING`: bundled `pw-ac3-live` binary was not found in plugin paths.
- `BUNDLED_BINARY_NOT_EXECUTABLE`: bundled binary exists but is not executable.
- `BINARY_NOT_FOUND`: debug override path does not resolve to executable.
- `HEALTHCHECK_FAILED`: one or more required runtime checks failed.
- `RESUME_RESTART_SKIPPED`: resume detected but restart skipped because health checks failed.
- `DEPENDENCY_MISSING`: required command unavailable (`aplay`, `pactl`, etc.).
- `DEVICE_UNAVAILABLE`: ALSA target unavailable or busy.
- `START_REJECTED_STATE`: start called outside `stopped`/`error` state.
- `START_PROCESS_FAILED`: producer or consumer failed to spawn.
- `PIPELINE_BROKEN`: producer/consumer exited unexpectedly during runtime.
- `STOP_REJECTED_STATE`: stop called while already `stopped`.
- `STOP_TIMEOUT`: process group did not terminate within timeout budget.
- `AUDIO_SNAPSHOT_FAILED`: unable to capture pre-start audio state.
- `AUDIO_RESTORE_PARTIAL`: cleanup ran but one or more restore steps failed.
- `CLEANUP_FAILED`: non-audio cleanup failure during unload/uninstall.
- `PERMISSION_DENIED`: command failed due to missing privileges.

### Settings and persistence
Persisted settings keys:
- `binary_path_override`: `string | null`
- `allow_external_binary_override`: `boolean`
- `alsa_device`: `string`
- `latency`: `string`
- `buffer_size`: `number | null`
- `output_buffer_size`: `number | null`
- `ffmpeg_thread_queue_size`: `number`
- `ffmpeg_chunk_frames`: `number`
- `aplay_buffer_time_us`: `number`
- `aplay_period_time_us`: `number`
- `extra_args`: `string[]`
- `log_lines`: `number`
- `auto_recover_on_start`: `boolean`
- `auto_restart_after_resume`: `boolean`

Validation rules:
- `latency` must match `N/48000` format.
- `ffmpeg_thread_queue_size >= 1`.
- `ffmpeg_chunk_frames >= 1`.
- `aplay_buffer_time_us > 0` and `aplay_period_time_us > 0`.
- `log_lines` range: `200..5000`.
- `binary_path_override` must resolve to executable file if set.
- `binary_path_override` is ignored unless `allow_external_binary_override=true`.
- Release profile must set `allow_external_binary_override=false`.
- `auto_restart_after_resume=true` still requires passing health checks before restart.

Migration rules:
- Unknown keys are ignored and preserved only if explicitly whitelisted later.
- Missing keys are backfilled with defaults on load.
- Invalid values are replaced with defaults and logged with `CONFIG_INVALID`.

Decky persistence rules:
- Plugin settings are stored under `decky.DECKY_PLUGIN_SETTINGS_DIR`.
- Runtime artifacts (state files, pid snapshots if needed) are stored under `decky.DECKY_PLUGIN_RUNTIME_DIR`.
- Logs are written under `decky.DECKY_PLUGIN_LOG_DIR`.
- Do not write outside Decky-managed directories.

Bundled encoder artifact layout:
- `bin/pw-ac3-live`: bundled executable included in plugin package.
- `bin/pw_ac3_live_build.json`: provenance manifest for bundled binary.

### Runtime shutdown and pipeline policy
- `stop_encoder()` sequence:
  - transition `running -> stopping`
  - send `SIGTERM` to full process group
  - wait graceful timeout (`3000ms`)
  - if still alive, send `SIGKILL`
  - wait hard timeout (`2000ms`)
  - if still alive, emit `STOP_TIMEOUT` and set `error` state
- If consumer (`aplay`) exits first, terminate producer immediately and mark `PIPELINE_BROKEN`.
- If producer exits first, allow short drain window (`<=500ms`) then stop consumer.
- Always store producer/consumer exit codes in recent logs.

### Audio snapshot and restore contract
Before startup, capture:
- default sink (`pactl get-default-sink`)
- active card profile for target sink card (`pactl list cards`)
- optional internal speaker card active profile (when present)
- HDMI mute / IEC958 state (`amixer` / `iecset` where available)

On stop/unload/uninstall, restore in order:
- card profile
- default sink
- internal speaker card profile (if captured)
- IEC958 and mute state

Restore behavior:
- restore is best-effort but never skipped.
- partial restore emits `AUDIO_RESTORE_PARTIAL` with failed step details.
- repeated restore calls are idempotent and safe.

## Lifecycle Hook Mapping
- `_main`:
  - initialize state machine
  - load persisted settings
  - publish initial `stopped` status
- `_unload`:
  - call `stop_encoder()` with timeout
  - run cleanup even if process already exited
- `_uninstall`:
  - call shared cleanup path again (idempotent)
  - remove plugin-owned runtime artifacts only

## Verification Plan

Automated/local gate:
- `pnpm run build`
- `python -m py_compile main.py`
- `python -m json.tool plugin.json >/dev/null`
- `python -m json.tool package.json >/dev/null`
- verify bundled binary exists and is executable in package staging.
- verify bundled binary reports version (`bin/pw-ac3-live --version`) in packaging environment.

Manual Steam Deck validation:
- Functional:
  - Start/stop cycles (>= 20)
  - Plugin unload while running
  - Decky restart while running
- System events:
  - HDMI unplug/replug while stopped, then start (must recover cleanly)
  - suspend/resume while stopped, then start (must recover cleanly)
  - HDMI unplug/replug while running (must surface recovery guidance if stream is impacted)
  - suspend/resume while running (must stop before suspend and restart after resume only when health checks pass)
  - audio device busy at startup
- Regression:
  - Ensure no stale `pw-ac3-live` or `aplay` children
  - Confirm sink/profile restoration after stop

Manual validation commands and pass criteria:
- Process hygiene:
  - command: `pgrep -fa "pw-ac3-live|aplay"`
  - pass: no plugin-owned processes remain within 5 seconds after stop/unload.
- Sink restoration:
  - command: `pactl get-default-sink` (capture before start and after stop)
  - pass: post-stop value equals pre-start value.
- Profile restoration:
  - command: `pactl list cards | rg -n "Name:|Active Profile:"`
  - pass: target card active profile equals pre-start profile.
- Runtime status accuracy:
  - command: call `get_status()` during each transition
  - pass: observed transitions follow legal path with no illegal jumps.
- Failure visibility:
  - command: induce busy device then call `start_encoder()`
  - pass: UI shows explicit error code/message/hint and logs include command context.

## Support Policy
- Supported hardware: Steam Deck OLED.
- Supported OS channel: SteamOS stable.
- Unsupported for this release: Steam Deck LCD, non-stable SteamOS channels, non-Deck systems.

## Risks
- Steam Deck audio profile drift after unexpected exits.
- Race conditions between Decky lifecycle hooks and process shutdown.
- Inconsistent environment between desktop mode sessions.

Mitigation:
- Idempotent cleanup path called from stop, unload, and exception handlers.
- Strict state machine in backend with guarded transitions.
- Startup preflight checks with explicit user errors.

## Definition of Done
- Plugin can be used daily from Decky UI with no manual shell steps.
- Failure modes are visible and recoverable from the plugin.
- Start/stop/unload behavior is deterministic and leaves the system clean.
- README and release notes describe supported hardware path and limitations.
