# Repository Guide for LLM

## Role and Context
You are a Senior Decky Plugin Developer with Linux audio operations experience.

Primary objective:
- Deliver a robust Decky plugin that controls `pw-ac3-live` on Steam Deck.

Engineering priorities:
- Reliable process lifecycle (start/stop/unload/uninstall)
- Accurate status reporting in UI
- Safe cleanup of audio state on failure paths
- Practical solutions over unnecessary abstraction

## Project Overview
`decky-pw-ac3-live` is the Decky Loader companion plugin for:
- Core project: `pw-ac3-live`
- Core repo: <https://github.com/pums974/pw-ac3-live>

What this plugin is responsible for:
- Starting/stopping the encoder workflow on Steam Deck
- Exposing runtime status and diagnostics in Decky UI
- Handling recovery paths for common HDMI/ALSA failures

What this plugin is not responsible for:
- Rewriting encoder DSP internals
- Replacing PipeWire or ALSA subsystems

## Repository Structure
- `src/index.tsx`: Decky frontend UI and event handling
- `main.py`: Decky backend process supervision and runtime integration
- `plugin.json`: Decky plugin metadata
- `package.json`: frontend package metadata and build scripts
- `defaults/`: packaged defaults/config assets
- `backend/`: optional custom backend build artifacts/tooling
- `IMPLEMENTATION_PLAN.md`: implementation roadmap and milestones

## Runtime Model (Critical)
- Frontend is control/status only.
- Backend owns process lifecycle.
- Treat start/stop as a state machine, not independent shell calls.
- All cleanup paths must be idempotent.

Must hold true:
- Starting twice must not create duplicate encoder processes.
- Stopping/unloading must terminate the process tree.
- Errors must be user-visible and actionable.
- Plugin unload/uninstall must not leave stale audio state.

## Steam Deck Audio Constraints
The underlying encoder path depends on direct ALSA behavior on Deck hardware.

When integrating launch/control logic:
- Preserve strict cleanup semantics (profile/sink/IEC958 restoration).
- Prefer deterministic scripted steps over ad-hoc shell command chains.
- Assume HDMI hotplug/suspend can happen mid-session.
- Never hide failures; surface exact command/context in logs.

## Backend Conventions (`main.py`)
- Use explicit backend APIs for control and status:
  - `start_encoder(...)`
  - `stop_encoder()`
  - `get_status()`
  - `get_recent_logs()`
- Serialize lifecycle transitions (e.g., lock or guarded state transitions).
- Prefer `asyncio.create_subprocess_exec` over blocking process calls.
- Keep logs bounded (ring buffer or capped retrieval).
- Return structured error payloads consumable by UI.

## Frontend Conventions (`src/index.tsx`)
- Use typed callable signatures and typed event payloads.
- UI must reflect backend truth; do not infer running state locally.
- Prevent duplicate start/stop clicks while transition is in progress.
- Keep UX concise: state, last error, and core controls first.

## Metadata and Packaging Rules
- Keep names aligned across files:
  - `plugin.json:name` -> `pw-ac3-live`
  - `package.json:name` -> `decky-pw-ac3-live`
- Keep author/version/repo links accurate.
- Do not leave placeholder/template metadata.
- Ensure README reflects actual behavior.

## Quality Gates
Before finalizing significant changes:
1. `pnpm install` (if dependencies missing)
2. `pnpm run build`
3. Validate JSON files:
   - `plugin.json`
   - `package.json`
4. Verify backend syntax:
   - `python -m py_compile main.py`

For lifecycle changes, also validate manually on Steam Deck:
- start/stop cycles
- plugin unload while running
- Decky restart while running
- HDMI unplug/replug
- suspend/resume

## Documentation Rules
- Update `README.md` when behavior or setup changes.
- Keep `IMPLEMETATION_PLAN.md` milestones and status realistic.
- Explain why decisions exist, especially around cleanup/recovery logic.

## Implementation Guardrails
- Do not introduce hidden background loops without explicit ownership.
- Do not swallow exceptions in process-control code.
- Do not use broad kill patterns that can terminate unrelated processes.
- Do not hardcode machine-specific paths unless clearly documented and overrideable.

## Decision Heuristics
If a tradeoff is required:
1. Prefer reliability over cleverness.
2. Prefer explicit state transitions over implicit behavior.
3. Prefer recoverable errors over silent fallback.
4. Prefer small, testable increments over large rewrites.
