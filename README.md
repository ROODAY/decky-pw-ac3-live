# decky-pw-ac3-live

Decky Loader plugin for controlling `pw-ac3-live` (real-time 5.1 PCM -> AC-3 encoder) on Steam Deck.

Plugin releases are intended to bundle the `pw-ac3-live` executable so users do not need to install it separately.

## Overview

This repository contains the Decky plugin implementation for `pw-ac3-live`, including:

- Frontend UI (`src/`)
- Python backend (`main.py`)
- Decky metadata (`plugin.json`, `package.json`)
- Packaged defaults (`defaults/`)

## About pw-ac3-live

`pw-ac3-live` is the core encoder daemon used by this plugin. It captures 5.1 LPCM, encodes it to AC-3 in real time, and outputs an IEC61937 stream for HDMI/SPDIF passthrough.

It supports two output paths:

- PipeWire native output (desktop Linux)
- Direct ALSA output (required on Steam Deck to avoid PipeWire ALSA scheduling jitter)

Links:

- Core repository: <https://github.com/pums974/pw-ac3-live>

## Distribution Model

- End-user plugin package: includes `pw-ac3-live` binary.
- Runtime expectation: no external `pw-ac3-live` installation required.
- Development: optional local override may be used for testing only.

## Development

### Prerequisites

- Node.js >= 16.14
- `pnpm` v9
- Decky Loader development environment

### Install dependencies

```bash
pnpm install
```

### Build frontend

```bash
pnpm run build
```

### Watch mode

```bash
pnpm run watch
```

## Plugin Identity

- Plugin name: `pw-ac3-live`
- Package name: `decky-pw-ac3-live`
- Author: `Alexandre Poux`
- API version: `1`

## License

BSD-3-Clause. See `LICENSE`.
