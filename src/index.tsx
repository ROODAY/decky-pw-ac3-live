import {
  ButtonItem,
  PanelSection,
  PanelSectionRow,
  TextField,
  ToggleField,
  staticClasses,
} from "@decky/ui";
import {
  addEventListener,
  callable,
  definePlugin,
  removeEventListener,
  toaster,
  useQuickAccessVisible,
} from "@decky/api";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FaVolumeUp } from "react-icons/fa";

type LifecycleState = "stopped" | "starting" | "running" | "stopping" | "error";

interface BackendErrorPayload {
  code: string;
  message: string;
  detail: string;
  hint: string;
  timestamp: string;
}

interface EncoderStatusPayload {
  state: LifecycleState;
  pid: number | null;
  started_at: string | null;
  last_error: BackendErrorPayload | null;
}

interface PipelineStatus {
  producer_pid: number | null;
  consumer_pid: number | null;
}

interface StartEncoderConfig {
  allow_external_binary_override?: boolean;
  binary_path?: string | null;
  alsa_device?: string;
  latency?: string;
  buffer_size?: number | null;
  output_buffer_size?: number | null;
  ffmpeg_thread_queue_size?: number;
  ffmpeg_chunk_frames?: number;
  aplay_buffer_time_us?: number;
  aplay_period_time_us?: number;
}

interface StartEncoderResponse {
  ok: boolean;
  status: EncoderStatusPayload;
  error: BackendErrorPayload | null;
}

interface StopEncoderResponse {
  ok: boolean;
  status: EncoderStatusPayload;
  error: BackendErrorPayload | null;
  stopped_pid: number | null;
  duration_ms: number;
}

interface GetStatusResponse {
  status: EncoderStatusPayload;
  pipeline: PipelineStatus;
  last_transition_at: string;
}

interface RuntimeConfigForm {
  allowExternalBinaryOverride: boolean;
  binaryPath: string;
  alsaDevice: string;
  latency: string;
  bufferSize: string;
  outputBufferSize: string;
  ffmpegThreadQueueSize: string;
  ffmpegChunkFrames: string;
  aplayBufferTimeUs: string;
  aplayPeriodTimeUs: string;
}

const DEFAULT_RUNTIME_CONFIG: RuntimeConfigForm = {
  allowExternalBinaryOverride: false,
  binaryPath: "",
  alsaDevice: "hw:0,8",
  latency: "1536/48000",
  bufferSize: "",
  outputBufferSize: "",
  ffmpegThreadQueueSize: "4",
  ffmpegChunkFrames: "1536",
  aplayBufferTimeUs: "60000",
  aplayPeriodTimeUs: "15000",
};

const startEncoder = callable<[config?: StartEncoderConfig], StartEncoderResponse>(
  "start_encoder",
);
const stopEncoder = callable<
  [force?: boolean, timeoutMs?: number],
  StopEncoderResponse
>("stop_encoder");
const getStatus = callable<[], GetStatusResponse>("get_status");

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
}

function errorToMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function Content() {
  const [status, setStatus] = useState<EncoderStatusPayload | null>(null);
  const [pipeline, setPipeline] = useState<PipelineStatus>({
    producer_pid: null,
    consumer_pid: null,
  });
  const [lastTransitionAt, setLastTransitionAt] = useState<string | null>(null);
  const [runtimeConfig, setRuntimeConfig] =
    useState<RuntimeConfigForm>(DEFAULT_RUNTIME_CONFIG);
  const [showConfig, setShowConfig] = useState(false);
  const [isLoadingStatus, setIsLoadingStatus] = useState(true);
  const [isMutating, setIsMutating] = useState(false);
  const [frontendError, setFrontendError] = useState<string | null>(null);

  const quickAccessVisible = useQuickAccessVisible();

  const previousStateRef = useRef<LifecycleState | null>(null);
  const previousVisibleRef = useRef<boolean>(quickAccessVisible);
  const lastErrorToastKeyRef = useRef<string | null>(null);

  const toastBackendError = useCallback((error: BackendErrorPayload) => {
    const toastKey = `${error.code}|${error.timestamp}`;
    if (lastErrorToastKeyRef.current === toastKey) {
      return;
    }

    lastErrorToastKeyRef.current = toastKey;
    toaster.toast({
      title: `Encoder error: ${error.code}`,
      body: error.message,
      subtext: error.hint,
      critical: true,
    });
  }, []);

  const applyBackendStatus = useCallback(
    (
      nextStatus: EncoderStatusPayload,
      nextPipeline: PipelineStatus | null,
      nextLastTransitionAt: string | null,
    ) => {
      setStatus(nextStatus);
      if (nextPipeline !== null) {
        setPipeline(nextPipeline);
      }
      if (nextLastTransitionAt !== null) {
        setLastTransitionAt(nextLastTransitionAt);
      }

      const previousState = previousStateRef.current;
      if (previousState !== null && previousState !== nextStatus.state) {
        if (nextStatus.state === "running") {
          toaster.toast({
            title: "pw-ac3-live",
            body: "Encoder is running.",
          });
        } else if (nextStatus.state === "stopped") {
          toaster.toast({
            title: "pw-ac3-live",
            body: "Encoder is stopped.",
          });
        } else if (nextStatus.state === "error" && nextStatus.last_error) {
          toastBackendError(nextStatus.last_error);
        }
      }

      previousStateRef.current = nextStatus.state;
    },
    [toastBackendError],
  );

  const refreshStatus = useCallback(async () => {
    try {
      const response = await getStatus();
      applyBackendStatus(response.status, response.pipeline, response.last_transition_at);
      setFrontendError(null);
    } catch (error) {
      const message = `Failed to load status: ${errorToMessage(error)}`;
      setFrontendError(message);
      toaster.toast({
        title: "pw-ac3-live",
        body: message,
        critical: true,
      });
    } finally {
      setIsLoadingStatus(false);
    }
  }, [applyBackendStatus]);

  useEffect(() => {
    void refreshStatus();

    const stateListener = addEventListener<[status: EncoderStatusPayload]>(
      "encoder_state_changed",
      (nextStatus) => {
        applyBackendStatus(nextStatus, null, null);
        void refreshStatus();
      },
    );

    const errorListener = addEventListener<[error: BackendErrorPayload]>(
      "encoder_error",
      (error) => {
        setFrontendError(`${error.code}: ${error.message}`);
        toastBackendError(error);
        void refreshStatus();
      },
    );

    return () => {
      removeEventListener("encoder_state_changed", stateListener);
      removeEventListener("encoder_error", errorListener);
    };
  }, [applyBackendStatus, refreshStatus, toastBackendError]);

  useEffect(() => {
    const wasVisible = previousVisibleRef.current;
    previousVisibleRef.current = quickAccessVisible;

    if (!wasVisible && quickAccessVisible) {
      void refreshStatus();
    }
  }, [quickAccessVisible, refreshStatus]);

  const isTransitioning =
    status?.state === "starting" || status?.state === "stopping";
  const controlsLocked = isMutating || isTransitioning || isLoadingStatus;

  const canStart = status !== null && (status.state === "stopped" || status.state === "error");
  const canStop =
    status !== null &&
    (status.state === "running" || status.state === "starting" || status.state === "error");

  const validateAndBuildStartConfig = useCallback((): StartEncoderConfig | null => {
    const fail = (message: string): null => {
      setFrontendError(message);
      toaster.toast({
        title: "Invalid runtime config",
        body: message,
        critical: true,
      });
      return null;
    };

    const parsePositiveInt = (rawValue: string): number | null => {
      const parsed = Number.parseInt(rawValue.trim(), 10);
      if (!Number.isInteger(parsed) || parsed <= 0) {
        return null;
      }
      return parsed;
    };

    const parseOptionalPositiveInt = (rawValue: string): number | null | undefined => {
      if (!rawValue.trim()) {
        return undefined;
      }
      const parsed = parsePositiveInt(rawValue);
      if (parsed === null) {
        return null;
      }
      return parsed;
    };

    const latency = runtimeConfig.latency.trim();
    const alsaDevice = runtimeConfig.alsaDevice.trim();
    if (!latency) {
      return fail("Latency cannot be empty.");
    }
    if (!alsaDevice) {
      return fail("ALSA device cannot be empty.");
    }

    const ffmpegThreadQueueSize = parsePositiveInt(runtimeConfig.ffmpegThreadQueueSize);
    if (ffmpegThreadQueueSize === null) {
      return fail("ffmpeg thread queue size must be a positive integer.");
    }

    const ffmpegChunkFrames = parsePositiveInt(runtimeConfig.ffmpegChunkFrames);
    if (ffmpegChunkFrames === null) {
      return fail("ffmpeg chunk frames must be a positive integer.");
    }

    const aplayBufferTimeUs = parsePositiveInt(runtimeConfig.aplayBufferTimeUs);
    if (aplayBufferTimeUs === null) {
      return fail("aplay buffer time must be a positive integer.");
    }

    const aplayPeriodTimeUs = parsePositiveInt(runtimeConfig.aplayPeriodTimeUs);
    if (aplayPeriodTimeUs === null) {
      return fail("aplay period time must be a positive integer.");
    }

    const bufferSize = parseOptionalPositiveInt(runtimeConfig.bufferSize);
    if (bufferSize === null) {
      return fail("buffer size must be a positive integer when provided.");
    }

    const outputBufferSize = parseOptionalPositiveInt(runtimeConfig.outputBufferSize);
    if (outputBufferSize === null) {
      return fail("output buffer size must be a positive integer when provided.");
    }

    const payload: StartEncoderConfig = {
      allow_external_binary_override: runtimeConfig.allowExternalBinaryOverride,
      binary_path: runtimeConfig.binaryPath.trim() || null,
      alsa_device: alsaDevice,
      latency,
      buffer_size: bufferSize ?? null,
      output_buffer_size: outputBufferSize ?? null,
      ffmpeg_thread_queue_size: ffmpegThreadQueueSize,
      ffmpeg_chunk_frames: ffmpegChunkFrames,
      aplay_buffer_time_us: aplayBufferTimeUs,
      aplay_period_time_us: aplayPeriodTimeUs,
    };

    return payload;
  }, [runtimeConfig]);

  const handleStart = useCallback(async () => {
    const config = validateAndBuildStartConfig();
    if (config === null) {
      return;
    }

    setIsMutating(true);
    try {
      const response = await startEncoder(config);
      applyBackendStatus(response.status, null, null);

      if (!response.ok && response.error) {
        setFrontendError(`${response.error.code}: ${response.error.message}`);
        toastBackendError(response.error);
      } else {
        setFrontendError(null);
      }
    } catch (error) {
      const message = `Start request failed: ${errorToMessage(error)}`;
      setFrontendError(message);
      toaster.toast({
        title: "pw-ac3-live",
        body: message,
        critical: true,
      });
    } finally {
      setIsMutating(false);
      void refreshStatus();
    }
  }, [applyBackendStatus, refreshStatus, toastBackendError, validateAndBuildStartConfig]);

  const handleStop = useCallback(async () => {
    setIsMutating(true);
    try {
      const response = await stopEncoder(false, 5000);
      applyBackendStatus(response.status, null, null);

      if (!response.ok && response.error) {
        setFrontendError(`${response.error.code}: ${response.error.message}`);
        toastBackendError(response.error);
      } else {
        setFrontendError(null);
      }
    } catch (error) {
      const message = `Stop request failed: ${errorToMessage(error)}`;
      setFrontendError(message);
      toaster.toast({
        title: "pw-ac3-live",
        body: message,
        critical: true,
      });
    } finally {
      setIsMutating(false);
      void refreshStatus();
    }
  }, [applyBackendStatus, refreshStatus, toastBackendError]);

  const currentError = useMemo(() => {
    if (status?.last_error) {
      return status.last_error;
    }
    return null;
  }, [status]);

  return (
    <>
      <PanelSection title="Encoder Control" spinner={isLoadingStatus}>
        <PanelSectionRow>
          <div>State: <strong>{status?.state ?? "loading"}</strong></div>
        </PanelSectionRow>
        <PanelSectionRow>
          <div>Producer PID: <strong>{pipeline.producer_pid ?? "-"}</strong></div>
        </PanelSectionRow>
        <PanelSectionRow>
          <div>Consumer PID: <strong>{pipeline.consumer_pid ?? "-"}</strong></div>
        </PanelSectionRow>
        <PanelSectionRow>
          <div>Started At: <strong>{formatTimestamp(status?.started_at ?? null)}</strong></div>
        </PanelSectionRow>
        <PanelSectionRow>
          <div>Last Transition: <strong>{formatTimestamp(lastTransitionAt)}</strong></div>
        </PanelSectionRow>
        <PanelSectionRow>
          <ButtonItem
            layout="below"
            disabled={!canStart || controlsLocked}
            onClick={() => void handleStart()}
          >
            Start Encoder
          </ButtonItem>
        </PanelSectionRow>
        <PanelSectionRow>
          <ButtonItem
            layout="below"
            disabled={!canStop || controlsLocked}
            onClick={() => void handleStop()}
          >
            Stop Encoder
          </ButtonItem>
        </PanelSectionRow>
        <PanelSectionRow>
          <ButtonItem
            layout="below"
            disabled={isMutating || isLoadingStatus}
            onClick={() => void refreshStatus()}
          >
            Refresh Status
          </ButtonItem>
        </PanelSectionRow>
      </PanelSection>

      <PanelSection title="Last Error">
        <PanelSectionRow>
          <div>
            {currentError
              ? `${currentError.code}: ${currentError.message}`
              : frontendError ?? "No backend error"}
          </div>
        </PanelSectionRow>
        {currentError && (
          <PanelSectionRow>
            <div>{currentError.detail}</div>
          </PanelSectionRow>
        )}
        {currentError?.hint && (
          <PanelSectionRow>
            <div>Hint: {currentError.hint}</div>
          </PanelSectionRow>
        )}
      </PanelSection>

      <PanelSection title="Runtime Config">
        <PanelSectionRow>
          <ToggleField
            label="Show runtime config"
            description="Optional start parameters for this session"
            checked={showConfig}
            disabled={controlsLocked}
            onChange={(value) => setShowConfig(value)}
          />
        </PanelSectionRow>

        {showConfig && (
          <>
            <PanelSectionRow>
              <ToggleField
                label="Allow external binary override"
                description="Development-only override"
                checked={runtimeConfig.allowExternalBinaryOverride}
                disabled={controlsLocked}
                onChange={(value) =>
                  setRuntimeConfig((current) => ({
                    ...current,
                    allowExternalBinaryOverride: value,
                  }))
                }
              />
            </PanelSectionRow>

            {runtimeConfig.allowExternalBinaryOverride && (
              <PanelSectionRow>
                <TextField
                  label="External binary path"
                  value={runtimeConfig.binaryPath}
                  disabled={controlsLocked}
                  onChange={(event) =>
                    setRuntimeConfig((current) => ({
                      ...current,
                      binaryPath: event.target.value,
                    }))
                  }
                />
              </PanelSectionRow>
            )}

            <PanelSectionRow>
              <TextField
                label="ALSA device"
                value={runtimeConfig.alsaDevice}
                disabled={controlsLocked}
                onChange={(event) =>
                  setRuntimeConfig((current) => ({
                    ...current,
                    alsaDevice: event.target.value,
                  }))
                }
              />
            </PanelSectionRow>

            <PanelSectionRow>
              <TextField
                label="Latency"
                value={runtimeConfig.latency}
                disabled={controlsLocked}
                onChange={(event) =>
                  setRuntimeConfig((current) => ({
                    ...current,
                    latency: event.target.value,
                  }))
                }
              />
            </PanelSectionRow>

            <PanelSectionRow>
              <TextField
                label="FFmpeg thread queue size"
                value={runtimeConfig.ffmpegThreadQueueSize}
                mustBeNumeric
                disabled={controlsLocked}
                onChange={(event) =>
                  setRuntimeConfig((current) => ({
                    ...current,
                    ffmpegThreadQueueSize: event.target.value,
                  }))
                }
              />
            </PanelSectionRow>

            <PanelSectionRow>
              <TextField
                label="FFmpeg chunk frames"
                value={runtimeConfig.ffmpegChunkFrames}
                mustBeNumeric
                disabled={controlsLocked}
                onChange={(event) =>
                  setRuntimeConfig((current) => ({
                    ...current,
                    ffmpegChunkFrames: event.target.value,
                  }))
                }
              />
            </PanelSectionRow>

            <PanelSectionRow>
              <TextField
                label="aplay buffer time (us)"
                value={runtimeConfig.aplayBufferTimeUs}
                mustBeNumeric
                disabled={controlsLocked}
                onChange={(event) =>
                  setRuntimeConfig((current) => ({
                    ...current,
                    aplayBufferTimeUs: event.target.value,
                  }))
                }
              />
            </PanelSectionRow>

            <PanelSectionRow>
              <TextField
                label="aplay period time (us)"
                value={runtimeConfig.aplayPeriodTimeUs}
                mustBeNumeric
                disabled={controlsLocked}
                onChange={(event) =>
                  setRuntimeConfig((current) => ({
                    ...current,
                    aplayPeriodTimeUs: event.target.value,
                  }))
                }
              />
            </PanelSectionRow>

            <PanelSectionRow>
              <TextField
                label="buffer size (optional)"
                value={runtimeConfig.bufferSize}
                mustBeNumeric
                disabled={controlsLocked}
                onChange={(event) =>
                  setRuntimeConfig((current) => ({
                    ...current,
                    bufferSize: event.target.value,
                  }))
                }
              />
            </PanelSectionRow>

            <PanelSectionRow>
              <TextField
                label="output buffer size (optional)"
                value={runtimeConfig.outputBufferSize}
                mustBeNumeric
                disabled={controlsLocked}
                onChange={(event) =>
                  setRuntimeConfig((current) => ({
                    ...current,
                    outputBufferSize: event.target.value,
                  }))
                }
              />
            </PanelSectionRow>
          </>
        )}
      </PanelSection>
    </>
  );
}

export default definePlugin(() => {
  console.log("pw-ac3-live plugin initializing");

  return {
    name: "pw-ac3-live",
    titleView: <div className={staticClasses.Title}>pw-ac3-live</div>,
    content: <Content />,
    icon: <FaVolumeUp />,
    onDismount() {
      console.log("pw-ac3-live plugin unloading");
    },
  };
});
