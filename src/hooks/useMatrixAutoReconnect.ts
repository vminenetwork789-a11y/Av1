import { useState, useEffect, useCallback, useRef } from 'react';

export type ConnectionStatus = 'connected' | 'reconnecting' | 'disconnected';

export interface UseMatrixAutoReconnectOptions {
  /** Whether automatic reconnection monitoring is enabled (default: true) */
  enabled?: boolean;
  /** Health check endpoint for the backend API (default: '/api/health') */
  healthEndpoint?: string;
  /** Optional WebSocket URL if a live socket endpoint is used */
  wsUrl?: string | null;
  /** Base delay in ms before first reconnect attempt (default: 1500ms) */
  baseRetryDelayMs?: number;
  /** Maximum backoff delay in ms between reconnect attempts (default: 25000ms) */
  maxRetryDelayMs?: number;
  /** Request timeout in ms for health check probes (default: 5000ms) */
  probeTimeoutMs?: number;
  /** Callback fired immediately when connection to backend API / WebSocket is restored */
  onReconnect?: () => void | Promise<void>;
}

export interface UseMatrixAutoReconnectResult {
  connectionStatus: ConnectionStatus;
  isOffline: boolean;
  isReconnecting: boolean;
  retryCount: number;
  nextRetryInSeconds: number | null;
  lastError: string | null;
  lastReconnectedAt: string | null;
  /** Call this whenever a network error is caught during the Matrix refresh interval */
  reportNetworkError: (error?: unknown, source?: string) => void;
  /** Call this whenever a Matrix API fetch completes successfully */
  reportConnectionSuccess: () => void;
  /** Force an immediate reconnection attempt */
  triggerReconnectNow: () => Promise<boolean>;
}

const NETWORK_ERROR_EVENT = 'matrix:network:error';
const NETWORK_RECONNECTED_EVENT = 'matrix:network:reconnected';

/**
 * Helper to broadcast a Matrix network error from anywhere in the app
 * (e.g., services, hooks, or components during refresh intervals).
 */
export function notifyMatrixNetworkError(error?: unknown, source?: string) {
  if (typeof window !== 'undefined') {
    window.dispatchEvent(
      new CustomEvent(NETWORK_ERROR_EVENT, {
        detail: { error, source, timestamp: Date.now() },
      })
    );
  }
}

/**
 * Hook that monitors network errors during the Matrix refresh interval
 * and automatically attempts to reconnect to the WebSocket or backend API
 * using exponential backoff with jitter, browser online recovery, and health probes.
 */
export function useMatrixAutoReconnect(
  options: UseMatrixAutoReconnectOptions = {}
): UseMatrixAutoReconnectResult {
  const {
    enabled = true,
    healthEndpoint = '/api/health',
    wsUrl = null,
    baseRetryDelayMs = 1500,
    maxRetryDelayMs = 25000,
    probeTimeoutMs = 5000,
    onReconnect,
  } = options;

  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('connected');
  const [retryCount, setRetryCount] = useState<number>(0);
  const [nextRetryInSeconds, setNextRetryInSeconds] = useState<number | null>(null);
  const [lastError, setLastError] = useState<string | null>(null);
  const [lastReconnectedAt, setLastReconnectedAt] = useState<string | null>(null);

  const isMountedRef = useRef<boolean>(true);
  const isProbeInFlightRef = useRef<boolean>(false);
  const retryCountRef = useRef<number>(0);
  const statusRef = useRef<ConnectionStatus>('connected');
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const countdownIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const onReconnectRef = useRef(onReconnect);

  useEffect(() => {
    onReconnectRef.current = onReconnect;
  }, [onReconnect]);

  const clearScheduledTimers = useCallback(() => {
    if (retryTimerRef.current) {
      clearTimeout(retryTimerRef.current);
      retryTimerRef.current = null;
    }
    if (countdownIntervalRef.current) {
      clearInterval(countdownIntervalRef.current);
      countdownIntervalRef.current = null;
    }
    if (isMountedRef.current) {
      setNextRetryInSeconds(null);
    }
  }, []);

  const connectWebSocketIfConfigured = useCallback((): Promise<boolean> => {
    if (!wsUrl || typeof window === 'undefined' || typeof WebSocket === 'undefined') {
      return Promise.resolve(true);
    }

    return new Promise((resolve) => {
      try {
        if (wsRef.current) {
          if (wsRef.current.readyState === WebSocket.OPEN) {
            resolve(true);
            return;
          }
          try {
            wsRef.current.close();
          } catch {
            // Ignore close errors on stale socket
          }
        }

        const socket = new WebSocket(wsUrl);
        wsRef.current = socket;

        const timeoutId = setTimeout(() => {
          try {
            socket.close();
          } catch {
            // Ignore
          }
          resolve(false);
        }, probeTimeoutMs);

        socket.onopen = () => {
          clearTimeout(timeoutId);
          resolve(true);
        };

        socket.onerror = () => {
          clearTimeout(timeoutId);
          resolve(false);
        };

        socket.onclose = () => {
          clearTimeout(timeoutId);
          if (isMountedRef.current && enabled && statusRef.current === 'connected') {
            notifyMatrixNetworkError(new Error('WebSocket closed unexpectedly'), 'websocket');
          }
        };
      } catch {
        resolve(false);
      }
    });
  }, [wsUrl, probeTimeoutMs, enabled]);

  const probeBackendConnection = useCallback(async (): Promise<boolean> => {
    if (typeof navigator !== 'undefined' && navigator.onLine === false) {
      return false;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), probeTimeoutMs);

    try {
      const separator = healthEndpoint.includes('?') ? '&' : '?';
      const res = await fetch(`${healthEndpoint}${separator}_t=${Date.now()}`, {
        method: 'GET',
        headers: {
          Accept: 'application/json',
          'Cache-Control': 'no-cache, no-store',
        },
        cache: 'no-store',
        signal: controller.signal,
      });
      clearTimeout(timeoutId);

      if (!res.ok) {
        return false;
      }

      const data = await res.json().catch(() => null);
      const apiHealthy = Boolean(data && (data.status === 'ok' || data.success !== false));
      if (!apiHealthy) {
        return false;
      }

      if (wsUrl) {
        await connectWebSocketIfConfigured();
      }

      return true;
    } catch {
      clearTimeout(timeoutId);
      return false;
    }
  }, [healthEndpoint, probeTimeoutMs, wsUrl, connectWebSocketIfConfigured]);

  const handleConnectionRestored = useCallback(
    async (wasReconnecting: boolean) => {
      clearScheduledTimers();
      retryCountRef.current = 0;
      statusRef.current = 'connected';

      if (isMountedRef.current) {
        setRetryCount(0);
        setConnectionStatus('connected');
        setLastError(null);
        setNextRetryInSeconds(null);
      }

      if (wasReconnecting) {
        const nowStr = new Date().toLocaleTimeString('th-TH', {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
        });
        if (isMountedRef.current) {
          setLastReconnectedAt(nowStr);
        }

        if (typeof window !== 'undefined') {
          window.dispatchEvent(new CustomEvent(NETWORK_RECONNECTED_EVENT));
          window.dispatchEvent(new CustomEvent('matrix:tree:refresh'));
        }

        if (onReconnectRef.current) {
          try {
            await onReconnectRef.current();
          } catch {
            // Ignore callback error
          }
        }
      }
    },
    [clearScheduledTimers]
  );

  const scheduleNextAttempt = useCallback(
    (attemptNumber: number) => {
      if (!enabled || !isMountedRef.current) return;
      clearScheduledTimers();

      // Exponential backoff with small jitter
      const expDelay = Math.min(
        maxRetryDelayMs,
        baseRetryDelayMs * Math.pow(1.75, Math.max(0, attemptNumber - 1))
      );
      const jitter = Math.floor(Math.random() * 400);
      const totalDelayMs = Math.max(1000, Math.round(expDelay + jitter));
      const initialSeconds = Math.max(1, Math.ceil(totalDelayMs / 1000));

      setNextRetryInSeconds(initialSeconds);

      const targetTime = Date.now() + totalDelayMs;
      countdownIntervalRef.current = setInterval(() => {
        if (!isMountedRef.current) return;
        const remaining = Math.max(0, Math.ceil((targetTime - Date.now()) / 1000));
        setNextRetryInSeconds(remaining);
      }, 500);

      retryTimerRef.current = setTimeout(async () => {
        if (!isMountedRef.current || !enabled) return;
        clearScheduledTimers();

        if (isProbeInFlightRef.current) return;
        isProbeInFlightRef.current = true;

        const ok = await probeBackendConnection();
        isProbeInFlightRef.current = false;

        if (!isMountedRef.current) return;

        if (ok) {
          await handleConnectionRestored(true);
        } else {
          const nextCount = retryCountRef.current + 1;
          retryCountRef.current = nextCount;
          statusRef.current = 'reconnecting';
          setRetryCount(nextCount);
          setConnectionStatus('reconnecting');
          scheduleNextAttempt(nextCount);
        }
      }, totalDelayMs);
    },
    [
      enabled,
      baseRetryDelayMs,
      maxRetryDelayMs,
      clearScheduledTimers,
      probeBackendConnection,
      handleConnectionRestored,
    ]
  );

  const triggerReconnectNow = useCallback(async (): Promise<boolean> => {
    if (!isMountedRef.current) return false;
    clearScheduledTimers();

    if (isProbeInFlightRef.current) return false;
    isProbeInFlightRef.current = true;

    statusRef.current = 'reconnecting';
    setConnectionStatus('reconnecting');

    const ok = await probeBackendConnection();
    isProbeInFlightRef.current = false;

    if (!isMountedRef.current) return ok;

    if (ok) {
      await handleConnectionRestored(true);
      return true;
    }

    const nextCount = retryCountRef.current + 1;
    retryCountRef.current = nextCount;
    setRetryCount(nextCount);
    scheduleNextAttempt(nextCount);
    return false;
  }, [clearScheduledTimers, probeBackendConnection, handleConnectionRestored, scheduleNextAttempt]);

  const reportNetworkError = useCallback(
    (error?: unknown, source?: string) => {
      if (!enabled || !isMountedRef.current) return;

      // Ignore intentional AbortController cancellations unless caused by offline state
      if (
        error &&
        typeof error === 'object' &&
        (error as any).name === 'AbortError' &&
        typeof navigator !== 'undefined' &&
        navigator.onLine !== false
      ) {
        return;
      }

      const errMsg =
        error instanceof Error
          ? error.message
          : typeof error === 'string'
          ? error
          : source
          ? `Network error during ${source}`
          : 'Network connection interrupted';

      setLastError(errMsg);

      // If already scheduled for reconnection, let the active backoff timer run
      if (statusRef.current === 'reconnecting' && retryTimerRef.current) {
        return;
      }

      const nextCount = retryCountRef.current + 1;
      retryCountRef.current = nextCount;
      statusRef.current = 'reconnecting';
      setRetryCount(nextCount);
      setConnectionStatus('reconnecting');

      scheduleNextAttempt(nextCount);
    },
    [enabled, scheduleNextAttempt]
  );

  const reportConnectionSuccess = useCallback(() => {
    if (!isMountedRef.current) return;
    if (statusRef.current !== 'connected' || retryCountRef.current > 0) {
      handleConnectionRestored(true);
    }
  }, [handleConnectionRestored]);

  // Listen for global browser online/offline, tab visibility recovery, and custom matrix network error events
  useEffect(() => {
    isMountedRef.current = true;

    const handleWindowOnline = () => {
      if (!enabled) return;
      triggerReconnectNow();
    };

    const handleWindowOffline = () => {
      if (!enabled) return;
      statusRef.current = 'disconnected';
      setConnectionStatus('disconnected');
      reportNetworkError(new Error('Browser reported network offline'), 'window-offline');
    };

    const handleVisibilityChange = () => {
      if (
        enabled &&
        typeof document !== 'undefined' &&
        document.visibilityState === 'visible' &&
        statusRef.current !== 'connected'
      ) {
        triggerReconnectNow();
      }
    };

    const handleCustomNetworkError = (event: Event) => {
      const customEvt = event as CustomEvent<{ error?: unknown; source?: string }>;
      reportNetworkError(customEvt.detail?.error, customEvt.detail?.source);
    };

    if (typeof window !== 'undefined') {
      window.addEventListener('online', handleWindowOnline);
      window.addEventListener('offline', handleWindowOffline);
      window.addEventListener(NETWORK_ERROR_EVENT, handleCustomNetworkError);
      document.addEventListener('visibilitychange', handleVisibilityChange);
    }

    return () => {
      isMountedRef.current = false;
      clearScheduledTimers();
      if (wsRef.current) {
        try {
          wsRef.current.close();
        } catch {
          // Ignore
        }
      }
      if (typeof window !== 'undefined') {
        window.removeEventListener('online', handleWindowOnline);
        window.removeEventListener('offline', handleWindowOffline);
        window.removeEventListener(NETWORK_ERROR_EVENT, handleCustomNetworkError);
        document.removeEventListener('visibilitychange', handleVisibilityChange);
      }
    };
  }, [enabled, triggerReconnectNow, reportNetworkError, clearScheduledTimers]);

  return {
    connectionStatus,
    isOffline: connectionStatus === 'disconnected',
    isReconnecting: connectionStatus === 'reconnecting',
    retryCount,
    nextRetryInSeconds,
    lastError,
    lastReconnectedAt,
    reportNetworkError,
    reportConnectionSuccess,
    triggerReconnectNow,
  };
}

export default useMatrixAutoReconnect;
