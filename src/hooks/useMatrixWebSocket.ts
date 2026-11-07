import { useState, useEffect, useRef, useCallback } from 'react';

export interface MatrixWebSocketState {
  isConnected: boolean;
  transportType: 'websocket' | 'sse' | 'http' | 'none';
  connectionStatus: 'connected' | 'reconnecting' | 'disconnected';
  retryCount: number;
  nextRetryInSeconds: number;
  lastSyncedAt: string | null;
  rankCounts: Record<number, number>;
  nodesByRank: Record<number, any[]>;
  contractAddress: string | null;
  transactions: any[];
  rewardLogs: any[];
  upgradeLogs: any[];
}

export function useMatrixWebSocket(activeRank: number = 1) {
  const [wsState, setWsState] = useState<MatrixWebSocketState>({
    isConnected: true,
    transportType: 'sse',
    connectionStatus: 'connected',
    retryCount: 0,
    nextRetryInSeconds: 0,
    lastSyncedAt: null,
    rankCounts: {},
    nodesByRank: {},
    contractAddress: null,
    transactions: [],
    rewardLogs: [],
    upgradeLogs: [],
  });

  const wsRef = useRef<WebSocket | null>(null);
  const sseRef = useRef<EventSource | null>(null);
  const retryCountRef = useRef<number>(0);
  const reconnectTimerRef = useRef<NodeJS.Timeout | null>(null);
  const countdownTimerRef = useRef<NodeJS.Timeout | null>(null);
  const httpPollTimerRef = useRef<NodeJS.Timeout | null>(null);
  const isConnectingRef = useRef<boolean>(false);
  const prevConnectedRef = useRef<boolean>(true);
  const hasActiveTransportRef = useRef<boolean>(false);

  // Exponential Backoff Delay calculation: 1s, 2s, 4s, 8s, 16s, up to 30s max (+ jitter)
  const calculateBackoffDelay = useCallback((attempt: number) => {
    const baseDelay = Math.min(1000 * Math.pow(2, Math.max(0, attempt - 1)), 30000);
    const jitter = Math.floor(Math.random() * 500);
    return baseDelay + jitter;
  }, []);

  const processStreamPayload = useCallback((data: any) => {
    if (!data || typeof data !== 'object') return;
    hasActiveTransportRef.current = true;
    if (data.type === 'INIT_MATRIX' || data.type === 'ALL_MATRIX_UPDATED') {
      setWsState((prev) => ({
        ...prev,
        isConnected: true,
        connectionStatus: 'connected',
        retryCount: 0,
        nextRetryInSeconds: 0,
        lastSyncedAt: data.lastSyncedAt || prev.lastSyncedAt,
        rankCounts: data.rankCounts || prev.rankCounts,
        nodesByRank: data.nodesByRank || prev.nodesByRank,
        contractAddress: data.contractAddress || prev.contractAddress,
        transactions: Array.isArray(data.transactions) ? data.transactions : prev.transactions,
        rewardLogs: Array.isArray(data.rewardLogs) ? data.rewardLogs : prev.rewardLogs,
        upgradeLogs: Array.isArray(data.upgradeLogs) ? data.upgradeLogs : prev.upgradeLogs,
      }));
    } else if (data.type === 'TRANSACTIONS_DATA' || data.type === 'TRANSACTIONS_UPDATED') {
      setWsState((prev) => ({
        ...prev,
        isConnected: true,
        connectionStatus: 'connected',
        retryCount: 0,
        nextRetryInSeconds: 0,
        lastSyncedAt: data.lastSyncedAt || prev.lastSyncedAt,
        transactions: Array.isArray(data.transactions) ? data.transactions : prev.transactions,
        rewardLogs: Array.isArray(data.rewardLogs) ? data.rewardLogs : prev.rewardLogs,
        upgradeLogs: Array.isArray(data.upgradeLogs) ? data.upgradeLogs : prev.upgradeLogs,
      }));
    } else if (data.type === 'RANK_TREE_DATA') {
      setWsState((prev) => ({
        ...prev,
        isConnected: true,
        connectionStatus: 'connected',
        retryCount: 0,
        nextRetryInSeconds: 0,
        lastSyncedAt: data.lastSyncedAt || prev.lastSyncedAt,
        nodesByRank: {
          ...prev.nodesByRank,
          [data.rank]: data.nodes || [],
        },
        rankCounts: {
          ...prev.rankCounts,
          [data.rank]: data.rankCount || 0,
        },
      }));
    } else if (data.type === 'MATRIX_UPDATED') {
      setWsState((prev) => ({
        ...prev,
        isConnected: true,
        connectionStatus: 'connected',
        retryCount: 0,
        nextRetryInSeconds: 0,
        lastSyncedAt: data.lastSyncedAt || prev.lastSyncedAt,
        rankCounts: data.rankCounts || prev.rankCounts,
        nodesByRank: {
          ...prev.nodesByRank,
          [data.rank]: data.nodes || [],
        },
      }));
    }
  }, []);

  const notifyConnectionChange = useCallback(
    (status: 'connected' | 'reconnecting' | 'disconnected', _attempt = 0, _delaySec = 0) => {
      if (status === 'connected') {
        prevConnectedRef.current = true;
      } else {
        prevConnectedRef.current = false;
      }
    },
    []
  );

  // Instant HTTP JSON Sync fallback so even if Cloud Run / mobile carrier proxy blocks both WS and SSE,
  // data and green connected state stay 100% live without hitting database limits
  const fetchHttpSnapshot = useCallback(async (): Promise<boolean> => {
    try {
      const res = await fetch('/api/transactions', { headers: { Accept: 'application/json' } });
      if (res.ok) {
        const json = await res.json();
        if (json?.success) {
          hasActiveTransportRef.current = true;
          retryCountRef.current = 0;
          setWsState((prev) => ({
            ...prev,
            isConnected: true,
            transportType: prev.transportType === 'none' ? 'http' : prev.transportType,
            connectionStatus: 'connected',
            retryCount: 0,
            nextRetryInSeconds: 0,
            lastSyncedAt: json.lastSyncedAt || prev.lastSyncedAt,
            contractAddress: json.contractAddress || prev.contractAddress,
            transactions: Array.isArray(json.data) ? json.data : prev.transactions,
          }));
          notifyConnectionChange('connected');
          return true;
        }
      }
    } catch {
      // ignore
    }
    return false;
  }, [notifyConnectionChange]);

  const triggerReconnectWithBackoff = useCallback(async () => {
    // First check if HTTP API is reachable; if so, keep connection green via HTTP Sync while retrying stream silently
    const httpOk = await fetchHttpSnapshot();
    if (httpOk) {
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = setTimeout(() => {
        isConnectingRef.current = false;
        connectStream();
      }, 5000);
      return;
    }

    if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
    if (countdownTimerRef.current) clearInterval(countdownTimerRef.current);

    retryCountRef.current += 1;
    const currentAttempt = retryCountRef.current;
    const delayMs = calculateBackoffDelay(currentAttempt);
    let secondsLeft = Math.ceil(delayMs / 1000);

    setWsState((prev) => ({
      ...prev,
      isConnected: false,
      connectionStatus: 'reconnecting',
      retryCount: currentAttempt,
      nextRetryInSeconds: secondsLeft,
    }));

    notifyConnectionChange('reconnecting', currentAttempt, secondsLeft);

    countdownTimerRef.current = setInterval(() => {
      secondsLeft -= 1;
      setWsState((prev) => ({
        ...prev,
        nextRetryInSeconds: Math.max(0, secondsLeft),
      }));
      if (secondsLeft <= 0) {
        if (countdownTimerRef.current) clearInterval(countdownTimerRef.current);
      }
    }, 1000);

    reconnectTimerRef.current = setTimeout(() => {
      isConnectingRef.current = false;
      connectStream();
    }, delayMs);
  }, [calculateBackoffDelay, fetchHttpSnapshot, notifyConnectionChange]);

  const connectSSE = useCallback(() => {
    if (sseRef.current) return;
    try {
      const sse = new EventSource('/api/stream');
      sseRef.current = sse;

      sse.onopen = () => {
        hasActiveTransportRef.current = true;
        if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
        if (countdownTimerRef.current) clearInterval(countdownTimerRef.current);
        retryCountRef.current = 0;

        setWsState((prev) => ({
          ...prev,
          isConnected: true,
          transportType: 'sse',
          connectionStatus: 'connected',
          retryCount: 0,
          nextRetryInSeconds: 0,
        }));

        notifyConnectionChange('connected');
      };

      sse.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data);
          processStreamPayload(parsed);
        } catch {
          // ignore
        }
      };

      sse.onerror = () => {
        if (sseRef.current) {
          sseRef.current.close();
          sseRef.current = null;
        }
        if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
          triggerReconnectWithBackoff();
        }
      };
    } catch {
      triggerReconnectWithBackoff();
    }
  }, [notifyConnectionChange, processStreamPayload, triggerReconnectWithBackoff]);

  const connectStream = useCallback(() => {
    if (isConnectingRef.current) return;
    isConnectingRef.current = true;

    // 1. Fetch initial snapshot over HTTP immediately so state is green & populated within milliseconds
    fetchHttpSnapshot().catch(() => {});

    // 2. Start SSE stream for instant HTTP reverse proxy compatibility
    connectSSE();

    // 3. Attempt WebSocket upgrade in parallel
    if (
      wsRef.current &&
      (wsRef.current.readyState === WebSocket.CONNECTING || wsRef.current.readyState === WebSocket.OPEN)
    ) {
      isConnectingRef.current = false;
      return;
    }

    try {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const host = window.location.host;
      const wsUrl = `${protocol}//${host}/ws`;

      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        hasActiveTransportRef.current = true;
        if (sseRef.current) {
          sseRef.current.close();
          sseRef.current = null;
        }
        if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
        if (countdownTimerRef.current) clearInterval(countdownTimerRef.current);
        retryCountRef.current = 0;
        isConnectingRef.current = false;

        setWsState((prev) => ({
          ...prev,
          isConnected: true,
          transportType: 'websocket',
          connectionStatus: 'connected',
          retryCount: 0,
          nextRetryInSeconds: 0,
        }));

        notifyConnectionChange('connected');

        ws.send(JSON.stringify({ type: 'GET_RANK_TREE', rank: activeRank }));
        ws.send(JSON.stringify({ type: 'GET_TRANSACTIONS' }));
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          processStreamPayload(data);
        } catch {
          // ignore
        }
      };

      ws.onerror = () => {
        wsRef.current = null;
        isConnectingRef.current = false;
      };

      ws.onclose = () => {
        wsRef.current = null;
        isConnectingRef.current = false;
        if (!sseRef.current && !hasActiveTransportRef.current) {
          triggerReconnectWithBackoff();
        }
      };
    } catch {
      isConnectingRef.current = false;
    }
  }, [activeRank, connectSSE, fetchHttpSnapshot, notifyConnectionChange, processStreamPayload, triggerReconnectWithBackoff]);

  useEffect(() => {
    connectStream();

    httpPollTimerRef.current = setInterval(() => {
      if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
        fetchHttpSnapshot().catch(() => {});
      }
    }, 15000);

    return () => {
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
      if (countdownTimerRef.current) clearInterval(countdownTimerRef.current);
      if (httpPollTimerRef.current) clearInterval(httpPollTimerRef.current);
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      if (sseRef.current) {
        sseRef.current.close();
        sseRef.current = null;
      }
    };
  }, [connectStream, fetchHttpSnapshot]);

  useEffect(() => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'GET_RANK_TREE', rank: activeRank }));
    }
  }, [activeRank]);

  const requestRankTree = useCallback((rank: number) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'GET_RANK_TREE', rank }));
    }
  }, []);

  const requestTransactions = useCallback(
    (address?: string) => {
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        wsRef.current.send(JSON.stringify({ type: 'GET_TRANSACTIONS', address }));
      } else {
        fetchHttpSnapshot().catch(() => {});
      }
    },
    [fetchHttpSnapshot]
  );

  const manualReconnect = useCallback(() => {
    retryCountRef.current = 0;
    if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
    if (countdownTimerRef.current) clearInterval(countdownTimerRef.current);
    if (wsRef.current) wsRef.current.close();
    if (sseRef.current) sseRef.current.close();
    wsRef.current = null;
    sseRef.current = null;
    isConnectingRef.current = false;
    connectStream();
  }, [connectStream]);

  return {
    ...wsState,
    requestRankTree,
    requestTransactions,
    manualReconnect,
    activeRankNodes: wsState.nodesByRank[activeRank] || [],
  };
}
