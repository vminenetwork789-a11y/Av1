import React, { useEffect, useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  CheckCircle2,
  AlertTriangle,
  Loader2,
  ExternalLink,
  Copy,
  Check,
  X,
  Clock,
  ShieldCheck,
  Sparkles,
} from 'lucide-react';
export interface TxState {
  isLoading: boolean;
  step: string;
  txHash: string | null;
  error: string | null;
  successMessage?: string | null;
}

export type Language = 'th' | 'en';

export interface CustomTransactionToastEvent {
  id?: string;
  status: 'pending' | 'confirmed' | 'error';
  step?: string;
  message: string;
  txHash?: string | null;
  timestamp?: string;
  durationMs?: number;
}

const TX_TOAST_EVENT_NAME = 'matrix:tx:toast';

/**
 * Helper to trigger a Framer Motion blockchain transaction toast from anywhere in the app
 * (e.g. inside modal registration, modal upgrade, or useWeb3 handlers).
 */
export function emitTransactionToast(payload: CustomTransactionToastEvent) {
  if (typeof window === 'undefined') return;
  window.dispatchEvent(
    new CustomEvent<CustomTransactionToastEvent>(TX_TOAST_EVENT_NAME, {
      detail: {
        ...payload,
        id: payload.id || `tx-toast-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        timestamp: payload.timestamp || new Date().toISOString(),
      },
    })
  );
}

interface ActiveToastItem {
  id: string;
  status: 'pending' | 'confirmed' | 'error';
  step: string;
  message: string;
  txHash: string | null;
  timestamp: string;
  durationMs: number;
  source: 'web3' | 'custom';
}

interface TransactionToastProps {
  txState?: TxState;
  onClear?: () => void;
  language?: Language;
  explorerBaseUrl?: string;
}

export const TransactionToast: React.FC<TransactionToastProps> = ({
  txState,
  onClear,
  language = 'th',
  explorerBaseUrl = 'https://testnet.bscscan.com/tx/',
}) => {
  const [customToasts, setCustomToasts] = useState<ActiveToastItem[]>([]);
  const [web3ConfirmedAt, setWeb3ConfirmedAt] = useState<string>(() => new Date().toISOString());
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [dismissedWeb3Key, setDismissedWeb3Key] = useState<string | null>(null);

  const isTh = language === 'th';

  const formatLatestTime = useCallback(
    (iso: string) => {
      try {
        const d = new Date(iso);
        if (isNaN(d.getTime())) return iso;
        return d.toLocaleString(isTh ? 'th-TH' : 'en-US', {
          timeZone: 'Asia/Bangkok',
          hour12: false,
          day: '2-digit',
          month: 'short',
          year: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
        });
      } catch {
        return iso;
      }
    },
    [isTh]
  );

  // Track timestamp when txState changes
  useEffect(() => {
    if (txState && (txState.isLoading || txState.error || txState.successMessage)) {
      setWeb3ConfirmedAt(new Date().toISOString());
      setDismissedWeb3Key(null);
    }
  }, [txState?.isLoading, txState?.error, txState?.successMessage, txState?.txHash, txState?.step]);

  // Listen to custom transaction toast events from modals / other components
  useEffect(() => {
    const handleCustomToast = (event: Event) => {
      const customEvent = event as CustomEvent<CustomTransactionToastEvent>;
      const detail = customEvent.detail;
      if (!detail || !detail.message) return;

      const id = detail.id || `tx-custom-${Date.now()}`;
      const newToast: ActiveToastItem = {
        id,
        status: detail.status,
        step:
          detail.step ||
          (detail.status === 'confirmed'
            ? isTh
              ? 'ยืนยันธุรกรรมบนบล็อกเชนสำเร็จ'
              : 'Transaction Confirmed on Blockchain'
            : detail.status === 'error'
            ? isTh
              ? 'ธุรกรรมไม่สำเร็จ'
              : 'Transaction Failed'
            : isTh
            ? 'กำลังดำเนินการบนบล็อกเชน...'
            : 'Processing on Blockchain...'),
        message: detail.message,
        txHash: detail.txHash || null,
        timestamp: detail.timestamp || new Date().toISOString(),
        durationMs: detail.durationMs ?? (detail.status === 'confirmed' ? 8000 : detail.status === 'error' ? 9000 : 0),
        source: 'custom',
      };

      setCustomToasts((prev) => {
        // Replace existing pending toast if same id or keep up to 3 latest toasts
        const filtered = prev.filter((item) => item.id !== id && !(detail.status !== 'pending' && item.status === 'pending'));
        return [newToast, ...filtered].slice(0, 3);
      });
    };

    window.addEventListener(TX_TOAST_EVENT_NAME, handleCustomToast);
    return () => window.removeEventListener(TX_TOAST_EVENT_NAME, handleCustomToast);
  }, [isTh]);

  // Auto-dismiss custom toasts when not hovered
  useEffect(() => {
    if (customToasts.length === 0) return;
    const timers = customToasts
      .filter((t) => t.durationMs > 0 && hoveredId !== t.id)
      .map((t) =>
        window.setTimeout(() => {
          setCustomToasts((prev) => prev.filter((item) => item.id !== t.id));
        }, t.durationMs)
      );
    return () => timers.forEach((timer) => clearTimeout(timer));
  }, [customToasts, hoveredId]);

  const currentWeb3Key = txState
    ? `${txState.isLoading}-${txState.step}-${txState.txHash || ''}-${txState.error || ''}-${txState.successMessage || ''}`
    : null;

  // Auto-dismiss confirmed web3 toast after 8 seconds unless hovered
  useEffect(() => {
    if (!txState || txState.isLoading || (!txState.successMessage && !txState.error)) return;
    if (hoveredId === 'web3-active-toast') return;

    const timeout = window.setTimeout(() => {
      if (onClear) {
        onClear();
      } else if (currentWeb3Key) {
        setDismissedWeb3Key(currentWeb3Key);
      }
    }, 8500);

    return () => clearTimeout(timeout);
  }, [txState, hoveredId, onClear, currentWeb3Key]);

  const activeToasts: ActiveToastItem[] = [];

  if (
    txState &&
    (txState.isLoading || txState.error || txState.successMessage) &&
    currentWeb3Key !== dismissedWeb3Key
  ) {
    const status: 'pending' | 'confirmed' | 'error' = txState.error
      ? 'error'
      : txState.isLoading
      ? 'pending'
      : 'confirmed';

    activeToasts.push({
      id: 'web3-active-toast',
      status,
      step:
        txState.step ||
        (status === 'confirmed'
          ? isTh
            ? 'ยืนยันธุรกรรมบนบล็อกเชนสำเร็จ'
            : 'Confirmed on Blockchain'
          : status === 'error'
          ? isTh
            ? 'ธุรกรรมล้มเหลว'
            : 'Transaction Error'
          : isTh
          ? 'กำลังประมวลผลธุรกรรม...'
          : 'Processing Transaction...'),
      message: txState.error || txState.successMessage || txState.step || '',
      txHash: txState.txHash,
      timestamp: web3ConfirmedAt,
      durationMs: status === 'pending' ? 0 : 8500,
      source: 'web3',
    });
  }

  for (const ct of customToasts) {
    // Avoid duplicate if same txHash is already shown in web3-active-toast
    if (ct.txHash && activeToasts.some((a) => a.txHash === ct.txHash && a.status === ct.status)) {
      continue;
    }
    activeToasts.push(ct);
  }

  const handleDismiss = (toast: ActiveToastItem) => {
    if (toast.source === 'web3') {
      if (onClear) {
        onClear();
      } else if (currentWeb3Key) {
        setDismissedWeb3Key(currentWeb3Key);
      }
    } else {
      setCustomToasts((prev) => prev.filter((item) => item.id !== toast.id));
    }
  };

  const handleCopyHash = (id: string, hash: string) => {
    navigator.clipboard.writeText(hash);
    setCopiedId(id);
    setTimeout(() => setCopiedId((prev) => (prev === id ? null : prev)), 2000);
  };

  return (
    <div
      className="fixed top-20 right-4 sm:right-6 z-[9999] flex flex-col gap-3 w-[calc(100vw-2rem)] sm:w-[430px] pointer-events-none"
      aria-live="polite"
    >
      <AnimatePresence mode="popLayout">
        {activeToasts.map((toast) => {
          const isConfirmed = toast.status === 'confirmed';
          const isError = toast.status === 'error';
          const isPending = toast.status === 'pending';

          return (
            <motion.div
              key={`${toast.id}-${toast.status}-${toast.txHash || ''}`}
              layout
              initial={{ opacity: 0, y: -28, x: 40, scale: 0.92 }}
              animate={{ opacity: 1, y: 0, x: 0, scale: 1 }}
              exit={{ opacity: 0, x: 80, scale: 0.9, transition: { duration: 0.22, ease: 'easeIn' } }}
              transition={{ type: 'spring', stiffness: 420, damping: 28, mass: 0.85 }}
              onMouseEnter={() => setHoveredId(toast.id)}
              onMouseLeave={() => setHoveredId((prev) => (prev === toast.id ? null : prev))}
              className={`pointer-events-auto relative overflow-hidden rounded-2xl border backdrop-blur-xl shadow-2xl transition-colors ${
                isConfirmed
                  ? 'bg-slate-950/95 border-emerald-500/50 shadow-emerald-500/20'
                  : isError
                  ? 'bg-slate-950/95 border-red-500/50 shadow-red-500/20'
                  : 'bg-slate-950/95 border-amber-500/45 shadow-amber-500/15'
              }`}
            >
              {/* Ambient Top Glow */}
              <div
                className={`absolute inset-x-0 top-0 h-1 ${
                  isConfirmed
                    ? 'bg-gradient-to-r from-emerald-400 via-teal-300 to-cyan-400'
                    : isError
                    ? 'bg-gradient-to-r from-red-500 via-rose-400 to-orange-500'
                    : 'bg-gradient-to-r from-amber-400 via-yellow-300 to-amber-500'
                }`}
              />

              <div className="p-4 sm:p-4.5">
                {/* Top Status & Latest Reference Time Row */}
                <div className="flex items-center justify-between gap-2 mb-2.5">
                  <div className="flex items-center gap-2">
                    <span
                      className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-black uppercase tracking-wider border ${
                        isConfirmed
                          ? 'bg-emerald-500/15 text-emerald-300 border-emerald-500/40'
                          : isError
                          ? 'bg-red-500/15 text-red-300 border-red-500/40'
                          : 'bg-amber-500/15 text-amber-300 border-amber-500/40'
                      }`}
                    >
                      {isConfirmed ? (
                        <>
                          <ShieldCheck className="w-3 h-3 text-emerald-400" />
                          {isTh ? 'ยืนยันบนบล็อกเชนแล้ว' : 'BLOCKCHAIN CONFIRMED'}
                        </>
                      ) : isError ? (
                        <>
                          <AlertTriangle className="w-3 h-3 text-red-400" />
                          {isTh ? 'ธุรกรรมขัดข้อง' : 'TRANSACTION FAILED'}
                        </>
                      ) : (
                        <>
                          <Sparkles className="w-3 h-3 text-amber-400 animate-pulse" />
                          {isTh ? 'กำลังส่งธุรกรรม...' : 'BROADCASTING TX'}
                        </>
                      )}
                    </span>
                  </div>

                  <div className="flex items-center gap-2">
                    <span className="inline-flex items-center gap-1 text-[10px] font-mono text-slate-400 bg-slate-900/90 px-2 py-0.5 rounded-md border border-slate-800">
                      <Clock className="w-2.5 h-2.5 text-cyan-400" />
                      {formatLatestTime(toast.timestamp)}
                    </span>
                    <button
                      type="button"
                      onClick={() => handleDismiss(toast)}
                      className="p-1 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800/80 transition-colors"
                      title={isTh ? 'ปิดการแจ้งเตือน' : 'Dismiss'}
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                </div>

                {/* Main Body */}
                <div className="flex items-start gap-3.5">
                  <motion.div
                    initial={{ scale: 0.5, rotate: -15 }}
                    animate={{ scale: 1, rotate: 0 }}
                    transition={{ type: 'spring', stiffness: 500, damping: 20 }}
                    className={`w-10 h-10 rounded-xl flex items-center justify-center shrink-0 border ${
                      isConfirmed
                        ? 'bg-emerald-500/15 border-emerald-500/40 text-emerald-400 shadow-inner shadow-emerald-500/20'
                        : isError
                        ? 'bg-red-500/15 border-red-500/40 text-red-400'
                        : 'bg-amber-500/15 border-amber-500/40 text-amber-400'
                    }`}
                  >
                    {isPending ? (
                      <Loader2 className="w-5 h-5 animate-spin" />
                    ) : isError ? (
                      <AlertTriangle className="w-5 h-5" />
                    ) : (
                      <CheckCircle2 className="w-5 h-5" />
                    )}
                  </motion.div>

                  <div className="flex-1 min-w-0">
                    <h4
                      className={`text-sm font-black tracking-tight leading-snug ${
                        isConfirmed
                          ? 'text-emerald-300'
                          : isError
                          ? 'text-red-300'
                          : 'text-amber-300'
                      }`}
                    >
                      {toast.step}
                    </h4>
                    {toast.message && toast.message !== toast.step && (
                      <p className="text-xs text-slate-200/90 mt-1 leading-relaxed break-words">
                        {toast.message}
                      </p>
                    )}

                    {/* Transaction Hash Interactive Pill */}
                    {toast.txHash && (
                      <motion.div
                        initial={{ opacity: 0, y: 6 }}
                        animate={{ opacity: 1, y: 0 }}
                        className="mt-3 flex items-center justify-between gap-2 px-2.5 py-1.5 rounded-xl bg-slate-900/95 border border-slate-800/90"
                      >
                        <div className="flex items-center gap-2 min-w-0">
                          <span className="text-[10px] font-bold uppercase tracking-wider text-slate-400 shrink-0">
                            TxHash:
                          </span>
                          <span className="text-xs font-mono text-cyan-300 truncate">
                            {toast.txHash.slice(0, 10)}...{toast.txHash.slice(-8)}
                          </span>
                        </div>

                        <div className="flex items-center gap-1.5 shrink-0">
                          <button
                            type="button"
                            onClick={() => handleCopyHash(toast.id, toast.txHash!)}
                            className="inline-flex items-center gap-1 px-2 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-[10px] font-semibold text-slate-200 transition-colors"
                            title={isTh ? 'คัดลอก TxHash' : 'Copy TxHash'}
                          >
                            {copiedId === toast.id ? (
                              <>
                                <Check className="w-3 h-3 text-emerald-400" />
                                <span className="text-emerald-300">{isTh ? 'คัดลอกแล้ว' : 'Copied'}</span>
                              </>
                            ) : (
                              <>
                                <Copy className="w-3 h-3 text-slate-400" />
                                <span>{isTh ? 'คัดลอก' : 'Copy'}</span>
                              </>
                            )}
                          </button>

                          <a
                            href={`${explorerBaseUrl}${toast.txHash}`}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="inline-flex items-center gap-1 px-2.5 py-1 rounded-lg bg-emerald-500/15 hover:bg-emerald-500/25 border border-emerald-500/35 text-[10px] font-bold text-emerald-300 transition-colors"
                          >
                            <span>BscScan</span>
                            <ExternalLink className="w-3 h-3" />
                          </a>
                        </div>
                      </motion.div>
                    )}
                  </div>
                </div>
              </div>

              {/* Animated Countdown Progress Bar for Confirmed / Error Toasts */}
              {toast.durationMs > 0 && hoveredId !== toast.id && (
                <div className="h-1 w-full bg-slate-900">
                  <motion.div
                    initial={{ width: '100%' }}
                    animate={{ width: '0%' }}
                    transition={{ duration: toast.durationMs / 1000, ease: 'linear' }}
                    className={`h-full ${
                      isConfirmed
                        ? 'bg-gradient-to-r from-emerald-500 to-cyan-400'
                        : 'bg-gradient-to-r from-red-500 to-orange-400'
                    }`}
                  />
                </div>
              )}
            </motion.div>
          );
        })}
      </AnimatePresence>
    </div>
  );
};
