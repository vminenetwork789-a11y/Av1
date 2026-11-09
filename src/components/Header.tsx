import React, { useState } from 'react';
import { CONTRACT_ADDRESS, clearDynamicContractConfig } from '../contracts/config';
import {
  ExternalLink,
  Wallet,
  CheckCircle2,
  AlertTriangle,
  RefreshCw,
  Zap,
  LogOut,
  Shield,
  Trash2,
} from 'lucide-react';

interface HeaderProps {
  account: string | null;
  chainId: number | null;
  isBscTestnet: boolean;
  isConnecting: boolean;
  onConnect: () => void;
  onDisconnect: () => void;
  onSwitchNetwork: () => void;
  onRefresh: () => void;
  language: 'th' | 'en';
  onToggleLanguage: () => void;
  onOpenDeployModal: () => void;
  contractAddress?: string;
  isAdminUser?: boolean;
  isAdminView?: boolean;
  onToggleAdminView?: () => void;
}

export const Header: React.FC<HeaderProps> = ({
  account,
  isBscTestnet,
  isConnecting,
  onConnect,
  onDisconnect,
  onSwitchNetwork,
  onRefresh,
  language,
  onToggleLanguage,
  onOpenDeployModal,
  contractAddress,
  isAdminUser = false,
  isAdminView = false,
  onToggleAdminView,
}) => {
  const [isClearingCache, setIsClearingCache] = useState(false);
  const [cacheClearedDone, setCacheClearedDone] = useState(false);

  const shortAddress = account
    ? `${account.substring(0, 6)}...${account.substring(account.length - 4)}`
    : '';
  const currentContract = contractAddress || CONTRACT_ADDRESS;
  const shortContract = `${currentContract.substring(0, 6)}...${currentContract.substring(
    currentContract.length - 4
  )}`;

  const handleClearCacheAndResync = async () => {
    if (isClearingCache) return;
    setIsClearingCache(true);
    setCacheClearedDone(false);
    try {
      // 1. Clear browser localStorage stale contract/cache keys
      if (typeof window !== 'undefined') {
        try {
          clearDynamicContractConfig();
          localStorage.removeItem('F27_BUILD_DEFAULT_CONTRACT');
        } catch {
          // ignore
        }
        if ('caches' in window) {
          try {
            const keys = await window.caches.keys();
            await Promise.all(keys.map((k) => window.caches.delete(k)));
          } catch {
            // ignore
          }
        }
      }

      // 2. Clear server-side matrix-db.json cache and force full 27-rank blockchain resync
      await fetch('/api/admin/clear-cache', {
        method: 'POST',
        headers: { 'Cache-Control': 'no-cache, no-store' },
      }).catch(() => {});

      // 3. Refresh frontend user profile & tree visualizer
      onRefresh();
      if (typeof window !== 'undefined') {
        window.dispatchEvent(new CustomEvent('matrix:tree:refresh'));
      }
      setCacheClearedDone(true);
      setTimeout(() => setCacheClearedDone(false), 3000);
    } finally {
      setIsClearingCache(false);
    }
  };

  return (
    <header className="w-full bg-slate-900/95 backdrop-blur-md border-b border-slate-800/90 sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-3 sm:px-6 lg:px-8 py-2.5 sm:py-3.5">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-2.5 sm:gap-4">
          {/* Top Row on Mobile: Brand Lockup + Language & Refresh */}
          <div className="flex items-center justify-between gap-2 min-w-0">
            <div className="flex items-center gap-2.5 sm:gap-3 min-w-0">
              <div className="relative flex items-center justify-center w-10 h-10 sm:w-11 sm:h-11 rounded-xl bg-gradient-to-br from-emerald-500/20 via-teal-500/20 to-indigo-500/20 border border-emerald-500/30 text-emerald-400 shadow-lg shadow-emerald-950/40 shrink-0">
                <span className="font-mono font-black text-base sm:text-xl tracking-tighter">
                  F27
                </span>
              </div>

              <div className="min-w-0">
                <div className="flex items-center gap-2 min-w-0">
                  <h1 className="text-base sm:text-xl font-bold bg-gradient-to-r from-emerald-400 via-teal-300 to-indigo-300 bg-clip-text text-transparent truncate">
                    FractalFlowMatrix27
                  </h1>
                  <span className="hidden sm:inline-flex items-center text-[10px] font-semibold uppercase px-2 py-0.5 rounded-md bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 whitespace-nowrap shrink-0">
                    DApp Control Panel
                  </span>
                </div>

                <div className="flex items-center gap-1.5 sm:gap-2 text-[11px] sm:text-xs text-slate-400 mt-0.5 flex-wrap">
                  <span className="whitespace-nowrap">
                    {language === 'th' ? 'สัญญา:' : 'Contract:'}
                  </span>
                  <a
                    href={`https://testnet.bscscan.com/address/${currentContract}`}
                    target="_blank"
                    rel="noreferrer"
                    className="text-amber-400 hover:text-amber-300 font-mono inline-flex items-center gap-1 transition-colors whitespace-nowrap shrink-0"
                    title={currentContract}
                  >
                    <span>{shortContract}</span>
                    <ExternalLink className="w-3 h-3 shrink-0" />
                  </a>
                  <span className="text-slate-600">·</span>
                  <span className="text-emerald-400 font-medium whitespace-nowrap">
                    BNB Testnet (97)
                  </span>
                </div>
              </div>
            </div>

            {/* Mobile-only quick clear-cache, refresh & language controls on top-right */}
            <div className="flex md:hidden items-center gap-1.5 shrink-0">
              <button
                onClick={handleClearCacheAndResync}
                disabled={isClearingCache}
                className={`px-2 py-1.5 rounded-xl border text-[10px] font-semibold inline-flex items-center gap-1 transition cursor-pointer ${
                  cacheClearedDone
                    ? 'bg-emerald-500/20 border-emerald-500/40 text-emerald-300'
                    : 'bg-rose-500/15 hover:bg-rose-500/25 border-rose-500/30 text-rose-300'
                }`}
                title={
                  language === 'th'
                    ? 'ล้างแคชเบราว์เซอร์และดึงข้อมูลสดจาก Smart Contract ใหม่'
                    : 'Clear cache and force resync from Smart Contract'
                }
              >
                {isClearingCache ? (
                  <RefreshCw className="w-3 h-3 animate-spin" />
                ) : (
                  <Trash2 className="w-3 h-3" />
                )}
                <span>
                  {cacheClearedDone
                    ? language === 'th'
                      ? 'ล้างแล้ว!'
                      : 'Cleared!'
                    : language === 'th'
                    ? 'ล้างแคช'
                    : 'Clear Cache'}
                </span>
              </button>
              {account && (
                <button
                  onClick={onRefresh}
                  className="p-2 rounded-xl bg-slate-800/90 hover:bg-slate-700 border border-slate-700/80 text-slate-300 hover:text-white transition cursor-pointer"
                  title={language === 'th' ? 'รีเฟรชข้อมูลบล็อกเชน' : 'Refresh blockchain data'}
                >
                  <RefreshCw className="w-3.5 h-3.5" />
                </button>
              )}
              <button
                onClick={onToggleLanguage}
                className="text-[11px] bg-slate-800/90 hover:bg-slate-700 border border-slate-700/80 px-2.5 py-1.5 rounded-xl font-mono text-slate-200 hover:text-white transition whitespace-nowrap shrink-0 cursor-pointer"
              >
                {language === 'th' ? '🇬🇧 EN' : '🇹🇭 TH'}
              </button>
            </div>
          </div>

          {/* Action Toolbar: Clean single-line pills on mobile & desktop */}
          <div className="flex items-center justify-between md:justify-end gap-2 overflow-x-auto no-scrollbar pt-1 md:pt-0 border-t border-slate-800/60 md:border-t-0">
            <div className="flex items-center gap-1.5 sm:gap-2 shrink-0">
              {/* Admin Control Center Toggle Button (Visible ONLY to Admin) */}
              {isAdminUser && onToggleAdminView && (
                <button
                  type="button"
                  onClick={onToggleAdminView}
                  className={`text-[11px] sm:text-xs px-3 py-1.5 rounded-xl font-bold transition duration-200 inline-flex items-center gap-1.5 shadow-sm whitespace-nowrap shrink-0 cursor-pointer ${
                    isAdminView
                      ? 'bg-amber-500 text-slate-950 border border-amber-400'
                      : 'bg-amber-500/15 hover:bg-amber-500/25 border border-amber-500/40 text-amber-300'
                  }`}
                >
                  <Shield className="w-3.5 h-3.5 shrink-0" />
                  <span>
                    {isAdminView
                      ? language === 'th'
                        ? 'กลับหน้าผังหลัก'
                        : 'Back to Matrix'
                      : language === 'th'
                      ? 'หน้า Admin'
                      : 'Admin Panel'}
                  </span>
                </button>
              )}

              {/* Deploy Contract Shortcut (Visible to Everyone) */}
              <button
                onClick={onOpenDeployModal}
                className="text-[11px] sm:text-xs bg-slate-800/80 hover:bg-slate-700/80 border border-indigo-500/30 text-indigo-300 hover:text-indigo-200 px-2.5 sm:px-3 py-1.5 rounded-xl font-medium transition duration-200 inline-flex items-center gap-1.5 shadow-sm whitespace-nowrap shrink-0 cursor-pointer"
                title={
                  language === 'th'
                    ? 'ศูนย์จัดการและทดสอบ Deploy Smart Contract (เปิดให้ทุกคนเข้าดูได้)'
                    : 'Contract deployment manager (Public)'
                }
              >
                <Zap className="w-3.5 h-3.5 text-amber-400 shrink-0" />
                <span>{language === 'th' ? 'Deploy สัญญา' : 'Deploy Contract'}</span>
              </button>

              {/* Desktop 1-Click Clear Cache & Resync Button */}
              <button
                onClick={handleClearCacheAndResync}
                disabled={isClearingCache}
                className={`hidden md:inline-flex items-center gap-1.5 text-xs px-2.5 py-1.5 rounded-xl font-semibold border transition whitespace-nowrap shrink-0 cursor-pointer ${
                  cacheClearedDone
                    ? 'bg-emerald-500/20 border-emerald-500/40 text-emerald-300'
                    : 'bg-rose-500/15 hover:bg-rose-500/25 border-rose-500/30 text-rose-300 hover:text-rose-200'
                }`}
                title={
                  language === 'th'
                    ? 'ล้างแคชเบราว์เซอร์ + ล้างแคชฐานข้อมูลเซิร์ฟเวอร์ และดึงข้อมูลสดจาก Smart Contract ใหม่ครบ 27 แรงค์'
                    : 'Clear browser & server cache and resync all 27 ranks from Smart Contract'
                }
              >
                {isClearingCache ? (
                  <RefreshCw className="w-3.5 h-3.5 animate-spin shrink-0" />
                ) : (
                  <Trash2 className="w-3.5 h-3.5 shrink-0" />
                )}
                <span>
                  {isClearingCache
                    ? language === 'th'
                      ? 'กำลังล้างแคช...'
                      : 'Clearing...'
                    : cacheClearedDone
                    ? language === 'th'
                      ? 'ล้างแคชสำเร็จ!'
                      : 'Cache Cleared!'
                    : language === 'th'
                    ? 'ล้างแคช'
                    : 'Clear Cache'}
                </span>
              </button>

              {/* Desktop Language Switch */}
              <button
                onClick={onToggleLanguage}
                className="hidden md:inline-flex text-xs bg-slate-800/80 hover:bg-slate-700/80 border border-slate-700 px-2.5 py-1.5 rounded-xl font-mono text-slate-300 hover:text-white transition whitespace-nowrap shrink-0 cursor-pointer"
              >
                {language === 'th' ? '🇬🇧 EN' : '🇹🇭 TH'}
              </button>

              {/* Network Indicator / Switcher */}
              {account &&
                (isBscTestnet ? (
                  <div className="inline-flex items-center gap-1.5 bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 px-2.5 sm:px-3 py-1.5 rounded-xl text-[11px] sm:text-xs font-medium whitespace-nowrap shrink-0">
                    <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400 shrink-0" />
                    <span>BSC Testnet</span>
                  </div>
                ) : (
                  <button
                    onClick={onSwitchNetwork}
                    className="inline-flex items-center gap-1.5 bg-amber-500/20 hover:bg-amber-500/30 border border-amber-500/40 text-amber-300 px-2.5 sm:px-3 py-1.5 rounded-xl text-[11px] sm:text-xs font-medium transition whitespace-nowrap shrink-0 cursor-pointer"
                  >
                    <AlertTriangle className="w-3.5 h-3.5 text-amber-400 shrink-0" />
                    <span>{language === 'th' ? 'สลับไป BNB Testnet' : 'Switch Network'}</span>
                  </button>
                ))}

              {/* Desktop Refresh Profile */}
              {account && (
                <button
                  onClick={onRefresh}
                  className="hidden md:inline-flex p-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-400 hover:text-white transition shrink-0 cursor-pointer"
                  title={language === 'th' ? 'รีเฟรชข้อมูลบล็อกเชน' : 'Refresh blockchain data'}
                >
                  <RefreshCw className="w-4 h-4" />
                </button>
              )}
            </div>

            {/* Connect / Disconnect Button */}
            {account ? (
              <div className="inline-flex items-center gap-2 bg-slate-950 border border-slate-800 px-2.5 sm:px-3 py-1.5 rounded-xl shrink-0">
                <span className="w-2 h-2 rounded-full bg-emerald-400 shrink-0" />
                <span className="text-[11px] sm:text-xs font-mono text-slate-200 whitespace-nowrap">
                  {shortAddress}
                </span>
                <button
                  onClick={onDisconnect}
                  className="inline-flex items-center gap-1 text-[11px] text-rose-400 hover:text-rose-300 font-medium transition pl-1.5 border-l border-slate-800 whitespace-nowrap cursor-pointer"
                  title={language === 'th' ? 'ตัดการเชื่อมต่อกระเป๋า' : 'Disconnect Wallet'}
                >
                  <LogOut className="w-3 h-3 sm:hidden" />
                  <span>{language === 'th' ? 'ออก' : 'Exit'}</span>
                </button>
              </div>
            ) : (
              <button
                onClick={onConnect}
                disabled={isConnecting}
                id="walletBtn"
                className="inline-flex items-center gap-1.5 bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white font-semibold text-xs px-3.5 sm:px-4 py-1.5 sm:py-2 rounded-xl transition duration-200 shadow-lg shadow-emerald-950/50 cursor-pointer disabled:opacity-50 whitespace-nowrap shrink-0"
              >
                <Wallet className="w-3.5 h-3.5 shrink-0" />
                <span>
                  {isConnecting
                    ? language === 'th'
                      ? 'กำลังเชื่อมต่อ...'
                      : 'Connecting...'
                    : language === 'th'
                    ? 'เชื่อมต่อกระเป๋า'
                    : 'Connect Wallet'}
                </span>
              </button>
            )}
          </div>
        </div>
      </div>
    </header>
  );
};
