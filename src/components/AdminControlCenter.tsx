import React, { useState, useEffect, useCallback } from 'react';
import { ethers } from 'ethers';
import { MATRIX_ABI, RANK_PRICES_USDT } from '../contracts/config';
import {
  Shield,
  RefreshCw,
  CheckCircle2,
  AlertCircle,
  ExternalLink,
  Copy,
  Check,
  Zap,
  Database,
  Wallet,
  ArrowUpCircle,
  Coins,
  KeyRound,
  Layers,
  Play,
  Trash2,
  Save,
  RotateCcw,
  Search,
  Sparkles,
  Lock,
  UserCheck,
  Send,
  X,
} from 'lucide-react';

interface AdminControlCenterProps {
  account: string | null;
  language: 'th' | 'en';
  contractAddress: string;
  usdtAddress: string;
  isLoading: boolean;
  onAdminUpgrade: (targetUserOrId: string, targetRank: number) => Promise<any>;
  onUpdateContractAddress: (newContract: string, newUsdt?: string) => Promise<boolean>;
  onResetContractAddress: () => Promise<boolean>;
  onOpenDeployModal: () => void;
  onRefreshOnChainData: () => void;
  onInspectRank: (rank: number) => Promise<{
    firstNodeId: string;
    latestNodeId: string;
    upgradeVaultBalance: string;
    nextRankPrice: string;
  }>;
  onClose?: () => void;
}

export const AdminControlCenter: React.FC<AdminControlCenterProps> = ({
  account,
  language,
  contractAddress,
  usdtAddress,
  isLoading,
  onAdminUpgrade,
  onUpdateContractAddress,
  onResetContractAddress,
  onOpenDeployModal,
  onRefreshOnChainData,
  onInspectRank,
  onClose,
}) => {
  const [activeSection, setActiveSection] = useState<
    'all' | 'owner' | 'autopools' | 'allsell' | 'database' | 'members'
  >('all');
  const [execMode, setExecMode] = useState<'server_key' | 'web3_wallet'>('server_key');
  const [customAdminPk, setCustomAdminPk] = useState<string>('');
  const [overview, setOverview] = useState<any | null>(null);
  const [loadingOverview, setLoadingOverview] = useState<boolean>(false);
  const [actionStatus, setActionStatus] = useState<{
    type: 'success' | 'error' | 'loading';
    title: string;
    message: string;
    txHash?: string;
  } | null>(null);
  const [copiedText, setCopiedText] = useState<string | null>(null);

  // 1. Admin Upgrade State
  const [upgradeTarget, setUpgradeTarget] = useState<string>('2');
  const [upgradeRank, setUpgradeRank] = useState<string>('5');
  const [upgradeExecutionMode, setUpgradeExecutionMode] = useState<'both' | 'onchain' | 'db_only'>('both');
  const [targetLookup, setTargetLookup] = useState<any | null>(null);

  // 2. setSystemWallets State
  const [sysAllSell, setSysAllSell] = useState<string>('');
  const [sysBot, setSysBot] = useState<string>('');
  const [sysFamily, setSysFamily] = useState<string>('');

  // 3. emergencyWithdrawToken & Native State
  const [withdrawTokenAddr, setWithdrawTokenAddr] = useState<string>(usdtAddress || '');
  const [withdrawTokenAmount, setWithdrawTokenAmount] = useState<string>('');

  // 4. Ownership State
  const [newOwnerAddr, setNewOwnerAddr] = useState<string>('');
  const [renounceConfirmText, setRenounceConfirmText] = useState<string>('');

  // 5. Auto-Pool On-Chain Trigger State
  const [autoMainNodeId, setAutoMainNodeId] = useState<string>('1');
  const [autoMainUserAddr, setAutoMainUserAddr] = useState<string>('');
  const [autoExtraUserAddr, setAutoExtraUserAddr] = useState<string>('');
  const [autoExtraRank, setAutoExtraRank] = useState<string>('4');
  const [autoExtraSponsorId, setAutoExtraSponsorId] = useState<string>('1');

  // 6. Contract & DB Config State
  const [editContractAddr, setEditContractAddr] = useState<string>(contractAddress || '');
  const [editUsdtAddr, setEditUsdtAddr] = useState<string>(usdtAddress || '');
  const [batchStatus, setBatchStatus] = useState<any | null>(null);

  // 7. Rank Inspector State
  const [inspectRankNum, setInspectRankNum] = useState<number>(1);
  const [inspectStats, setInspectStats] = useState<{
    firstNodeId: string;
    latestNodeId: string;
    upgradeVaultBalance: string;
    nextRankPrice: string;
  }>({
    firstNodeId: '-',
    latestNodeId: '-',
    upgradeVaultBalance: '0.00',
    nextRankPrice: '10.00 USDT',
  });
  const [memberSearch, setMemberSearch] = useState<string>('');

  const handleCopy = (text: string) => {
    if (!text) return;
    navigator.clipboard.writeText(text);
    setCopiedText(text);
    setTimeout(() => setCopiedText(null), 1800);
  };

  const fetchAdminOverview = useCallback(async () => {
    setLoadingOverview(true);
    try {
      const [ovRes, bqRes] = await Promise.all([
        fetch('/api/admin/overview').then((r) => r.json()).catch(() => null),
        fetch('/api/batch-queue').then((r) => r.json()).catch(() => null),
      ]);
      if (ovRes?.success) {
        setOverview(ovRes);
        setSysAllSell((prev) => prev || ovRes.wallets?.allSell?.address || '');
        setSysBot((prev) => prev || ovRes.wallets?.bot?.address || '');
        setSysFamily((prev) => prev || ovRes.wallets?.familyPool?.address || '');
      }
      if (bqRes) {
        setBatchStatus(bqRes);
      }
    } finally {
      setLoadingOverview(false);
    }
  }, []);

  useEffect(() => {
    fetchAdminOverview();
  }, [fetchAdminOverview, contractAddress]);

  useEffect(() => {
    setEditContractAddr(contractAddress || '');
    setEditUsdtAddr(usdtAddress || '');
    setWithdrawTokenAddr(usdtAddress || '');
  }, [contractAddress, usdtAddress]);

  // Live lookup for Admin Upgrade target
  useEffect(() => {
    const q = upgradeTarget.trim().replace(/^#/, '');
    if (!q) {
      setTargetLookup(null);
      return;
    }
    const timer = setTimeout(async () => {
      try {
        const res = await fetch(`/api/admin/lookup-target?query=${encodeURIComponent(q)}`);
        const data = await res.json();
        setTargetLookup(data);
      } catch {
        setTargetLookup(null);
      }
    }, 200);
    return () => clearTimeout(timer);
  }, [upgradeTarget]);

  // Load Rank Inspector Data
  useEffect(() => {
    let active = true;
    onInspectRank(inspectRankNum)
      .then((info) => {
        if (active) setInspectStats(info);
      })
      .catch(() => {});
    return () => {
      active = false;
    };
  }, [inspectRankNum, onInspectRank]);

  // Generic Executor for Smart Contract Admin Functions (supports both Server Private Key & Connected Web3 Wallet)
  const executeContractAdminFunction = async (
    actionName: string,
    params: Record<string, any>,
    labelTh: string
  ) => {
    setActionStatus({
      type: 'loading',
      title: `กำลังรันคำสั่ง ${actionName}...`,
      message: labelTh,
    });

    try {
      if (execMode === 'web3_wallet' && (window as any).ethereum) {
        const provider = new ethers.BrowserProvider((window as any).ethereum);
        const signer = await provider.getSigner();
        const contract = new ethers.Contract(contractAddress, MATRIX_ABI, signer);
        let tx: any;

        if (actionName === 'adminUpgradeRankForMainId') {
          tx = await contract.adminUpgradeRankForMainId(Number(params.mainId), Number(params.targetRank));
        } else if (actionName === 'adminUpgradeRank') {
          tx = await contract.adminUpgradeRank(params.targetUser, Number(params.targetRank));
        } else if (actionName === 'setSystemWallets') {
          tx = await contract.setSystemWallets(params.allSell, params.bot, params.familyPool);
        } else if (actionName === 'emergencyWithdrawToken') {
          const wei = ethers.parseUnits(String(params.amount), 18);
          tx = await contract.emergencyWithdrawToken(params.token, wei);
        } else if (actionName === 'emergencyWithdrawNative') {
          tx = await contract.emergencyWithdrawNative();
        } else if (actionName === 'transferOwnership') {
          tx = await contract.transferOwnership(params.newOwner);
        } else if (actionName === 'renounceOwnership') {
          tx = await contract.renounceOwnership();
        } else if (actionName === 'checkAndProcessAutoNewMainIdForNode') {
          tx = await contract.checkAndProcessAutoNewMainIdForNode(Number(params.mainNodeId));
        } else if (actionName === 'checkAndProcessAutoNewMainId') {
          tx = await contract.checkAndProcessAutoNewMainId(params.user);
        } else if (actionName === 'checkAndProcessAutoExtraIdForNode') {
          tx = await contract.checkAndProcessAutoExtraIdForNode(
            params.user,
            Number(params.currentRank),
            Number(params.sponsorNodeId)
          );
        } else if (actionName === 'checkAndProcessAutoExtraId') {
          tx = await contract.checkAndProcessAutoExtraId(params.user, Number(params.currentRank));
        } else {
          throw new Error(`Unsupported Web3 action: ${actionName}`);
        }

        // กำหนด Timeout การรอตรวจสอบธุรกรรม 2.5 นาที (150,000 ms) ถ้าเกินเวลา ไม่สำเร็จทันที
        const waitWith2_5MinTimeout = (txObj: any) =>
          Promise.race([
            txObj.wait(),
            new Promise((_, reject) =>
              setTimeout(
                () => reject(new Error('⏱️ หมดเวลาการรอตรวจสอบธุรกรรม (เกิน 2.5 นาที) การทำรายการไม่สำเร็จทันที')),
                150000
              )
            ),
          ]);

        const receipt: any = await waitWith2_5MinTimeout(tx);
        const hash = receipt?.hash || tx.hash;
        await fetch('/api/db/sync', { method: 'POST' }).catch(() => {});
        await fetchAdminOverview();
        onRefreshOnChainData();
        setActionStatus({
          type: 'success',
          title: `รันคำสั่ง ${actionName} สำเร็จ!`,
          message: `${labelTh} ยืนยันบนบล็อกเชนเรียบร้อยแล้ว`,
          txHash: hash,
        });
        return;
      }

      // Server-side Admin Key Execution (จำกัดเวลารอไม่เกิน 2.5 นาที)
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 150000);

      let res: Response;
      try {
        res = await fetch('/api/admin/contract-action', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            action: actionName,
            params,
            privateKey: customAdminPk.trim() || undefined,
          }),
        });
      } catch (fetchErr: any) {
        if (fetchErr?.name === 'AbortError') {
          throw new Error('⏱️ หมดเวลาการรอตรวจสอบธุรกรรมของ Server (เกิน 2.5 นาที) การทำรายการไม่สำเร็จทันที');
        }
        throw fetchErr;
      } finally {
        clearTimeout(timeoutId);
      }
      const data = await res.json();
      if (!res.ok || !data.success) {
        throw new Error(data.error || `รันคำสั่ง ${actionName} ไม่สำเร็จ`);
      }

      await fetchAdminOverview();
      onRefreshOnChainData();
      if (typeof window !== 'undefined') {
        window.dispatchEvent(new CustomEvent('matrix:tree:refresh'));
      }
      setActionStatus({
        type: 'success',
        title: `รันคำสั่ง ${actionName} สำเร็จ!`,
        message: data.message || labelTh,
        txHash: data.txHash,
      });
    } catch (err: any) {
      setActionStatus({
        type: 'error',
        title: `คำสั่ง ${actionName} ล้มเหลว`,
        message: err?.shortMessage || err?.message || 'เกิดข้อผิดพลาดในการส่งธุรกรรม',
      });
    }
  };

  const handleExecuteAdminUpgrade = async (e: React.FormEvent) => {
    e.preventDefault();
    const raw = upgradeTarget.trim().replace(/^#/, '');
    const targetRankNum = Math.min(27, Math.max(2, parseInt(upgradeRank, 10) || 2));
    if (!raw) return;

    if (upgradeExecutionMode === 'onchain') {
      const isId = /^\d+$/.test(raw);
      if (isId) {
        await executeContractAdminFunction(
          'adminUpgradeRankForMainId',
          { mainId: Number(raw), targetRank: targetRankNum },
          `อัปเกรดรหัสหลัก #${raw} สู่ Rank ${targetRankNum} บน Smart Contract`
        );
      } else {
        await executeContractAdminFunction(
          'adminUpgradeRank',
          { targetUser: raw, targetRank: targetRankNum },
          `อัปเกรดกระเป๋า ${raw.substring(0, 8)}... สู่ Rank ${targetRankNum} บน Smart Contract`
        );
      }
      return;
    }

    if (upgradeExecutionMode === 'db_only') {
      setActionStatus({
        type: 'loading',
        title: 'กำลังอัปเกรดแรงค์ในระบบผัง Database...',
        message: `อัปเกรด ${raw} ไปยัง Rank ${targetRankNum} ฟรี`,
      });
      try {
        const res = await fetch('/api/admin/upgrade-rank', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            targetUser: ethers.isAddress(raw) ? raw : undefined,
            mainId: /^\d+$/.test(raw) ? Number(raw) : undefined,
            targetRank: targetRankNum,
          }),
        });
        const data = await res.json();
        if (!res.ok || !data.success) throw new Error(data.error || 'อัปเกรดไม่สำเร็จ');
        await fetchAdminOverview();
        onRefreshOnChainData();
        if (typeof window !== 'undefined') {
          window.dispatchEvent(new CustomEvent('matrix:tree:refresh'));
        }
        setActionStatus({
          type: 'success',
          title: 'Admin Upgrade สำเร็จ!',
          message: data.message,
        });
      } catch (err: any) {
        setActionStatus({
          type: 'error',
          title: 'Admin Upgrade ล้มเหลว',
          message: err.message,
        });
      }
      return;
    }

    // Both (On-chain + DB fallback via useWeb3.handleAdminUpgrade)
    await onAdminUpgrade(raw, targetRankNum);
    setTimeout(() => fetchAdminOverview(), 1200);
  };

  const filteredMainIds = (overview?.mainIds || []).filter((m: any) => {
    if (!memberSearch.trim()) return true;
    const q = memberSearch.trim().toLowerCase().replace(/^#/, '');
    return (
      String(m.mainId) === q ||
      String(m.owner || '').toLowerCase().includes(q) ||
      String(m.directSponsorNodeId) === q
    );
  });

  const showSection = (sec: typeof activeSection) => activeSection === 'all' || activeSection === sec;

  return (
    <div className="w-full max-w-7xl mx-auto space-y-5 pb-10 text-slate-100">
      {/* Top Header Banner */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-6 flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
        <div className="space-y-1">
          <div className="flex items-center gap-2.5">
            <div className="w-10 h-10 rounded-xl bg-amber-500/15 border border-amber-500/30 flex items-center justify-center text-amber-400 shrink-0">
              <Shield className="w-5 h-5" />
            </div>
            <div>
              <h1 className="text-lg sm:text-xl font-bold text-white tracking-tight">
                {language === 'th'
                  ? 'ศูนย์ควบคุมผู้ดูแลระบบ (Admin Control Center — ครบทุกฟังก์ชัน)'
                  : 'Admin Control Center (All Contract & System Functions)'}
              </h1>
              <p className="text-xs text-slate-400">
                {language === 'th'
                  ? 'รวมฟังก์ชัน onlyOwner บน Smart Contract, ระบบสั่งแตกรหัส Auto/Extra ID, ระบบโอนจ่าย AllSell 4% และจัดการฐานข้อมูล'
                  : 'All Smart Contract onlyOwner methods, Auto/Extra ID triggers, AllSell 4% payout engine, and Database controls'}
              </p>
            </div>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={fetchAdminOverview}
            disabled={loadingOverview}
            className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-xl bg-slate-800 hover:bg-slate-700 border border-slate-700 text-xs font-medium text-slate-200 transition cursor-pointer whitespace-nowrap"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loadingOverview ? 'animate-spin text-emerald-400' : ''}`} />
            <span>{language === 'th' ? 'รีเฟรชข้อมูล Admin' : 'Refresh Admin Data'}</span>
          </button>

          <button
            type="button"
            onClick={onOpenDeployModal}
            className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-xs font-semibold text-white transition cursor-pointer whitespace-nowrap"
          >
            <Zap className="w-3.5 h-3.5" />
            <span>{language === 'th' ? 'Deploy สัญญาใหม่' : 'Deploy Contract'}</span>
          </button>

          {onClose && (
            <button
              type="button"
              onClick={onClose}
              className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-xl bg-rose-600/20 hover:bg-rose-600/30 border border-rose-500/40 text-xs font-semibold text-rose-300 transition cursor-pointer whitespace-nowrap"
            >
              <X className="w-3.5 h-3.5" />
              <span>{language === 'th' ? 'กลับหน้าผังหลัก' : 'Back to Matrix Tree'}</span>
            </button>
          )}
        </div>
      </div>

      {/* Action Status Feedback Banner */}
      {actionStatus && (
        <div
          className={`rounded-xl p-4 border flex items-start justify-between gap-3 ${
            actionStatus.type === 'success'
              ? 'bg-emerald-950/60 border-emerald-500/40 text-emerald-200'
              : actionStatus.type === 'error'
              ? 'bg-rose-950/60 border-rose-500/40 text-rose-200'
              : 'bg-indigo-950/60 border-indigo-500/40 text-indigo-200'
          }`}
        >
          <div className="flex items-start gap-2.5 text-xs">
            {actionStatus.type === 'success' ? (
              <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0 mt-0.5" />
            ) : actionStatus.type === 'error' ? (
              <AlertCircle className="w-4 h-4 text-rose-400 shrink-0 mt-0.5" />
            ) : (
              <RefreshCw className="w-4 h-4 text-indigo-400 animate-spin shrink-0 mt-0.5" />
            )}
            <div className="space-y-1">
              <div className="font-bold text-sm">{actionStatus.title}</div>
              <div>{actionStatus.message}</div>
              {actionStatus.txHash && (
                <div className="pt-1">
                  <a
                    href={`https://testnet.bscscan.com/tx/${actionStatus.txHash}`}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-1 font-mono text-amber-300 hover:underline"
                  >
                    <span>Tx: {actionStatus.txHash}</span>
                    <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
              )}
            </div>
          </div>
          <button
            type="button"
            onClick={() => setActionStatus(null)}
            className="text-slate-400 hover:text-white p-1"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* System Wallets & On-Chain Balances Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3">
        {[
          {
            label: 'Smart Contract (ตัวสัญญา)',
            role: 'CONTRACT',
            data: overview?.wallets?.contract,
            accent: 'text-amber-400',
          },
          {
            label: 'Contract Owner (onlyOwner)',
            role: 'OWNER',
            data: overview?.wallets?.owner,
            accent: 'text-emerald-400',
          },
          {
            label: 'AllSell Wallet (4%)',
            role: 'ALLSELL 4%',
            data: overview?.wallets?.allSell,
            accent: 'text-cyan-400',
          },
          {
            label: 'Bot Wallet (1%)',
            role: 'BOT 1%',
            data: overview?.wallets?.bot,
            accent: 'text-indigo-400',
          },
          {
            label: 'Family Pool (Unpaid 15 ชั้น)',
            role: 'FAMILY POOL',
            data: overview?.wallets?.familyPool,
            accent: 'text-purple-400',
          },
        ].map((item) => {
          const addr = item.data?.address || '-';
          const shortAddr =
            addr && addr.length > 12 ? `${addr.substring(0, 6)}...${addr.substring(addr.length - 4)}` : addr;
          return (
            <div
              key={item.role}
              className="bg-slate-900 border border-slate-800 rounded-xl p-3.5 flex flex-col justify-between space-y-2.5"
            >
              <div>
                <div className="text-[11px] text-slate-400 flex items-center justify-between">
                  <span>{item.label}</span>
                  <span className="font-mono text-[10px] text-slate-500">{item.role}</span>
                </div>
                <div className="mt-1 flex items-center justify-between gap-1">
                  <span className={`font-mono text-xs font-semibold ${item.accent}`} title={addr}>
                    {shortAddr}
                  </span>
                  {addr && addr !== '-' && (
                    <div className="flex items-center gap-1">
                      <button
                        type="button"
                        onClick={() => handleCopy(addr)}
                        className="p-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 cursor-pointer"
                        title="คัดลอก Address"
                      >
                        {copiedText === addr ? (
                          <Check className="w-3 h-3 text-emerald-400" />
                        ) : (
                          <Copy className="w-3 h-3" />
                        )}
                      </button>
                      <a
                        href={`https://testnet.bscscan.com/address/${addr}`}
                        target="_blank"
                        rel="noreferrer"
                        className="p-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300"
                        title="ดูบน BscScan"
                      >
                        <ExternalLink className="w-3 h-3" />
                      </a>
                    </div>
                  )}
                </div>
              </div>

              <div className="pt-2 border-t border-slate-800/80 flex items-center justify-between text-xs font-mono tabular-nums">
                <div>
                  <span className="text-slate-500 text-[10px] block">USDT Balance</span>
                  <span className="text-white font-bold">{item.data?.usdt || '0.00'} U</span>
                </div>
                <div className="text-right">
                  <span className="text-slate-500 text-[10px] block">tBNB Gas</span>
                  <span className="text-slate-300">{item.data?.bnb || '0.0000'}</span>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Signer Execution Mode & Category Filter Tabs */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-3.5 flex flex-col lg:flex-row lg:items-center lg:justify-between gap-3">
        {/* Category Tabs */}
        <div className="flex items-center gap-1.5 overflow-x-auto pb-1 lg:pb-0">
          {[
            { id: 'all', label: 'แสดงทุกฟังก์ชันทั้งหมด (All)' },
            { id: 'owner', label: '1. คำสั่ง onlyOwner บนสัญญา (7)' },
            { id: 'autopools', label: '2. สั่งแตก Auto / Extra ID (4)' },
            { id: 'allsell', label: '3. จัดการโอนจ่าย AllSell 4%' },
            { id: 'database', label: '4. สัญญา & ซิงค์ Database' },
            { id: 'members', label: '5. รายชื่อรหัสหลัก & ตรวจ 27 แรงค์' },
          ].map((tab) => (
            <button
              key={tab.id}
              type="button"
              onClick={() => setActiveSection(tab.id as any)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium whitespace-nowrap shrink-0 transition cursor-pointer ${
                activeSection === tab.id
                  ? 'bg-emerald-500 text-slate-950 font-bold'
                  : 'bg-slate-800/80 text-slate-300 hover:bg-slate-800 hover:text-white'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Signer Selector */}
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="text-slate-400 whitespace-nowrap">โหมดเซ็นคำสั่งบนเชน:</span>
          <div className="inline-flex rounded-lg bg-slate-950 p-1 border border-slate-800">
            <button
              type="button"
              onClick={() => setExecMode('server_key')}
              className={`px-2.5 py-1 rounded-md text-xs font-medium transition cursor-pointer whitespace-nowrap ${
                execMode === 'server_key'
                  ? 'bg-amber-500 text-slate-950 font-bold'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              Server Admin Key (อัตโนมัติ)
            </button>
            <button
              type="button"
              onClick={() => setExecMode('web3_wallet')}
              className={`px-2.5 py-1 rounded-md text-xs font-medium transition cursor-pointer whitespace-nowrap ${
                execMode === 'web3_wallet'
                  ? 'bg-emerald-500 text-slate-950 font-bold'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              กระเป๋า Web3 ที่เชื่อมต่อ
            </button>
          </div>
          {execMode === 'server_key' && (
            <input
              type="password"
              value={customAdminPk}
              onChange={(e) => setCustomAdminPk(e.target.value)}
              placeholder="เว้นว่างเพื่อใช้ Key หลักของเซิร์ฟเวอร์ หรือใส่ Private Key 0x..."
              className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1 text-xs font-mono text-slate-200 w-56 focus:outline-none focus:border-amber-500"
            />
          )}
        </div>
      </div>

      {/* =====================================================================
          SECTION 1: SMART CONTRACT onlyOwner FUNCTIONS (7 FUNCTIONS)
         ===================================================================== */}
      {showSection('owner') && (
        <div className="space-y-4">
          <div className="flex items-center justify-between border-b border-slate-800 pb-2">
            <h2 className="text-base font-bold text-white flex items-center gap-2">
              <KeyRound className="w-4 h-4 text-amber-400" />
              <span>หมวดที่ 1: ฟังก์ชันผู้ดูแลบน Smart Contract (onlyOwner Functions ทั้งหมด 7 ฟังก์ชัน)</span>
            </h2>
            <span className="text-xs font-mono text-slate-400">
              Owner Signer: {overview?.serverSignerAddress ? `${overview.serverSignerAddress.substring(0, 8)}...` : '-'}
            </span>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* Function 1 & 2: adminUpgradeRankForMainId & adminUpgradeRank */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="flex items-start justify-between gap-2 border-b border-slate-800 pb-3">
                <div>
                  <h3 className="text-sm font-bold text-white flex items-center gap-2">
                    <ArrowUpCircle className="w-4 h-4 text-rose-400" />
                    <span>1 & 2. อัปเกรดแรงค์ฟรี (Admin Free Rank Upgrade)</span>
                  </h3>
                  <p className="text-[11px] font-mono text-slate-400 mt-0.5">
                    adminUpgradeRankForMainId(uint256 _r1MainId, uint8 _targetRank) / adminUpgradeRank(address _user, uint8 _targetRank)
                  </p>
                </div>
                <span className="text-xs font-mono text-rose-400 shrink-0">onlyOwner</span>
              </div>

              <form onSubmit={handleExecuteAdminUpgrade} className="space-y-3">
                <div>
                  <div className="flex items-center justify-between text-xs text-slate-300 mb-1">
                    <span>ระบุเลขรหัสหลัก (เช่น 2, #2, 5) หรือ Wallet Address (0x...)</span>
                    <div className="flex items-center gap-1.5">
                      {['2', '3', '4', '5'].map((idStr) => (
                        <button
                          key={idStr}
                          type="button"
                          onClick={() => setUpgradeTarget(idStr)}
                          className="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300 font-mono text-[11px] cursor-pointer"
                        >
                          #{idStr}
                        </button>
                      ))}
                      {account && (
                        <button
                          type="button"
                          onClick={() => setUpgradeTarget(account)}
                          className="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-emerald-300 text-[11px] cursor-pointer"
                        >
                          กระเป๋าฉัน
                        </button>
                      )}
                    </div>
                  </div>
                  <input
                    type="text"
                    value={upgradeTarget}
                    onChange={(e) => setUpgradeTarget(e.target.value)}
                    placeholder="เช่น 2, #2 หรือ 0x..."
                    className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3.5 py-2 text-xs font-mono text-white focus:outline-none focus:border-emerald-500"
                  />

                  {targetLookup?.found && (
                    <div className="mt-2 p-2.5 rounded-xl bg-emerald-950/40 border border-emerald-500/30 text-xs flex flex-wrap items-center justify-between gap-2">
                      <div className="flex items-center gap-2">
                        <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />
                        <span className="font-bold text-white">รหัสหลัก #{targetLookup.mainId}</span>
                        <span className="font-mono text-slate-400">
                          ({targetLookup.owner?.substring(0, 8)}...{targetLookup.owner?.substring(38)})
                        </span>
                      </div>
                      <div className="flex items-center gap-2 font-mono text-xs">
                        <span className="text-amber-300 font-bold">แรงค์ปัจจุบัน: R{targetLookup.currentRank}</span>
                        <span className="text-slate-500">·</span>
                        <span className="text-indigo-300">ผู้แนะนำ: #{targetLookup.directSponsorNodeId}</span>
                      </div>
                    </div>
                  )}
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs text-slate-400 mb-1">แรงค์เป้าหมาย (Rank 2 ถึง 27)</label>
                    <select
                      value={upgradeRank}
                      onChange={(e) => setUpgradeRank(e.target.value)}
                      className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white focus:outline-none focus:border-emerald-500"
                    >
                      {RANK_PRICES_USDT.map((p, idx) => {
                        const r = idx + 1;
                        if (r < 2) return null;
                        return (
                          <option key={r} value={r}>
                            Rank {r} ({p.toLocaleString()} USDT)
                          </option>
                        );
                      })}
                    </select>
                  </div>

                  <div>
                    <label className="block text-xs text-slate-400 mb-1">รูปแบบการสั่งอัปเกรด</label>
                    <select
                      value={upgradeExecutionMode}
                      onChange={(e) => setUpgradeExecutionMode(e.target.value as any)}
                      className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-emerald-500"
                    >
                      <option value="both">On-Chain + ซิงค์ผัง Database (แนะนำ)</option>
                      <option value="onchain">ยิงขึ้น Smart Contract บนเชนโดยตรง</option>
                      <option value="db_only">อัปเกรดเฉพาะในผัง Database ทันที (Off-Chain)</option>
                    </select>
                  </div>
                </div>

                <button
                  type="submit"
                  disabled={isLoading || !upgradeTarget.trim()}
                  className="w-full py-2.5 px-4 rounded-xl bg-rose-600 hover:bg-rose-500 text-white font-bold text-xs transition cursor-pointer disabled:opacity-50"
                >
                  {isLoading
                    ? 'กำลังดำเนินการอัปเกรด...'
                    : `สั่งอัปเกรด ${upgradeTarget.startsWith('0x') ? upgradeTarget.substring(0, 8) + '...' : '#' + upgradeTarget.replace(/^#/, '')} ไปยัง Rank ${upgradeRank} ฟรี`}
                </button>
              </form>
            </div>

            {/* Function 3: setSystemWallets */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="flex items-start justify-between gap-2 border-b border-slate-800 pb-3">
                <div>
                  <h3 className="text-sm font-bold text-white flex items-center gap-2">
                    <Wallet className="w-4 h-4 text-cyan-400" />
                    <span>3. ตั้งค่ากระเป๋ากองกลางทั้ง 3 ใบ (Set System Wallets)</span>
                  </h3>
                  <p className="text-[11px] font-mono text-slate-400 mt-0.5">
                    setSystemWallets(address _allSell, address _bot, address _familyPool)
                  </p>
                </div>
                <span className="text-xs font-mono text-amber-400 shrink-0">onlyOwner</span>
              </div>

              <div className="space-y-2.5">
                <div>
                  <label className="block text-[11px] text-slate-400 mb-1">
                    กระเป๋า AllSell 4% (_allSell)
                  </label>
                  <input
                    type="text"
                    value={sysAllSell}
                    onChange={(e) => setSysAllSell(e.target.value)}
                    placeholder="0x..."
                    className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white focus:outline-none focus:border-cyan-500"
                  />
                </div>
                <div>
                  <label className="block text-[11px] text-slate-400 mb-1">
                    กระเป๋า Bot 1% (_bot)
                  </label>
                  <input
                    type="text"
                    value={sysBot}
                    onChange={(e) => setSysBot(e.target.value)}
                    placeholder="0x..."
                    className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white focus:outline-none focus:border-cyan-500"
                  />
                </div>
                <div>
                  <label className="block text-[11px] text-slate-400 mb-1">
                    กระเป๋า Family Pool ค่าชั้นตกหล่น (_familyPool)
                  </label>
                  <input
                    type="text"
                    value={sysFamily}
                    onChange={(e) => setSysFamily(e.target.value)}
                    placeholder="0x..."
                    className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white focus:outline-none focus:border-cyan-500"
                  />
                </div>

                <button
                  type="button"
                  onClick={() =>
                    executeContractAdminFunction(
                      'setSystemWallets',
                      { allSell: sysAllSell, bot: sysBot, familyPool: sysFamily },
                      'อัปเดตที่อยู่กระเป๋า AllSell, Bot และ Family Pool บน Smart Contract'
                    )
                  }
                  className="w-full py-2.5 px-4 rounded-xl bg-cyan-600 hover:bg-cyan-500 text-slate-950 font-bold text-xs transition cursor-pointer"
                >
                  บันทึกกระเป๋ากองกลางทั้ง 3 ใบขึ้น Smart Contract (setSystemWallets)
                </button>
              </div>
            </div>

            {/* Function 4 & 5: emergencyWithdrawToken & emergencyWithdrawNative */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="flex items-start justify-between gap-2 border-b border-slate-800 pb-3">
                <div>
                  <h3 className="text-sm font-bold text-white flex items-center gap-2">
                    <Coins className="w-4 h-4 text-amber-400" />
                    <span>4 & 5. ถอนเหรียญฉุกเฉินออกจากสัญญา (Emergency Withdraw)</span>
                  </h3>
                  <p className="text-[11px] font-mono text-slate-400 mt-0.5">
                    emergencyWithdrawToken(address _token, uint256 _amount) & emergencyWithdrawNative()
                  </p>
                </div>
                <span className="text-xs font-mono text-amber-400 shrink-0">onlyOwner</span>
              </div>

              <div className="space-y-3">
                <div className="p-3 rounded-xl bg-slate-950 border border-slate-800 flex items-center justify-between text-xs font-mono">
                  <span className="text-slate-400">ยอดคงเหลือในสัญญาปัจจุบัน:</span>
                  <div className="flex items-center gap-3">
                    <span className="text-emerald-400 font-bold">
                      {overview?.wallets?.contract?.usdt || '0.00'} USDT
                    </span>
                    <span className="text-slate-600">·</span>
                    <span className="text-amber-300 font-bold">
                      {overview?.wallets?.contract?.bnb || '0.0000'} tBNB
                    </span>
                  </div>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
                  <div>
                    <label className="block text-[11px] text-slate-400 mb-1">Token Address (ค่าเริ่มต้น USDT)</label>
                    <input
                      type="text"
                      value={withdrawTokenAddr}
                      onChange={(e) => setWithdrawTokenAddr(e.target.value)}
                      placeholder="0x..."
                      className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white"
                    />
                  </div>
                  <div>
                    <div className="flex items-center justify-between text-[11px] text-slate-400 mb-1">
                      <span>จำนวนเหรียญที่ต้องการถอน</span>
                      <button
                        type="button"
                        onClick={() => setWithdrawTokenAmount(overview?.wallets?.contract?.usdt || '0')}
                        className="text-amber-400 hover:underline font-mono cursor-pointer"
                      >
                        ดึงยอดสูงสุด (MAX)
                      </button>
                    </div>
                    <input
                      type="text"
                      value={withdrawTokenAmount}
                      onChange={(e) => setWithdrawTokenAmount(e.target.value)}
                      placeholder="เช่น 100.00"
                      className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5 pt-1">
                  <button
                    type="button"
                    onClick={() =>
                      executeContractAdminFunction(
                        'emergencyWithdrawToken',
                        { token: withdrawTokenAddr, amount: withdrawTokenAmount },
                        `ถอนเหรียญ Token จำนวน ${withdrawTokenAmount} ออกจากสัญญาเข้ากระเป๋า Owner`
                      )
                    }
                    disabled={!withdrawTokenAmount || Number(withdrawTokenAmount) <= 0}
                    className="py-2.5 px-4 rounded-xl bg-amber-500 hover:bg-amber-400 text-slate-950 font-bold text-xs transition cursor-pointer disabled:opacity-50"
                  >
                    ถอนเหรียญ ERC20 / USDT (emergencyWithdrawToken)
                  </button>

                  <button
                    type="button"
                    onClick={() =>
                      executeContractAdminFunction(
                        'emergencyWithdrawNative',
                        {},
                        'ถอนเหรียญ Native BNB ทั้งหมดออกจากสัญญาเข้ากระเป๋า Owner'
                      )
                    }
                    className="py-2.5 px-4 rounded-xl bg-slate-800 hover:bg-slate-700 border border-amber-500/40 text-amber-300 font-bold text-xs transition cursor-pointer"
                  >
                    ถอน BNB ทั้งหมด (emergencyWithdrawNative)
                  </button>
                </div>
              </div>
            </div>

            {/* Function 6 & 7: transferOwnership & renounceOwnership */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="flex items-start justify-between gap-2 border-b border-slate-800 pb-3">
                <div>
                  <h3 className="text-sm font-bold text-white flex items-center gap-2">
                    <Lock className="w-4 h-4 text-purple-400" />
                    <span>6 & 7. จัดการสิทธิ์ความเป็นเจ้าของสัญญา (Ownable Controls)</span>
                  </h3>
                  <p className="text-[11px] font-mono text-slate-400 mt-0.5">
                    transferOwnership(address newOwner) & renounceOwnership()
                  </p>
                </div>
                <span className="text-xs font-mono text-purple-400 shrink-0">onlyOwner</span>
              </div>

              <div className="space-y-3">
                <div>
                  <label className="block text-[11px] text-slate-400 mb-1">
                    โอนสิทธิ์ Owner ไปยังกระเป๋าใหม่ (newOwner Address)
                  </label>
                  <div className="flex gap-2">
                    <input
                      type="text"
                      value={newOwnerAddr}
                      onChange={(e) => setNewOwnerAddr(e.target.value)}
                      placeholder="0x..."
                      className="flex-1 bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white"
                    />
                    <button
                      type="button"
                      onClick={() =>
                        executeContractAdminFunction(
                          'transferOwnership',
                          { newOwner: newOwnerAddr },
                          `โอนสิทธิ์ Owner ของสัญญาให้กับ ${newOwnerAddr}`
                        )
                      }
                      disabled={!ethers.isAddress(newOwnerAddr)}
                      className="px-4 py-2 rounded-xl bg-purple-600 hover:bg-purple-500 text-white font-bold text-xs transition cursor-pointer disabled:opacity-50 whitespace-nowrap"
                    >
                      โอนสิทธิ์ (transferOwnership)
                    </button>
                  </div>
                </div>

                <div className="pt-2 border-t border-slate-800/80">
                  <label className="block text-[11px] text-rose-400 mb-1">
                    สละสิทธิ์การเป็นเจ้าของสัญญาถาวร (renounceOwnership) — พิมพ์คำว่า RENOUNCE เพื่อปลดล็อกปุ่ม
                  </label>
                  <div className="flex gap-2">
                    <input
                      type="text"
                      value={renounceConfirmText}
                      onChange={(e) => setRenounceConfirmText(e.target.value)}
                      placeholder="พิมพ์ RENOUNCE เพื่อยืนยัน"
                      className="flex-1 bg-slate-950 border border-rose-500/30 rounded-xl px-3 py-2 text-xs font-mono text-rose-200"
                    />
                    <button
                      type="button"
                      onClick={() =>
                        executeContractAdminFunction(
                          'renounceOwnership',
                          {},
                          'สละสิทธิ์ Owner ของสัญญาถาวร (renounceOwnership)'
                        )
                      }
                      disabled={renounceConfirmText.trim() !== 'RENOUNCE'}
                      className="px-4 py-2 rounded-xl bg-rose-900/60 hover:bg-rose-700 border border-rose-500/40 text-rose-200 font-bold text-xs transition cursor-pointer disabled:opacity-40 whitespace-nowrap"
                    >
                      สละสิทธิ์ (renounceOwnership)
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* =====================================================================
          SECTION 2: AUTO NEW MAIN ID & AUTO EXTRA ID TRIGGERS (4 FUNCTIONS)
         ===================================================================== */}
      {showSection('autopools') && (
        <div className="space-y-4">
          <div className="flex items-center justify-between border-b border-slate-800 pb-2">
            <h2 className="text-base font-bold text-white flex items-center gap-2">
              <Sparkles className="w-4 h-4 text-emerald-400" />
              <span>หมวดที่ 2: คำสั่งบังคับตรวจกระปุกและแตกโหนด Auto ID / Extra ID บนเชน (4 ฟังก์ชัน)</span>
            </h2>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* Function 8 & 9: checkAndProcessAutoNewMainIdForNode & checkAndProcessAutoNewMainId */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="border-b border-slate-800 pb-3">
                <h3 className="text-sm font-bold text-amber-300">
                  8 & 9. สั่งประมวลผลกระปุก Pool 2 เพื่อแตก Auto ID (5 USDT) ในผัง Rank 1
                </h3>
                <p className="text-[11px] font-mono text-slate-400 mt-0.5">
                  checkAndProcessAutoNewMainIdForNode(uint256 _mainNodeId) & checkAndProcessAutoNewMainId(address _user)
                </p>
              </div>

              <div className="space-y-3">
                <div className="flex gap-2 items-end">
                  <div className="flex-1">
                    <label className="block text-[11px] text-slate-400 mb-1">
                      ระบุเลขรหัสหลัก Rank 1 (_mainNodeId)
                    </label>
                    <input
                      type="number"
                      min={1}
                      value={autoMainNodeId}
                      onChange={(e) => setAutoMainNodeId(e.target.value)}
                      className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white"
                    />
                  </div>
                  <button
                    type="button"
                    onClick={() =>
                      executeContractAdminFunction(
                        'checkAndProcessAutoNewMainIdForNode',
                        { mainNodeId: Number(autoMainNodeId) },
                        `สั่งตรวจและแตกรหัส Auto ID จากกระปุก Pool 2 ของรหัสหลัก #${autoMainNodeId}`
                      )
                    }
                    className="px-4 py-2 rounded-xl bg-amber-500 hover:bg-amber-400 text-slate-950 font-bold text-xs transition cursor-pointer whitespace-nowrap"
                  >
                    รันตาม #ID (ForNode)
                  </button>
                </div>

                <div className="flex gap-2 items-end pt-2 border-t border-slate-800/80">
                  <div className="flex-1">
                    <label className="block text-[11px] text-slate-400 mb-1">
                      หรือระบุ Wallet Address (_user)
                    </label>
                    <input
                      type="text"
                      value={autoMainUserAddr}
                      onChange={(e) => setAutoMainUserAddr(e.target.value)}
                      placeholder="0x..."
                      className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white"
                    />
                  </div>
                  <button
                    type="button"
                    onClick={() =>
                      executeContractAdminFunction(
                        'checkAndProcessAutoNewMainId',
                        { user: autoMainUserAddr || account },
                        `สั่งตรวจและแตกรหัส Auto ID ให้กระเป๋า ${autoMainUserAddr || account}`
                      )
                    }
                    className="px-4 py-2 rounded-xl bg-slate-800 hover:bg-slate-700 border border-amber-500/40 text-amber-300 font-bold text-xs transition cursor-pointer whitespace-nowrap"
                  >
                    รันตาม Wallet
                  </button>
                </div>
              </div>
            </div>

            {/* Function 10 & 11: checkAndProcessAutoExtraIdForNode & checkAndProcessAutoExtraId */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
              <div className="border-b border-slate-800 pb-3">
                <h3 className="text-sm font-bold text-rose-300">
                  10 & 11. สั่งประมวลผลกระปุก Pool 3 เพื่อแตก Extra ID (Rank 2–27)
                </h3>
                <p className="text-[11px] font-mono text-slate-400 mt-0.5">
                  checkAndProcessAutoExtraIdForNode(address _user, uint8 _currentRank, uint256 _sponsorNodeId)
                </p>
              </div>

              <div className="space-y-3">
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
                  <div>
                    <label className="block text-[11px] text-slate-400 mb-1">Wallet (_user)</label>
                    <input
                      type="text"
                      value={autoExtraUserAddr}
                      onChange={(e) => setAutoExtraUserAddr(e.target.value)}
                      placeholder={account || '0x...'}
                      className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white"
                    />
                  </div>
                  <div>
                    <label className="block text-[11px] text-slate-400 mb-1">Rank (_currentRank)</label>
                    <input
                      type="number"
                      min={2}
                      max={27}
                      value={autoExtraRank}
                      onChange={(e) => setAutoExtraRank(e.target.value)}
                      className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white"
                    />
                  </div>
                  <div>
                    <label className="block text-[11px] text-slate-400 mb-1">Main ID (_sponsorNodeId)</label>
                    <input
                      type="number"
                      min={1}
                      value={autoExtraSponsorId}
                      onChange={(e) => setAutoExtraSponsorId(e.target.value)}
                      className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
                  <button
                    type="button"
                    onClick={() =>
                      executeContractAdminFunction(
                        'checkAndProcessAutoExtraIdForNode',
                        {
                          user: autoExtraUserAddr || overview?.wallets?.owner?.address || account,
                          currentRank: Number(autoExtraRank),
                          sponsorNodeId: Number(autoExtraSponsorId),
                        },
                        `สั่งแตก Extra ID บนเชน (Rank ${autoExtraRank}, Main ID #${autoExtraSponsorId})`
                      )
                    }
                    className="py-2.5 px-3 rounded-xl bg-rose-600 hover:bg-rose-500 text-white font-bold text-xs transition cursor-pointer"
                  >
                    สั่งแตก Extra ID บนเชน (ForNode)
                  </button>

                  <button
                    type="button"
                    onClick={async () => {
                      setActionStatus({
                        type: 'loading',
                        title: 'กำลังคำนวณและซิงค์ Extra ID ในผัง Database...',
                        message: `ตรวจสอบกระปุก Extra Pool ของรหัสหลัก #${autoExtraSponsorId}`,
                      });
                      try {
                        const res = await fetch('/api/matrix/auto-extra-id', {
                          method: 'POST',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({
                            mainId: Number(autoExtraSponsorId),
                            currentRank: Number(autoExtraRank),
                          }),
                        });
                        const data = await res.json();
                        await fetchAdminOverview();
                        if (typeof window !== 'undefined') {
                          window.dispatchEvent(new CustomEvent('matrix:tree:refresh'));
                        }
                        setActionStatus({
                          type: 'success',
                          title: 'ประมวลผล Extra ID สำเร็จ!',
                          message: data.message || 'อัปเดตโหนด Extra ID เรียบร้อยแล้ว',
                        });
                      } catch (err: any) {
                        setActionStatus({
                          type: 'error',
                          title: 'ประมวลผล Extra ID ล้มเหลว',
                          message: err.message,
                        });
                      }
                    }}
                    className="py-2.5 px-3 rounded-xl bg-slate-800 hover:bg-slate-700 border border-rose-500/40 text-rose-300 font-bold text-xs transition cursor-pointer"
                  >
                    สั่งคำนวณ Extra ID ในระบบ Database
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* =====================================================================
          SECTION 3: ALLSELL 4% AUTO-PAYOUT ENGINE
         ===================================================================== */}
      {showSection('allsell') && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 border-b border-slate-800 pb-3">
            <div>
              <h2 className="text-base font-bold text-white flex items-center gap-2">
                <Send className="w-4 h-4 text-cyan-400" />
                <span>หมวดที่ 3: จัดการกองทุน AllSell 4% & สั่งโอนเหรียญ USDT จริงบนเชน</span>
              </h2>
              <p className="text-xs text-slate-400 mt-0.5">
                ตัดรอบทุกวันที่ 1 (ต้นเดือน) และวันที่ 15 (กลางเดือน) โอนตรงจากกระเป๋า AllSell ({overview?.wallets?.allSell?.address || '-'})
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={async () => {
                  const res = await fetch('/api/matrix/allsell-auto-payout/toggle', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ enabled: !overview?.allSell?.autoEnabled }),
                  });
                  const data = await res.json();
                  await fetchAdminOverview();
                  setActionStatus({
                    type: 'success',
                    title: 'อัปเดตสถานะ Auto AllSell',
                    message: data.message,
                  });
                }}
                className={`px-3.5 py-2 rounded-xl text-xs font-bold transition cursor-pointer ${
                  overview?.allSell?.autoEnabled
                    ? 'bg-emerald-500/20 border border-emerald-500/40 text-emerald-300'
                    : 'bg-slate-800 border border-slate-700 text-slate-400'
                }`}
              >
                {overview?.allSell?.autoEnabled
                  ? 'เปิดตัดรอบอัตโนมัติอยู่ (คลิกเพื่อปิด)'
                  : 'ปิดตัดรอบอัตโนมัติอยู่ (คลิกเพื่อเปิด)'}
              </button>

              <button
                type="button"
                onClick={async () => {
                  setActionStatus({
                    type: 'loading',
                    title: 'กำลังสั่งตัดรอบและโอนจ่าย AllSell 4%...',
                    message: 'กำลังตรวจสอบผู้ผ่านเกณฑ์และโอน USDT บนบล็อกเชน',
                  });
                  try {
                    const res = await fetch('/api/matrix/allsell-auto-payout/execute-now', {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({
                        privateKey: customAdminPk.trim() || undefined,
                      }),
                    });
                    const data = await res.json();
                    if (!res.ok || !data.success) throw new Error(data.error || 'ตัดรอบไม่สำเร็จ');
                    await fetchAdminOverview();
                    setActionStatus({
                      type: 'success',
                      title: 'ตัดรอบ AllSell 4% สำเร็จ!',
                      message: data.message,
                    });
                  } catch (err: any) {
                    setActionStatus({
                      type: 'error',
                      title: 'ตัดรอบ AllSell ล้มเหลว',
                      message: err.message,
                    });
                  }
                }}
                className="px-4 py-2 rounded-xl bg-cyan-500 hover:bg-cyan-400 text-slate-950 font-bold text-xs transition cursor-pointer flex items-center gap-1.5"
              >
                <Play className="w-3.5 h-3.5" />
                <span>สั่งตัดรอบและโอนจ่าย AllSell ทันที (Execute Now)</span>
              </button>
            </div>
          </div>

          {overview?.allSell?.history && overview.allSell.history.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-left border-collapse text-xs">
                <thead>
                  <tr className="border-b border-slate-800 text-slate-400">
                    <th className="py-2 px-3">รอบการจ่าย</th>
                    <th className="py-2 px-3">เวลาที่รัน</th>
                    <th className="py-2 px-3 text-right">กองทุนรวม</th>
                    <th className="py-2 px-3 text-right">จ่ายออกจริง</th>
                    <th className="py-2 px-3 text-right">ยกยอดสะสม</th>
                    <th className="py-2 px-3">หมายเหตุ</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 font-mono tabular-nums">
                  {overview.allSell.history.slice(0, 5).map((rec: any) => (
                    <tr key={rec.id} className="hover:bg-slate-800/40">
                      <td className="py-2 px-3 font-sans font-semibold text-cyan-300">{rec.cycleLabelTh}</td>
                      <td className="py-2 px-3 text-slate-400">
                        {new Date(rec.executedAt).toLocaleString('th-TH')}
                      </td>
                      <td className="py-2 px-3 text-right text-white">
                        {Number(rec.totalAllSellPoolUsdt || 0).toFixed(2)} U
                      </td>
                      <td className="py-2 px-3 text-right text-emerald-400 font-bold">
                        {Number(rec.totalDistributedUsdt || 0).toFixed(2)} U ({rec.qualifiedRecipientsCount} รายการ)
                      </td>
                      <td className="py-2 px-3 text-right text-amber-300">
                        {Number(rec.carriedOverUsdt || 0).toFixed(2)} U
                      </td>
                      <td className="py-2 px-3 font-sans text-slate-300">{rec.noteTh}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-xs text-slate-400 py-3">
              ยังไม่มีประวัติการตัดรอบโอนจ่าย AllSell ในสัญญานี้ (กดปุ่ม "สั่งตัดรอบและโอนจ่าย AllSell ทันที" ด้านบนเพื่อทดสอบได้เลย)
            </div>
          )}
        </div>
      )}

      {/* =====================================================================
          SECTION 4: CONTRACT ADDRESS, DATABASE CACHE & BATCH QUEUE CONTROLS
         ===================================================================== */}
      {showSection('database') && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {/* Smart Contract & DB Sync Manager */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
            <div className="border-b border-slate-800 pb-3 flex items-center justify-between">
              <h3 className="text-sm font-bold text-white flex items-center gap-2">
                <Database className="w-4 h-4 text-indigo-400" />
                <span>หมวดที่ 4.1: จัดการที่อยู่ Smart Contract & ล้างแคชฐานข้อมูล</span>
              </h3>
              <span className="text-xs font-mono text-emerald-400">
                รวม {overview?.stats?.totalNodesAllRanks ?? 0} โหนด (27 แรงค์)
              </span>
            </div>

            <div className="space-y-3">
              <div>
                <label className="block text-[11px] text-slate-400 mb-1">
                  Smart Contract Address ปัจจุบัน
                </label>
                <input
                  type="text"
                  value={editContractAddr}
                  onChange={(e) => setEditContractAddr(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-amber-300"
                />
              </div>

              <div>
                <label className="block text-[11px] text-slate-400 mb-1">
                  USDT Token Address
                </label>
                <input
                  type="text"
                  value={editUsdtAddr}
                  onChange={(e) => setEditUsdtAddr(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-slate-200"
                />
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                <button
                  type="button"
                  onClick={async () => {
                    const ok = await onUpdateContractAddress(editContractAddr.trim(), editUsdtAddr.trim());
                    if (ok) await fetchAdminOverview();
                  }}
                  className="py-2.5 px-3 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-white font-bold text-xs flex items-center justify-center gap-1.5 cursor-pointer"
                >
                  <Save className="w-3.5 h-3.5" />
                  <span>บันทึกเลขสัญญาลง Database</span>
                </button>

                <button
                  type="button"
                  onClick={async () => {
                    const ok = await onResetContractAddress();
                    if (ok) await fetchAdminOverview();
                  }}
                  className="py-2.5 px-3 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-200 font-semibold text-xs flex items-center justify-center gap-1.5 cursor-pointer"
                >
                  <RotateCcw className="w-3.5 h-3.5" />
                  <span>คืนค่าสัญญาเริ่มต้น</span>
                </button>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 pt-2 border-t border-slate-800/80">
                <button
                  type="button"
                  onClick={async () => {
                    setActionStatus({
                      type: 'loading',
                      title: 'กำลังซิงค์ข้อมูลจาก Smart Contract...',
                      message: 'ดึงโหนดและกระปุกสะสมทั้ง 27 แรงค์จากเชน',
                    });
                    await fetch('/api/db/sync', { method: 'POST' });
                    await fetchAdminOverview();
                    onRefreshOnChainData();
                    if (typeof window !== 'undefined') {
                      window.dispatchEvent(new CustomEvent('matrix:tree:refresh'));
                    }
                    setActionStatus({
                      type: 'success',
                      title: 'ซิงค์ข้อมูลสำเร็จ!',
                      message: 'อัปเดตฐานข้อมูลตรงกับบนเชนทั้ง 27 แรงค์เรียบร้อยแล้ว',
                    });
                  }}
                  className="py-2.5 px-3 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-slate-950 font-bold text-xs flex items-center justify-center gap-1.5 cursor-pointer"
                >
                  <RefreshCw className="w-3.5 h-3.5" />
                  <span>บังคับซิงค์ข้อมูลจากเชน (Force Sync)</span>
                </button>

                <button
                  type="button"
                  onClick={async () => {
                    setActionStatus({
                      type: 'loading',
                      title: 'กำลังล้างแคชและดึงข้อมูลใหม่จากเชน 100%...',
                      message: 'กำลังล้าง matrixDB และโหลดข้อมูลใหม่จาก Smart Contract',
                    });
                    const res = await fetch('/api/admin/clear-cache', { method: 'POST' });
                    const data = await res.json();
                    await fetchAdminOverview();
                    onRefreshOnChainData();
                    if (typeof window !== 'undefined') {
                      window.dispatchEvent(new CustomEvent('matrix:tree:refresh'));
                    }
                    setActionStatus({
                      type: 'success',
                      title: 'ล้างแคชฐานข้อมูลสำเร็จ!',
                      message: data.message || 'ดึงข้อมูลใหม่จากเชนเรียบร้อยแล้ว',
                    });
                  }}
                  className="py-2.5 px-3 rounded-xl bg-rose-600/20 hover:bg-rose-600/30 border border-rose-500/40 text-rose-300 font-bold text-xs flex items-center justify-center gap-1.5 cursor-pointer"
                >
                  <Trash2 className="w-3.5 h-3.5" />
                  <span>ล้างแคชเก่า & ดึงจากเชนใหม่ทั้งหมด</span>
                </button>
              </div>
            </div>
          </div>

          {/* Batch Queue & Rank Inspector */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
            <div className="border-b border-slate-800 pb-3 flex items-center justify-between">
              <h3 className="text-sm font-bold text-white flex items-center gap-2">
                <Layers className="w-4 h-4 text-emerald-400" />
                <span>หมวดที่ 4.2: จัดการคิว Batch Queue & ตรวจสอบรายแรงค์</span>
              </h3>
              <span className="text-xs font-mono text-slate-400">
                คิวรอประมวลผล: {batchStatus?.queueLength ?? 0} รายการ
              </span>
            </div>

            <div className="grid grid-cols-3 gap-2">
              <button
                type="button"
                onClick={async () => {
                  await fetch('/api/batch-queue/toggle', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ enabled: !batchStatus?.autoBatchEnabled }),
                  });
                  await fetchAdminOverview();
                }}
                className="py-2 px-3 rounded-xl bg-slate-800 hover:bg-slate-700 text-xs font-semibold text-slate-200 cursor-pointer"
              >
                {batchStatus?.autoBatchEnabled ? 'ปิด Auto-Batch' : 'เปิด Auto-Batch'}
              </button>

              <button
                type="button"
                onClick={async () => {
                  await fetch('/api/batch-queue/flush', { method: 'POST' });
                  await fetchAdminOverview();
                  setActionStatus({
                    type: 'success',
                    title: 'สั่งรันคิว Batch สำเร็จ',
                    message: 'ประมวลผลรายการในคิวทั้งหมดเรียบร้อยแล้ว',
                  });
                }}
                className="py-2 px-3 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-slate-950 text-xs font-bold cursor-pointer"
              >
                รันคิวทันที (Flush)
              </button>

              <button
                type="button"
                onClick={async () => {
                  await fetch('/api/batch-queue/clear', { method: 'POST' });
                  await fetchAdminOverview();
                }}
                className="py-2 px-3 rounded-xl bg-rose-950/60 hover:bg-rose-900/60 border border-rose-500/30 text-rose-300 text-xs font-semibold cursor-pointer"
              >
                ล้างคิว (Clear)
              </button>
            </div>

            {/* Quick Rank 1-27 Inspector */}
            <div className="pt-3 border-t border-slate-800/80 space-y-2.5">
              <div className="flex items-center justify-between">
                <span className="text-xs font-semibold text-slate-300">
                  ตรวจสอบข้อมูลรายแรงค์ (Rank 1–27 Inspector)
                </span>
                <select
                  value={inspectRankNum}
                  onChange={(e) => setInspectRankNum(Number(e.target.value))}
                  className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1 text-xs font-mono text-emerald-400"
                >
                  {RANK_PRICES_USDT.map((price, i) => (
                    <option key={i + 1} value={i + 1}>
                      Rank {i + 1} ({price} U) — {overview?.stats?.rankCounts?.[i + 1] ?? 0} โหนด
                    </option>
                  ))}
                </select>
              </div>

              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 bg-slate-950 border border-slate-800 rounded-xl p-3 font-mono text-xs tabular-nums">
                <div>
                  <span className="text-[10px] text-slate-500 block">จำนวนโหนดในผัง</span>
                  <span className="text-white font-bold">
                    {overview?.stats?.rankCounts?.[inspectRankNum] ?? 0} โหนด
                  </span>
                </div>
                <div>
                  <span className="text-[10px] text-slate-500 block">First Node ID</span>
                  <span className="text-emerald-400 font-bold">#{inspectStats.firstNodeId}</span>
                </div>
                <div>
                  <span className="text-[10px] text-slate-500 block">Latest Node ID</span>
                  <span className="text-cyan-400 font-bold">#{inspectStats.latestNodeId}</span>
                </div>
                <div>
                  <span className="text-[10px] text-slate-500 block">Next Rank Price</span>
                  <span className="text-amber-300 font-bold">{inspectStats.nextRankPrice}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* =====================================================================
          SECTION 5: ALL MAIN IDS TABLE & 1-CLICK ADMIN UPGRADE
         ===================================================================== */}
      {showSection('members') && (
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 border-b border-slate-800 pb-3">
            <div>
              <h2 className="text-base font-bold text-white flex items-center gap-2">
                <UserCheck className="w-4 h-4 text-emerald-400" />
                <span>
                  หมวดที่ 5: ตารางรหัสหลักทั้งหมดในระบบ ({overview?.stats?.totalMainIdsRank1 ?? 0} รหัสหลัก)
                </span>
              </h2>
              <p className="text-xs text-slate-400 mt-0.5">
                ดูสถานะแรงค์ปัจจุบัน, กระปุกสะสมทั้ง 3 Pool และกดเลือกเพื่อสั่ง Admin Upgrade ได้ในคลิกเดียว
              </p>
            </div>

            <div className="relative w-full sm:w-72">
              <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
              <input
                type="text"
                value={memberSearch}
                onChange={(e) => setMemberSearch(e.target.value)}
                placeholder="ค้นหาเลข #ID หรือ Wallet 0x..."
                className="w-full bg-slate-950 border border-slate-800 rounded-xl pl-8 pr-3 py-1.5 text-xs font-mono text-white focus:outline-none focus:border-emerald-500"
              />
            </div>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse text-xs">
              <thead>
                <tr className="border-b border-slate-800 text-slate-400">
                  <th className="py-2.5 px-3">รหัสหลัก</th>
                  <th className="py-2.5 px-3">ประเภท</th>
                  <th className="py-2.5 px-3">เจ้าของ (Wallet)</th>
                  <th className="py-2.5 px-3">ผู้แนะนำตรง</th>
                  <th className="py-2.5 px-3">แรงค์สูงสุด</th>
                  <th className="py-2.5 px-3 text-right">Upgrade Pool</th>
                  <th className="py-2.5 px-3 text-right">Auto ID Pool</th>
                  <th className="py-2.5 px-3 text-right">Extra ID Pool</th>
                  <th className="py-2.5 px-3 text-right">จัดการด่วน</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-mono tabular-nums">
                {filteredMainIds.map((m: any) => {
                  const nextR = Math.min(27, (m.currentRank || 1) + 1);
                  return (
                    <tr key={m.mainId} className="hover:bg-slate-800/40">
                      <td className="py-2.5 px-3 font-bold text-white">#{m.mainId}</td>
                      <td className="py-2.5 px-3 font-sans">
                        {m.originType === 'AUTO_MAIN' ? (
                          <span className="text-amber-400 font-semibold">Auto ID</span>
                        ) : (
                          <span className="text-emerald-400 font-semibold">Main ID</span>
                        )}
                      </td>
                      <td className="py-2.5 px-3 text-slate-300">
                        <div className="flex items-center gap-1.5">
                          <span>
                            {m.owner?.substring(0, 6)}...{m.owner?.substring(38)}
                          </span>
                          <button
                            type="button"
                            onClick={() => handleCopy(m.owner)}
                            className="text-slate-500 hover:text-white cursor-pointer"
                          >
                            <Copy className="w-3 h-3" />
                          </button>
                        </div>
                      </td>
                      <td className="py-2.5 px-3 text-indigo-300">#{m.directSponsorNodeId}</td>
                      <td className="py-2.5 px-3">
                        <span className="text-amber-300 font-bold">Rank {m.currentRank}</span>
                        <span className="text-slate-500 text-[10px] ml-1">
                          (กรอบ R{m.window5Start}–R{m.window5End})
                        </span>
                      </td>
                      <td className="py-2.5 px-3 text-right text-emerald-400 font-semibold">
                        {Number(m.upgradeRankPool || 0).toFixed(2)} U
                      </td>
                      <td className="py-2.5 px-3 text-right text-amber-300">
                        {Number(m.newAutoIdPool || 0).toFixed(2)} U
                      </td>
                      <td className="py-2.5 px-3 text-right text-rose-300">
                        {Number(m.newExtraIdPool || 0).toFixed(2)} U
                      </td>
                      <td className="py-2.5 px-3 text-right font-sans">
                        <div className="inline-flex items-center gap-1.5">
                          <button
                            type="button"
                            onClick={() => {
                              setUpgradeTarget(String(m.mainId));
                              setUpgradeRank(String(nextR));
                              setActiveSection('owner');
                              window.scrollTo({ top: 0, behavior: 'smooth' });
                            }}
                            className="px-2.5 py-1 rounded-lg bg-rose-600/20 hover:bg-rose-600/40 border border-rose-500/30 text-rose-300 font-semibold text-[11px] cursor-pointer whitespace-nowrap"
                          >
                            ตั้งค่าอัปแรงค์ #{m.mainId}
                          </button>
                          {m.currentRank < 27 && (
                            <button
                              type="button"
                              onClick={() => onAdminUpgrade(String(m.mainId), nextR)}
                              disabled={isLoading}
                              className="px-2.5 py-1 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-slate-950 font-bold text-[11px] cursor-pointer whitespace-nowrap disabled:opacity-50"
                            >
                              + ขึ้น R{nextR} ฟรีทันที
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  );
                })}
                {filteredMainIds.length === 0 && (
                  <tr>
                    <td colSpan={9} className="py-6 text-center text-slate-500 font-sans">
                      ไม่พบรหัสหลักที่ค้นหา
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};
