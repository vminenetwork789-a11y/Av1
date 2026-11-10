/**
 * FractalFlowMatrix27 — Real-Time WebSocket Clone & Auto/Extra ID Bot (Ethers.js v6)
 *
 * คุณสมบัติหลัก (Core Features):
 * 1. Hybrid Provider Architecture:
 *    - wsProvider (ethers.WebSocketProvider): ดักฟัง Event แบบเรียลไทม์ผ่านโปรโตคอล wss:// (เสี้ยววินาที)
 *      รองรับทั้ง Event CloneQueued, NodeCreated (ขาขวา), RewardDistributed, RankUpgraded
 *    - httpProvider (ethers.JsonRpcProvider): ใช้ส่งธุรกรรม (processCloneQueue / processPendingExtraIdQueue /
 *      checkAndProcessAutoNewMainIdForNode / checkAndProcessAutoExtraIdForNode) ผ่าน https://
 *      เพื่อให้การยิงธุรกรรมเสถียรสูงสุด ไม่ล่มกลางคันแม้สาย WebSocket กำลังสลับสาย
 * 2. Exponential Backoff + Random Jitter (scheduleReconnect):
 *    - คำนวณเวลารอต่อสายใหม่ตามสมการ: t = min(MAX_DELAY, INITIAL_DELAY * 2^attempt) + jitter(0..1000ms)
 *    - รีเซ็ตตัวนับ reconnectAttempts = 0 ทันทีเมื่อเชื่อมต่อสำเร็จ พร้อมวิ่งเช็คคิวตกค้างทันที
 * 3. Heartbeat / Ping-Pong Liveness Check (startHeartbeat):
 *    - ดึงเลขบล็อก (wsProvider.getBlockNumber()) ทุกๆ 30 วินาที เพื่อเลี้ยงสายไม่ให้โหนด RPC ตัด Idle Timeout
 *      และตรวจจับกรณีสายค้างเงียบ (Silent Drop)
 * 4. Memory-Leak Safe Cleanup (cleanupWebSocket):
 *    - ล้าง Event Listener ทั้งหมด (removeAllListeners) และสั่ง wsProvider.destroy() ทุกครั้งก่อนสร้างการเชื่อมต่อใหม่
 */

import { ethers } from 'ethers';
import dotenv from 'dotenv';

dotenv.config();

// ============================================================================
// 1. การตั้งค่าพารามิเตอร์จาก .env (Configuration)
// ============================================================================
const CONTRACT_ADDRESS =
  process.env.OVERRIDE_CONTRACT_ADDRESS ||
  process.env.CONTRACT_ADDRESS ||
  process.env.VITE_CONTRACT_ADDRESS ||
  '0x98c2B26E052C63E3F520c75428cEA3CB5E0649e0';

const WSS_ENDPOINTS = [
  process.env.WSS_URL || 'wss://bsc-testnet-rpc.publicnode.com',
  'wss://bsc-testnet.publicnode.com',
].filter(Boolean);

const HTTP_RPC_ENDPOINTS = [
  process.env.HTTP_RPC_URL || 'https://data-seed-prebsc-1-s1.bnbchain.org:8545',
  process.env.RPC_URL || 'https://bsc-testnet-rpc.publicnode.com',
  'https://data-seed-prebsc-1-s2.binance.org:8545',
  'https://bsc-testnet.publicnode.com',
].filter(Boolean);

const BOT_PRIVATE_KEY = process.env.BOT_PRIVATE_KEY || '';

const INITIAL_DELAY_MS = Number(process.env.INITIAL_RECONNECT_DELAY_MS || 2000);
const MAX_DELAY_MS = Number(process.env.MAX_RECONNECT_DELAY_MS || 60000);
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 30000);
const HEARTBEAT_TIMEOUT_MS = 10000;
const BATCH_SIZE = BigInt(process.env.BATCH_SIZE || 3);
const MAX_CLONE_BATCH_SIZE = Number(process.env.BATCH_SIZE || 3);

// ============================================================================
// 2. ABI สำหรับการดักฟัง Event และการสั่งประมวลผลคิวโคลน / Auto ID / Extra ID
// ============================================================================
const BOT_CONTRACT_ABI = [
  // Events สำหรับดักฟังแบบเรียลไทม์ผ่าน WebSocket (wss://)
  'event CloneQueued(uint8 indexed rank, uint256 indexed parentId, uint256 pendingCount)',
  'event NodeCreated(uint8 indexed rank, uint256 indexed nodeId, address indexed owner, address directSponsor, uint256 parentId, bool isLeft, bool isClone, bool isExtraId)',
  'event RewardDistributed(uint256 indexed fromNodeId, uint256 indexed toNodeId, address indexed recipient, uint8 rank, string rewardType, uint256 amount)',
  'event RankUpgraded(address indexed user, uint8 fromRank, uint8 toRank, uint256 newNodeId, bool isAutoUpgrade)',
  'event AutoNewMainIdCreated(address indexed user, uint256 count, uint256 vaultDiff)',
  'event AutoExtraIdCreated(address indexed user, uint8 indexed rank, uint256 nodeId)',

  // View Functions สำหรับตรวจสอบคิวตกค้าง
  'function getPendingCloneCount() external view returns (uint256)',
  'function totalNodesInRank(uint8 _rank) external view returns (uint256)',
  'function rankNodes(uint8 _rank, uint256 _nodeId) external view returns (uint256 id, address owner, address directSponsor, uint256 parentId, uint256 leftChild, uint256 rightChild, bool isClone, bool isExtraId)',
  'function getMainIdThreePools(address _user, uint256 _mainNodeIdRank1) external view returns (uint8 currentRank, uint8 window5Start, uint8 window5End, uint256 upgradeRankPool, uint256 newAutoIdPool, uint256 newExtraIdPool)',
  'function rankPrice(uint8 _rank) external view returns (uint256)',

  // State-Changing Functions สำหรับส่งธุรกรรมผ่าน HTTP Provider (https://)
  'function processCloneQueue(uint256 _maxIterations) external',
  'function processPendingExtraIdQueue(uint256 _r1MainId, uint8 _maxBatch) external returns (uint8 processed)',
  'function checkAndProcessAutoNewMainIdForNode(uint256 _mainNodeId) external',
  'function checkAndProcessAutoExtraIdForNode(address _user, uint8 _currentRank, uint256 _sponsorNodeId) external',
];

// ============================================================================
// 3. ตัวแปรสถานะของระบบเชื่อมต่อ (Connection & Worker State)
// ============================================================================
let wsProvider: ethers.WebSocketProvider | null = null;
let wsContract: ethers.Contract | null = null;
let httpProviderIndex = 0;
let wssEndpointIndex = 0;

let reconnectAttempts = 0;
let reconnectTimer: NodeJS.Timeout | null = null;
let heartbeatTimer: NodeJS.Timeout | null = null;
let isReconnecting = false;
let isProcessingQueue = false;
let isShuttingDown = false;

/**
 * สร้าง HTTP JsonRpcProvider สำหรับการอ่านข้อมูลและส่ง Transaction อย่างเสถียร
 */
function getHttpProvider(): ethers.JsonRpcProvider {
  const rpcUrl = HTTP_RPC_ENDPOINTS[httpProviderIndex % HTTP_RPC_ENDPOINTS.length];
  const fetchReq = new ethers.FetchRequest(rpcUrl);
  fetchReq.timeout = 12000;
  return new ethers.JsonRpcProvider(
    fetchReq,
    { chainId: 97, name: 'bnbt' },
    { staticNetwork: true, batchMaxCount: 1 }
  );
}

function rotateHttpProvider(): void {
  httpProviderIndex = (httpProviderIndex + 1) % HTTP_RPC_ENDPOINTS.length;
}

// ============================================================================
// 4. ฟังก์ชันล้างการเชื่อมต่อเก่าป้องกัน Memory Leak (cleanupWebSocket)
// ============================================================================
export async function cleanupWebSocket(): Promise<void> {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }

  if (wsContract) {
    try {
      await wsContract.removeAllListeners();
    } catch {
      // ignore cleanup errors
    }
    wsContract = null;
  }

  if (wsProvider) {
    try {
      await wsProvider.removeAllListeners();
    } catch {
      // ignore
    }

    try {
      const rawWs = (wsProvider as unknown as { websocket?: { removeAllListeners?: () => void; close?: () => void } }).websocket;
      if (rawWs && typeof rawWs.removeAllListeners === 'function') {
        rawWs.removeAllListeners();
      }
    } catch {
      // ignore
    }

    try {
      await wsProvider.destroy();
    } catch {
      // ignore destroy errors on already closed socket
    }
    wsProvider = null;
  }
}

// ============================================================================
// 5. สมการ Exponential Backoff พร้อมค่าสุ่ม Jitter (scheduleReconnect)
// ============================================================================
function scheduleReconnect(reason: string): void {
  if (isShuttingDown || isReconnecting) return;
  isReconnecting = true;

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  // สมการ: t = min(MAX_DELAY_MS, INITIAL_DELAY_MS * 2^attempt) + jitter(0..1000ms)
  const exponentialDelay = Math.min(
    MAX_DELAY_MS,
    INITIAL_DELAY_MS * Math.pow(2, reconnectAttempts)
  );
  const jitter = Math.floor(Math.random() * 1000);
  const totalDelayMs = exponentialDelay + jitter;

  reconnectAttempts += 1;
  wssEndpointIndex = (wssEndpointIndex + 1) % WSS_ENDPOINTS.length;

  console.warn(
    `⚠️ [WS-Bot] สายหลุด (${reason}) | กำลังเตรียมต่อสายใหม่ครั้งที่ #${reconnectAttempts} ในอีก ${(
      totalDelayMs / 1000
    ).toFixed(2)} วินาที (Base: ${exponentialDelay}ms + Jitter: ${jitter}ms)...`
  );

  reconnectTimer = setTimeout(async () => {
    isReconnecting = false;
    await connectWebSocket();
  }, totalDelayMs);
}

// ============================================================================
// 6. ระบบส่งสัญญาณเลี้ยงสาย (Heartbeat / Ping-Pong Check ทุก 30 วินาที)
// ============================================================================
function startHeartbeat(): void {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
  }

  heartbeatTimer = setInterval(async () => {
    if (!wsProvider || isReconnecting || isShuttingDown) return;

    try {
      const blockNumber = await Promise.race<number>([
        wsProvider.getBlockNumber(),
        new Promise<number>((_, reject) =>
          setTimeout(() => reject(new Error('Heartbeat getBlockNumber timeout')), HEARTBEAT_TIMEOUT_MS)
        ),
      ]);
      console.log(`💓 [WS-Bot Heartbeat] สายสัญญาณปกติ | บล็อกปัจจุบัน: #${blockNumber}`);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`❌ [WS-Bot Heartbeat] ตรวจพบสายค้างหรือขาดการตอบสนอง (${msg})`);
      await cleanupWebSocket();
      scheduleReconnect('Heartbeat Timeout / Silent Drop');
    }
  }, HEARTBEAT_INTERVAL_MS);
}

// ============================================================================
// 7. ฟังก์ชันตรวจสอบและสั่งประมวลผลคิวผ่าน HTTP Provider (processCloneAndPoolQueues)
// ============================================================================
export async function checkAndProcessPendingQueues(triggerSource: string): Promise<void> {
  if (isProcessingQueue || isShuttingDown) return;
  isProcessingQueue = true;

  try {
    const httpProvider = getHttpProvider();
    const readContract = new ethers.Contract(CONTRACT_ADDRESS, BOT_CONTRACT_ABI, httpProvider);

    // 7.1 ตรวจสอบ getPendingCloneCount() หาก Smart Contract รุ่นนั้นมีฟังก์ชันคิวโคลนโดยตรง
    let pendingCloneCount = 0n;
    try {
      pendingCloneCount = await readContract.getPendingCloneCount();
    } catch {
      // สัญญาที่ประมวลผลโคลนอัตโนมัติในธุรกรรมอาจไม่มี getPendingCloneCount()
      pendingCloneCount = 0n;
    }

    if (pendingCloneCount > 0n) {
      console.log(
        `🔔 [WS-Bot] (${triggerSource}) พบคิวโคลนรอประมวลผล: ${pendingCloneCount.toString()} รายการ`
      );
      if (BOT_PRIVATE_KEY && BOT_PRIVATE_KEY.length >= 64) {
        const botWallet = new ethers.Wallet(BOT_PRIVATE_KEY, httpProvider);
        const writeContract = new ethers.Contract(CONTRACT_ADDRESS, BOT_CONTRACT_ABI, botWallet);

        while (pendingCloneCount > 0n) {
          const iterationsToRun =
            pendingCloneCount < BATCH_SIZE ? pendingCloneCount : BATCH_SIZE;
          console.log(
            `⏳ [WS-Bot] (${triggerSource}) กำลังรันคิวจำนวน ${iterationsToRun.toString()} รายการเพื่อประหยัด Gas...`
          );

          // ตรวจสอบ Gas Limit ตอนส่งธุรกรรม: คำนวณ estimateGas และเผื่อ Buffer 20%
          let gasLimit = 600000n;
          try {
            const estGas = await writeContract.processCloneQueue.estimateGas(iterationsToRun);
            gasLimit = (estGas * 120n) / 100n;
          } catch {
            gasLimit = 600000n;
          }

          const tx = await writeContract.processCloneQueue(iterationsToRun, { gasLimit });
          console.log(
            `⏳ [WS-Bot] ส่ง TX สำเร็จ: ${tx.hash} (GasLimit: ${gasLimit.toString()}) | รอการยืนยันบล็อก...`
          );
          await tx.wait(1);
          console.log(`✅ [WS-Bot] ประมวลผลคิวโคลนสำเร็จ! TX: ${tx.hash}`);
          pendingCloneCount = await readContract.getPendingCloneCount().catch(() => 0n);
        }
      } else {
        console.warn(
          '⚠️ [WS-Bot] พบคิวโคลนค้างแต่ยังไม่ได้ตั้งค่า BOT_PRIVATE_KEY ใน .env (ทำงานในโหมดดักฟังและซิงค์ข้อมูล)'
        );
      }
    }
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`⚠️ [WS-Bot] ข้อผิดพลาดขณะเช็คคิวผ่าน HTTP RPC (${triggerSource}): ${msg}`);
    rotateHttpProvider();
  } finally {
    isProcessingQueue = false;
  }
}

// ============================================================================
// 8. ฟังก์ชันหลักสำหรับเชื่อมต่อ WebSocket และผูก Event Listener (connectWebSocket)
// ============================================================================
export async function connectWebSocket(): Promise<void> {
  if (isShuttingDown) return;

  await cleanupWebSocket();

  const wssUrl = WSS_ENDPOINTS[wssEndpointIndex % WSS_ENDPOINTS.length];
  console.log(`🔌 [WS-Bot] กำลังเชื่อมต่อ WebSocket RPC: ${wssUrl} (Contract: ${CONTRACT_ADDRESS})...`);

  try {
    wsProvider = new ethers.WebSocketProvider(wssUrl, {
      chainId: 97,
      name: 'bnbt',
    });

    const rawWs = (wsProvider as unknown as {
      websocket?: {
        on?: (event: string, cb: (...args: unknown[]) => void) => void;
        addEventListener?: (event: string, cb: (...args: unknown[]) => void) => void;
      };
    }).websocket;

    const handleSocketClose = async (code?: unknown) => {
      if (isShuttingDown) return;
      console.warn(`🔌 [WS-Bot] WebSocket ถูกตัดการเชื่อมต่อ (Code: ${String(code ?? 'unknown')})`);
      await cleanupWebSocket();
      scheduleReconnect(`Socket Closed (${String(code ?? 'unknown')})`);
    };

    const handleSocketError = async (err: unknown) => {
      if (isShuttingDown) return;
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`⚠️ [WS-Bot] WebSocket Error: ${msg}`);
    };

    if (rawWs) {
      if (typeof rawWs.on === 'function') {
        rawWs.on('close', handleSocketClose);
        rawWs.on('error', handleSocketError);
      } else if (typeof rawWs.addEventListener === 'function') {
        rawWs.addEventListener('close', handleSocketClose);
        rawWs.addEventListener('error', handleSocketError);
      }
    }

    // ทดสอบดึงเลขบล็อกแรกเพื่อยืนยันว่าท่อ WebSocket พร้อมทำงานจริง
    const currentBlock = await Promise.race<number>([
      wsProvider.getBlockNumber(),
      new Promise<number>((_, reject) =>
        setTimeout(() => reject(new Error('Initial WebSocket handshake timeout')), 12000)
      ),
    ]);

    // เชื่อมต่อสำเร็จ -> รีเซ็ตตัวนับ Exponential Backoff เป็น 0 ทันที
    reconnectAttempts = 0;
    isReconnecting = false;
    console.log(
      `🟢 [WS-Bot] เชื่อมต่อ WebSocket สำเร็จ! (บล็อกปัจจุบัน: #${currentBlock}) | รีเซ็ตตัวนับ Reconnect = 0`
    );

    // ผูกสัญญาเพื่อดักฟัง Event แบบเรียลไทม์
    wsContract = new ethers.Contract(CONTRACT_ADDRESS, BOT_CONTRACT_ABI, wsProvider);

    // 1) ดักฟัง Event CloneQueued (กรณีใช้โหมดคิวโคลน)
    wsContract.on('CloneQueued', async (rank: bigint, parentId: bigint, pendingCount: bigint) => {
      console.log(
        `⚡ [WS-Event: CloneQueued] Rank ${rank.toString()} | Parent #${parentId.toString()} | คิวสะสม: ${pendingCount.toString()}`
      );
      await checkAndProcessPendingQueues(`Event:CloneQueued(R${rank},#${parentId})`);
    });

    // 2) ดักฟัง Event NodeCreated (ตรวจจับทันทีที่มีโหนดตกขาขวา หรือเกิดโคลน/Extra ID)
    wsContract.on(
      'NodeCreated',
      async (
        rank: bigint,
        nodeId: bigint,
        owner: string,
        _directSponsor: string,
        parentId: bigint,
        isLeft: boolean,
        isClone: boolean,
        isExtraId: boolean
      ) => {
        const legLabel = isLeft ? 'ขาซ้าย (LEFT)' : 'ขาขวา (RIGHT)';
        const typeLabel = isExtraId ? 'EXTRA_ID' : isClone ? 'CLONE' : 'MAIN/DIRECT';
        console.log(
          `⚡ [WS-Event: NodeCreated] Rank ${rank.toString()} | Node #${nodeId.toString()} (${typeLabel}) | Parent #${parentId.toString()} [${legLabel}] | Owner: ${owner}`
        );
        if (!isLeft) {
          await checkAndProcessPendingQueues(`Event:RightLegNode(R${rank},#${nodeId})`);
        }
      }
    );

    // 3) ดักฟัง Event RankUpgraded
    wsContract.on(
      'RankUpgraded',
      async (user: string, fromRank: bigint, toRank: bigint, newNodeId: bigint, isAutoUpgrade: boolean) => {
        console.log(
          `⚡ [WS-Event: RankUpgraded] User ${user} | R${fromRank.toString()} -> R${toRank.toString()} (Node #${newNodeId.toString()}) | Auto: ${isAutoUpgrade}`
        );
        await checkAndProcessPendingQueues(`Event:RankUpgraded(R${toRank},#${newNodeId})`);
      }
    );

    // เริ่มระบบส่งสัญญาณเลี้ยงสาย (Heartbeat) ทุกๆ 30 วินาที
    startHeartbeat();

    // วิ่งเช็คคิวตกค้างทันทีหลังต่อสายสำเร็จ (เผื่อมีรายการเกิดขึ้นระหว่างที่สายหลุด)
    await checkAndProcessPendingQueues('RECONNECT_RECOVERY');
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`❌ [WS-Bot] เชื่อมต่อ WebSocket ไม่สำเร็จ: ${msg}`);
    await cleanupWebSocket();
    scheduleReconnect(`Connection Failed (${msg})`);
  }
}

// ============================================================================
// 9. การปิดระบบอย่างปลอดภัยเมื่อหยุดโปรเซส (Graceful Shutdown)
// ============================================================================
async function gracefulShutdown(signal: string): Promise<void> {
  console.log(`\n🛑 [WS-Bot] ได้รับสัญญาณ ${signal} | กำลังปิดการเชื่อมต่อ WebSocket อย่างปลอดภัย...`);
  isShuttingDown = true;
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  await cleanupWebSocket();
  process.exit(0);
}

// รันบอททันทีเมื่อถูกเรียกใช้งานโดยตรง (npx tsx scripts/ws-clone-bot.ts)
if (import.meta.url === `file://${process.argv[1]}`) {
  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  connectWebSocket().catch((err) => {
    console.error('Fatal WS-Bot startup error:', err);
  });
}
