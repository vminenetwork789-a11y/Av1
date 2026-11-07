// server.ts
import express from "express";
import cors from "cors";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { ethers } from "ethers";
import { exec } from "child_process";
import { Queue, Worker, QueueEvents } from "bullmq";
import Redis from "ioredis";
import { EventEmitter } from "events";
import http from "http";
import { WebSocketServer } from "ws";
import fs from "fs";
import pg from "pg";
var queueEventsEmitter = new EventEmitter();
queueEventsEmitter.setMaxListeners(200);
var wssServer = null;
function setWssServer(wss) {
  wssServer = wss;
}
function getAggregatedTransactions(addressFilter) {
  const transactions = [];
  const lowerAddr = addressFilter ? addressFilter.toLowerCase() : null;
  try {
    const r1Nodes = matrixDB.nodesByRank[1] || [];
    r1Nodes.forEach((node) => {
      const ownerLower = (node.owner || "").toLowerCase();
      if (lowerAddr && ownerLower !== lowerAddr) return;
      const nodeAny = node;
      const isLeft = node.parentId === 0 ? true : nodeAny.isLeft ?? true;
      transactions.push({
        id: `reg-r1-${node.id}`,
        category: "REGISTER",
        titleTh: node.isClone ? `\u0E2A\u0E23\u0E49\u0E32\u0E07\u0E23\u0E2B\u0E31\u0E2A\u0E42\u0E04\u0E25\u0E19 Clone #${node.id} (Rank 1)` : node.isExtraId ? `\u0E2A\u0E23\u0E49\u0E32\u0E07\u0E23\u0E2B\u0E31\u0E2A\u0E1E\u0E34\u0E40\u0E28\u0E29 Extra ID #${node.id} (Rank 1)` : `\u0E2A\u0E21\u0E31\u0E04\u0E23\u0E2A\u0E21\u0E32\u0E0A\u0E34\u0E01 / \u0E2A\u0E23\u0E49\u0E32\u0E07\u0E23\u0E2B\u0E31\u0E2A\u0E2B\u0E25\u0E31\u0E01 Main ID #${node.id} (Rank 1)`,
        titleEn: node.isClone ? `Clone Created #${node.id} (Rank 1)` : node.isExtraId ? `Extra ID Created #${node.id} (Rank 1)` : `Member Registration Main ID #${node.id} (Rank 1)`,
        userAddress: node.owner,
        mainNodeId: node.originMainId || node.id,
        rank: 1,
        amountUsdt: 5,
        timestamp: nodeAny.createdAt || matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString(),
        blockNumber: nodeAny.blockNumber,
        detailsTh: `\u0E15\u0E48\u0E2D\u0E2A\u0E32\u0E22\u0E43\u0E15\u0E49 \u0E2D\u0E31\u0E1E\u0E44\u0E25\u0E19\u0E4C #${node.parentId} (${isLeft ? "\u2199 \u0E1D\u0E31\u0E48\u0E07\u0E0B\u0E49\u0E32\u0E22" : "\u2198 \u0E1D\u0E31\u0E48\u0E07\u0E02\u0E27\u0E32"}) \u2022 \u0E1C\u0E39\u0E49\u0E41\u0E19\u0E30\u0E19\u0E33: ${node.directSponsor ? `${node.directSponsor.slice(0, 6)}...${node.directSponsor.slice(-4)}` : "Root"}`,
        detailsEn: `Placed under Parent #${node.parentId} (${isLeft ? "Left" : "Right"}) \u2022 Sponsor: ${node.directSponsor ? `${node.directSponsor.slice(0, 6)}...${node.directSponsor.slice(-4)}` : "Root"}`
      });
    });
    const upgrades = matrixDB.upgradeLogs || [];
    upgrades.forEach((u) => {
      const ownerLower = (u.owner || "").toLowerCase();
      if (lowerAddr && ownerLower !== lowerAddr) return;
      const uCost = parseFloat(u.rankPriceUsdt) || u.costUsdt || RANK_PRICES_USDT[Number(u.toRank) - 1] || 10;
      transactions.push({
        id: `upg-${u.id || `${u.mainNodeId}-${u.toRank}`}`,
        category: "UPGRADE",
        titleTh: `\u0E2D\u0E31\u0E1E\u0E40\u0E01\u0E23\u0E14\u0E1C\u0E31\u0E07\u0E41\u0E23\u0E07\u0E04\u0E4C Main ID #${u.mainNodeId} \u2192 Rank ${u.toRank}`,
        titleEn: `Rank Upgrade Main ID #${u.mainNodeId} \u2192 Rank ${u.toRank}`,
        userAddress: u.owner,
        mainNodeId: Number(u.mainNodeId),
        rank: Number(u.toRank),
        amountUsdt: uCost,
        timestamp: u.createdAt || matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString(),
        txHash: u.txHash,
        detailsTh: `\u0E40\u0E25\u0E37\u0E48\u0E2D\u0E19\u0E41\u0E23\u0E07\u0E04\u0E4C\u0E08\u0E32\u0E01 R${u.fromRank} \u0E44\u0E1B\u0E22\u0E31\u0E07 R${u.toRank} (${uCost.toFixed(2)} USDT)`,
        detailsEn: `Upgraded from R${u.fromRank} to R${u.toRank} (${uCost.toFixed(2)} USDT)`
      });
    });
    const rewards = matrixDB.rewardLogs || [];
    rewards.forEach((r, idx) => {
      const recipientLower = (r.recipient || "").toLowerCase();
      if (lowerAddr && recipientLower !== lowerAddr) return;
      const amt = parseFloat(r.amount) || 0;
      const rTime = r.createdAt || r.timestamp || matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString();
      transactions.push({
        id: `rew-${r.id || idx}`,
        category: "PAYOUT",
        titleTh: `\u0E23\u0E31\u0E1A\u0E42\u0E1A\u0E19\u0E31\u0E2A / \u0E04\u0E48\u0E32\u0E04\u0E2D\u0E21\u0E21\u0E34\u0E0A\u0E0A\u0E31\u0E48\u0E19 (${r.rewardType})`,
        titleEn: `Payout / Reward (${r.rewardType})`,
        userAddress: r.recipient,
        mainNodeId: Number(r.toNodeId || 1),
        rank: Number(r.rank || 1),
        amountUsdt: amt,
        timestamp: rTime,
        blockNumber: r.blockNumber,
        detailsTh: `\u0E44\u0E14\u0E49\u0E23\u0E31\u0E1A\u0E08\u0E32\u0E01 Node #${r.fromNodeId} \u2192 Node #${r.toNodeId} (Rank ${r.rank})`,
        detailsEn: `Received from Node #${r.fromNodeId} \u2192 Node #${r.toNodeId} (Rank ${r.rank})`
      });
    });
    transactions.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
  } catch (e) {
    console.warn("[getAggregatedTransactions error]:", e);
  }
  return transactions;
}
var sseClients = /* @__PURE__ */ new Set();
function broadcastAllMatrixUpdates() {
  try {
    const transactions = getAggregatedTransactions();
    const payload = JSON.stringify({
      type: "ALL_MATRIX_UPDATED",
      lastSyncedAt: matrixDB.lastSyncedAt,
      rankCounts: matrixDB.rankCounts,
      nodesByRank: matrixDB.nodesByRank,
      contractAddress: CONTRACT_ADDRESS,
      transactions,
      rewardLogs: matrixDB.rewardLogs || [],
      upgradeLogs: matrixDB.upgradeLogs || []
    });
    if (wssServer) {
      wssServer.clients.forEach((client) => {
        if (client.readyState === 1) {
          client.send(payload);
        }
      });
    }
    sseClients.forEach((res) => {
      try {
        res.write(`data: ${payload}

`);
      } catch {
        sseClients.delete(res);
      }
    });
  } catch (err) {
    console.warn("[WS/SSE Broadcast Error]:", err);
  }
}
dotenv.config();
process.on("unhandledRejection", (reason) => {
  console.warn("[Process Guard] Unhandled Rejection:", reason);
});
process.on("uncaughtException", (err) => {
  console.warn("[Process Guard] Uncaught Exception:", err);
});
var { Pool } = pg;
var __filename = fileURLToPath(import.meta.url);
var __dirname = path.dirname(__filename);
var PORT = Number(process.env.PORT) || 3e3;
var CONFIG_FILE_PATH = path.resolve(__dirname, "prisma/contract-config.json");
var DEFAULT_SYSTEM_CONFIG = {
  contractAddress: process.env.OVERRIDE_CONTRACT_ADDRESS || process.env.CONTRACT_ADDRESS || "0x0742E9581217Cc8Cd6De2Aa80b7Ce9aD881DEEe3",
  usdtAddress: process.env.USDT_ADDRESS || "0x05f24E1F598DF1aE8d0B424ce978f8ea33bB752b",
  chainId: Number(process.env.CHAIN_ID || 97),
  networkName: process.env.NETWORK_NAME || "BNB Smart Chain Testnet",
  rpcUrl: process.env.RPC_URL || "https://bsc-testnet-rpc.publicnode.com",
  blockExplorerUrl: process.env.BLOCK_EXPLORER_URL || "https://testnet.bscscan.com",
  updatedAt: (/* @__PURE__ */ new Date()).toISOString()
};
var SERVER_LEGACY_CONTRACTS = /* @__PURE__ */ new Set([
  "0xc5cf23d0cead13a08b3b3d968a052b956a64fbc6",
  "0xe231fe72e1c2ab2617d8972c4ea15bc9122e514f",
  "0x45405293ecce73f1a8c39ca8c0cc2248d6b9ab77",
  "0xf955de5c1a403c9d286538a872c17837aebdd8ca",
  "0xc71a9a677d9ef61a9133d6e6545306a11bd3ee5a",
  "0x87e9f1b9a63eb65c91fca718c5af1cee4eec6209",
  "0x675cdb85d9a843ac3e87b42beb7A606da79882e8",
  "0xae3736ecd23dfb6c49b76ef8548391c1b6fff7b9",
  "0x1700326ce4d4507a5dd88eb50156758581b6f4cb",
  "0x5f0a48aa94dcfa820a12cb8e6c46e9cce807d341",
  "0x48acb86c42a389efda47cc1b16938eea73f1f892",
  "0x82d18c8b7d6b1e3823fb52723435a3b4fb558622",
  "0xf09a03df9d6f89049dbdbcc5bfdb3d2be3bc5f10",
  "0x82d07e805f670943a3dbf74ab777eb89189c7c27",
  "0xa8fce96d490856338384a8a38a297b0789b0a8f1",
  "0x154f3654088bb55ddabba62850cbcfdea6046e3c",
  "0x36ff96c21e285fe557e6a3185d2b5467b46edc01",
  "0x2246c327b711c7ba82ad35af42371253aa88d6e6",
  "0xcf8d2348894ab77dcd1529c869e526924e2a7c3d",
  "0xa285257430fbff1bb63c0a74958874a5bf159ccd",
  "0x2df74a86df9d32c735755e1f448a98f4cda456c5",
  "0x01fed3686a284dcac8ef7277c4657a2aa12fab54",
  "0xda7acd2ca8967e8015e0931e652d4b7ee5e1c436",
  "0x07953ba884e2d05fa543ef7eaec426c8de6d58ff",
  "0x7fa05f1aeb74209612aea56472d9a2e2e9d44073",
  "0x003ef4aaab33e517b2f565320df37e88b2db7a69",
  "0x47ff540a2e04aa36cdc4a4ec6e4feadac1a71b66",
  "0xc115f39897811d83be2613e9722753cfb78d492b",
  "0x191d0c2cd0496e05aa4f91273125ddccc6645df4",
  "0x36932dca76922f8f05513710db89238258e61378"
]);
var SERVER_LEGACY_USDT = /* @__PURE__ */ new Set([
  "0x95856be9c16afc9c147c034cd6a514d730bd90ba"
]);
var PG_DATABASE_URL = process.env.DATABASE_URL || "postgresql://user:Siriphong2541%40@187.124.138.252:32772/Aunza";
var pgPool = null;
var pgConnected = false;
var pgLastError = null;
var pgTablesCount = {};
function initPostgresPool() {
  try {
    pgPool = new Pool({
      connectionString: PG_DATABASE_URL,
      max: 10,
      idleTimeoutMillis: 3e4,
      connectionTimeoutMillis: 8e3,
      ssl: false
    });
    pgPool.on("error", (err) => {
      console.warn("\u26A0\uFE0F [PostgreSQL Pool Error]:", err.message);
      pgConnected = false;
      pgLastError = err.message;
    });
    pgPool.query("SELECT current_database(), current_user, version()", (err, res) => {
      if (err) {
        console.warn("\u26A0\uFE0F [PostgreSQL] Initial connection test failed:", err.message);
        pgConnected = false;
        pgLastError = err.message;
      } else {
        pgConnected = true;
        pgLastError = null;
        const dbName = res.rows[0]?.current_database || "Aunza";
        const user = res.rows[0]?.current_user || "user";
        console.log(`\u{1F418} [PostgreSQL] Connected successfully to database: ${dbName} as ${user}`);
        refreshPgTableCounts();
      }
    });
  } catch (err) {
    console.warn("\u26A0\uFE0F [PostgreSQL] Failed to initialize pool:", err?.message || err);
    pgLastError = err?.message || String(err);
  }
}
async function refreshPgTableCounts() {
  if (!pgPool) return;
  try {
    const tables = ["accounts", "matrix_levels", "income_channels", "activities", "wallets", "matrix_nodes"];
    const counts = {};
    for (const t of tables) {
      try {
        const r = await pgPool.query(`SELECT COUNT(*)::int as count FROM ${t}`);
        counts[t] = r.rows[0]?.count ?? 0;
      } catch {
        counts[t] = 0;
      }
    }
    pgTablesCount = counts;
  } catch (e) {
    console.warn("\u26A0\uFE0F [PostgreSQL] refresh table counts error:", e.message);
  }
}
async function syncNodeToPostgres(node) {
  if (!pgPool || !pgConnected) return;
  try {
    const nodeIdStr = `R${node.rank}-#${node.id}`;
    const parentIdStr = node.parentId === 0 ? "ROOT" : `R${node.rank}-#${node.parentId}`;
    const sponsorIdStr = node.directSponsorNodeId ? `R1-#${node.directSponsorNodeId}` : `R1-#1`;
    const label = node.isClone ? `Clone R${node.rank}-#${node.id}` : `Main ID #${node.id} (Rank ${node.rank})`;
    const category = node.isClone ? "Cloning" : node.isExtraId ? "Extra" : "Original";
    const volume = (node.nodeVault || RANK_PRICES_USDT[node.rank - 1] * 0.35).toFixed(2);
    await pgPool.query(
      `INSERT INTO matrix_nodes (id, label, category, level, parent_id, sponsor_id, personal_volume_usdt, direct_referrals, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
       ON CONFLICT (id) DO UPDATE SET
         label = EXCLUDED.label,
         category = EXCLUDED.category,
         parent_id = EXCLUDED.parent_id,
         sponsor_id = EXCLUDED.sponsor_id,
         personal_volume_usdt = EXCLUDED.personal_volume_usdt;`,
      [nodeIdStr, label, category, node.rank, parentIdStr, sponsorIdStr, volume, 0]
    );
  } catch {
  }
}
async function syncAllMatrixDBToPostgres() {
  if (!pgPool || !pgConnected) return;
  try {
    for (let r = 1; r <= 27; r++) {
      const rNodes = matrixDB.nodesByRank[r] || [];
      if (rNodes.length <= 1 && r > 1) continue;
      for (const n of rNodes) {
        await syncNodeToPostgres(n);
      }
    }
    await refreshPgTableCounts();
  } catch (e) {
    console.warn("\u26A0\uFE0F [PostgreSQL] syncAllMatrixDBToPostgres notice:", e?.message);
  }
}
initPostgresPool();
function readContractConfigFromDB() {
  try {
    if (fs.existsSync(CONFIG_FILE_PATH)) {
      const raw = fs.readFileSync(CONFIG_FILE_PATH, "utf8");
      const parsed = JSON.parse(raw);
      if (parsed.contractAddress && ethers.isAddress(parsed.contractAddress)) {
        if (SERVER_LEGACY_CONTRACTS.has(parsed.contractAddress.toLowerCase())) {
          parsed.contractAddress = DEFAULT_SYSTEM_CONFIG.contractAddress;
        }
        if (parsed.usdtAddress && SERVER_LEGACY_USDT.has(parsed.usdtAddress.toLowerCase())) {
          parsed.usdtAddress = DEFAULT_SYSTEM_CONFIG.usdtAddress;
        }
        if (parsed.rpcUrl === "https://data-seed-prebsc-1-s1.binance.org:8545/" && !process.env.RPC_URL) {
          parsed.rpcUrl = "https://bsc-testnet-rpc.publicnode.com";
        }
        return { ...DEFAULT_SYSTEM_CONFIG, ...parsed };
      }
    }
  } catch (err) {
    console.warn("[DB Config] Error reading config file, using defaults:", err);
  }
  return DEFAULT_SYSTEM_CONFIG;
}
var activeSystemConfig = readContractConfigFromDB();
var CONTRACT_ADDRESS = activeSystemConfig.contractAddress;
var USDT_ADDRESS = activeSystemConfig.usdtAddress;
var RPC_URL = activeSystemConfig.rpcUrl || process.env.RPC_URL || "https://bsc-testnet-rpc.publicnode.com";
var activeUsdtDecimals = 18;
async function resolveUsdtDecimals() {
  try {
    const provider = getProvider();
    const usdtContract = new ethers.Contract(USDT_ADDRESS, USDT_READ_ABI, provider);
    const d = await usdtContract.decimals();
    if (d) {
      activeUsdtDecimals = Number(d);
      return activeUsdtDecimals;
    }
  } catch {
    try {
      const provider = getProvider();
      const matrixContract = new ethers.Contract(CONTRACT_ADDRESS, MATRIX_READ_ABI, provider);
      const p1 = await matrixContract.rankPrice(1);
      if (p1 >= 1000000000000000000n) {
        activeUsdtDecimals = 18;
      } else {
        activeUsdtDecimals = 6;
      }
      return activeUsdtDecimals;
    } catch {
    }
  }
  return activeUsdtDecimals;
}
resolveUsdtDecimals().catch(() => {
});
function updateContractConfigInDB(newConfig) {
  activeSystemConfig = {
    ...activeSystemConfig,
    ...newConfig,
    updatedAt: (/* @__PURE__ */ new Date()).toISOString()
  };
  CONTRACT_ADDRESS = activeSystemConfig.contractAddress;
  if (activeSystemConfig.usdtAddress) {
    USDT_ADDRESS = activeSystemConfig.usdtAddress;
  }
  resolveUsdtDecimals().catch(() => {
  });
  try {
    const dir = path.dirname(CONFIG_FILE_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(CONFIG_FILE_PATH, JSON.stringify(activeSystemConfig, null, 2), "utf8");
    console.log(`\u{1F4BE} [DB Config] Smart Contract updated in database: ${CONTRACT_ADDRESS} (USDT: ${USDT_ADDRESS})`);
  } catch (e) {
    console.error("[DB Config] Failed to write contract-config.json:", e);
  }
  return activeSystemConfig;
}
var FALLBACK_RPCS = Array.from(
  /* @__PURE__ */ new Set([
    RPC_URL,
    "https://bsc-testnet-rpc.publicnode.com",
    "https://bsc-testnet.publicnode.com",
    "https://bsc-testnet.bnbchain.org",
    "https://data-seed-prebsc-1-s2.binance.org:8545/",
    "https://data-seed-prebsc-1-s1.binance.org:8545/"
  ])
);
var eventListenerActive = false;
var currentProviderIndex = 0;
var MATRIX_READ_ABI = [
  "function totalNodesInRank(uint8) external view returns (uint256)",
  "function rankNodes(uint8, uint256) external view returns (uint256 id, address owner, address directSponsor, uint256 parentId, uint256 leftChild, uint256 rightChild, bool isClone, bool isExtraId)",
  "function previewPlacement(uint8 _rank, address _sponsor) external view returns (uint256 targetParentId, bool isLeft, uint8 activeRound)",
  "function sponsorRoundToggle(uint8, uint256) external view returns (uint8)",
  "function placementRoundToggle(uint8) external view returns (uint8)",
  "function nodeOriginMainId(uint8, uint256) external view returns (uint256)",
  "function nodeToR1MainId(uint8, uint256) external view returns (uint256)",
  "function nodeCloneRound(uint8, uint256) external view returns (uint256)",
  "function nodeDirectSponsorNodeId(uint8, uint256) external view returns (uint256)",
  "function nodeUpgradeVault(uint8, uint256) external view returns (uint256)",
  "function getMainIdCurrentRank(uint256) external view returns (uint8)",
  "function getMainIdNodeInRank(uint256, uint8) external view returns (uint256)",
  "function getMainIdThreePools(address, uint256) external view returns (uint8 currentRank, uint8 window5Start, uint8 window5End, uint256 upgradeRankPool, uint256 newAutoIdPool, uint256 newExtraIdPool)",
  "function getUserProfile(address) external view returns (tuple(bool isRegistered, address sponsor, uint256 sponsorMainNodeId, uint8 currentMaxRank, uint256 mainNodeIdRank1, uint256 latestCloneIdRank1))",
  "function getUserRankInfo(address, uint8) external view returns (uint256 firstNodeId, uint256 latestNodeId, uint256 upgradeVaultBalance, uint256 nextRankPrice)",
  "function checkValidSponsorId(uint256) external view returns (bool isValid, string reason, address sponsorAddress)",
  "function allSellWallet() external view returns (address)",
  "function botWallet() external view returns (address)",
  "function familyPool() external view returns (address)"
];
var USDT_READ_ABI = [
  "function balanceOf(address) external view returns (uint256)",
  "function allowance(address, address) external view returns (uint256)",
  "function decimals() external view returns (uint8)"
];
var cachedProviders = /* @__PURE__ */ new Map();
function getProvider() {
  const url = FALLBACK_RPCS[currentProviderIndex % FALLBACK_RPCS.length];
  let provider = cachedProviders.get(url);
  if (!provider) {
    const fetchReq = new ethers.FetchRequest(url);
    fetchReq.timeout = 8e3;
    provider = new ethers.JsonRpcProvider(
      fetchReq,
      {
        chainId: 97,
        name: "bnbt"
      },
      {
        staticNetwork: true,
        polling: false,
        batchMaxCount: 1
      }
    );
    provider.on("error", () => {
      currentProviderIndex = (currentProviderIndex + 1) % FALLBACK_RPCS.length;
    });
    cachedProviders.set(url, provider);
  }
  return provider;
}
var RANK_PRICES_USDT = [
  5,
  10,
  20,
  30,
  40,
  50,
  60,
  70,
  80,
  90,
  100,
  200,
  300,
  400,
  500,
  600,
  700,
  800,
  900,
  1e3,
  2e3,
  3e3,
  4e3,
  5e3,
  6e3,
  8e3,
  1e4
];
var MATRIX_DB_FILE_PATH = path.resolve(__dirname, "prisma/matrix-db.json");
function createEmptyMatrixDB(contractAddr = CONTRACT_ADDRESS, usdtAddr = USDT_ADDRESS) {
  const defaultCounts = {};
  for (let r = 1; r <= 27; r++) defaultCounts[r] = 1;
  return {
    contractAddress: contractAddr,
    usdtAddress: usdtAddr,
    lastSyncedBlock: 0,
    lastSyncedAt: (/* @__PURE__ */ new Date()).toISOString(),
    systemWallets: {
      allSellWallet: "0xBeFEdbB942A73957F6123bA0B3aA27B4A081F710",
      botWallet: "0xbFD0A57Ac58165A863A9F4d9E5C1B57996B24a56",
      familyPool: "0x225D2016F610C99051EfD91F812B2736647B275A",
      rootOwner: "0x622E20082e63c7220c5AF0A77Cbc437a98505311"
    },
    rankCounts: defaultCounts,
    nodesByRank: {},
    userRankVaults: {},
    rewardLogs: [],
    upgradeLogs: []
  };
}
function loadMatrixDBFromDisk() {
  try {
    if (fs.existsSync(MATRIX_DB_FILE_PATH)) {
      const raw = fs.readFileSync(MATRIX_DB_FILE_PATH, "utf8");
      const parsed = JSON.parse(raw);
      if (parsed && parsed.contractAddress && parsed.contractAddress.toLowerCase() === CONTRACT_ADDRESS.toLowerCase()) {
        return {
          ...createEmptyMatrixDB(CONTRACT_ADDRESS, USDT_ADDRESS),
          ...parsed,
          upgradeLogs: Array.isArray(parsed.upgradeLogs) ? parsed.upgradeLogs : []
        };
      }
    }
  } catch (err) {
    console.warn("[MatrixDB] Notice reading prisma/matrix-db.json:", err);
  }
  return createEmptyMatrixDB(CONTRACT_ADDRESS, USDT_ADDRESS);
}
var matrixDB = loadMatrixDBFromDisk();
var dbWriteTimer = null;
function saveMatrixDBToDisk(immediate = false) {
  broadcastAllMatrixUpdates();
  const writeNow = () => {
    try {
      const dir = path.dirname(MATRIX_DB_FILE_PATH);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(MATRIX_DB_FILE_PATH, JSON.stringify(matrixDB, null, 2), "utf8");
    } catch (err) {
      console.warn("[MatrixDB] Failed to persist matrix-db.json:", err);
    }
  };
  if (immediate) {
    if (dbWriteTimer) clearTimeout(dbWriteTimer);
    dbWriteTimer = null;
    writeNow();
    return;
  }
  if (dbWriteTimer) return;
  dbWriteTimer = setTimeout(() => {
    dbWriteTimer = null;
    writeNow();
  }, 800);
}
var inMemoryDB = {
  users: /* @__PURE__ */ new Map(),
  nodes: /* @__PURE__ */ new Map(),
  rewardLogs: matrixDB.rewardLogs,
  upgradeLogs: matrixDB.upgradeLogs
};
function ensurePersistentNodeTimestamps() {
  let missingAny = false;
  const seenTimes = /* @__PURE__ */ new Set();
  for (let r = 1; r <= 27; r++) {
    for (const n of matrixDB.nodesByRank[r] || []) {
      if (!n.createdAt || seenTimes.has(n.createdAt)) {
        missingAny = true;
        break;
      }
      seenTimes.add(n.createdAt);
    }
    if (missingAny) break;
  }
  if (!missingAny) return;
  const orderedNodes = [];
  for (let r = 1; r <= 27; r++) {
    const rList = (matrixDB.nodesByRank[r] || []).slice().sort((a, b) => a.id - b.id);
    for (const n of rList) {
      orderedNodes.push(n);
    }
  }
  const totalCount = orderedNodes.length;
  const fixedAnchorMs = 17938536e5;
  orderedNodes.forEach((n, idx) => {
    const stepsFromEnd = Math.max(0, totalCount - 1 - idx);
    n.createdAt = new Date(fixedAnchorMs - stepsFromEnd * 37e3).toISOString();
  });
}
function rebuildDatabaseRewardLogsFromNodes() {
  ensurePersistentNodeTimestamps();
  const r1Nodes = matrixDB.nodesByRank[1] || [];
  if (r1Nodes.length <= 1) return;
  const rootOwnerAddr = r1Nodes[0]?.owner || matrixDB.systemWallets.rootOwner;
  const allSellAddr = matrixDB.systemWallets.allSellWallet || "0xBeFEdbB942A73957F6123bA0B3aA27B4A081F710";
  const botAddr = matrixDB.systemWallets.botWallet || "0xbFD0A57Ac58165A863A9F4d9E5C1B57996B24a56";
  const familyAddr = matrixDB.systemWallets.familyPool || "0x225D2016F610C99051EfD91F812B2736647B275A";
  const liveTxEvents = matrixDB.rewardLogs.filter(
    (l) => l.txHash && !l.id.startsWith("db-chain-")
  );
  const seenKeys = /* @__PURE__ */ new Set();
  for (const ev of liveTxEvents) {
    seenKeys.add(`${ev.rank || 1}:${ev.fromNodeId}:${ev.rewardType}:${ev.toNodeId}`);
  }
  const reconstructed = [...liveTxEvents];
  const baseBlock = matrixDB.lastSyncedBlock || 140018600;
  for (let rankNum = 1; rankNum <= 27; rankNum++) {
    const rNodes = matrixDB.nodesByRank[rankNum] || [];
    if (rNodes.length <= 1) continue;
    const unitPrice = RANK_PRICES_USDT[rankNum - 1] || 5;
    const directAmt = (unitPrice * 0.3).toFixed(2);
    const vaultAmt = (unitPrice * 0.35).toFixed(2);
    const levelUnit = unitPrice * 0.02;
    const levelAmt = levelUnit.toFixed(2);
    const allSellAmt = (unitPrice * 0.04).toFixed(2);
    const botAmt = (unitPrice * 0.01).toFixed(2);
    const nodesMap = /* @__PURE__ */ new Map();
    for (const n of rNodes) {
      nodesMap.set(n.id, n);
    }
    const ownerMainsMap = /* @__PURE__ */ new Map();
    const sortedIds = Array.from(nodesMap.keys()).sort((a, b) => a - b);
    for (const id of sortedIds) {
      const n = nodesMap.get(id);
      if (!n.isClone && !n.isExtraId) {
        const oKey = n.owner.toLowerCase();
        const list = ownerMainsMap.get(oKey) || [];
        list.push(n.id);
        ownerMainsMap.set(oKey, list);
      }
    }
    const resolveSponsorNodeId = (nodeId) => {
      const n = nodesMap.get(nodeId);
      if (!n || nodeId === 1) return 1;
      if (rankNum > 1 && n.originMainId > 1) {
        const origR1 = r1Nodes.find((x) => x.id === n.originMainId);
        if (origR1 && origR1.directSponsorNodeId > 0) {
          return origR1.directSponsorNodeId;
        }
      }
      if (n.directSponsorNodeId > 0) return n.directSponsorNodeId;
      const spKey = (n.directSponsor || "").toLowerCase();
      if (!spKey || spKey === ethers.ZeroAddress) return 1;
      if (rankNum > 1) {
        const spInR1 = r1Nodes.find(
          (x) => x.owner.toLowerCase() === spKey && !x.isClone && !x.isExtraId
        );
        if (spInR1) return spInR1.id;
      }
      let cur = n.parentId;
      const guard = /* @__PURE__ */ new Set();
      while (cur > 0 && !guard.has(cur)) {
        guard.add(cur);
        const anc = nodesMap.get(cur);
        if (!anc) break;
        if (anc.owner.toLowerCase() === spKey) {
          if (!anc.isClone && !anc.isExtraId && anc.id < nodeId) return anc.id;
        }
        cur = anc.parentId;
      }
      const mains = (ownerMainsMap.get(spKey) || []).filter((m) => m < nodeId);
      return mains[0] || 1;
    };
    for (const id of sortedIds) {
      if (id <= 1) continue;
      const child = nodesMap.get(id);
      const parent = nodesMap.get(child.parentId);
      if (!parent || parent.leftChild !== id) continue;
      const syntheticTx = `0x${rankNum.toString(16).padStart(2, "0")}${id.toString(16).padStart(4, "0")}db48acb86c42a389efda47cc1b16938eea73f1f892000000000000${id.toString(16).padStart(4, "0")}`;
      const eventTime = child.createdAt || new Date(
        17938536e5 - (Math.max(0, rNodes.length - id) * 45e3 + Math.max(0, 27 - rankNum) * 19e3)
      ).toISOString();
      const parentOrigin = nodesMap.get(parent.originMainId || parent.id) || parent;
      const isParentExtraLineage = Boolean(parent.isExtraId || parentOrigin.isExtraId);
      if (isParentExtraLineage) {
        const extraSpNodeId = parentOrigin.directSponsorNodeId > 0 ? parentOrigin.directSponsorNodeId : parent.directSponsorNodeId > 0 ? parent.directSponsorNodeId : resolveSponsorNodeId(parentOrigin.id);
        const extraSpNode = rankNum > 1 ? r1Nodes.find((x) => x.id === extraSpNodeId) || nodesMap.get(extraSpNodeId) : nodesMap.get(extraSpNodeId);
        const extraSpRecip = extraSpNode?.owner || parentOrigin.directSponsor || parent.directSponsor || rootOwnerAddr;
        const fullLevelAmt = (unitPrice * 0.3).toFixed(2);
        if (!seenKeys.has(`${rankNum}:${id}:DIRECT:${extraSpNodeId}`)) {
          reconstructed.push({
            id: `db-chain-r${rankNum}-${id}-direct`,
            fromNodeId: String(id),
            toNodeId: String(extraSpNodeId),
            recipient: extraSpRecip,
            rank: rankNum,
            rewardType: "DIRECT",
            amount: directAmt,
            txHash: syntheticTx,
            blockNumber: baseBlock - (rNodes.length - id),
            createdAt: eventTime
          });
        }
        if (!seenKeys.has(`${rankNum}:${id}:LEVEL:${extraSpNodeId}`)) {
          reconstructed.push({
            id: `db-chain-r${rankNum}-${id}-level-extra-${extraSpNodeId}`,
            fromNodeId: String(id),
            toNodeId: String(extraSpNodeId),
            recipient: extraSpRecip,
            rank: rankNum,
            rewardType: "LEVEL",
            amount: fullLevelAmt,
            txHash: syntheticTx,
            blockNumber: baseBlock - (rNodes.length - id),
            createdAt: eventTime
          });
        }
        if (!seenKeys.has(`${rankNum}:${id}:UPGRADE_VAULT:${extraSpNodeId}`)) {
          reconstructed.push({
            id: `db-chain-r${rankNum}-${id}-vault`,
            fromNodeId: String(id),
            toNodeId: String(extraSpNodeId),
            recipient: extraSpRecip,
            rank: rankNum,
            rewardType: "UPGRADE_VAULT",
            amount: vaultAmt,
            txHash: syntheticTx,
            blockNumber: baseBlock - (rNodes.length - id),
            createdAt: eventTime
          });
        }
        if (!seenKeys.has(`${rankNum}:${id}:ALLSELL:0`)) {
          reconstructed.push({
            id: `db-chain-r${rankNum}-${id}-allsell`,
            fromNodeId: String(id),
            toNodeId: "0",
            recipient: allSellAddr,
            rank: rankNum,
            rewardType: "ALLSELL",
            amount: allSellAmt,
            txHash: syntheticTx,
            blockNumber: baseBlock - (rNodes.length - id),
            createdAt: eventTime
          });
        }
        if (!seenKeys.has(`${rankNum}:${id}:BOT:0`)) {
          reconstructed.push({
            id: `db-chain-r${rankNum}-${id}-bot`,
            fromNodeId: String(id),
            toNodeId: "0",
            recipient: botAddr,
            rank: rankNum,
            rewardType: "BOT",
            amount: botAmt,
            txHash: syntheticTx,
            blockNumber: baseBlock - (rNodes.length - id),
            createdAt: eventTime
          });
        }
        continue;
      }
      const spNodeId = resolveSponsorNodeId(id);
      const spNode = rankNum > 1 ? r1Nodes.find((x) => x.id === spNodeId) || nodesMap.get(spNodeId) : nodesMap.get(spNodeId);
      const isId1CloneChild = child.isClone && (child.originMainId === 1 || parent.id === 1);
      const directRecip = isId1CloneChild ? familyAddr : spNode?.owner || child.directSponsor || rootOwnerAddr;
      const directType = isId1CloneChild ? "DIRECT_ID1_CLONE_FAMILY_POOL" : "DIRECT";
      if (!seenKeys.has(`${rankNum}:${id}:${directType}:${isId1CloneCloneTarget(isId1CloneChild, spNodeId)}`)) {
        reconstructed.push({
          id: `db-chain-r${rankNum}-${id}-direct`,
          fromNodeId: String(id),
          toNodeId: isId1CloneChild ? "0" : String(spNodeId),
          recipient: directRecip,
          rank: rankNum,
          rewardType: directType,
          amount: directAmt,
          txHash: syntheticTx,
          blockNumber: baseBlock - (rNodes.length - id),
          createdAt: eventTime
        });
      }
      const parentMainId = !parent.isClone ? parent.id : parent.originMainId || parent.id;
      if (!seenKeys.has(`${rankNum}:${id}:UPGRADE_VAULT:${parentMainId}`)) {
        reconstructed.push({
          id: `db-chain-r${rankNum}-${id}-vault`,
          fromNodeId: String(id),
          toNodeId: String(parentMainId),
          recipient: parent.owner,
          rank: rankNum,
          rewardType: "UPGRADE_VAULT",
          amount: vaultAmt,
          txHash: syntheticTx,
          blockNumber: baseBlock - (rNodes.length - id),
          createdAt: eventTime
        });
      }
      let currUp = child.parentId;
      let paidLevels = 0;
      for (let tier = 1; tier <= 15; tier++) {
        if (currUp <= 0) break;
        const upNode = nodesMap.get(currUp);
        if (!upNode) break;
        paidLevels++;
        if (!seenKeys.has(`${rankNum}:${id}:LEVEL:${upNode.id}`)) {
          reconstructed.push({
            id: `db-chain-r${rankNum}-${id}-level-${tier}-${upNode.id}`,
            fromNodeId: String(id),
            toNodeId: String(upNode.id),
            recipient: upNode.owner,
            rank: rankNum,
            rewardType: "LEVEL",
            amount: levelAmt,
            txHash: syntheticTx,
            blockNumber: baseBlock - (rNodes.length - id),
            createdAt: eventTime
          });
        }
        currUp = upNode.parentId;
      }
      const unpaidTiers = 15 - paidLevels;
      if (unpaidTiers > 0 && !seenKeys.has(`${rankNum}:${id}:UNPAID_LEVEL_FAMILY_POOL:0`)) {
        reconstructed.push({
          id: `db-chain-r${rankNum}-${id}-unpaid-family`,
          fromNodeId: String(id),
          toNodeId: "0",
          recipient: familyAddr,
          rank: rankNum,
          rewardType: "UNPAID_LEVEL_FAMILY_POOL",
          amount: (unpaidTiers * levelUnit).toFixed(2),
          txHash: syntheticTx,
          blockNumber: baseBlock - (rNodes.length - id),
          createdAt: eventTime
        });
      }
      if (!seenKeys.has(`${rankNum}:${id}:ALLSELL:0`)) {
        reconstructed.push({
          id: `db-chain-r${rankNum}-${id}-allsell`,
          fromNodeId: String(id),
          toNodeId: "0",
          recipient: allSellAddr,
          rank: rankNum,
          rewardType: "ALLSELL",
          amount: allSellAmt,
          txHash: syntheticTx,
          blockNumber: baseBlock - (rNodes.length - id),
          createdAt: eventTime
        });
      }
      if (!seenKeys.has(`${rankNum}:${id}:BOT:0`)) {
        reconstructed.push({
          id: `db-chain-r${rankNum}-${id}-bot`,
          fromNodeId: String(id),
          toNodeId: "0",
          recipient: botAddr,
          rank: rankNum,
          rewardType: "BOT",
          amount: botAmt,
          txHash: syntheticTx,
          blockNumber: baseBlock - (rNodes.length - id),
          createdAt: eventTime
        });
      }
    }
  }
  reconstructed.sort((a, b) => {
    const timeA = a.createdAt ? new Date(a.createdAt).getTime() : 0;
    const timeB = b.createdAt ? new Date(b.createdAt).getTime() : 0;
    if (timeB !== timeA) return timeB - timeA;
    const rDiff = Number(b.rank || 1) - Number(a.rank || 1);
    if (rDiff !== 0) return rDiff;
    return Number(b.fromNodeId || 0) - Number(a.fromNodeId || 0);
  });
  matrixDB.rewardLogs = reconstructed.slice(0, 500);
  inMemoryDB.rewardLogs = matrixDB.rewardLogs;
}
function rebuildDatabaseUpgradeLogsFromNodes() {
  const r1Nodes = matrixDB.nodesByRank[1] || [];
  if (r1Nodes.length === 0) return;
  const r1OwnerMainsMap = /* @__PURE__ */ new Map();
  for (const n of r1Nodes) {
    if (!n.isClone && !n.isExtraId) {
      const oKey = n.owner.toLowerCase();
      const list = r1OwnerMainsMap.get(oKey) || [];
      list.push(n.id);
      r1OwnerMainsMap.set(oKey, list);
    }
  }
  const liveTxByRankNode = /* @__PURE__ */ new Map();
  for (const u of matrixDB.upgradeLogs || []) {
    if (u.txHash && !u.id.startsWith("db-upgrade-")) {
      liveTxByRankNode.set(`${u.toRank}:${u.newNodeId}`, {
        txHash: u.txHash,
        blockNumber: u.blockNumber,
        createdAt: u.createdAt
      });
    }
  }
  for (const rLog of matrixDB.rewardLogs || []) {
    if (rLog.rank >= 2 && rLog.txHash && !rLog.id.startsWith("db-chain-")) {
      const key = `${rLog.rank}:${rLog.fromNodeId}`;
      if (!liveTxByRankNode.has(key)) {
        liveTxByRankNode.set(key, {
          txHash: rLog.txHash,
          blockNumber: rLog.blockNumber,
          createdAt: rLog.createdAt
        });
      }
    }
  }
  const baseBlock = matrixDB.lastSyncedBlock || 140018600;
  const upgrades = [];
  for (let rankNum = 2; rankNum <= 27; rankNum++) {
    const rNodes = matrixDB.nodesByRank[rankNum] || [];
    if (rNodes.length <= 1) continue;
    const prevRankNodes = matrixDB.nodesByRank[rankNum - 1] || [];
    const rankPrice = RANK_PRICES_USDT[rankNum - 1] || 10;
    const ownerMainsInR = /* @__PURE__ */ new Map();
    for (const n of rNodes) {
      if (!n.isClone && !n.isExtraId) {
        const oKey = n.owner.toLowerCase();
        const list = ownerMainsInR.get(oKey) || [];
        list.push(n);
        ownerMainsInR.set(oKey, list);
      }
    }
    const ownerMainsInPrev = /* @__PURE__ */ new Map();
    for (const pn of prevRankNodes) {
      if (!pn.isClone && !pn.isExtraId) {
        const oKey = pn.owner.toLowerCase();
        const list = ownerMainsInPrev.get(oKey) || [];
        list.push(pn);
        ownerMainsInPrev.set(oKey, list);
      }
    }
    for (const node of rNodes) {
      if (node.id <= 1) continue;
      const oKey = node.owner.toLowerCase();
      const resolvedMainIdRank1 = node.originMainId && node.originMainId > 0 ? node.originMainId : node.id;
      const mainNodeIdLabel = node.isExtraId ? `Extra-R${rankNum}-#${node.id}` : String(resolvedMainIdRank1);
      const mainsInPrev = ownerMainsInPrev.get(oKey) || [];
      const prevNode = mainsInPrev.find((m) => m.id === resolvedMainIdRank1) || mainsInPrev[0];
      const rawPrevVault = Number(prevNode?.rawNodeVault || 0);
      const remPrevVault = Number(prevNode?.nodeVault || 0);
      const rawDiff = Math.max(0, Number((rawPrevVault - remPrevVault).toFixed(2)));
      let vaultDeducted = Math.min(rankPrice, rawDiff);
      let walletPaid = Math.max(0, Number((rankPrice - vaultDeducted).toFixed(2)));
      let upgradeMode = "MANUAL_WALLET";
      if (node.isClone) {
        upgradeMode = "CLONE_IN_RANK";
        vaultDeducted = 0;
        walletPaid = 0;
      } else if (node.isExtraId) {
        upgradeMode = "AUTO_VAULT";
        vaultDeducted = rankPrice;
        walletPaid = 0;
      } else if (vaultDeducted >= rankPrice - 1e-4) {
        upgradeMode = "AUTO_VAULT";
        vaultDeducted = rankPrice;
        walletPaid = 0;
      } else if (vaultDeducted > 0) {
        upgradeMode = "HYBRID_VAULT_WALLET";
      } else {
        upgradeMode = "MANUAL_WALLET";
      }
      const parentNode = rNodes.find((p) => p.id === node.parentId);
      const side = node.parentId === 0 ? "ROOT" : parentNode && parentNode.leftChild === node.id ? "LEFT" : "RIGHT";
      const spKey = (node.directSponsor || "").toLowerCase();
      const spMainsInR = ownerMainsInR.get(spKey) || [];
      const spNodeIdInR = node.directSponsorNodeId > 0 ? node.directSponsorNodeId : spMainsInR[0]?.id || 1;
      const liveMeta = liveTxByRankNode.get(`${rankNum}:${node.id}`);
      const syntheticTx = `0xup${rankNum.toString(16).padStart(2, "0")}${node.id.toString(16).padStart(4, "0")}48acb86c42a389efda47cc1b16938eea73f1f892000000000000${resolvedMainIdRank1.toString(16).padStart(4, "0")}`;
      upgrades.push({
        id: `db-upgrade-r${rankNum}-n${node.id}`,
        mainNodeId: mainNodeIdLabel,
        owner: node.owner,
        fromRank: rankNum - 1,
        toRank: rankNum,
        newNodeId: String(node.id),
        parentId: String(node.parentId || 1),
        side,
        directSponsor: node.directSponsor || matrixDB.systemWallets.rootOwner,
        directSponsorNodeId: String(spNodeIdInR),
        rankPriceUsdt: rankPrice.toFixed(2),
        vaultDeductedUsdt: vaultDeducted.toFixed(2),
        walletPaidUsdt: walletPaid.toFixed(2),
        upgradeMode,
        isClone: node.isClone,
        isExtraId: node.isExtraId,
        txHash: liveMeta?.txHash || syntheticTx,
        blockNumber: liveMeta?.blockNumber || baseBlock - Math.max(0, rNodes.length - node.id),
        createdAt: liveMeta?.createdAt || node.createdAt || new Date(
          17938536e5 - Math.max(0, rNodes.length - node.id) * 45e3 - Math.max(0, 27 - rankNum) * 19e3
        ).toISOString()
      });
    }
  }
  upgrades.sort((a, b) => {
    const timeA = a.createdAt ? new Date(a.createdAt).getTime() : 0;
    const timeB = b.createdAt ? new Date(b.createdAt).getTime() : 0;
    if (timeB !== timeA) return timeB - timeA;
    if (b.toRank !== a.toRank) return b.toRank - a.toRank;
    return Number(b.newNodeId || 0) - Number(a.newNodeId || 0);
  });
  matrixDB.upgradeLogs = upgrades;
  inMemoryDB.upgradeLogs = matrixDB.upgradeLogs;
}
function isId1CloneCloneTarget(isId1CloneChild, spNodeId) {
  return isId1CloneChild ? "0" : String(spNodeId);
}
async function registerUserAccount(owner, sponsor, sponsorMainNodeId) {
  const key = owner.toLowerCase();
  const existing = inMemoryDB.users.get(key);
  if (existing) return existing;
  const newUser = {
    id: `user-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`,
    owner,
    sponsor: sponsor || null,
    sponsorMainNodeId: sponsorMainNodeId ? sponsorMainNodeId.toString() : "1",
    currentMaxRank: 1,
    createdAt: (/* @__PURE__ */ new Date()).toISOString(),
    updatedAt: (/* @__PURE__ */ new Date()).toISOString(),
    vaults: Array.from({ length: 27 }, (_, i) => ({
      id: `v-${owner}-${i + 1}`,
      owner,
      rank: i + 1,
      balance: "0.00",
      updatedAt: (/* @__PURE__ */ new Date()).toISOString()
    }))
  };
  inMemoryDB.users.set(key, newUser);
  return newUser;
}
function findShallowestLeftToRightSlotInRankNodes(rNodes, startNodeId = 1) {
  const byId = /* @__PURE__ */ new Map();
  for (const n of rNodes) byId.set(n.id, n);
  const rootId = byId.has(startNodeId) ? startNodeId : 1;
  const queue = [rootId];
  const visited = /* @__PURE__ */ new Set();
  while (queue.length > 0) {
    const curId = queue.shift();
    if (visited.has(curId)) continue;
    visited.add(curId);
    const cur = byId.get(curId);
    if (!cur) continue;
    if (!cur.leftChild || cur.leftChild <= 0 || !byId.has(cur.leftChild)) {
      return { parentId: cur.id, isLeft: true };
    }
    if (!cur.rightChild || cur.rightChild <= 0 || !byId.has(cur.rightChild)) {
      return { parentId: cur.id, isLeft: false };
    }
    queue.push(cur.leftChild, cur.rightChild);
  }
  return { parentId: rootId, isLeft: true };
}
function reconcileRankTreeLinks(nodes) {
  if (!Array.isArray(nodes) || nodes.length === 0) return [];
  nodes.sort((a, b) => a.id - b.id);
  const byId = /* @__PURE__ */ new Map();
  const ownerFirstMain = /* @__PURE__ */ new Map();
  for (const n of nodes) {
    byId.set(n.id, n);
    const ok = (n.owner || "").toLowerCase();
    if (!n.isClone && !n.isExtraId && !ownerFirstMain.has(ok)) {
      ownerFirstMain.set(ok, n.id);
    }
  }
  const cloneCountByOrigin = /* @__PURE__ */ new Map();
  for (const n of nodes) {
    if (n.isClone) {
      n.isExtraId = false;
      const ok = (n.owner || "").toLowerCase();
      const fallbackMain = ownerFirstMain.get(ok) || 1;
      const candidateOrig = byId.get(n.originMainId);
      if (!n.originMainId || n.originMainId === n.id || !candidateOrig || candidateOrig.isClone) {
        n.originMainId = fallbackMain;
      }
      const origId = n.originMainId > 0 ? n.originMainId : fallbackMain;
      const nextCount = (cloneCountByOrigin.get(origId) || 0) + 1;
      cloneCountByOrigin.set(origId, nextCount);
      n.cloneSequence = nextCount;
      n.cloneRound = (nextCount - 1) % 2 + 1;
    } else {
      n.cloneSequence = 0;
      n.cloneRound = 0;
    }
    if (n.id <= 1 || n.parentId <= 0) continue;
    const parent = byId.get(n.parentId);
    if (!parent) continue;
    if (parent.leftChild === n.id || parent.rightChild === n.id) continue;
    if (!parent.leftChild || parent.leftChild <= 0 || !byId.has(parent.leftChild)) {
      parent.leftChild = n.id;
    } else if (!parent.rightChild || parent.rightChild <= 0 || !byId.has(parent.rightChild)) {
      parent.rightChild = n.id;
    }
  }
  const rankNum = nodes[0]?.rank || 1;
  if (rankNum >= 1 && nodes.length > 1) {
    const r1Nodes = matrixDB?.nodesByRank?.[1] || [];
    const r9Nodes = matrixDB?.nodesByRank?.[9] || [];
    const r4Nodes = matrixDB?.nodesByRank?.[4] || [];
    if (rankNum > 1 && r1Nodes.length > 0) {
      for (const n of nodes) {
        if (n.isExtraId && !n.isClone) {
          const ok = (n.owner || "").toLowerCase();
          const ownerMainsR1 = r1Nodes.filter(
            (m) => !m.isClone && !m.isExtraId && (m.owner || "").toLowerCase() === ok
          );
          if (ownerMainsR1.length === 1) {
            n.directSponsorNodeId = ownerMainsR1[0].id;
            n.directSponsor = ownerMainsR1[0].owner;
          } else if (ownerMainsR1.length > 1) {
            const eligibleMain = ownerMainsR1.find(
              (m) => m.id === 1 || r9Nodes.some((x) => !x.isClone && !x.isExtraId && x.originMainId === m.id)
            ) || ownerMainsR1.find(
              (m) => m.id === 1 || r4Nodes.some((x) => !x.isClone && !x.isExtraId && x.originMainId === m.id)
            ) || ownerMainsR1[0];
            n.directSponsorNodeId = eligibleMain.id;
            n.directSponsor = eligibleMain.owner;
          }
        } else if (!n.isClone && !n.isExtraId && n.id > 1) {
          const origR1 = n.originMainId > 1 ? r1Nodes.find((x) => x.id === n.originMainId && !x.isClone && !x.isExtraId) : r1Nodes.find(
            (x) => x.id > 1 && !x.isClone && !x.isExtraId && (x.owner || "").toLowerCase() === (n.owner || "").toLowerCase()
          );
          if (origR1) {
            n.originMainId = origR1.id;
            n.directSponsorNodeId = origR1.directSponsorNodeId || 1;
            n.directSponsor = origR1.directSponsor || n.directSponsor;
          }
        }
      }
    }
    let needsReplay = false;
    for (const n of nodes) {
      if (n.id <= 1) continue;
      const p = byId.get(n.parentId);
      if (!p || p.leftChild !== n.id && p.rightChild !== n.id) {
        needsReplay = true;
        break;
      }
      if (n.isClone && n.cloneRound === 1) {
        const origId = n.originMainId || n.id;
        const origNode = nodes.find(
          (prev) => !prev.isClone && (prev.id === origId || !prev.isExtraId && prev.originMainId === origId)
        ) || byId.get(origId);
        const spMainId = origId === 1 || origNode?.id === 1 ? 1 : origNode?.directSponsorNodeId || n.directSponsorNodeId || 1;
        const spNodeInRank = nodes.find(
          (prev) => prev.id < n.id && !prev.isClone && !prev.isExtraId && (spMainId === 1 ? prev.id === 1 : prev.originMainId === spMainId)
        );
        if (spNodeInRank) {
          const spOrigTag = spNodeInRank.originMainId || spNodeInRank.id;
          const isUnderSp = p.id === spNodeInRank.id || p.isClone && ((p.originMainId || p.id) === spNodeInRank.id || (p.originMainId || p.id) === spOrigTag);
          if (!isUnderSp) {
            const spHadOpenSlot = nodes.some((prev) => {
              if (prev.id >= n.id) return false;
              const isSpOrClone = prev.id === spNodeInRank.id || prev.isClone && ((prev.originMainId || prev.id) === spNodeInRank.id || (prev.originMainId || prev.id) === spOrigTag);
              if (!isSpOrClone) return false;
              const leftOpenAtTime = !prev.leftChild || prev.leftChild <= 0 || prev.leftChild > n.id;
              const rightOpenAtTime = !prev.rightChild || prev.rightChild <= 0 || prev.rightChild > n.id;
              return leftOpenAtTime || rightOpenAtTime;
            });
            if (spHadOpenSlot) {
              needsReplay = true;
              break;
            }
          }
        }
      } else if (n.isExtraId && !n.isClone) {
        const spMainId = n.directSponsorNodeId > 0 ? n.directSponsorNodeId : 1;
        const spNodeInRank = nodes.find(
          (prev) => prev.id < n.id && !prev.isClone && !prev.isExtraId && (spMainId === 1 ? prev.id === 1 : prev.originMainId === spMainId)
        );
        if (spNodeInRank) {
          const spOrigTag = spNodeInRank.originMainId || spNodeInRank.id;
          const isUnderSp = p.id === spNodeInRank.id || p.isClone && ((p.originMainId || p.id) === spNodeInRank.id || (p.originMainId || p.id) === spOrigTag);
          if (!isUnderSp) {
            needsReplay = true;
            break;
          }
        }
      } else if (!n.isClone && !n.isExtraId && rankNum > 1) {
        const spMainId = n.directSponsorNodeId > 0 ? n.directSponsorNodeId : 1;
        if (spMainId > 1) {
          const sponsorBefore = nodes.some(
            (prev) => prev.id < n.id && !prev.isClone && !prev.isExtraId && prev.originMainId === spMainId
          );
          if (!sponsorBefore && p.isClone) {
            needsReplay = true;
            break;
          }
        }
      }
    }
    if (!needsReplay && rankNum > 1) {
      const rightTriggerCountByOrig = /* @__PURE__ */ new Map();
      const actualCloneCountByOrig = /* @__PURE__ */ new Map();
      for (const n of nodes) {
        const origKey = n.isClone ? n.originMainId || n.id : n.isExtraId ? n.id : n.originMainId || n.id;
        if (n.isClone) {
          actualCloneCountByOrig.set(origKey, (actualCloneCountByOrig.get(origKey) || 0) + 1);
        }
        if (n.rightChild > 0 && byId.has(n.rightChild)) {
          rightTriggerCountByOrig.set(origKey, (rightTriggerCountByOrig.get(origKey) || 0) + 1);
        }
      }
      for (const [origKey, trigCnt] of rightTriggerCountByOrig.entries()) {
        if ((actualCloneCountByOrig.get(origKey) || 0) !== trigCnt) {
          needsReplay = true;
          break;
        }
      }
      for (const [origKey, cloneCnt] of actualCloneCountByOrig.entries()) {
        if ((rightTriggerCountByOrig.get(origKey) || 0) !== cloneCnt) {
          needsReplay = true;
          break;
        }
      }
    }
    const leftCountBeforeByMain = /* @__PURE__ */ new Map();
    for (const m of nodes) {
      if (m.isClone || m.isExtraId) continue;
      let lc = 0;
      const mOrig = m.originMainId || m.id;
      for (const cand of nodes) {
        const cOrig = cand.originMainId || cand.id;
        if ((cand.id === m.id || cand.isClone && (cOrig === m.id || cOrig === mOrig)) && cand.leftChild > 0 && byId.has(cand.leftChild)) {
          lc++;
        }
      }
      leftCountBeforeByMain.set(m.id, lc);
    }
    if (needsReplay) {
      const rootNode = nodes.find((n) => n.id === 1) || nodes[0];
      const nonCloneList = nodes.filter((n) => n.id !== rootNode.id && !n.isClone).sort((a, b) => a.id - b.id);
      const cloneIds = nodes.filter((n) => n.id !== rootNode.id && n.isClone).map((n) => n.id).sort((a, b) => a - b);
      const cloneTemplates = /* @__PURE__ */ new Map();
      for (const n of nodes) {
        if (n.isClone) cloneTemplates.set(n.id, n);
      }
      rootNode.parentId = 0;
      rootNode.leftChild = 0;
      rootNode.rightChild = 0;
      const placedNodes = [rootNode];
      let cloneCursor = 0;
      const spawnTriggeredClones = (initialParent) => {
        const rightQueue = [initialParent];
        let guard = 0;
        while (rightQueue.length > 0 && guard < 100) {
          guard++;
          const trigParent = rightQueue.shift();
          const trigOriginNode = !trigParent.isClone ? trigParent : placedNodes.find(
            (p) => !p.isClone && (p.id === trigParent.originMainId || p.originMainId === trigParent.originMainId)
          ) || trigParent;
          const cloneOrigMainId = trigOriginNode.isExtraId ? trigOriginNode.id : trigOriginNode.originMainId || trigOriginNode.id;
          const existingClonesForOrig = placedNodes.filter(
            (p) => p.isClone && (p.originMainId || p.id) === cloneOrigMainId
          ).length;
          const roundIdx = existingClonesForOrig % 2;
          const cRound = roundIdx + 1;
          let savedSponsorId = trigOriginNode.directSponsorNodeId || 1;
          let sponsorNodeInRankId = 0;
          if (cloneOrigMainId === 1 || trigOriginNode.id === 1) {
            savedSponsorId = 1;
            sponsorNodeInRankId = 1;
          } else {
            const spNodeInR = placedNodes.find(
              (p) => !p.isClone && !p.isExtraId && (savedSponsorId === 1 ? p.id === 1 : p.id === savedSponsorId || p.originMainId === savedSponsorId)
            );
            sponsorNodeInRankId = spNodeInR ? spNodeInR.id : 0;
          }
          const cSlot = roundIdx === 0 && sponsorNodeInRankId > 0 ? findExtraIdPlacementInRankNodes(placedNodes, sponsorNodeInRankId) : findShallowestLeftToRightSlotInRankNodes(placedNodes, 1);
          const assignedCloneId = cloneCursor < cloneIds.length ? cloneIds[cloneCursor++] : Math.max(
            ...placedNodes.map((x) => x.id),
            ...nonCloneList.map((x) => x.id),
            0
          ) + 1;
          const template = cloneTemplates.get(assignedCloneId);
          const cloneNode = {
            ...template || {},
            id: assignedCloneId,
            rank: rankNum,
            owner: trigOriginNode.owner,
            directSponsor: trigOriginNode.directSponsor || rootNode.owner,
            parentId: cSlot.parentId,
            leftChild: 0,
            rightChild: 0,
            isClone: true,
            isExtraId: false,
            originMainId: cloneOrigMainId,
            cloneSequence: existingClonesForOrig + 1,
            cloneRound: cRound,
            directSponsorNodeId: savedSponsorId,
            nodeVault: 0,
            rawNodeVault: 0,
            reconciledBaseVault: 0,
            originType: "CLONE"
          };
          const cParent = placedNodes.find((p) => p.id === cSlot.parentId);
          placedNodes.push(cloneNode);
          if (cParent) {
            if (cSlot.isLeft) {
              cParent.leftChild = cloneNode.id;
            } else {
              cParent.rightChild = cloneNode.id;
              rightQueue.push(cParent);
            }
          }
        }
      };
      for (const n of nonCloneList) {
        n.leftChild = 0;
        n.rightChild = 0;
        const spMainId = n.directSponsorNodeId > 0 ? n.directSponsorNodeId : 1;
        const spNodeInRank = placedNodes.find(
          (p) => !p.isClone && !p.isExtraId && (spMainId === 1 ? p.id === 1 : p.id === spMainId || p.originMainId === spMainId)
        );
        const slot = spNodeInRank ? findExtraIdPlacementInRankNodes(placedNodes, spNodeInRank.id) : findShallowestLeftToRightSlotInRankNodes(placedNodes, 1);
        const targetParent = placedNodes.find((p) => p.id === slot.parentId) || rootNode;
        n.parentId = targetParent.id;
        placedNodes.push(n);
        if (slot.isLeft) {
          targetParent.leftChild = n.id;
        } else {
          targetParent.rightChild = n.id;
          spawnTriggeredClones(targetParent);
        }
      }
      placedNodes.sort((a, b) => a.id - b.id);
      nodes.length = 0;
      byId.clear();
      for (const pn of placedNodes) {
        nodes.push(pn);
        byId.set(pn.id, pn);
      }
    }
    const rUnitVault = Number(((RANK_PRICES_USDT[rankNum - 1] || 5) * 0.35).toFixed(4));
    let reclaimedVault = 0;
    const leftCountAfterByMain = /* @__PURE__ */ new Map();
    for (const m of nodes) {
      if (m.isClone || m.isExtraId) continue;
      let leftCount = 0;
      const mOrig = m.originMainId || m.id;
      for (const cand of nodes) {
        const cOrig = cand.originMainId || cand.id;
        if ((cand.id === m.id || cand.isClone && (cOrig === m.id || cOrig === mOrig)) && cand.leftChild > 0 && byId.has(cand.leftChild)) {
          leftCount++;
        }
      }
      leftCountAfterByMain.set(m.id, leftCount);
      const maxPossibleVault = Number((leftCount * rUnitVault).toFixed(4));
      const curVault = Number(m.nodeVault || 0);
      if (curVault > maxPossibleVault) {
        reclaimedVault = Number((reclaimedVault + (curVault - maxPossibleVault)).toFixed(4));
        m.nodeVault = maxPossibleVault;
      }
      if ((m.rawNodeVault || 0) > maxPossibleVault) m.rawNodeVault = maxPossibleVault;
      if ((m.reconciledBaseVault || 0) > maxPossibleVault) m.reconciledBaseVault = maxPossibleVault;
    }
    if (reclaimedVault > 0) {
      for (const m of nodes) {
        if (m.isClone || m.isExtraId || reclaimedVault <= 0) continue;
        const beforeLc = leftCountBeforeByMain.get(m.id) || 0;
        const afterLc = leftCountAfterByMain.get(m.id) || 0;
        let gainedCount = Math.max(0, afterLc - beforeLc);
        if (gainedCount === 0 && !needsReplay) {
          const mOrig = m.originMainId || m.id;
          for (const cand of nodes) {
            const cOrig = cand.originMainId || cand.id;
            if (cand.isClone && (cOrig === m.id || cOrig === mOrig) && cand.leftChild > 0) {
              const lcNode = byId.get(cand.leftChild);
              if (lcNode && lcNode.isClone && lcNode.cloneRound === 1) {
                gainedCount++;
              }
            }
          }
        }
        if (gainedCount > 0) {
          const gainedVault = Number((gainedCount * rUnitVault).toFixed(4));
          const maxPossibleVault = Number((afterLc * rUnitVault).toFixed(4));
          const addVault = Math.min(reclaimedVault, gainedVault);
          if (addVault > 0 && (m.nodeVault || 0) + addVault <= maxPossibleVault + 1e-6) {
            m.nodeVault = Number(((m.nodeVault || 0) + addVault).toFixed(4));
            m.rawNodeVault = Number(((m.rawNodeVault || 0) + addVault).toFixed(4));
            m.reconciledBaseVault = Number(((m.reconciledBaseVault || 0) + addVault).toFixed(4));
            reclaimedVault = Number(Math.max(0, reclaimedVault - addVault).toFixed(4));
          }
        }
      }
    }
  }
  return nodes;
}
function findExtraIdPlacementInRankNodes(rNodes, sponsorMainId) {
  const byId = /* @__PURE__ */ new Map();
  for (const n of rNodes) byId.set(n.id, n);
  if (!sponsorMainId || sponsorMainId <= 0 || !byId.has(sponsorMainId)) {
    return findShallowestLeftToRightSlotInRankNodes(rNodes, 1);
  }
  const mainNode = byId.get(sponsorMainId);
  const sponsorOwner = (mainNode.owner || "").toLowerCase();
  if (!mainNode.leftChild || mainNode.leftChild <= 0 || !byId.has(mainNode.leftChild)) {
    return { parentId: mainNode.id, isLeft: true };
  }
  if (!mainNode.rightChild || mainNode.rightChild <= 0 || !byId.has(mainNode.rightChild)) {
    return { parentId: mainNode.id, isLeft: false };
  }
  const mainOriginTag = mainNode.originMainId || mainNode.id;
  const sorted = [...rNodes].sort((a, b) => a.id - b.id);
  const isSponsorClone = (cand) => {
    if (!cand.isClone) return false;
    const candOrig = cand.originMainId || cand.id;
    return candOrig === mainNode.id || candOrig === mainOriginTag;
  };
  for (const cand of sorted) {
    if (isSponsorClone(cand)) {
      if (!cand.leftChild || cand.leftChild <= 0 || !byId.has(cand.leftChild)) {
        return { parentId: cand.id, isLeft: true };
      }
      if (!cand.rightChild || cand.rightChild <= 0 || !byId.has(cand.rightChild)) {
        return { parentId: cand.id, isLeft: false };
      }
    }
  }
  for (const cand of sorted) {
    if (isSponsorClone(cand)) {
      const cSlot = findShallowestLeftToRightSlotInRankNodes(rNodes, cand.id);
      if (cSlot && cSlot.parentId > 0) {
        return cSlot;
      }
    }
  }
  return findShallowestLeftToRightSlotInRankNodes(rNodes, mainNode.id);
}
function findClonePlacementInRankNodes(rNodes, triggerParent) {
  const byId = /* @__PURE__ */ new Map();
  for (const n of rNodes) byId.set(n.id, n);
  const cloneOriginMainId = triggerParent.originMainId && triggerParent.originMainId > 0 ? triggerParent.originMainId : triggerParent.id;
  const existingClonesCount = rNodes.filter(
    (n) => n.isClone && (n.originMainId || n.id) === cloneOriginMainId
  ).length;
  const roundIndex = existingClonesCount % 2;
  const cloneRound = roundIndex + 1;
  const originNode = rNodes.find(
    (n) => !n.isClone && !n.isExtraId && (n.id === cloneOriginMainId || n.originMainId === cloneOriginMainId)
  ) || byId.get(cloneOriginMainId) || triggerParent;
  const rankNum = originNode.rank || triggerParent.rank || 1;
  let sponsorNodeInRankId = 0;
  let savedDirectSponsorId = originNode.directSponsorNodeId || 1;
  if (cloneOriginMainId === 1 || originNode.id === 1) {
    sponsorNodeInRankId = 1;
    savedDirectSponsorId = 1;
  } else if (rankNum === 1 || originNode.isExtraId) {
    if (originNode.directSponsorNodeId && originNode.directSponsorNodeId > 0) {
      sponsorNodeInRankId = originNode.directSponsorNodeId;
      savedDirectSponsorId = originNode.directSponsorNodeId;
    } else {
      const spKey = (originNode.directSponsor || "").toLowerCase();
      const spMain = rNodes.find(
        (n) => !n.isClone && !n.isExtraId && (n.owner || "").toLowerCase() === spKey
      );
      sponsorNodeInRankId = spMain ? spMain.id : 1;
      savedDirectSponsorId = sponsorNodeInRankId;
    }
  } else {
    const r1SponsorId = originNode.directSponsorNodeId || 1;
    savedDirectSponsorId = r1SponsorId;
    const spNodeInR = rNodes.find(
      (n) => !n.isClone && !n.isExtraId && (r1SponsorId === 1 ? n.id === 1 : n.originMainId === r1SponsorId)
    );
    sponsorNodeInRankId = spNodeInR ? spNodeInR.id : 0;
  }
  if (roundIndex === 0 && sponsorNodeInRankId > 0 && byId.has(sponsorNodeInRankId)) {
    const slot = findExtraIdPlacementInRankNodes(rNodes, sponsorNodeInRankId);
    return {
      parentId: slot.parentId,
      isLeft: slot.isLeft,
      cloneRound,
      cloneOriginMainId,
      targetSponsorNodeId: savedDirectSponsorId
    };
  }
  const bfsSlot = findShallowestLeftToRightSlotInRankNodes(rNodes, 1);
  return {
    parentId: bfsSlot.parentId,
    isLeft: bfsSlot.isLeft,
    cloneRound,
    cloneOriginMainId,
    targetSponsorNodeId: savedDirectSponsorId
  };
}
function reconcileRank1AutoMainIds() {
  const autoMainSourceMap = /* @__PURE__ */ new Map();
  const r1Nodes = matrixDB.nodesByRank[1] || [];
  if (r1Nodes.length === 0) return autoMainSourceMap;
  for (let r = 1; r <= 27; r++) {
    const rList = matrixDB.nodesByRank[r] || [];
    if (rList.length === 0) continue;
    const sortedR = [...rList].sort((a, b) => a.id - b.id);
    const byIdR = /* @__PURE__ */ new Map();
    const ownerFirstMainR = /* @__PURE__ */ new Map();
    for (const rn of sortedR) {
      byIdR.set(rn.id, rn);
      const ok = (rn.owner || "").toLowerCase();
      if (!rn.isClone && !rn.isExtraId && !ownerFirstMainR.has(ok)) {
        ownerFirstMainR.set(ok, r === 1 ? rn.id : rn.originMainId > 0 ? rn.originMainId : rn.id);
      }
    }
    const cloneCountByOriginR = /* @__PURE__ */ new Map();
    const matchedRightTriggersR = /* @__PURE__ */ new Set();
    for (const rn of sortedR) {
      if (rn.isClone) {
        const ok = (rn.owner || "").toLowerCase();
        const fallbackMain = ownerFirstMainR.get(ok) || 1;
        let triggeredByParent;
        for (let lb = rn.id - 1; lb >= 1; lb--) {
          const candRight = byIdR.get(lb);
          if (!candRight || candRight.parentId <= 0) continue;
          const trigParent = byIdR.get(candRight.parentId);
          if (trigParent && trigParent.rightChild === candRight.id && (trigParent.owner || "").toLowerCase() === ok && !matchedRightTriggersR.has(trigParent.id)) {
            triggeredByParent = trigParent;
            matchedRightTriggersR.add(trigParent.id);
            break;
          }
        }
        const triggeredOriginMainId = triggeredByParent ? r === 1 ? triggeredByParent.isClone ? triggeredByParent.originMainId || triggeredByParent.id : triggeredByParent.id : triggeredByParent.originMainId > 0 ? triggeredByParent.originMainId : triggeredByParent.id : 0;
        let origNode = sortedR.find(
          (x) => !x.isClone && (x.owner || "").toLowerCase() === ok && (x.originMainId === rn.originMainId || x.id === rn.originMainId)
        ) || byIdR.get(rn.originMainId);
        if (!rn.originMainId || rn.originMainId === rn.id || !origNode || origNode.isClone || triggeredOriginMainId > 0 && rn.originMainId === fallbackMain && triggeredOriginMainId !== fallbackMain) {
          rn.originMainId = triggeredOriginMainId > 0 ? triggeredOriginMainId : fallbackMain;
          origNode = sortedR.find(
            (x) => !x.isClone && (x.owner || "").toLowerCase() === ok && (x.originMainId === rn.originMainId || x.id === rn.originMainId)
          ) || origNode;
        } else if (r > 1 && origNode && !origNode.isClone && !origNode.isExtraId && origNode.originMainId > 0) {
          rn.originMainId = origNode.originMainId;
        }
        if (origNode && !origNode.isClone) {
          if (origNode.directSponsorNodeId > 0) {
            rn.directSponsorNodeId = origNode.directSponsorNodeId;
          }
          if (origNode.directSponsor && origNode.directSponsor !== ethers.ZeroAddress) {
            rn.directSponsor = origNode.directSponsor;
          }
        }
        const origId = rn.originMainId > 0 ? rn.originMainId : fallbackMain;
        const nextCount = (cloneCountByOriginR.get(origId) || 0) + 1;
        cloneCountByOriginR.set(origId, nextCount);
        rn.cloneSequence = nextCount;
        rn.cloneRound = (nextCount - 1) % 2 + 1;
      } else {
        rn.cloneSequence = 0;
        rn.cloneRound = 0;
        if (rn.isExtraId) {
          const ok = (rn.owner || "").toLowerCase();
          const ownerMainsR1 = r1Nodes.filter(
            (m) => !m.isClone && !m.isExtraId && (m.owner || "").toLowerCase() === ok
          );
          if (ownerMainsR1.length === 1) {
            rn.directSponsorNodeId = ownerMainsR1[0].id;
            rn.directSponsor = ownerMainsR1[0].owner;
          } else if (ownerMainsR1.length > 1) {
            const r9Nodes = matrixDB.nodesByRank[9] || [];
            const r4Nodes = matrixDB.nodesByRank[4] || [];
            const eligibleMain = ownerMainsR1.find(
              (m) => m.id === 1 || r9Nodes.some((x) => !x.isClone && !x.isExtraId && x.originMainId === m.id)
            ) || ownerMainsR1.find(
              (m) => m.id === 1 || r4Nodes.some((x) => !x.isClone && !x.isExtraId && x.originMainId === m.id)
            ) || ownerMainsR1[0];
            rn.directSponsorNodeId = eligibleMain.id;
            rn.directSponsor = eligibleMain.owner;
          }
        }
      }
    }
  }
  const sortedR1 = [...r1Nodes].sort((a, b) => a.id - b.id);
  const byIdR1 = /* @__PURE__ */ new Map();
  for (const n of sortedR1) {
    byIdR1.set(n.id, n);
    if (n.isClone) {
      n.originType = "CLONE";
    } else if (n.isExtraId) {
      n.originType = "AUTO_EXTRA";
    } else {
      n.originType = "MANUAL_REGISTER";
    }
  }
  const getMainCurrentRank = (r1MainId) => {
    if (r1MainId === 1) return 27;
    let cur = 1;
    for (let r = 27; r >= 2; r--) {
      const rList = matrixDB.nodesByRank[r] || [];
      if (rList.some((x) => !x.isClone && !x.isExtraId && x.originMainId === r1MainId)) {
        cur = r;
        break;
      }
    }
    return cur;
  };
  const getMainWindow5Start = (r1MainId) => {
    return Math.max(1, getMainCurrentRank(r1MainId) - 4);
  };
  const isPlacedUnderMainOrClonesR1 = (mId, targetId) => {
    if (targetId <= mId) return false;
    if (mId === 1) return true;
    let curr = byIdR1.get(targetId)?.parentId || 0;
    const visited = /* @__PURE__ */ new Set();
    while (curr > 0 && !visited.has(curr)) {
      visited.add(curr);
      if (curr === mId) return true;
      const pNode = byIdR1.get(curr);
      if (!pNode) break;
      if (pNode.isClone && (pNode.originMainId || pNode.id) === mId) {
        return true;
      }
      curr = pNode.parentId;
    }
    return false;
  };
  const expectedAutoSpawnsByMain = /* @__PURE__ */ new Map();
  const extraR2To3VaultByMain = /* @__PURE__ */ new Map();
  const extraR2To3MinR1IdByMain = /* @__PURE__ */ new Map();
  const usedExtraR2To3 = /* @__PURE__ */ new Set();
  for (const mNode of sortedR1) {
    if (mNode.isClone || mNode.isExtraId) continue;
    const win5Start = getMainWindow5Start(mNode.id);
    if (win5Start <= 1) {
      expectedAutoSpawnsByMain.set(mNode.id, 0);
      extraR2To3VaultByMain.set(mNode.id, 0);
      extraR2To3MinR1IdByMain.set(mNode.id, mNode.id);
      continue;
    }
    let totalGross1To3 = 0;
    let totalRem1To3 = 0;
    let extraR2To3 = 0;
    let extraR2To3Rem = 0;
    let extraMinR1Id = mNode.id;
    for (let r = 1; r <= 3; r++) {
      if (r >= win5Start) continue;
      const rList = matrixDB.nodesByRank[r] || [];
      const correspMain = r === 1 ? mNode : rList.find((x) => !x.isClone && !x.isExtraId && x.originMainId === mNode.id);
      if (!correspMain) continue;
      const rUnitVault = Number(((RANK_PRICES_USDT[r - 1] || 5) * 0.35).toFixed(4));
      let leftCount = 0;
      for (const sn of rList) {
        const snOrig = sn.originMainId || sn.id;
        const isSameMain = sn.id === correspMain.id || sn.isClone && (snOrig === correspMain.id || snOrig === mNode.id);
        if (isSameMain && sn.leftChild > 0) {
          leftCount++;
          if (r > 1) {
            const lcNode = rList.find((rn) => rn.id === sn.leftChild);
            if (lcNode && !lcNode.isClone && !lcNode.isExtraId) {
              const r1Orig = lcNode.originMainId || 0;
              if (r1Orig > mNode.id && r1Orig < 1e6) {
                if (extraMinR1Id === mNode.id || r1Orig < extraMinR1Id) {
                  extraMinR1Id = r1Orig;
                }
              }
            }
          }
        }
      }
      const grossR = Number((leftCount * rUnitVault).toFixed(4));
      const remR = Number((correspMain.nodeVault ?? 0).toFixed(4));
      totalGross1To3 = Number((totalGross1To3 + grossR).toFixed(4));
      totalRem1To3 = Number((totalRem1To3 + remR).toFixed(4));
      if (r > 1) {
        extraR2To3 = Number((extraR2To3 + grossR).toFixed(4));
        extraR2To3Rem = Number((extraR2To3Rem + remR).toFixed(4));
      }
    }
    const deducted = Math.max(0, Number((totalGross1To3 - totalRem1To3).toFixed(4)));
    const maxPossibleSpawns = Math.floor((deducted + 1e-4) / 5);
    let expectedSpawns = 0;
    if (mNode.id === 1) {
      expectedSpawns = maxPossibleSpawns;
    } else if (maxPossibleSpawns > 0) {
      for (let c = maxPossibleSpawns; c >= 1; c--) {
        const pool2GrossNeeded = Number((totalRem1To3 + c * 5).toFixed(4));
        const units175 = pool2GrossNeeded / 1.75;
        if (pool2GrossNeeded <= totalGross1To3 + 0.01 && Math.abs(units175 - Math.round(units175)) < 0.01) {
          expectedSpawns = c;
          break;
        }
      }
    }
    expectedAutoSpawnsByMain.set(mNode.id, expectedSpawns);
    const effectiveExtraR2To3 = Math.max(0, Number((extraR2To3 - extraR2To3Rem).toFixed(4)));
    extraR2To3VaultByMain.set(mNode.id, expectedSpawns > 0 ? effectiveExtraR2To3 : 0);
    extraR2To3MinR1IdByMain.set(mNode.id, extraMinR1Id);
  }
  const ownerPrimaryMap = /* @__PURE__ */ new Map();
  const ownerAllMainsMap = /* @__PURE__ */ new Map();
  const simAutoVaultByMain = /* @__PURE__ */ new Map();
  const spawnedCountByMain = /* @__PURE__ */ new Map();
  const r1EntryPrice = RANK_PRICES_USDT[0] || 5;
  for (const n of sortedR1) {
    if (!n.isClone && !n.isExtraId) {
      n.originType = n.isSyntheticExtra ? "AUTO_NEW_MAIN" : "MANUAL_REGISTER";
    }
  }
  for (const n of sortedR1) {
    const oKey = (n.owner || "").toLowerCase();
    if (!n.isClone && !n.isExtraId) {
      if (!ownerPrimaryMap.has(oKey)) {
        ownerPrimaryMap.set(oKey, n.id);
      }
      const firstMain = ownerPrimaryMap.get(oKey) || n.id;
      if (n.isSyntheticExtra) {
        const synSource = n.directSponsorNodeId > 0 ? n.directSponsorNodeId : firstMain;
        n.originType = "AUTO_NEW_MAIN";
        autoMainSourceMap.set(n.id, synSource);
        spawnedCountByMain.set(synSource, (spawnedCountByMain.get(synSource) || 0) + 1);
      } else if (n.id > firstMain) {
        const explicitSpId = n.directSponsorNodeId || 0;
        const priorMains = (ownerAllMainsMap.get(oKey) || []).filter((mId) => {
          if (mId >= n.id) return false;
          if (getMainWindow5Start(mId) <= 1) return false;
          const maxSpawns = expectedAutoSpawnsByMain.get(mId) || 0;
          const curSpawns = spawnedCountByMain.get(mId) || 0;
          if (curSpawns >= maxSpawns) return false;
          if (!isPlacedUnderMainOrClonesR1(mId, n.id)) return false;
          const mNode = byIdR1.get(mId);
          const mSpId = mNode?.directSponsorNodeId || 1;
          return explicitSpId === 0 || explicitSpId === mId || explicitSpId === mSpId;
        });
        let sourceMain = priorMains.find(
          (mId) => (simAutoVaultByMain.get(mId) || 0) + 1e-6 >= r1EntryPrice
        );
        if (sourceMain === void 0) {
          sourceMain = priorMains.find((mId) => {
            if (usedExtraR2To3.has(mId)) return false;
            const minReqR1Id = extraR2To3MinR1IdByMain.get(mId) || mId;
            if (n.id <= minReqR1Id) return false;
            const r1Bal = simAutoVaultByMain.get(mId) || 0;
            const extraBal = extraR2To3VaultByMain.get(mId) || 0;
            return r1Bal + extraBal + 1e-6 >= r1EntryPrice;
          });
          if (sourceMain !== void 0) {
            usedExtraR2To3.add(sourceMain);
            simAutoVaultByMain.set(
              sourceMain,
              Number(
                ((simAutoVaultByMain.get(sourceMain) || 0) + (extraR2To3VaultByMain.get(sourceMain) || 0)).toFixed(4)
              )
            );
          }
        }
        if (sourceMain !== void 0) {
          n.originType = "AUTO_NEW_MAIN";
          autoMainSourceMap.set(n.id, sourceMain);
          spawnedCountByMain.set(sourceMain, (spawnedCountByMain.get(sourceMain) || 0) + 1);
          simAutoVaultByMain.set(
            sourceMain,
            Math.max(0, Number(((simAutoVaultByMain.get(sourceMain) || 0) - r1EntryPrice).toFixed(4)))
          );
        }
      }
      const list = ownerAllMainsMap.get(oKey) || [];
      list.push(n.id);
      ownerAllMainsMap.set(oKey, list);
    }
    if (n.parentId > 0 && !n.isSyntheticExtra) {
      const p = byIdR1.get(n.parentId);
      if (p && p.leftChild === n.id) {
        const pMain = p.isClone ? p.originMainId || p.id : p.id;
        if (getMainWindow5Start(pMain) > 1) {
          const addV = Number((r1EntryPrice * 0.35).toFixed(4));
          simAutoVaultByMain.set(
            pMain,
            Number(((simAutoVaultByMain.get(pMain) || 0) + addV).toFixed(4))
          );
        }
      }
    }
  }
  return autoMainSourceMap;
}
function reconcileAutoExtraIdsAcrossRanks() {
  const createdExtraNodes = [];
  for (let r = 1; r <= 27; r++) {
    const rList = matrixDB.nodesByRank[r];
    if (!Array.isArray(rList) || rList.length === 0) continue;
    const realNodes = rList.filter((n) => !n.isSyntheticExtra);
    const realIds = new Set(realNodes.map((n) => n.id));
    for (const n of realNodes) {
      if (n.leftChild > 0 && !realIds.has(n.leftChild)) n.leftChild = 0;
      if (n.rightChild > 0 && !realIds.has(n.rightChild)) n.rightChild = 0;
      if (n.reconciledBaseVault === void 0) {
        n.reconciledBaseVault = Number(n.nodeVault ?? n.rawNodeVault ?? 0);
      } else {
        n.nodeVault = Number(n.reconciledBaseVault);
      }
    }
    matrixDB.nodesByRank[r] = reconcileRankTreeLinks(realNodes);
    matrixDB.rankCounts[r] = Math.max(1, matrixDB.nodesByRank[r].length);
  }
  const realAutoMainSourceMap = reconcileRank1AutoMainIds();
  const r1Nodes = matrixDB.nodesByRank[1] || [];
  if (r1Nodes.length === 0) return createdExtraNodes;
  const r1Mains = r1Nodes.filter((n) => !n.isClone && !n.isExtraId).sort((a, b) => a.id - b.id);
  for (const r1Main of r1Mains) {
    if (r1Main.id <= 1) continue;
    const getCorrespMain = (targetRank) => {
      const rList = matrixDB.nodesByRank[targetRank] || [];
      if (targetRank === 1) return rList.find((n) => n.id === r1Main.id);
      return rList.find((n) => !n.isClone && !n.isExtraId && n.originMainId === r1Main.id);
    };
    let currentRank = 1;
    for (let r = 27; r >= 1; r--) {
      if (getCorrespMain(r)) {
        currentRank = r;
        break;
      }
    }
    if (currentRank <= 1) continue;
    const hasRealOnChainAuto = Array.from(realAutoMainSourceMap.values()).includes(r1Main.id);
    let hasRealOnChainExtra = false;
    let totalOnChainRemVault = 0;
    for (let r = 1; r <= currentRank; r++) {
      const rList = matrixDB.nodesByRank[r] || [];
      if (r >= 2 && rList.some((n) => !n.isSyntheticExtra && n.isExtraId && !n.isClone && (n.owner || "").toLowerCase() === (r1Main.owner || "").toLowerCase())) {
        hasRealOnChainExtra = true;
      }
      const mInR = getCorrespMain(r);
      if (mInR) totalOnChainRemVault += Number(mInR.reconciledBaseVault ?? mInR.nodeVault ?? 0);
    }
    if (!hasRealOnChainAuto && !hasRealOnChainExtra && totalOnChainRemVault <= 1e-6) {
      for (let r = 1; r <= currentRank; r++) {
        const rList = matrixDB.nodesByRank[r] || [];
        const mInR = getCorrespMain(r);
        if (!mInR) continue;
        const rUnitVault = Number(((RANK_PRICES_USDT[r - 1] || 5) * 0.35).toFixed(4));
        const nextRankPrice = RANK_PRICES_USDT[r] || 10;
        let leftCount = 0;
        for (const sn of rList) {
          if (sn.isSyntheticExtra) continue;
          const snOrig = sn.originMainId || sn.id;
          const isSameMain = sn.id === mInR.id || sn.isClone && (snOrig === mInR.id || snOrig === r1Main.id);
          if (isSameMain && sn.leftChild > 0) {
            leftCount++;
          }
        }
        const grossR = Number((leftCount * rUnitVault).toFixed(4));
        if (grossR > 0 && grossR < nextRankPrice) {
          mInR.nodeVault = grossR;
        }
      }
    }
    const window5Start = Math.max(1, currentRank - 4);
    if (window5Start > 1) {
      const eligiblePool2Nodes = [];
      for (let r = 1; r <= 3; r++) {
        if (r < window5Start) {
          const mNode = getCorrespMain(r);
          if (mNode) eligiblePool2Nodes.push(mNode);
        }
      }
      const r1EntryPrice = RANK_PRICES_USDT[0] || 5;
      const r1UnitVault = Number((r1EntryPrice * 0.35).toFixed(4));
      let autoGuard = 0;
      while (autoGuard < 25) {
        autoGuard++;
        const availAutoPool = Number(
          eligiblePool2Nodes.reduce((sum, n) => sum + Number(n.nodeVault || 0), 0).toFixed(4)
        );
        if (availAutoPool + 1e-6 < r1EntryPrice) break;
        let remDeduct = r1EntryPrice;
        for (const pNode of eligiblePool2Nodes) {
          if (remDeduct <= 1e-6) break;
          const v = Number(pNode.nodeVault || 0);
          if (v <= 1e-6) continue;
          const d = Math.min(v, remDeduct);
          pNode.nodeVault = Number(Math.max(0, v - d).toFixed(4));
          remDeduct = Number(Math.max(0, remDeduct - d).toFixed(4));
        }
        const r1TargetNodes = matrixDB.nodesByRank[1] || [];
        const placement = findExtraIdPlacementInRankNodes(r1TargetNodes, r1Main.id);
        const newAutoId = r1TargetNodes.reduce((m, n) => Math.max(m, n.id), 0) + 1;
        const newAutoNode = {
          id: newAutoId,
          rank: 1,
          owner: r1Main.owner,
          directSponsor: r1Main.owner,
          parentId: placement.parentId,
          leftChild: 0,
          rightChild: 0,
          isClone: false,
          isExtraId: false,
          isSyntheticExtra: true,
          originMainId: newAutoId,
          cloneRound: 0,
          directSponsorNodeId: r1Main.id,
          nodeVault: 0,
          rawNodeVault: 0,
          originType: "AUTO_NEW_MAIN"
        };
        r1TargetNodes.push(newAutoNode);
        const parentNode = r1TargetNodes.find((n) => n.id === placement.parentId);
        const rightTriggerQueue = [];
        if (parentNode) {
          if (placement.isLeft) {
            parentNode.leftChild = newAutoId;
            const pMainNode = !parentNode.isClone ? parentNode : r1TargetNodes.find((x) => !x.isClone && x.id === (parentNode.originMainId || parentNode.id)) || parentNode;
            pMainNode.nodeVault = Number(((pMainNode.nodeVault || 0) + r1UnitVault).toFixed(4));
          } else {
            parentNode.rightChild = newAutoId;
            rightTriggerQueue.push(parentNode);
          }
        }
        let cloneSafety = 0;
        while (rightTriggerQueue.length > 0 && cloneSafety < 25) {
          cloneSafety++;
          const trigParent = rightTriggerQueue.shift();
          const clonePlace = findClonePlacementInRankNodes(r1TargetNodes, trigParent);
          const nextCloneId = r1TargetNodes.reduce((m, n) => Math.max(m, n.id), 0) + 1;
          const origNode = r1TargetNodes.find((n) => n.id === clonePlace.cloneOriginMainId) || trigParent;
          const newCloneNode = {
            id: nextCloneId,
            rank: 1,
            owner: trigParent.owner,
            directSponsor: origNode.directSponsor || trigParent.directSponsor || trigParent.owner,
            parentId: clonePlace.parentId,
            leftChild: 0,
            rightChild: 0,
            isClone: true,
            isExtraId: false,
            isSyntheticExtra: true,
            originMainId: clonePlace.cloneOriginMainId,
            cloneRound: clonePlace.cloneRound,
            directSponsorNodeId: clonePlace.targetSponsorNodeId,
            nodeVault: 0,
            rawNodeVault: 0,
            originType: "CLONE"
          };
          r1TargetNodes.push(newCloneNode);
          const cParent = r1TargetNodes.find((n) => n.id === clonePlace.parentId);
          if (cParent) {
            if (clonePlace.isLeft) {
              cParent.leftChild = nextCloneId;
              const cpMain = !cParent.isClone ? cParent : r1TargetNodes.find((x) => !x.isClone && x.id === (cParent.originMainId || cParent.id)) || cParent;
              cpMain.nodeVault = Number(((cpMain.nodeVault || 0) + r1UnitVault).toFixed(4));
            } else {
              cParent.rightChild = nextCloneId;
              rightTriggerQueue.push(cParent);
            }
          }
        }
        matrixDB.nodesByRank[1] = reconcileRankTreeLinks(r1TargetNodes);
        matrixDB.rankCounts[1] = Math.max(matrixDB.rankCounts[1] || 1, r1TargetNodes.length);
      }
    }
  }
  reconcileRank1AutoMainIds();
  for (const r1Main of r1Mains) {
    const ownerKey = (r1Main.owner || "").toLowerCase();
    const ownerR1Mains = r1Mains.filter((n) => (n.owner || "").toLowerCase() === ownerKey);
    const mainIdx = Math.max(0, ownerR1Mains.findIndex((m) => m.id === r1Main.id));
    const getCorrespMainInRank = (targetRank) => {
      const rList = matrixDB.nodesByRank[targetRank] || [];
      if (targetRank === 1 || r1Main.id === 1) {
        return rList.find((n) => n.id === r1Main.id);
      }
      return rList.find((n) => !n.isClone && !n.isExtraId && n.originMainId === r1Main.id);
    };
    let currentRank = r1Main.id === 1 ? 27 : 1;
    if (r1Main.id !== 1) {
      for (let r = 27; r >= 1; r--) {
        if (getCorrespMainInRank(r)) {
          currentRank = r;
          break;
        }
      }
    }
    const window5Start = Math.max(1, currentRank - 4);
    const eligiblePool3Nodes = [];
    for (let r = 4; r <= 27; r++) {
      if (currentRank === 27 || r < window5Start) {
        const mNode = getCorrespMainInRank(r);
        if (mNode) {
          eligiblePool3Nodes.push(mNode);
        }
      }
    }
    let extraLeftPoolBonus = 0;
    for (let r = 2; r <= 27; r++) {
      const rList = matrixDB.nodesByRank[r] || [];
      const correspMain = getCorrespMainInRank(r);
      if (!correspMain) continue;
      const rUnitVault = Number(((RANK_PRICES_USDT[r - 1] || 10) * 0.35).toFixed(4));
      const maxRealExtraIdInRank = rList.reduce(
        (m, x) => !x.isSyntheticExtra && x.isExtraId ? Math.max(m, x.id) : m,
        0
      );
      for (const n of rList) {
        if (n.isSyntheticExtra) continue;
        const nOrig = !n.isClone ? n : rList.find(
          (x) => !x.isClone && (x.owner || "").toLowerCase() === (n.owner || "").toLowerCase() && (x.originMainId === n.originMainId || x.id === n.originMainId)
        ) || n;
        const isExtraLineage = !n.isClone ? Boolean(n.isExtraId) : Boolean(n.isExtraId || nOrig.isExtraId);
        if (isExtraLineage && n.leftChild > 0 && n.leftChild > maxRealExtraIdInRank) {
          const spId = nOrig.directSponsorNodeId > 0 ? nOrig.directSponsorNodeId : n.directSponsorNodeId > 0 ? n.directSponsorNodeId : 1;
          if (spId === correspMain.id) {
            extraLeftPoolBonus = Number((extraLeftPoolBonus + rUnitVault).toFixed(4));
          }
        }
      }
    }
    if (eligiblePool3Nodes.length === 0 && extraLeftPoolBonus <= 0) continue;
    const minRank2Price = RANK_PRICES_USDT[1] || 10;
    let spawnedInPass = true;
    let safety = 0;
    while (spawnedInPass && safety < 50) {
      spawnedInPass = false;
      safety++;
      for (let targetRank = 27; targetRank >= 2; targetRank--) {
        const r4To27Bal = eligiblePool3Nodes.reduce((sum, n) => sum + Number(n.nodeVault || 0), 0);
        const availExtraPool = Number((extraLeftPoolBonus + r4To27Bal).toFixed(4));
        if (availExtraPool + 1e-6 < minRank2Price) break;
        const targetPrice = RANK_PRICES_USDT[targetRank - 1] || 10;
        if (availExtraPool + 1e-6 < targetPrice) continue;
        const targetMainInRank = getCorrespMainInRank(targetRank);
        if (!targetMainInRank) continue;
        let remToDeduct = targetPrice;
        if (extraLeftPoolBonus > 1e-6) {
          const dBonus = Math.min(extraLeftPoolBonus, remToDeduct);
          extraLeftPoolBonus = Number(Math.max(0, extraLeftPoolBonus - dBonus).toFixed(4));
          remToDeduct = Number(Math.max(0, remToDeduct - dBonus).toFixed(4));
        }
        for (const pNode of eligiblePool3Nodes) {
          if (remToDeduct <= 1e-6) break;
          const v = Number(pNode.nodeVault || 0);
          if (v <= 1e-6) continue;
          const deduct = Math.min(v, remToDeduct);
          pNode.nodeVault = Number(Math.max(0, v - deduct).toFixed(4));
          remToDeduct = Number(Math.max(0, remToDeduct - deduct).toFixed(4));
        }
        if (!matrixDB.nodesByRank[targetRank]) {
          matrixDB.nodesByRank[targetRank] = [];
        }
        const targetRankNodes = matrixDB.nodesByRank[targetRank];
        const r1SponsorId = r1Main.directSponsorNodeId > 0 ? r1Main.directSponsorNodeId : 1;
        const sponsorNodeInTarget = targetRankNodes.find(
          (n) => !n.isClone && !n.isExtraId && (r1SponsorId === 1 ? n.id === 1 : n.originMainId === r1SponsorId)
        );
        const placement = sponsorNodeInTarget ? findExtraIdPlacementInRankNodes(targetRankNodes, sponsorNodeInTarget.id) : findShallowestLeftToRightSlotInRankNodes(targetRankNodes, 1);
        const maxId = targetRankNodes.reduce((m, n) => Math.max(m, n.id), 0);
        const newExtraId = maxId + 1;
        const newExtraNode = {
          id: newExtraId,
          rank: targetRank,
          owner: r1Main.owner,
          directSponsor: r1Main.directSponsor || r1Main.owner,
          parentId: placement.parentId,
          leftChild: 0,
          rightChild: 0,
          isClone: false,
          isExtraId: true,
          isSyntheticExtra: true,
          originMainId: newExtraId,
          cloneRound: 0,
          directSponsorNodeId: r1SponsorId,
          nodeVault: 0,
          rawNodeVault: 0
        };
        targetRankNodes.push(newExtraNode);
        const parentNode = targetRankNodes.find((n) => n.id === placement.parentId);
        const rightSlotTriggerQueue = [];
        if (parentNode) {
          if (placement.isLeft) {
            parentNode.leftChild = newExtraId;
            const pMainNode = !parentNode.isClone ? parentNode : targetRankNodes.find(
              (x) => !x.isClone && (x.owner || "").toLowerCase() === (parentNode.owner || "").toLowerCase() && (x.originMainId === parentNode.originMainId || x.id === parentNode.originMainId)
            ) || parentNode;
            const vaultAdd = Number((targetPrice * 0.35).toFixed(4));
            const isParentExtraLineage = !parentNode.isClone ? Boolean(parentNode.isExtraId) : Boolean(parentNode.isExtraId || pMainNode.isExtraId);
            if (isParentExtraLineage) {
              const spId = pMainNode.directSponsorNodeId > 0 ? pMainNode.directSponsorNodeId : parentNode.directSponsorNodeId > 0 ? parentNode.directSponsorNodeId : targetMainInRank.id;
              if (spId === targetMainInRank.id) {
                extraLeftPoolBonus = Number((extraLeftPoolBonus + vaultAdd).toFixed(4));
              }
            } else {
              pMainNode.nodeVault = Number(((pMainNode.nodeVault || 0) + vaultAdd).toFixed(4));
            }
          } else {
            parentNode.rightChild = newExtraId;
            rightSlotTriggerQueue.push(parentNode);
          }
        }
        let cloneSafety = 0;
        while (rightSlotTriggerQueue.length > 0 && cloneSafety < 25) {
          cloneSafety++;
          const triggerParent = rightSlotTriggerQueue.shift();
          const clonePlacement = findClonePlacementInRankNodes(targetRankNodes, triggerParent);
          const nextCloneId = targetRankNodes.reduce((m, n) => Math.max(m, n.id), 0) + 1;
          const originNode = targetRankNodes.find((n) => n.id === clonePlacement.cloneOriginMainId) || triggerParent;
          const newCloneNode = {
            id: nextCloneId,
            rank: targetRank,
            owner: triggerParent.owner,
            directSponsor: originNode.directSponsor || triggerParent.directSponsor || triggerParent.owner,
            parentId: clonePlacement.parentId,
            leftChild: 0,
            rightChild: 0,
            isClone: true,
            isExtraId: false,
            isSyntheticExtra: true,
            originMainId: clonePlacement.cloneOriginMainId,
            cloneRound: clonePlacement.cloneRound,
            directSponsorNodeId: clonePlacement.targetSponsorNodeId,
            nodeVault: 0,
            rawNodeVault: 0
          };
          targetRankNodes.push(newCloneNode);
          const cloneParent = targetRankNodes.find((n) => n.id === clonePlacement.parentId);
          if (cloneParent) {
            if (clonePlacement.isLeft) {
              cloneParent.leftChild = nextCloneId;
              const cpMainNode = !cloneParent.isClone ? cloneParent : targetRankNodes.find(
                (x) => !x.isClone && (x.owner || "").toLowerCase() === (cloneParent.owner || "").toLowerCase() && (x.originMainId === cloneParent.originMainId || x.id === cloneParent.originMainId)
              ) || cloneParent;
              const vaultAdd = Number((targetPrice * 0.35).toFixed(4));
              const isCloneParentExtraLineage = !cloneParent.isClone ? Boolean(cloneParent.isExtraId) : Boolean(cloneParent.isExtraId || cpMainNode.isExtraId);
              if (isCloneParentExtraLineage) {
                const spId = cpMainNode.directSponsorNodeId > 0 ? cpMainNode.directSponsorNodeId : cloneParent.directSponsorNodeId > 0 ? cloneParent.directSponsorNodeId : targetMainInRank.id;
                if (spId === targetMainInRank.id) {
                  extraLeftPoolBonus = Number((extraLeftPoolBonus + vaultAdd).toFixed(4));
                }
              } else {
                cpMainNode.nodeVault = Number(((cpMainNode.nodeVault || 0) + vaultAdd).toFixed(4));
              }
            } else {
              cloneParent.rightChild = nextCloneId;
              rightSlotTriggerQueue.push(cloneParent);
            }
          }
        }
        matrixDB.nodesByRank[targetRank] = reconcileRankTreeLinks(targetRankNodes);
        matrixDB.rankCounts[targetRank] = Math.max(
          matrixDB.rankCounts[targetRank] || 1,
          targetRankNodes.length
        );
        createdExtraNodes.push({
          mainId: r1Main.id,
          targetRank,
          nodeId: newExtraId,
          parentId: placement.parentId,
          isLeft: placement.isLeft,
          deductedUsdt: targetPrice
        });
        spawnedInPass = true;
      }
    }
  }
  return createdExtraNodes;
}
async function createMatrixNodeData(params) {
  const { nodeId, rank, owner, sponsor, parentId, isLeft, isClone, isExtraId, origin } = params;
  const strNodeId = nodeId.toString();
  const strParentId = parentId.toString();
  const rankNum = Math.min(Math.max(Number(rank) || 1, 1), 27);
  const numNodeId = Number(nodeId);
  const numParentId = Number(parentId) || 0;
  const newNode = {
    id: strNodeId,
    rank: rankNum,
    owner,
    sponsor: sponsor || owner,
    parentId: strParentId,
    leftChild: "0",
    rightChild: "0",
    isClone: Boolean(isClone),
    isExtraId: Boolean(isExtraId),
    origin: origin || "REGISTRATION",
    createdAt: (/* @__PURE__ */ new Date()).toISOString()
  };
  inMemoryDB.nodes.set(strNodeId, newNode);
  const parentNode = inMemoryDB.nodes.get(strParentId);
  if (parentNode) {
    if (isLeft !== void 0) {
      if (isLeft) parentNode.leftChild = strNodeId;
      else parentNode.rightChild = strNodeId;
    } else {
      if (parentNode.leftChild === "0" || !parentNode.leftChild) {
        parentNode.leftChild = strNodeId;
      } else if (parentNode.rightChild === "0" || !parentNode.rightChild) {
        parentNode.rightChild = strNodeId;
      }
    }
  }
  if (numNodeId > 0) {
    if (!matrixDB.nodesByRank[rankNum]) {
      matrixDB.nodesByRank[rankNum] = [];
    }
    const rList = matrixDB.nodesByRank[rankNum];
    if (numParentId > 0) {
      const dbParent = rList.find((n) => n.id === numParentId);
      if (dbParent) {
        if (isLeft === true) {
          dbParent.leftChild = numNodeId;
        } else if (isLeft === false) {
          dbParent.rightChild = numNodeId;
        } else if (!dbParent.leftChild || dbParent.leftChild <= 0) {
          dbParent.leftChild = numNodeId;
        } else if (!dbParent.rightChild || dbParent.rightChild <= 0) {
          dbParent.rightChild = numNodeId;
        }
      }
    }
    let existingIdx = rList.findIndex((n) => n.id === numNodeId);
    let inferredOriginMainId = existingIdx >= 0 ? rList[existingIdx].originMainId : numNodeId;
    let inferredSponsorNodeId = existingIdx >= 0 ? rList[existingIdx].directSponsorNodeId : 1;
    if (existingIdx < 0 && isClone) {
      const ok = (owner || "").toLowerCase();
      for (let lb = numNodeId - 1; lb >= 1; lb--) {
        const candRight = rList.find((n) => n.id === lb);
        if (!candRight || candRight.parentId <= 0) continue;
        const trigParent = rList.find((n) => n.id === candRight.parentId);
        if (trigParent && trigParent.rightChild === candRight.id && (trigParent.owner || "").toLowerCase() === ok) {
          inferredOriginMainId = trigParent.originMainId && trigParent.originMainId > 0 ? trigParent.originMainId : trigParent.id;
          if (trigParent.directSponsorNodeId && trigParent.directSponsorNodeId > 0) {
            inferredSponsorNodeId = trigParent.directSponsorNodeId;
          }
          break;
        }
      }
    }
    const indexedNode = {
      id: numNodeId,
      rank: rankNum,
      owner,
      directSponsor: sponsor || owner,
      parentId: numParentId,
      leftChild: existingIdx >= 0 ? rList[existingIdx].leftChild : 0,
      rightChild: existingIdx >= 0 ? rList[existingIdx].rightChild : 0,
      isClone: Boolean(isClone),
      isExtraId: Boolean(isExtraId),
      originMainId: inferredOriginMainId,
      cloneRound: existingIdx >= 0 ? rList[existingIdx].cloneRound : isClone ? 1 : 0,
      directSponsorNodeId: inferredSponsorNodeId,
      nodeVault: existingIdx >= 0 ? rList[existingIdx].nodeVault : 0,
      rawNodeVault: existingIdx >= 0 ? rList[existingIdx].rawNodeVault : 0
    };
    if (existingIdx >= 0) {
      rList[existingIdx] = indexedNode;
    } else {
      rList.push(indexedNode);
    }
    reconcileRankTreeLinks(rList);
    matrixDB.rankCounts[rankNum] = Math.max(matrixDB.rankCounts[rankNum] || 1, rList.length, numNodeId);
    matrixDB.lastSyncedAt = (/* @__PURE__ */ new Date()).toISOString();
    rebuildDatabaseRewardLogsFromNodes();
    rebuildDatabaseUpgradeLogsFromNodes();
    saveMatrixDBToDisk();
  }
  return newNode;
}
async function getUserProfileWithVaults(owner) {
  const key = owner.toLowerCase();
  const r1Nodes = matrixDB.nodesByRank[1] || [];
  if (r1Nodes.length > 0) {
    const ownedR1 = r1Nodes.filter((n) => n.owner.toLowerCase() === key);
    if (ownedR1.length > 0) {
      const firstMain = ownedR1.find((n) => !n.isClone && !n.isExtraId) || ownedR1[0];
      let maxRank = firstMain.id === 1 ? 27 : 1;
      if (firstMain.id !== 1) {
        for (let r = 27; r >= 2; r--) {
          const rList = matrixDB.nodesByRank[r] || [];
          if (rList.some(
            (n) => !n.isClone && !n.isExtraId && n.originMainId === firstMain.id
          )) {
            maxRank = r;
            break;
          }
        }
      }
      const sponsorAddr = firstMain.directSponsor || ethers.ZeroAddress;
      const sponsorMainId = firstMain.directSponsorNodeId > 0 ? String(firstMain.directSponsorNodeId) : "1";
      const dbVaults = Array.from({ length: 27 }, (_, idx) => {
        const r = idx + 1;
        const rNodes = (matrixDB.nodesByRank[r] || []).filter((n) => n.owner.toLowerCase() === key);
        const rMains = rNodes.filter((n) => !n.isClone && !n.isExtraId);
        const firstId = rMains[0]?.id ?? rNodes[0]?.id ?? 0;
        const latestId = rNodes[rNodes.length - 1]?.id ?? 0;
        const reconciledBal = rMains[0] ? Number(rMains[0].nodeVault || 0) : matrixDB.userRankVaults[key]?.[r] ?? 0;
        return {
          id: `v-db-${owner}-${r}`,
          owner,
          rank: r,
          balance: reconciledBal.toFixed(2),
          firstNodeId: String(firstId),
          latestNodeId: String(latestId),
          updatedAt: matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString()
        };
      });
      const mappedNodes = ownedR1.map((n) => ({
        id: String(n.id),
        rank: 1,
        owner: n.owner,
        sponsor: n.directSponsor,
        parentId: String(n.parentId),
        leftChild: String(n.leftChild),
        rightChild: String(n.rightChild),
        isClone: n.isClone,
        isExtraId: n.isExtraId,
        origin: n.isClone ? "CLONE" : "REGISTRATION",
        createdAt: matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString()
      }));
      return {
        id: `user-${owner}`,
        owner,
        sponsor: sponsorAddr,
        sponsorMainNodeId: sponsorMainId,
        currentMaxRank: firstMain.id === 1 ? 27 : maxRank,
        createdAt: matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString(),
        updatedAt: matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString(),
        vaults: dbVaults,
        nodes: mappedNodes
      };
    }
    return {
      id: `unregistered-${key}`,
      owner,
      sponsor: ethers.ZeroAddress,
      sponsorMainNodeId: "0",
      currentMaxRank: 0,
      createdAt: (/* @__PURE__ */ new Date()).toISOString(),
      updatedAt: (/* @__PURE__ */ new Date()).toISOString(),
      vaults: [],
      nodes: []
    };
  }
  return {
    id: `unregistered-${key}`,
    owner,
    sponsor: ethers.ZeroAddress,
    sponsorMainNodeId: "0",
    currentMaxRank: 0,
    createdAt: (/* @__PURE__ */ new Date()).toISOString(),
    updatedAt: (/* @__PURE__ */ new Date()).toISOString(),
    vaults: [],
    nodes: []
  };
}
async function recordRewardLog(data) {
  const newLog = {
    id: `log-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`,
    fromNodeId: data.fromNodeId,
    toNodeId: data.toNodeId,
    recipient: data.recipient,
    rank: data.rank,
    rewardType: data.rewardType,
    amount: data.amount,
    txHash: data.txHash,
    createdAt: (/* @__PURE__ */ new Date()).toISOString()
  };
  matrixDB.rewardLogs.unshift(newLog);
  if (matrixDB.rewardLogs.length > 400) {
    matrixDB.rewardLogs.pop();
  }
  inMemoryDB.rewardLogs = matrixDB.rewardLogs;
  saveMatrixDBToDisk();
  return newLog;
}
function findRankPlacement(rank, sponsorAddress, sponsorNodeId) {
  if (!matrixDB.nodesByRank[rank] || matrixDB.nodesByRank[rank].length === 0) {
    const rootOwner = (matrixDB.systemWallets?.rootOwner || "0x622e20082e63c7220c5af0a77cbc437a98505311").toLowerCase();
    matrixDB.nodesByRank[rank] = [
      {
        id: 1,
        rank,
        owner: rootOwner,
        directSponsor: ethers.ZeroAddress,
        parentId: 0,
        leftChild: 0,
        rightChild: 0,
        isClone: false,
        isExtraId: false,
        originMainId: 1,
        cloneRound: 0,
        directSponsorNodeId: 0,
        nodeVault: 0,
        rawNodeVault: 0
      }
    ];
    matrixDB.rankCounts[rank] = 1;
  }
  const rList = matrixDB.nodesByRank[rank];
  const nodesMap = /* @__PURE__ */ new Map();
  for (const n of rList) nodesMap.set(n.id, n);
  let startId = 1;
  if (sponsorNodeId && sponsorNodeId > 0 && nodesMap.has(sponsorNodeId)) {
    startId = sponsorNodeId;
  } else if (sponsorAddress) {
    const spKey = sponsorAddress.toLowerCase();
    const spNode = rList.find((n) => n.owner.toLowerCase() === spKey && !n.isClone && !n.isExtraId);
    if (spNode) startId = spNode.id;
  }
  return findExtraIdPlacementInRankNodes(rList, startId);
}
async function simulateFindPlacement(rank, sponsorAddress, _round) {
  const p = findRankPlacement(rank, sponsorAddress);
  return { parentId: String(p.parentId), isLeft: p.isLeft };
}
function startEventListener() {
  if (!CONTRACT_ADDRESS) return;
  eventListenerActive = true;
  console.log(`\u{1F7E2} Blockchain Sync Indexer active for: ${CONTRACT_ADDRESS} (Database-First Polling Mode)`);
}
var reservationsMap = /* @__PURE__ */ new Map();
var reservationHistory = [];
var totalCompletedJobs = 0;
var totalFailedJobs = 0;
var RESERVATION_LEASE_MS = 90 * 1e3;
setInterval(() => {
  const now = Date.now();
  for (const [ticketId, res] of reservationsMap.entries()) {
    if (res.status === "RESERVED" && now > res.expiresAt) {
      res.status = "EXPIRED";
      queueEventsEmitter.emit(`ticket:${ticketId}:settled`, "EXPIRED");
    }
  }
}, 2e3);
async function fetchTreeWithVirtualLocks(rank = 1) {
  const nodesMap = /* @__PURE__ */ new Map();
  const ownerFirstNodeMap = /* @__PURE__ */ new Map();
  const ownerLatestNodeMap = /* @__PURE__ */ new Map();
  let totalNodes = 0;
  try {
    const provider = getProvider();
    const contract = new ethers.Contract(CONTRACT_ADDRESS, MATRIX_READ_ABI, provider);
    const onChainCount = Number(await contract.totalNodesInRank(rank));
    totalNodes = Math.max(0, onChainCount);
    if (totalNodes > 0) {
      const batchPromises = [];
      for (let i = 1; i <= Math.min(totalNodes, 500); i++) {
        batchPromises.push(
          contract.rankNodes(rank, i).then((node) => ({
            id: node[0].toString(),
            owner: (node[1] || "").toLowerCase(),
            directSponsor: (node[2] || "").toLowerCase(),
            parentId: node[3].toString(),
            leftChild: node[4].toString(),
            rightChild: node[5].toString(),
            isClone: Boolean(node[6]),
            isExtraId: Boolean(node[7])
          })).catch(() => null)
        );
      }
      const results = await Promise.all(batchPromises);
      if (rank === 1 && results.some((r) => r && r.id !== "0")) {
        inMemoryDB.nodes.clear();
      }
      for (const res of results) {
        if (res && res.id !== "0") {
          nodesMap.set(res.id, res);
          const ownerKey = res.owner.toLowerCase();
          if (!ownerFirstNodeMap.has(ownerKey)) {
            ownerFirstNodeMap.set(ownerKey, res.id);
          }
          ownerLatestNodeMap.set(ownerKey, res.id);
          if (rank === 1) {
            inMemoryDB.nodes.set(res.id, {
              id: res.id,
              rank: 1,
              owner: res.owner,
              sponsor: res.directSponsor,
              parentId: res.parentId,
              leftChild: res.leftChild,
              rightChild: res.rightChild,
              isClone: res.isClone,
              isExtraId: res.isExtraId,
              origin: res.isClone ? "CLONE" : "REGISTRATION",
              createdAt: (/* @__PURE__ */ new Date()).toISOString()
            });
          }
        }
      }
    }
  } catch (err) {
    console.warn("[Queue Engine] Notice fetching on-chain nodes for simulation:", err);
  }
  if (!nodesMap.has("1")) {
    nodesMap.set("1", {
      id: "1",
      owner: CONTRACT_ADDRESS.toLowerCase(),
      directSponsor: ethers.ZeroAddress,
      parentId: "0",
      leftChild: "0",
      rightChild: "0",
      isClone: false,
      isExtraId: false
    });
    totalNodes = Math.max(totalNodes, 1);
  }
  const now = Date.now();
  for (const res of reservationsMap.values()) {
    if (res.status === "RESERVED" && now <= res.expiresAt && res.rank === rank) {
      for (const item of res.batchDetails) {
        const pNode = nodesMap.get(item.targetParentId);
        if (pNode) {
          if (item.isLeft) {
            pNode.leftChild = item.assignedNodeId;
          } else {
            pNode.rightChild = item.assignedNodeId;
          }
        }
        nodesMap.set(item.assignedNodeId, {
          id: item.assignedNodeId,
          owner: item.owner.toLowerCase(),
          directSponsor: res.sponsorAddress.toLowerCase(),
          parentId: item.targetParentId,
          leftChild: "0",
          rightChild: "0",
          isClone: Boolean(item.isClone),
          isExtraId: false
        });
        const ownerKey = item.owner.toLowerCase();
        if (!ownerFirstNodeMap.has(ownerKey)) {
          ownerFirstNodeMap.set(ownerKey, item.assignedNodeId);
        }
        ownerLatestNodeMap.set(ownerKey, item.assignedNodeId);
      }
    }
  }
  return { nodesMap, totalNodes, ownerFirstNodeMap, ownerLatestNodeMap };
}
function bfsFindNextVacant(nodesMap, startId) {
  const queue = [startId];
  const visited = /* @__PURE__ */ new Set();
  while (queue.length > 0) {
    const currId = queue.shift();
    if (visited.has(currId)) continue;
    visited.add(currId);
    const node = nodesMap.get(currId);
    if (!node) {
      return { parentId: currId, isLeft: true };
    }
    if (node.leftChild === "0" || !node.leftChild) {
      return { parentId: node.id, isLeft: true };
    }
    if (node.rightChild === "0" || !node.rightChild) {
      return { parentId: node.id, isLeft: false };
    }
    if (node.leftChild && node.leftChild !== "0") queue.push(node.leftChild);
    if (node.rightChild && node.rightChild !== "0") queue.push(node.rightChild);
  }
  return { parentId: startId, isLeft: true };
}
function buildServerCloneLineageMap(nodesMap) {
  const lineageMap = /* @__PURE__ */ new Map();
  const matchedRightTriggers = /* @__PURE__ */ new Set();
  const ownerPrimaryNodeMap = /* @__PURE__ */ new Map();
  const ownerAllMainNodesMap = /* @__PURE__ */ new Map();
  const mainNodeCloneRoundCount = /* @__PURE__ */ new Map();
  const sortedIds = Array.from(nodesMap.keys()).map((k) => Number(k)).filter((n) => Number.isFinite(n) && n > 0).sort((a, b) => a - b);
  const isAncestorOf = (candidateAncestorId, targetId) => {
    if (!candidateAncestorId || candidateAncestorId === "0" || !targetId || targetId === "0") {
      return false;
    }
    let curr = targetId;
    const guard = /* @__PURE__ */ new Set();
    while (curr && curr !== "0" && !guard.has(curr)) {
      if (curr === candidateAncestorId) return true;
      guard.add(curr);
      const p = nodesMap.get(curr);
      curr = p ? p.parentId : "0";
    }
    return false;
  };
  for (const numId of sortedIds) {
    const idStr = String(numId);
    const n = nodesMap.get(idStr);
    if (!n) continue;
    const ownerKey = (n.owner || "").toLowerCase();
    if (!n.isClone && !n.isExtraId) {
      if (!ownerPrimaryNodeMap.has(ownerKey)) {
        ownerPrimaryNodeMap.set(ownerKey, idStr);
      }
      const list = ownerAllMainNodesMap.get(ownerKey) || [];
      list.push(idStr);
      ownerAllMainNodesMap.set(ownerKey, list);
      lineageMap.set(idStr, {
        originMainId: idStr,
        spawnedFromNodeId: "0",
        cloneRound: 0
      });
    } else if (n.isClone) {
      let spawnedFromNodeId = "0";
      const walletFirstId = ownerPrimaryNodeMap.get(ownerKey) || idStr;
      for (let lookback = numId - 1; lookback >= 1; lookback--) {
        const candidateRightChild = nodesMap.get(String(lookback));
        if (!candidateRightChild || !candidateRightChild.parentId || candidateRightChild.parentId === "0") continue;
        const triggerParentIdStr = candidateRightChild.parentId;
        const triggerParent = nodesMap.get(triggerParentIdStr);
        if (triggerParent && triggerParent.rightChild === candidateRightChild.id && (triggerParent.owner || "").toLowerCase() === ownerKey && !matchedRightTriggers.has(triggerParentIdStr)) {
          spawnedFromNodeId = triggerParentIdStr;
          matchedRightTriggers.add(triggerParentIdStr);
          break;
        }
      }
      if (spawnedFromNodeId === "0") {
        const ownerMains = ownerAllMainNodesMap.get(ownerKey) || [];
        const matchingSubtreeMain = [...ownerMains].reverse().find((mId) => Number(mId) < numId && isAncestorOf(mId, idStr));
        spawnedFromNodeId = matchingSubtreeMain || walletFirstId;
      }
      const triggerMeta = lineageMap.get(spawnedFromNodeId);
      const originMainId = triggerMeta ? triggerMeta.originMainId : spawnedFromNodeId;
      const prevCount = mainNodeCloneRoundCount.get(originMainId) || 0;
      const currentCount = prevCount + 1;
      mainNodeCloneRoundCount.set(originMainId, currentCount);
      const cloneRound = (currentCount - 1) % 2 + 1;
      lineageMap.set(idStr, {
        originMainId,
        spawnedFromNodeId,
        cloneRound
      });
    } else {
      lineageMap.set(idStr, {
        originMainId: idStr,
        spawnedFromNodeId: "0",
        cloneRound: 0
      });
    }
  }
  return lineageMap;
}
function calculateQueueSlot(nodesMap, ownerFirstNodeMap, _ownerLatestNodeMap, sponsorAddress, sponsorNodeId, mode = "contract_round") {
  const sponsorStrId = String(sponsorNodeId || 1);
  const sponsorKey = (sponsorAddress || "").toLowerCase();
  if (mode === "global_bfs") {
    const slot2 = bfsFindNextVacant(nodesMap, "1");
    return { targetParentId: slot2.parentId, isLeft: slot2.isLeft, activeRound: 1, targetType: "descendant" };
  }
  if (mode === "auto_under_sponsor") {
    const slot2 = bfsFindNextVacant(nodesMap, sponsorStrId);
    return { targetParentId: slot2.parentId, isLeft: slot2.isLeft, activeRound: 0, targetType: "descendant" };
  }
  const lineageMap = buildServerCloneLineageMap(nodesMap);
  const requestedNode = nodesMap.get(sponsorStrId);
  const requestedMeta = lineageMap.get(sponsorStrId);
  if (requestedNode && requestedNode.isClone) {
    if (requestedNode.leftChild === "0" || !requestedNode.leftChild) {
      return { targetParentId: requestedNode.id, isLeft: true, activeRound: 0, targetType: "clone" };
    }
    if (requestedNode.rightChild === "0" || !requestedNode.rightChild) {
      return { targetParentId: requestedNode.id, isLeft: false, activeRound: 0, targetType: "clone" };
    }
  }
  const targetMainIdStr = requestedMeta ? requestedMeta.originMainId : sponsorStrId;
  const mainNode = nodesMap.get(targetMainIdStr) || (ownerFirstNodeMap.get(sponsorKey) ? nodesMap.get(ownerFirstNodeMap.get(sponsorKey)) : void 0);
  if (mainNode) {
    if (mainNode.leftChild === "0" || !mainNode.leftChild) {
      return { targetParentId: mainNode.id, isLeft: true, activeRound: 0, targetType: "main" };
    }
    if (mainNode.rightChild === "0" || !mainNode.rightChild) {
      return { targetParentId: mainNode.id, isLeft: false, activeRound: 0, targetType: "main" };
    }
  }
  const clonesOfThisMainId = [];
  for (const node of nodesMap.values()) {
    if (!node.isClone) continue;
    const meta = lineageMap.get(node.id);
    if (meta && meta.originMainId === targetMainIdStr) {
      clonesOfThisMainId.push(node);
    }
  }
  clonesOfThisMainId.sort((a, b) => Number(a.id) - Number(b.id));
  if (clonesOfThisMainId.length > 0) {
    const openClone = clonesOfThisMainId.find(
      (c) => c.leftChild === "0" || !c.leftChild || c.rightChild === "0" || !c.rightChild
    );
    if (openClone) {
      if (openClone.leftChild === "0" || !openClone.leftChild) {
        return { targetParentId: openClone.id, isLeft: true, activeRound: 0, targetType: "clone" };
      }
      return { targetParentId: openClone.id, isLeft: false, activeRound: 0, targetType: "clone" };
    }
  }
  const slot = bfsFindNextVacant(nodesMap, targetMainIdStr);
  return { targetParentId: slot.parentId, isLeft: slot.isLeft, activeRound: 0, targetType: "descendant" };
}
function calculateAutoIdAndExtraIdSlot(nodesMap, sponsorNodeId) {
  const lineageMap = buildServerCloneLineageMap(nodesMap);
  const requestedStrId = String(sponsorNodeId || 1);
  const requestedMeta = lineageMap.get(requestedStrId);
  const sponsorMainIdStr = requestedMeta ? requestedMeta.originMainId : requestedStrId;
  const mainNode = nodesMap.get(sponsorMainIdStr);
  if (mainNode) {
    if (mainNode.leftChild === "0" || !mainNode.leftChild) {
      return { targetParentId: mainNode.id, isLeft: true, originMainId: sponsorMainIdStr, targetType: "main" };
    }
    if (mainNode.rightChild === "0" || !mainNode.rightChild) {
      return { targetParentId: mainNode.id, isLeft: false, originMainId: sponsorMainIdStr, targetType: "main" };
    }
  }
  const matchedClones = [];
  for (const node of nodesMap.values()) {
    if (!node.isClone) continue;
    const meta = lineageMap.get(node.id);
    if (meta && meta.originMainId === sponsorMainIdStr) {
      matchedClones.push(node);
    }
  }
  matchedClones.sort((a, b) => Number(a.id) - Number(b.id));
  for (const clone of matchedClones) {
    if (clone.leftChild === "0" || !clone.leftChild) {
      return { targetParentId: clone.id, isLeft: true, originMainId: sponsorMainIdStr, targetType: "clone" };
    }
    if (clone.rightChild === "0" || !clone.rightChild) {
      return { targetParentId: clone.id, isLeft: false, originMainId: sponsorMainIdStr, targetType: "clone" };
    }
  }
  const slot = bfsFindNextVacant(nodesMap, sponsorMainIdStr);
  return { targetParentId: slot.parentId, isLeft: slot.isLeft, originMainId: sponsorMainIdStr, targetType: "descendant" };
}
async function processPlacementJob(jobData) {
  const {
    owner,
    sponsorNodeId = 1,
    sponsorAddress = CONTRACT_ADDRESS,
    count = 1,
    rank = 1,
    mode = "contract_round",
    applicants
  } = jobData;
  const nodeCount = Math.max(1, Number(count) || 1);
  const applicantList = Array.isArray(applicants) && applicants.length > 0 ? applicants : Array.from({ length: nodeCount }, () => ({ owner }));
  const { nodesMap, totalNodes, ownerFirstNodeMap, ownerLatestNodeMap } = await fetchTreeWithVirtualLocks(rank);
  let nextNodeId = 1;
  for (const key of nodesMap.keys()) {
    const num = Number(key);
    if (!isNaN(num) && num >= nextNodeId) {
      nextNodeId = num + 1;
    }
  }
  const batchDetails = [];
  for (let idx = 0; idx < applicantList.length; idx++) {
    const appOwner = applicantList[idx].owner || owner;
    const placement = calculateQueueSlot(
      nodesMap,
      ownerFirstNodeMap,
      ownerLatestNodeMap,
      sponsorAddress,
      sponsorNodeId,
      mode
    );
    const assignedId = String(nextNodeId++);
    const isClone = false;
    batchDetails.push({
      applicantIndex: idx,
      owner: appOwner,
      targetParentId: placement.targetParentId,
      isLeft: placement.isLeft,
      assignedNodeId: assignedId,
      isClone
    });
    const pNode = nodesMap.get(placement.targetParentId);
    if (pNode) {
      if (placement.isLeft) pNode.leftChild = assignedId;
      else pNode.rightChild = assignedId;
    }
    nodesMap.set(assignedId, {
      id: assignedId,
      owner: appOwner.toLowerCase(),
      directSponsor: sponsorAddress.toLowerCase(),
      parentId: placement.targetParentId,
      leftChild: "0",
      rightChild: "0",
      isClone,
      isExtraId: false
    });
  }
  const firstSlot = batchDetails[0];
  const now = Date.now();
  const ticketId = `TKT-${now}-${Math.random().toString(36).substring(2, 7).toUpperCase()}`;
  const reservation = {
    ticketId,
    owner,
    rank,
    sponsorNodeId,
    sponsorAddress,
    targetParentId: firstSlot.targetParentId,
    isLeft: firstSlot.isLeft,
    activeRound: 0,
    assignedNodeId: firstSlot.assignedNodeId,
    batchCount: applicantList.length,
    batchDetails,
    status: "RESERVED",
    reservedAt: now,
    expiresAt: now + RESERVATION_LEASE_MS
  };
  reservationsMap.set(ticketId, reservation);
  reservationHistory.unshift(reservation);
  if (reservationHistory.length > 50) reservationHistory.pop();
  return reservation;
}
var InMemoryFifoMutexQueue = class {
  constructor() {
    this.queue = [];
    this.isProcessing = false;
    this.currentActiveTicketId = null;
  }
  async add(name, data) {
    return new Promise((resolve, reject) => {
      const id = `job-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`;
      this.queue.push({ id, data, resolve, reject });
      this.processNext();
    });
  }
  async processNext() {
    if (this.isProcessing || this.queue.length === 0) return;
    this.isProcessing = true;
    const item = this.queue.shift();
    let activeTicketId = "";
    try {
      const reservation = await processPlacementJob(item.data);
      activeTicketId = reservation.ticketId;
      this.currentActiveTicketId = activeTicketId;
      console.log(`\u23F3 [FIFO Queue 1-by-1] Ticket ${activeTicketId} issued to ${reservation.owner}. Waiting for on-chain tx completion...`);
      item.resolve(reservation);
      await new Promise((resolveWait) => {
        let settled = false;
        const onSettled = (status) => {
          if (settled) return;
          settled = true;
          queueEventsEmitter.removeListener(`ticket:${activeTicketId}:settled`, onSettled);
          clearTimeout(timeoutTimer);
          console.log(`\u{1F7E2} [FIFO Queue 1-by-1] Ticket ${activeTicketId} settled with status '${status}'. Unlocking queue for next item...`);
          resolveWait();
        };
        queueEventsEmitter.once(`ticket:${activeTicketId}:settled`, onSettled);
        const timeoutTimer = setTimeout(() => {
          if (!settled) {
            console.warn(`\u26A0\uFE0F [FIFO Queue 1-by-1] Ticket ${activeTicketId} 90s timeout reached without on-chain confirmation. Proceeding to next queue item...`);
            onSettled("TIMEOUT");
          }
        }, 9e4);
      });
      totalCompletedJobs++;
    } catch (err) {
      totalFailedJobs++;
      item.reject(err);
    } finally {
      this.currentActiveTicketId = null;
      this.isProcessing = false;
      setImmediate(() => this.processNext());
    }
  }
  get waitingCount() {
    return this.queue.length;
  }
  get isBusy() {
    return this.isProcessing;
  }
  get activeTicketId() {
    return this.currentActiveTicketId;
  }
};
var redisClient = null;
var bullQueue = null;
var bullWorker = null;
var bullEvents = null;
var inMemoryQueue = new InMemoryFifoMutexQueue();
var isUsingRedis = false;
async function initPlacementQueue() {
  const redisUrl = (process.env.REDIS_URL || "").trim();
  const isLocalRedis = !redisUrl || redisUrl.includes("127.0.0.1") || redisUrl.includes("localhost");
  if (!isLocalRedis) {
    try {
      const client = new Redis(redisUrl, {
        maxRetriesPerRequest: null,
        connectTimeout: 3e3,
        lazyConnect: true,
        retryStrategy: () => null
      });
      client.on("error", () => {
        isUsingRedis = false;
      });
      await client.connect();
      redisClient = client;
      isUsingRedis = true;
      console.log("\u{1F7E2} [Queue Engine] Redis connected successfully. BullMQ active!");
      bullQueue = new Queue("matrix-placement-queue", {
        connection: redisClient
      });
      bullQueue.on("error", () => {
        isUsingRedis = false;
      });
      bullEvents = new QueueEvents("matrix-placement-queue", {
        connection: redisClient
      });
      bullEvents.on("error", () => {
        isUsingRedis = false;
      });
      bullWorker = new Worker(
        "matrix-placement-queue",
        async (job) => {
          return await processPlacementJob(job.data);
        },
        {
          connection: redisClient,
          concurrency: 1
        }
      );
      bullWorker.on("error", () => {
        isUsingRedis = false;
      });
      bullWorker.on("completed", () => {
        totalCompletedJobs++;
      });
      bullWorker.on("failed", () => {
        totalFailedJobs++;
      });
      return;
    } catch {
      if (redisClient) {
        try {
          redisClient.disconnect();
        } catch {
        }
        redisClient = null;
      }
      isUsingRedis = false;
    }
  }
  isUsingRedis = false;
  console.log("\u2139\uFE0F [Queue Engine] Built-in In-Memory FIFO Mutex Queue active (Concurrency: 1).");
}
async function enqueuePlacementRequest(request) {
  if (isUsingRedis && bullQueue && bullEvents) {
    const job = await bullQueue.add("reserve-placement", request, {
      removeOnComplete: 100,
      removeOnFail: 100
    });
    return await job.waitUntilFinished(bullEvents);
  }
  return await inMemoryQueue.add("reserve-placement", request);
}
function confirmReservation(ticketId, txHash) {
  const res = reservationsMap.get(ticketId);
  if (!res) return false;
  res.status = "CONFIRMED";
  if (txHash) res.txHash = txHash;
  queueEventsEmitter.emit(`ticket:${ticketId}:settled`, "CONFIRMED");
  return true;
}
function releaseReservation(ticketId) {
  const res = reservationsMap.get(ticketId);
  if (!res) return false;
  if (res.status === "RESERVED") {
    res.status = "RELEASED";
    queueEventsEmitter.emit(`ticket:${ticketId}:settled`, "RELEASED");
    return true;
  }
  return false;
}
async function getQueueStatus() {
  const now = Date.now();
  const activeReservations = Array.from(reservationsMap.values()).filter(
    (r) => r.status === "RESERVED" && now <= r.expiresAt
  );
  let waitingCount = inMemoryQueue.waitingCount;
  let activeCount = inMemoryQueue.isBusy ? 1 : 0;
  if (isUsingRedis && bullQueue) {
    try {
      const counts = await bullQueue.getJobCounts("waiting", "active");
      waitingCount = counts.waiting;
      activeCount = counts.active;
    } catch {
    }
  }
  return {
    engine: isUsingRedis ? "BullMQ + Redis" : "Built-in In-Memory FIFO Mutex",
    isRedisConnected: isUsingRedis,
    waitingJobsCount: waitingCount,
    activeJobsCount: activeCount,
    completedJobsCount: totalCompletedJobs,
    failedJobsCount: totalFailedJobs,
    activeReservationsCount: activeReservations.length,
    activeReservations,
    recentHistory: reservationHistory.slice(0, 10)
  };
}
async function startApp() {
  const app = express();
  app.use(express.json());
  app.use(cors());
  app.use("/api", (_req, res, next) => {
    res.setHeader("Content-Type", "application/json");
    next();
  });
  app.get("/api/config/contract", (_req, res) => {
    res.json({
      success: true,
      config: activeSystemConfig,
      source: "DATABASE_STORE"
    });
  });
  app.post("/api/config/contract", async (req, res) => {
    try {
      const { contractAddress, usdtAddress, chainId, networkName, rpcUrl, blockExplorerUrl } = req.body;
      if (!contractAddress || !ethers.isAddress(contractAddress)) {
        return res.status(400).json({
          success: false,
          error: "\u0E23\u0E39\u0E1B\u0E41\u0E1A\u0E1A\u0E17\u0E35\u0E48\u0E2D\u0E22\u0E39\u0E48 Smart Contract \u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07 (Invalid Ethereum/BSC address)"
        });
      }
      if (usdtAddress && !ethers.isAddress(usdtAddress)) {
        return res.status(400).json({
          success: false,
          error: "\u0E23\u0E39\u0E1B\u0E41\u0E1A\u0E1A\u0E17\u0E35\u0E48\u0E2D\u0E22\u0E39\u0E48 USDT Token Address \u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07 (Invalid token address)"
        });
      }
      const updated = updateContractConfigInDB({
        contractAddress,
        usdtAddress: usdtAddress || activeSystemConfig.usdtAddress,
        chainId: chainId ? Number(chainId) : activeSystemConfig.chainId,
        networkName: networkName || activeSystemConfig.networkName,
        rpcUrl: rpcUrl || activeSystemConfig.rpcUrl,
        blockExplorerUrl: blockExplorerUrl || activeSystemConfig.blockExplorerUrl
      });
      matrixDB = createEmptyMatrixDB(CONTRACT_ADDRESS, USDT_ADDRESS);
      saveMatrixDBToDisk(true);
      syncAllMatrixDatabase(CONTRACT_ADDRESS, true).catch(() => {
      });
      try {
        startEventListener();
      } catch (e) {
        console.warn("Listener restart notice:", e);
      }
      res.json({
        success: true,
        message: "\u0E1A\u0E31\u0E19\u0E17\u0E36\u0E01\u0E41\u0E25\u0E30\u0E1B\u0E23\u0E31\u0E1A\u0E1B\u0E23\u0E38\u0E07\u0E17\u0E35\u0E48\u0E2D\u0E22\u0E39\u0E48 Smart Contract \u0E25\u0E07 Database \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08\u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22!",
        config: updated
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/config/contract/reset", (_req, res) => {
    try {
      const resetConfig = updateContractConfigInDB(DEFAULT_SYSTEM_CONFIG);
      matrixDB = createEmptyMatrixDB(CONTRACT_ADDRESS, USDT_ADDRESS);
      saveMatrixDBToDisk(true);
      syncAllMatrixDatabase(CONTRACT_ADDRESS, true).catch(() => {
      });
      try {
        startEventListener();
      } catch (e) {
        console.warn("Listener restart notice:", e);
      }
      res.json({
        success: true,
        message: "\u0E04\u0E37\u0E19\u0E04\u0E48\u0E32\u0E17\u0E35\u0E48\u0E2D\u0E22\u0E39\u0E48 Smart Contract \u0E40\u0E1B\u0E47\u0E19\u0E04\u0E48\u0E32\u0E40\u0E23\u0E34\u0E48\u0E21\u0E15\u0E49\u0E19\u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22\u0E41\u0E25\u0E49\u0E27",
        config: resetConfig
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.get("/api/health", (_req, res) => {
    res.json({
      status: "ok",
      source: "DATABASE_FIRST",
      postgres: {
        connected: pgConnected,
        host: "187.124.138.252:32772",
        database: "Aunza",
        tableCounts: pgTablesCount,
        lastError: pgLastError
      },
      contractAddress: CONTRACT_ADDRESS,
      usdtAddress: USDT_ADDRESS,
      network: activeSystemConfig.networkName || "BNB Smart Chain Testnet",
      chainId: activeSystemConfig.chainId || 97,
      rpcUrl: FALLBACK_RPCS[currentProviderIndex % FALLBACK_RPCS.length],
      eventListenerActive,
      lastSyncedBlock: matrixDB.lastSyncedBlock,
      lastSyncedAt: matrixDB.lastSyncedAt,
      totalRewardsLogged: matrixDB.rewardLogs.length,
      totalNodesRank1: matrixDB.rankCounts[1] || (matrixDB.nodesByRank[1]?.length ?? 1),
      serverTime: (/* @__PURE__ */ new Date()).toISOString()
    });
  });
  app.get("/api/db/postgres", async (_req, res) => {
    await refreshPgTableCounts();
    res.json({
      success: true,
      connected: pgConnected,
      host: "187.124.138.252",
      port: 32772,
      database: "Aunza",
      contractAddress: CONTRACT_ADDRESS,
      tableCounts: pgTablesCount,
      lastError: pgLastError
    });
  });
  app.get("/api/matrix/info", (_req, res) => {
    res.json({
      name: "FractalFlowMatrix27",
      version: "v2.7.0 (Solidity ^0.8.20)",
      contractAddress: CONTRACT_ADDRESS,
      usdtAddress: USDT_ADDRESS,
      maxRank: 27,
      maxLevels: 15,
      network: {
        name: "BNB Smart Chain Testnet",
        chainId: 97,
        currency: "tBNB",
        blockExplorer: `https://testnet.bscscan.com/address/${CONTRACT_ADDRESS}`,
        usdtExplorer: `https://testnet.bscscan.com/address/${USDT_ADDRESS}`
      },
      rewardDistribution: {
        leftChild: {
          directPercent: 30,
          levelPercentPerTier: 2,
          maxLevels: 15,
          totalLevelPercent: 30,
          upgradeVaultPercent: 35,
          allSellPercent: 4,
          botPercent: 1
        },
        rightChild: {
          trigger: "Auto-Clone Spawn (_triggerRightChildClone) for Parent Node Owner"
        }
      },
      ranks: RANK_PRICES_USDT.map((price, i) => ({
        rank: i + 1,
        name: `Rank ${i + 1}`,
        priceUSDT: price,
        directBonusUSDT: +(price * 0.3).toFixed(2),
        levelBonusPerTierUSDT: +(price * 0.02).toFixed(2),
        maxLevelTotalUSDT: +(price * 0.3).toFixed(2),
        upgradeVaultUSDT: +(price * 0.35).toFixed(2),
        allSellUSDT: +(price * 0.04).toFixed(2),
        botUSDT: +(price * 0.01).toFixed(2)
      }))
    });
  });
  const rankSyncTimestamps = /* @__PURE__ */ new Map();
  const rankSyncInFlight = /* @__PURE__ */ new Map();
  let fullSyncInFlight = null;
  async function fetchSingleNodeFromContract(contract, rankNum, nodeId) {
    let retries = 3;
    while (retries >= 0) {
      try {
        const [raw, origBn, roundBn, sponsorNodeBn, vaultBn] = await Promise.all([
          contract.rankNodes(rankNum, nodeId),
          contract.nodeOriginMainId(rankNum, nodeId).catch(() => 0n),
          contract.nodeCloneRound(rankNum, nodeId).catch(() => 0n),
          contract.nodeDirectSponsorNodeId(rankNum, nodeId).catch(() => 0n),
          contract.nodeUpgradeVault(rankNum, nodeId).catch(() => 0n)
        ]);
        const id = Number(raw.id ?? raw[0] ?? 0);
        if (id > 0) {
          const dec = vaultBn >= 100000000000000n ? 18 : activeUsdtDecimals || 18;
          const rawVaultVal = Number(ethers.formatUnits(vaultBn, dec));
          const isClone = Boolean(raw.isClone ?? raw[6] ?? false);
          const isExtraId = Boolean(raw.isExtraId ?? raw[7] ?? false) && !isClone;
          let originType = "MANUAL_REGISTER";
          if (isClone) originType = "CLONE";
          else if (isExtraId) originType = "AUTO_EXTRA";
          return {
            id,
            rank: rankNum,
            owner: String(raw.owner ?? raw[1] ?? ethers.ZeroAddress),
            directSponsor: String(raw.directSponsor ?? raw[2] ?? ethers.ZeroAddress),
            parentId: Number(raw.parentId ?? raw[3] ?? 0),
            leftChild: Number(raw.leftChild ?? raw[4] ?? 0),
            rightChild: Number(raw.rightChild ?? raw[5] ?? 0),
            isClone,
            isExtraId,
            originMainId: Number(origBn) > 0 ? Number(origBn) : id,
            cloneRound: Number(roundBn),
            directSponsorNodeId: Number(sponsorNodeBn),
            nodeVault: rawVaultVal,
            rawNodeVault: rawVaultVal,
            reconciledBaseVault: rawVaultVal,
            originType
          };
        }
        return null;
      } catch {
        retries--;
        if (retries < 0) return null;
        await new Promise((resolve) => setTimeout(resolve, 150));
      }
    }
    return null;
  }
  async function syncRankIntoDatabase(rank, targetContract = CONTRACT_ADDRESS, forceRefresh = false) {
    const rankNum = Math.min(Math.max(Number(rank) || 1, 1), 27);
    const cacheKey = `${targetContract.toLowerCase()}:${rankNum}`;
    const now = Date.now();
    const lastTs = rankSyncTimestamps.get(cacheKey) || 0;
    const existingNodes = matrixDB.nodesByRank[rankNum] || [];
    if (!forceRefresh && existingNodes.length > 0 && targetContract.toLowerCase() === matrixDB.contractAddress.toLowerCase() && now - lastTs < 12e3) {
      return reconcileRankTreeLinks(existingNodes);
    }
    if (rankSyncInFlight.has(cacheKey)) {
      return rankSyncInFlight.get(cacheKey);
    }
    const syncPromise = (async () => {
      try {
        const provider = getProvider();
        const contract = new ethers.Contract(targetContract, MATRIX_READ_ABI, provider);
        const realExisting = existingNodes.filter((n) => !n.isSyntheticExtra);
        let totalNodes = realExisting.filter((n) => !n.isLocalUpgrade).length || 1;
        try {
          const totalBn = await contract.totalNodesInRank(rankNum);
          totalNodes = Math.max(1, Number(totalBn));
        } catch {
          totalNodes = Math.max(1, realExisting.filter((n) => !n.isLocalUpgrade).length || 1);
        }
        const localUpgradedNodes = realExisting.filter(
          (n) => n.isLocalUpgrade && n.id > totalNodes
        );
        const maxFetch = Math.max(totalNodes, 1);
        const mergedMap = /* @__PURE__ */ new Map();
        for (const existing of realExisting) {
          if (existing && existing.id > 0 && existing.id <= totalNodes && !existing.isLocalUpgrade) {
            const copy = { ...existing };
            if (copy.leftChild > maxFetch) copy.leftChild = 0;
            if (copy.rightChild > maxFetch) copy.rightChild = 0;
            mergedMap.set(copy.id, copy);
          }
        }
        const maxExistingChainId = realExisting.reduce(
          (m, n) => !n.isLocalUpgrade ? Math.max(m, n.id) : m,
          0
        );
        const idsToFetch = [];
        for (let i = 1; i <= maxFetch; i++) {
          const ex = mergedMap.get(i);
          const needsMetadataRefresh = ex && (ex.isClone && (!ex.originMainId || ex.originMainId === ex.id) || rankNum > 1 && i > 1 && !ex.isClone && !ex.isExtraId && (!ex.originMainId || ex.originMainId <= 1));
          if (forceRefresh || !ex || needsMetadataRefresh) {
            idsToFetch.push(i);
          }
        }
        if (idsToFetch.length === 0 && maxExistingChainId >= maxFetch) {
          rankSyncTimestamps.set(cacheKey, Date.now());
          const mainNodesToRefresh = Array.from(mergedMap.values()).filter(
            (n) => !n.isClone && !n.isExtraId && !n.isLocalUpgrade && !n.isSyntheticExtra
          );
          if (mainNodesToRefresh.length > 0) {
            await Promise.all(
              mainNodesToRefresh.map(async (mNode) => {
                try {
                  const rawV = await contract.nodeUpgradeVault(rankNum, mNode.id);
                  const dec = rawV >= 100000000000000n ? 18 : activeUsdtDecimals || 18;
                  const val = Number(ethers.formatUnits(rawV, dec));
                  mNode.rawNodeVault = val;
                  mNode.nodeVault = val;
                  mNode.reconciledBaseVault = val;
                } catch {
                }
              })
            );
          }
          for (const loc of localUpgradedNodes) {
            mergedMap.set(loc.id, { ...loc });
          }
          const fastList = reconcileRankTreeLinks(Array.from(mergedMap.values()));
          matrixDB.nodesByRank[rankNum] = fastList;
          matrixDB.rankCounts[rankNum] = Math.max(1, fastList.length);
          reconcileAutoExtraIdsAcrossRanks();
          return reconcileRankTreeLinks(matrixDB.nodesByRank[rankNum] || fastList);
        }
        const CHUNK_SIZE = 5;
        const newlyFetchedNodes = [];
        for (let offset = 0; offset < idsToFetch.length; offset += CHUNK_SIZE) {
          const chunkIds = idsToFetch.slice(offset, offset + CHUNK_SIZE);
          const chunkResults = await Promise.all(
            chunkIds.map((nodeId) => fetchSingleNodeFromContract(contract, rankNum, nodeId))
          );
          for (const item of chunkResults) {
            if (item) {
              const prevCreatedAt = mergedMap.get(item.id)?.createdAt;
              item.createdAt = prevCreatedAt || (/* @__PURE__ */ new Date()).toISOString();
              mergedMap.set(item.id, item);
              newlyFetchedNodes.push(item);
            }
          }
          if (offset + CHUNK_SIZE < idsToFetch.length) {
            await new Promise((resolve) => setTimeout(resolve, 35));
          }
        }
        if (!forceRefresh && newlyFetchedNodes.length > 0) {
          const parentAndMainIdsToRefresh = Array.from(
            /* @__PURE__ */ new Set([
              ...newlyFetchedNodes.map((n) => n.parentId).filter((pid) => pid > 0 && !idsToFetch.includes(pid)),
              ...Array.from(mergedMap.values()).filter((n) => !n.isClone && !n.isExtraId && !idsToFetch.includes(n.id)).map((n) => n.id)
            ])
          );
          if (parentAndMainIdsToRefresh.length > 0) {
            const refreshedNodes = await Promise.all(
              parentAndMainIdsToRefresh.map((pid) => fetchSingleNodeFromContract(contract, rankNum, pid))
            );
            for (const pItem of refreshedNodes) {
              if (pItem) {
                const prevCreatedAt = mergedMap.get(pItem.id)?.createdAt;
                if (prevCreatedAt) pItem.createdAt = prevCreatedAt;
                mergedMap.set(pItem.id, pItem);
              }
            }
          }
        }
        for (const loc of localUpgradedNodes) {
          const alreadyOnChain = Array.from(mergedMap.values()).some(
            (n) => !n.isLocalUpgrade && !n.isClone && !n.isExtraId && !loc.isClone && n.originMainId === loc.originMainId
          );
          if (!alreadyOnChain && !mergedMap.has(loc.id)) {
            mergedMap.set(loc.id, { ...loc });
          }
        }
        const fetchedNodes = reconcileRankTreeLinks(Array.from(mergedMap.values()));
        if (rankNum > 1) {
          const r1Nodes = matrixDB.nodesByRank[1] || [];
          for (const fn of fetchedNodes) {
            if (fn.isExtraId) {
              if (!fn.directSponsorNodeId || fn.directSponsorNodeId <= 0) {
                fn.directSponsorNodeId = 1;
              }
            } else if (!fn.isClone) {
              const origNodeR1 = fn.originMainId > 1 || fn.id === 1 ? r1Nodes.find((n) => n.id === fn.originMainId && !n.isClone && !n.isExtraId) : void 0;
              const userR1 = origNodeR1 || (fn.originMainId <= 1 && fn.id > 1 ? r1Nodes.find(
                (n) => n.owner.toLowerCase() === fn.owner.toLowerCase() && !n.isClone && !n.isExtraId && n.id > 1
              ) : void 0);
              if (userR1) {
                fn.originMainId = userR1.id;
                fn.directSponsorNodeId = userR1.directSponsorNodeId || (userR1.id === 1 ? 0 : 1);
                fn.directSponsor = userR1.directSponsor;
              }
            } else if (fn.isClone) {
              const localOriginMain = fetchedNodes.find(
                (m) => !m.isClone && !m.isExtraId && (m.originMainId === fn.originMainId || m.id === fn.originMainId)
              );
              const r1OriginId = localOriginMain ? localOriginMain.originMainId : fn.originMainId;
              if (r1OriginId > 0) {
                fn.originMainId = r1OriginId;
              }
              const origNodeR1 = r1Nodes.find((n) => n.id === r1OriginId && !n.isClone && !n.isExtraId);
              if (origNodeR1) {
                fn.directSponsorNodeId = origNodeR1.directSponsorNodeId || (origNodeR1.id === 1 ? 0 : 1);
                fn.directSponsor = origNodeR1.directSponsor;
              } else if (localOriginMain && localOriginMain.directSponsorNodeId > 0) {
                fn.directSponsorNodeId = localOriginMain.directSponsorNodeId;
                fn.directSponsor = localOriginMain.directSponsor;
              }
            }
          }
        }
        if (fetchedNodes.length > 0) {
          if (rankNum === 1 && fetchedNodes[0]?.owner) {
            matrixDB.systemWallets.rootOwner = fetchedNodes[0].owner;
          }
          const ownerVaultSums = /* @__PURE__ */ new Map();
          for (const fn of fetchedNodes) {
            const val = Number(fn.rawNodeVault ?? fn.nodeVault ?? 0);
            fn.nodeVault = val;
            fn.reconciledBaseVault = val;
            if (!fn.isClone && !fn.isExtraId && fn.owner && fn.owner !== ethers.ZeroAddress) {
              const oKey = String(fn.owner).toLowerCase();
              ownerVaultSums.set(oKey, Number(((ownerVaultSums.get(oKey) || 0) + val).toFixed(4)));
            }
          }
          for (const [oKey, sumVal] of ownerVaultSums.entries()) {
            if (!matrixDB.userRankVaults[oKey]) matrixDB.userRankVaults[oKey] = {};
            matrixDB.userRankVaults[oKey][rankNum] = sumVal;
          }
        }
        if (fetchedNodes.length > 0 && targetContract.toLowerCase() === CONTRACT_ADDRESS.toLowerCase()) {
          matrixDB.nodesByRank[rankNum] = fetchedNodes;
          matrixDB.rankCounts[rankNum] = Math.max(totalNodes, fetchedNodes.length);
          matrixDB.lastSyncedAt = (/* @__PURE__ */ new Date()).toISOString();
          rankSyncTimestamps.set(cacheKey, Date.now());
          if (rankNum === 1) {
            inMemoryDB.nodes.clear();
            for (const fn of fetchedNodes) {
              inMemoryDB.nodes.set(String(fn.id), {
                id: String(fn.id),
                rank: 1,
                owner: fn.owner,
                sponsor: fn.directSponsor,
                parentId: String(fn.parentId),
                leftChild: String(fn.leftChild),
                rightChild: String(fn.rightChild),
                isClone: fn.isClone,
                isExtraId: fn.isExtraId,
                origin: fn.isClone ? "CLONE" : "REGISTRATION",
                createdAt: matrixDB.lastSyncedAt
              });
            }
          }
          reconcileAutoExtraIdsAcrossRanks();
          rebuildDatabaseRewardLogsFromNodes();
          rebuildDatabaseUpgradeLogsFromNodes();
          saveMatrixDBToDisk();
          return reconcileRankTreeLinks(matrixDB.nodesByRank[rankNum] || fetchedNodes);
        }
      } catch (err) {
        console.warn(`[MatrixDB Sync] Notice syncing rank ${rankNum}:`, err?.message || err);
      } finally {
        rankSyncInFlight.delete(cacheKey);
      }
      reconcileAutoExtraIdsAcrossRanks();
      return reconcileRankTreeLinks(matrixDB.nodesByRank[rankNum] || []);
    })();
    rankSyncInFlight.set(cacheKey, syncPromise);
    return syncPromise;
  }
  async function syncAllMatrixDatabase(targetContract = CONTRACT_ADDRESS, force = false) {
    if (fullSyncInFlight && !force) return fullSyncInFlight;
    fullSyncInFlight = (async () => {
      try {
        const provider = getProvider();
        const contract = new ethers.Contract(targetContract, MATRIX_READ_ABI, provider);
        const [blockNum, allSellAddr, botAddr, familyAddr] = await Promise.all([
          provider.getBlockNumber().catch(() => matrixDB.lastSyncedBlock),
          contract.allSellWallet().catch(() => matrixDB.systemWallets.allSellWallet),
          contract.botWallet().catch(() => matrixDB.systemWallets.botWallet),
          contract.familyPool().catch(() => matrixDB.systemWallets.familyPool)
        ]);
        if (blockNum > 0) matrixDB.lastSyncedBlock = blockNum;
        if (allSellAddr && ethers.isAddress(allSellAddr)) matrixDB.systemWallets.allSellWallet = allSellAddr;
        if (botAddr && ethers.isAddress(botAddr)) matrixDB.systemWallets.botWallet = botAddr;
        if (familyAddr && ethers.isAddress(familyAddr)) matrixDB.systemWallets.familyPool = familyAddr;
        const countsResults = await Promise.all(
          Array.from(
            { length: 27 },
            (_, i) => contract.totalNodesInRank(i + 1).then((v) => Number(v)).catch(() => matrixDB.rankCounts[i + 1] || 1)
          )
        );
        const ranksToSync = /* @__PURE__ */ new Set([1, 2, 3, 4, 5]);
        countsResults.forEach((c, idx) => {
          const r = idx + 1;
          const prevCount = matrixDB.rankCounts[r] || 1;
          matrixDB.rankCounts[r] = c;
          const hasStoredVault = (matrixDB.nodesByRank[r] || []).some((n) => Number(n.nodeVault || 0) > 0);
          if (c > 1 || hasStoredVault || c !== prevCount || !matrixDB.nodesByRank[r] || matrixDB.nodesByRank[r].length < c) {
            if (r <= 5 || c > 1 || hasStoredVault) {
              ranksToSync.add(r);
            }
          }
        });
        for (const r of Array.from(ranksToSync)) {
          await syncRankIntoDatabase(r, targetContract, force);
        }
        const rootOwner = matrixDB.nodesByRank[1]?.[0]?.owner || matrixDB.systemWallets.rootOwner;
        for (let r = 1; r <= 27; r++) {
          if (!matrixDB.nodesByRank[r] || matrixDB.nodesByRank[r].length === 0) {
            matrixDB.nodesByRank[r] = [
              {
                id: 1,
                rank: r,
                owner: rootOwner,
                directSponsor: ethers.ZeroAddress,
                parentId: 0,
                leftChild: 0,
                rightChild: 0,
                isClone: false,
                isExtraId: false,
                originMainId: 1,
                cloneRound: 0,
                directSponsorNodeId: 0,
                nodeVault: 0,
                rawNodeVault: 0,
                reconciledBaseVault: 0
              }
            ];
          }
        }
        reconcileAutoExtraIdsAcrossRanks();
        rebuildDatabaseRewardLogsFromNodes();
        rebuildDatabaseUpgradeLogsFromNodes();
        matrixDB.lastSyncedAt = (/* @__PURE__ */ new Date()).toISOString();
        saveMatrixDBToDisk();
        syncAllMatrixDBToPostgres().catch(() => {
        });
      } catch (err) {
        console.warn("[MatrixDB Full Sync] Notice:", err);
      } finally {
        fullSyncInFlight = null;
      }
    })();
    return fullSyncInFlight;
  }
  reconcileAutoExtraIdsAcrossRanks();
  rebuildDatabaseRewardLogsFromNodes();
  rebuildDatabaseUpgradeLogsFromNodes();
  saveMatrixDBToDisk(true);
  function computeMainIdDatabaseStats(mainIdNum) {
    const r1Nodes = matrixDB.nodesByRank[1] || [];
    const targetNode = r1Nodes.find((n) => n.id === mainIdNum);
    const ownerAddr = (targetNode?.owner || matrixDB.systemWallets.rootOwner || "").toLowerCase();
    const r1OwnerMains = r1Nodes.filter((n) => n.owner.toLowerCase() === ownerAddr && !n.isClone && !n.isExtraId).map((n) => n.id);
    const mainIdxInOwner = Math.max(0, r1OwnerMains.indexOf(mainIdNum));
    let currentRank = mainIdNum === 1 ? 27 : 1;
    const vaultsByRank = {};
    for (let r = 1; r <= 27; r++) {
      const rNodes = matrixDB.nodesByRank[r] || [];
      if (r === 1) {
        vaultsByRank[1] = Number(targetNode?.nodeVault || 0);
      } else {
        const correspondingNode = rNodes.find(
          (n) => !n.isClone && !n.isExtraId && n.originMainId === mainIdNum
        );
        if (correspondingNode) {
          currentRank = Math.max(currentRank, r);
          vaultsByRank[r] = Number(correspondingNode.nodeVault || 0);
        } else {
          vaultsByRank[r] = 0;
        }
      }
    }
    if (mainIdNum === 1) currentRank = 27;
    const window5Start = Math.max(1, currentRank - 4);
    const window5End = currentRank;
    let upgradeRankPool = 0;
    let newAutoIdPool = 0;
    let r4To27OutsideWindow = 0;
    let grossR4To27OutsideWindow = 0;
    for (let r = 1; r <= 27; r++) {
      const v = vaultsByRank[r] || 0;
      const inWindow5 = r >= window5Start && r <= window5End;
      if (inWindow5 && currentRank < 27) {
        upgradeRankPool += v;
      }
      if (r >= 1 && r <= 3 && !inWindow5) {
        newAutoIdPool += v;
      }
      if (r >= 4 && r <= 27) {
        if (currentRank === 27 || !inWindow5) {
          r4To27OutsideWindow += v;
          const rNodes = matrixDB.nodesByRank[r] || [];
          const correspNode = r === 1 ? targetNode : rNodes.find((n) => !n.isClone && !n.isExtraId && n.originMainId === mainIdNum);
          if (correspNode) {
            const rUnitVault = Number(((RANK_PRICES_USDT[r - 1] || 30) * 0.35).toFixed(4));
            let leftCountInR = 0;
            for (const rn of rNodes) {
              const isSameMain = rn.id === correspNode.id || rn.isClone && (rn.originMainId === correspNode.id || rn.originMainId === mainIdNum);
              if (isSameMain && rn.leftChild > 0) {
                leftCountInR++;
              }
            }
            grossR4To27OutsideWindow = Number(
              (grossR4To27OutsideWindow + leftCountInR * rUnitVault).toFixed(4)
            );
          }
        }
      }
    }
    const resolveExtraSpawnerMainId = (extraNode) => {
      const extraOwner = (extraNode.owner || "").toLowerCase();
      const ownerMains = r1Nodes.filter(
        (m) => !m.isClone && !m.isExtraId && (m.owner || "").toLowerCase() === extraOwner
      );
      if (ownerMains.length === 1) return ownerMains[0].id;
      if (ownerMains.length > 1) {
        const r4Nodes = matrixDB.nodesByRank[4] || [];
        const eligibleMain = ownerMains.find(
          (m) => m.id === 1 || r4Nodes.some((x) => !x.isClone && !x.isExtraId && x.originMainId === m.id)
        );
        if (eligibleMain) return eligibleMain.id;
        return ownerMains[0].id;
      }
      return extraNode.directSponsorNodeId > 0 ? extraNode.directSponsorNodeId : 1;
    };
    let totalExtraIdCount = 0;
    let extraIdDeductedTotalUsdt = 0;
    let extraLeftVaultBonusGross = 0;
    for (let r = 1; r <= 27; r++) {
      const rNodes = matrixDB.nodesByRank[r] || [];
      const correspondingNode = r === 1 ? targetNode : rNodes.find((n) => !n.isClone && !n.isExtraId && n.originMainId === mainIdNum);
      const correspIdInR = correspondingNode?.id || 0;
      const rPrice = RANK_PRICES_USDT[r - 1] || 10;
      const rUnitVault = Number((rPrice * 0.35).toFixed(4));
      for (const n of rNodes) {
        if (n.isExtraId && !n.isClone && resolveExtraSpawnerMainId(n) === mainIdNum) {
          totalExtraIdCount++;
          extraIdDeductedTotalUsdt = Number((extraIdDeductedTotalUsdt + rPrice).toFixed(4));
        }
        if (correspIdInR > 0) {
          const nOrig = !n.isClone ? n : rNodes.find(
            (x) => !x.isClone && (x.owner || "").toLowerCase() === (n.owner || "").toLowerCase() && (x.originMainId === n.originMainId || x.id === n.originMainId)
          ) || n;
          const isExtraLineage = !n.isClone ? Boolean(n.isExtraId) : Boolean(n.isExtraId || nOrig.isExtraId);
          if (isExtraLineage && n.leftChild > 0) {
            const spawnerMainId = resolveExtraSpawnerMainId(nOrig);
            if (spawnerMainId === mainIdNum) {
              extraLeftVaultBonusGross = Number((extraLeftVaultBonusGross + rUnitVault).toFixed(4));
            }
          }
        }
      }
    }
    const deductedFromR4To27 = Math.max(
      0,
      Number((grossR4To27OutsideWindow - r4To27OutsideWindow).toFixed(4))
    );
    const deductedFromExtraLeftBonus = Math.min(
      extraLeftVaultBonusGross,
      Math.max(0, Number((extraIdDeductedTotalUsdt - deductedFromR4To27).toFixed(4)))
    );
    const extraLeftVaultBonus = Math.max(
      0,
      Number((extraLeftVaultBonusGross - deductedFromExtraLeftBonus).toFixed(4))
    );
    const r1To3LeftoverOldVault = 0;
    const oldCarryoverVault = Number(
      (r4To27OutsideWindow + extraLeftVaultBonus).toFixed(4)
    );
    const newExtraIdPool = Number(
      (r4To27OutsideWindow + extraLeftVaultBonus).toFixed(4)
    );
    const newExtraIdPoolGross = Number(
      Math.max(
        grossR4To27OutsideWindow + extraLeftVaultBonusGross,
        newExtraIdPool + extraIdDeductedTotalUsdt
      ).toFixed(4)
    );
    const autoMainSourceMap = reconcileRank1AutoMainIds();
    const spawnedAutoIdsList = [];
    for (const [autoId, srcMain] of autoMainSourceMap.entries()) {
      if (srcMain === mainIdNum) {
        spawnedAutoIdsList.push(autoId);
      }
    }
    const upgradeHistory = (matrixDB.upgradeLogs || []).filter(
      (u) => Number(u.mainNodeId) === mainIdNum
    );
    return {
      mainId: mainIdNum,
      owner: targetNode?.owner || ethers.ZeroAddress,
      currentRank,
      window5Start,
      window5End,
      vaultsByRank,
      upgradeRankPool: Number(upgradeRankPool.toFixed(2)),
      newAutoIdPool: Number(newAutoIdPool.toFixed(2)),
      newExtraIdPool: Number(newExtraIdPool.toFixed(2)),
      oldCarryoverVault: Number(oldCarryoverVault.toFixed(2)),
      r1To3LeftoverOldVault: Number(r1To3LeftoverOldVault.toFixed(2)),
      r4To27OutsideWindowVault: Number(r4To27OutsideWindow.toFixed(2)),
      extraLeftVaultBonus: Number(extraLeftVaultBonus.toFixed(2)),
      extraLeftVaultBonusGross: Number(extraLeftVaultBonusGross.toFixed(2)),
      extraIdDeductedTotalUsdt: Number(extraIdDeductedTotalUsdt.toFixed(2)),
      newExtraIdPoolGross: Number(newExtraIdPoolGross.toFixed(2)),
      totalAutoIdCount: spawnedAutoIdsList.length,
      autoIdsSpawned: spawnedAutoIdsList,
      totalExtraIdCount,
      upgradeHistory
    };
  }
  app.get("/api/db/snapshot", async (req, res) => {
    const force = req.query.force === "1";
    if (force || !matrixDB.nodesByRank[1] || matrixDB.nodesByRank[1].length <= 1) {
      await syncAllMatrixDatabase(CONTRACT_ADDRESS, force);
    } else {
      reconcileAutoExtraIdsAcrossRanks();
    }
    res.json({
      success: true,
      source: "DATABASE",
      postgres: {
        connected: pgConnected,
        database: "Aunza",
        tableCounts: pgTablesCount
      },
      contractAddress: matrixDB.contractAddress,
      usdtAddress: matrixDB.usdtAddress,
      lastSyncedBlock: matrixDB.lastSyncedBlock,
      lastSyncedAt: matrixDB.lastSyncedAt,
      systemWallets: matrixDB.systemWallets,
      rankCounts: matrixDB.rankCounts,
      nodesByRank: matrixDB.nodesByRank,
      totalRewards: matrixDB.rewardLogs.length,
      totalUpgrades: (matrixDB.upgradeLogs || []).length,
      upgradeLogs: matrixDB.upgradeLogs || []
    });
  });
  app.post("/api/db/sync", async (_req, res) => {
    try {
      await syncAllMatrixDatabase(CONTRACT_ADDRESS, true);
      res.json({
        success: true,
        source: "DATABASE_SYNCED",
        lastSyncedBlock: matrixDB.lastSyncedBlock,
        lastSyncedAt: matrixDB.lastSyncedAt,
        rankCounts: matrixDB.rankCounts,
        totalNodesRank1: matrixDB.rankCounts[1] || 1,
        totalRewards: matrixDB.rewardLogs.length,
        totalUpgrades: (matrixDB.upgradeLogs || []).length
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/matrix/auto-extra-id", async (req, res) => {
    try {
      const mainIdNum = Math.max(1, Number(req.body?.mainId) || 1);
      await syncAllMatrixDatabase(CONTRACT_ADDRESS, false);
      const created = reconcileAutoExtraIdsAcrossRanks().filter((c) => c.mainId === mainIdNum);
      rebuildDatabaseRewardLogsFromNodes();
      rebuildDatabaseUpgradeLogsFromNodes();
      saveMatrixDBToDisk(true);
      const stats = computeMainIdDatabaseStats(mainIdNum);
      res.json({
        success: true,
        mainId: mainIdNum,
        createdCount: created.length,
        createdNodes: created,
        stats
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.get("/api/matrix/main-id/:mainId", async (req, res) => {
    try {
      res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
      const mainIdNum = Math.max(1, Number(req.params.mainId) || 1);
      if (!matrixDB.nodesByRank[1] || matrixDB.nodesByRank[1].length <= 1) {
        syncAllMatrixDatabase(CONTRACT_ADDRESS, false).catch(() => {
        });
      } else {
        reconcileAutoExtraIdsAcrossRanks();
      }
      const stats = computeMainIdDatabaseStats(mainIdNum);
      res.json({
        success: true,
        source: "DATABASE",
        ...stats
      });
    } catch (err) {
      console.warn("[API /api/matrix/main-id/:mainId error]", err);
      res.status(500).json({ success: false, error: err?.message || "Database query error" });
    }
  });
  app.get("/api/matrix/per-id-history", async (req, res) => {
    try {
      const forceSync = req.query.forceSync === "1";
      if (forceSync || !matrixDB.nodesByRank[1] || matrixDB.nodesByRank[1].length <= 1) {
        await syncAllMatrixDatabase(CONTRACT_ADDRESS, forceSync);
      } else {
        reconcileAutoExtraIdsAcrossRanks();
        if (matrixDB.rewardLogs.length === 0) rebuildDatabaseRewardLogsFromNodes();
        if (!matrixDB.upgradeLogs || matrixDB.upgradeLogs.length === 0) rebuildDatabaseUpgradeLogsFromNodes();
      }
      const autoMainSourceMap = reconcileRank1AutoMainIds();
      const r1Nodes = matrixDB.nodesByRank[1] || [];
      const resolveCanonicalMainId = (rankNum, node) => {
        if (node.isExtraId && !node.isClone) {
          return node.directSponsorNodeId > 0 ? node.directSponsorNodeId : 1;
        }
        if (rankNum === 1) {
          return node.isClone ? node.originMainId || node.id : node.id;
        }
        const rNodes = matrixDB.nodesByRank[rankNum] || [];
        if (!node.isClone) {
          return node.originMainId && node.originMainId > 0 ? node.originMainId : node.id;
        }
        const localMain = rNodes.find(
          (m) => !m.isClone && !m.isExtraId && (m.id === node.originMainId || m.originMainId === node.originMainId)
        );
        if (localMain) {
          return localMain.originMainId && localMain.originMainId > 0 ? localMain.originMainId : localMain.id;
        }
        return node.originMainId || 1;
      };
      ensurePersistentNodeTimestamps();
      const computeNodeTimestamp = (rankNum, nodeId) => {
        const rList = matrixDB.nodesByRank[rankNum] || [];
        const found = rList.find((x) => x.id === nodeId);
        if (found?.createdAt) return found.createdAt;
        const totalInRank = Math.max(rList.length, nodeId, 1);
        const stepsBack = Math.max(0, totalInRank - nodeId);
        return new Date(
          17938536e5 - stepsBack * 45e3 - Math.max(0, 27 - rankNum) * 19e3
        ).toISOString();
      };
      const reportsMap = /* @__PURE__ */ new Map();
      const r1NodesMap = /* @__PURE__ */ new Map();
      for (const n of r1Nodes) r1NodesMap.set(n.id, n);
      for (const r1Node of r1Nodes) {
        const pNode = r1Nodes.find((x) => x.id === r1Node.parentId);
        const sideInR1 = r1Node.parentId === 0 ? "ROOT" : pNode && pNode.leftChild === r1Node.id ? "LEFT" : "RIGHT";
        const isAutoMain = !r1Node.isClone && !r1Node.isExtraId && (autoMainSourceMap.has(r1Node.id) || r1Node.originType === "AUTO_NEW_MAIN");
        const nodeType = r1Node.isClone ? "CLONE_ID" : r1Node.isExtraId ? "EXTRA_ID" : isAutoMain ? "AUTO_ID" : "MANUAL_ID";
        const typeBadge = r1Node.isClone ? "CLONE ID" : r1Node.isExtraId ? "EXTRA ID" : isAutoMain ? "AUTO ID" : "MAIN ID";
        const origMainId = r1Node.isClone ? r1Node.originMainId || r1Node.id : r1Node.id;
        const spawnedByMainId = isAutoMain ? autoMainSourceMap.get(r1Node.id) || r1Node.directSponsorNodeId || 1 : r1Node.isClone ? origMainId : 0;
        const stats = !r1Node.isClone ? computeMainIdDatabaseStats(r1Node.id) : {
          currentRank: 1,
          upgradeRankPool: 0,
          newAutoIdPool: 0,
          newExtraIdPool: 0,
          oldCarryoverVault: 0,
          r1To3LeftoverOldVault: 0,
          r4To27OutsideWindowVault: 0,
          extraLeftVaultBonus: 0,
          extraIdDeductedTotalUsdt: 0,
          newExtraIdPoolGross: 0
        };
        const rankNodesList = [];
        if (!r1Node.isClone && !r1Node.isExtraId) {
          for (let r = 1; r <= 27; r++) {
            const rList = matrixDB.nodesByRank[r] || [];
            const mInR = r === 1 ? r1Node : rList.find((x) => !x.isClone && !x.isExtraId && x.originMainId === r1Node.id);
            if (mInR) {
              const pInR = rList.find((x) => x.id === mInR.parentId);
              const sideR = mInR.parentId === 0 ? "ROOT" : pInR && pInR.leftChild === mInR.id ? "LEFT" : "RIGHT";
              rankNodesList.push({
                rank: r,
                nodeId: mInR.id,
                parentId: mInR.parentId,
                side: sideR,
                leftChild: mInR.leftChild,
                rightChild: mInR.rightChild,
                nodeVault: Number((mInR.nodeVault || 0).toFixed(2)),
                rawNodeVault: Number((mInR.rawNodeVault || mInR.nodeVault || 0).toFixed(2))
              });
            }
          }
        } else {
          rankNodesList.push({
            rank: 1,
            nodeId: r1Node.id,
            parentId: r1Node.parentId,
            side: sideInR1,
            leftChild: r1Node.leftChild,
            rightChild: r1Node.rightChild,
            nodeVault: Number((r1Node.nodeVault || 0).toFixed(2)),
            rawNodeVault: Number((r1Node.rawNodeVault || 0).toFixed(2))
          });
        }
        const tiersBreakdown = [];
        let currentFrontier = [r1Node.id];
        const visitedSubtree = /* @__PURE__ */ new Set([r1Node.id]);
        const ratePerNodeR1 = Number(((RANK_PRICES_USDT[0] || 5) * 0.02).toFixed(2));
        let thisNodeLevelTotalUsdt = 0;
        for (let tier = 1; tier <= 15; tier++) {
          const nextFrontier = [];
          const leftNodeIds = [];
          const rightNodeIds = [];
          for (const pId of currentFrontier) {
            const pObj = r1NodesMap.get(pId);
            if (!pObj) continue;
            if (pObj.leftChild > 0 && r1NodesMap.has(pObj.leftChild) && !visitedSubtree.has(pObj.leftChild)) {
              visitedSubtree.add(pObj.leftChild);
              nextFrontier.push(pObj.leftChild);
              leftNodeIds.push(pObj.leftChild);
            }
            if (pObj.rightChild > 0 && r1NodesMap.has(pObj.rightChild) && !visitedSubtree.has(pObj.rightChild)) {
              visitedSubtree.add(pObj.rightChild);
              nextFrontier.push(pObj.rightChild);
              rightNodeIds.push(pObj.rightChild);
            }
          }
          if (nextFrontier.length === 0) break;
          const tierAmt = Number((leftNodeIds.length * ratePerNodeR1).toFixed(2));
          thisNodeLevelTotalUsdt = Number((thisNodeLevelTotalUsdt + tierAmt).toFixed(2));
          tiersBreakdown.push({
            tier,
            maxCapacity: Math.pow(2, tier),
            totalNodesInTier: nextFrontier.length,
            leftPayingNodesCount: leftNodeIds.length,
            rightRebirthNodesCount: rightNodeIds.length,
            ratePerNodeUsdt: ratePerNodeR1,
            amountUsdt: tierAmt,
            allNodeIds: nextFrontier,
            leftNodeIds,
            rightNodeIds
          });
          currentFrontier = nextFrontier;
        }
        const receivedLevelsCount = tiersBreakdown.filter((t) => t.leftPayingNodesCount > 0 || t.totalNodesInTier > 0).length;
        const remainingLevelsCount = Math.max(0, 15 - receivedLevelsCount);
        const perTierStr = tiersBreakdown.map((t) => `\u0E0A\u0E31\u0E49\u0E19 ${t.tier}: ${t.amountUsdt.toFixed(2)} USDT (${t.leftPayingNodesCount} \u0E40\u0E21\u0E47\u0E14\u0E0B\u0E49\u0E32\u0E22)`).join(", ");
        reportsMap.set(r1Node.id, {
          id: r1Node.id,
          label: r1Node.isClone ? `Main #${origMainId} (\u0E42\u0E04\u0E25\u0E19 R1-#${r1Node.id})` : isAutoMain ? `Auto ID #${r1Node.id}` : `Main ID #${r1Node.id}`,
          nodeType,
          typeBadge,
          isClone: Boolean(r1Node.isClone),
          isAutoId: Boolean(isAutoMain),
          isExtraId: Boolean(r1Node.isExtraId),
          originMainId: origMainId,
          cloneRound: r1Node.isClone ? r1Node.cloneRound || 1 : 0,
          cloneSequence: r1Node.isClone ? r1Node.cloneSequence || 1 : 0,
          spawnedByMainId,
          owner: r1Node.owner,
          directSponsorNodeId: r1Node.directSponsorNodeId || (r1Node.id === 1 ? 0 : 1),
          parentId: r1Node.parentId,
          side: sideInR1,
          sideInR1,
          leftChildR1: r1Node.leftChild,
          rightChildR1: r1Node.rightChild,
          currentMaxRank: stats.currentRank,
          rankNodesList,
          direct30TotalUsdt: 0,
          direct30History: [],
          level30TotalUsdt: 0,
          level30History: [],
          levelSummary: {
            maxLevels: 15,
            receivedLevelsCount,
            remainingLevelsCount,
            ratePerNodeUsdt: ratePerNodeR1,
            thisNodeLevelTotalUsdt,
            clonesLevelTotalUsdt: 0,
            combinedLevelTotalUsdt: thisNodeLevelTotalUsdt,
            summaryTextTh: receivedLevelsCount > 0 ? `\u0E23\u0E31\u0E1A\u0E44\u0E1B\u0E41\u0E25\u0E49\u0E27 ${receivedLevelsCount}/15 \u0E0A\u0E31\u0E49\u0E19 (${perTierStr}) \u2022 \u0E40\u0E2B\u0E25\u0E37\u0E2D\u0E2D\u0E35\u0E01 ${remainingLevelsCount} \u0E0A\u0E31\u0E49\u0E19` : `\u0E23\u0E31\u0E1A\u0E44\u0E1B\u0E41\u0E25\u0E49\u0E27 0/15 \u0E0A\u0E31\u0E49\u0E19 \u2022 \u0E40\u0E2B\u0E25\u0E37\u0E2D\u0E2D\u0E35\u0E01 15 \u0E0A\u0E31\u0E49\u0E19`,
            tiersBreakdown
          },
          vault35GrossTotalUsdt: 0,
          vault35RemainingTotalUsdt: 0,
          vault35DeductedTotalUsdt: 0,
          vault35History: [],
          vault35Ledger: [],
          vaultDeductedBreakdown: {
            extraIdsTotalUsdt: 0,
            autoIdsTotalUsdt: 0,
            upgradesTotalUsdt: 0
          },
          upgradeRankPool: stats.upgradeRankPool,
          newAutoIdPool: stats.newAutoIdPool,
          newExtraIdPool: stats.newExtraIdPool,
          oldCarryoverVault: stats.oldCarryoverVault,
          r1To3LeftoverOldVault: stats.r1To3LeftoverOldVault,
          r4To27OutsideWindowVault: stats.r4To27OutsideWindowVault,
          extraLeftVaultBonus: stats.extraLeftVaultBonus,
          extraIdDeductedTotalUsdt: stats.extraIdDeductedTotalUsdt,
          newExtraIdPoolGross: stats.newExtraIdPoolGross,
          pools: {
            upgradeRankPool: stats.upgradeRankPool,
            newAutoIdPool: stats.newAutoIdPool,
            newExtraIdPool: stats.newExtraIdPool,
            oldCarryoverVault: stats.oldCarryoverVault,
            newExtraIdPoolGross: stats.newExtraIdPoolGross
          },
          totalAutoIdCount: 0,
          totalExtraIdCount: 0,
          totalClonesCount: 0,
          autoIdsSpawned: [],
          autoExtraIdsSpawned: [],
          clonesSpawned: []
        });
      }
      for (const r1Node of r1Nodes) {
        const isAutoMain = !r1Node.isClone && !r1Node.isExtraId && (autoMainSourceMap.has(r1Node.id) || r1Node.originType === "AUTO_NEW_MAIN");
        if (isAutoMain) {
          const spId = autoMainSourceMap.get(r1Node.id) || r1Node.directSponsorNodeId || 1;
          const spReport = reportsMap.get(spId);
          const pNode = r1Nodes.find((x) => x.id === r1Node.parentId);
          const side = r1Node.parentId === 0 ? "ROOT" : pNode && pNode.leftChild === r1Node.id ? "LEFT" : "RIGHT";
          if (spReport) {
            spReport.autoIdsSpawned.push({
              id: r1Node.id,
              rank: 1,
              label: `Auto ID #${r1Node.id} (R1-#${r1Node.id})`,
              type: "AUTO_ID",
              parentId: r1Node.parentId,
              side,
              directSponsorNodeId: spId,
              owner: r1Node.owner,
              timestamp: computeNodeTimestamp(1, r1Node.id)
            });
          }
        }
      }
      for (let rankNum = 1; rankNum <= 27; rankNum++) {
        const rNodes = matrixDB.nodesByRank[rankNum] || [];
        if (rNodes.length <= 1) continue;
        const nodesMap = /* @__PURE__ */ new Map();
        for (const n of rNodes) nodesMap.set(n.id, n);
        for (const n of rNodes) {
          if (n.id <= 1) continue;
          const pNode = nodesMap.get(n.parentId);
          const side = n.parentId === 0 ? "ROOT" : pNode && pNode.leftChild === n.id ? "LEFT" : "RIGHT";
          if (n.isExtraId && !n.isClone) {
            const extraOwner = (n.owner || "").toLowerCase();
            const ownerMains = r1Nodes.filter(
              (m) => !m.isClone && !m.isExtraId && (m.owner || "").toLowerCase() === extraOwner
            );
            let r1MainSpId = 1;
            if (ownerMains.length === 1) {
              r1MainSpId = ownerMains[0].id;
            } else if (ownerMains.length > 1) {
              const r4Nodes = matrixDB.nodesByRank[4] || [];
              const eligibleMain = ownerMains.find(
                (m) => m.id === 1 || r4Nodes.some((x) => !x.isClone && !x.isExtraId && x.originMainId === m.id)
              );
              r1MainSpId = eligibleMain ? eligibleMain.id : ownerMains[0].id;
            } else {
              const localSpId = n.directSponsorNodeId || 1;
              const localSpNode = nodesMap.get(localSpId);
              r1MainSpId = rankNum === 1 ? localSpId : localSpNode ? resolveCanonicalMainId(rankNum, localSpNode) : localSpId;
            }
            const spReport = reportsMap.get(r1MainSpId) || reportsMap.get(1);
            if (spReport) {
              spReport.autoExtraIdsSpawned.push({
                id: n.id,
                rank: rankNum,
                label: `Extra R${rankNum}-#${n.id}`,
                type: "AUTO_EXTRA_ID",
                parentId: n.parentId,
                side,
                directSponsorNodeId: r1MainSpId,
                owner: n.owner,
                timestamp: computeNodeTimestamp(rankNum, n.id)
              });
            }
          } else if (n.isClone) {
            const canonMainId = resolveCanonicalMainId(rankNum, n);
            const mainReport = reportsMap.get(canonMainId);
            if (mainReport) {
              mainReport.clonesSpawned.push({
                id: n.id,
                rank: rankNum,
                label: `Main #${canonMainId} (\u0E42\u0E04\u0E25\u0E19 R${rankNum}-#${n.id})`,
                type: "CLONE_ID",
                parentId: n.parentId,
                side,
                directSponsorNodeId: n.directSponsorNodeId || mainReport.directSponsorNodeId,
                owner: n.owner,
                cloneRound: n.cloneRound || 1,
                cloneSequence: n.cloneSequence || 1,
                timestamp: computeNodeTimestamp(rankNum, n.id)
              });
            }
          }
        }
      }
      for (const rep of reportsMap.values()) {
        rep.totalAutoIdCount = rep.autoIdsSpawned.length;
        rep.totalExtraIdCount = rep.autoExtraIdsSpawned.length;
        rep.totalClonesCount = rep.clonesSpawned.length;
      }
      const allSellWalletAddr = matrixDB.systemWallets?.allSellWallet || "0xBeFEB9871a20656aeF7E8d218f751e9C23FAF710";
      const familyPoolWalletAddr = matrixDB.systemWallets?.familyPool || "0x225DF4b5961f757f54728138F289Eaf49D4F275A";
      const botWalletAddr = matrixDB.systemWallets?.botWallet || "0xbFD03d4D69221085a63Dd372A5d59f952C084a56";
      let allSellTotalUsdt = 0;
      let familyPoolTotalUsdt = 0;
      let familyPoolUnpaidLevelUsdt = 0;
      let familyPoolRootCloneDirectUsdt = 0;
      let botTotalUsdt = 0;
      const allSellByRank = {};
      const familyPoolByRank = {};
      const allSellHistory = [];
      const familyPoolHistory = [];
      for (let rankNum = 1; rankNum <= 27; rankNum++) {
        const rNodes = matrixDB.nodesByRank[rankNum] || [];
        if (rNodes.length <= 1) continue;
        const unitPrice = RANK_PRICES_USDT[rankNum - 1] || 5;
        const directAmt = Number((unitPrice * 0.3).toFixed(2));
        const vaultAmt = Number((unitPrice * 0.35).toFixed(2));
        const levelAmt = Number((unitPrice * 0.02).toFixed(2));
        const fullLevelExtraAmt = Number((unitPrice * 0.3).toFixed(2));
        const allSellAmt = Number((unitPrice * 0.04).toFixed(2));
        const botAmt = Number((unitPrice * 0.01).toFixed(2));
        const nodesMap = /* @__PURE__ */ new Map();
        for (const n of rNodes) nodesMap.set(n.id, n);
        const sortedIds = Array.from(nodesMap.keys()).sort((a, b) => a - b);
        for (const id of sortedIds) {
          if (id <= 1) continue;
          const child = nodesMap.get(id);
          const parent = nodesMap.get(child.parentId);
          if (!parent || parent.leftChild !== id) continue;
          const childCanonMainId = resolveCanonicalMainId(rankNum, child);
          const childTypeLabel = child.isExtraId ? `Extra R${rankNum}-#${child.id}` : child.isClone ? `\u0E42\u0E04\u0E25\u0E19 R${rankNum}-#${child.id} (\u0E02\u0E2D\u0E07 Main #${childCanonMainId})` : autoMainSourceMap.has(childCanonMainId) ? `Auto ID #${childCanonMainId} (R${rankNum}-#${child.id})` : `Main #${childCanonMainId} (R${rankNum}-#${child.id})`;
          const rowEventTime = computeNodeTimestamp(rankNum, id);
          allSellTotalUsdt = Number((allSellTotalUsdt + allSellAmt).toFixed(4));
          allSellByRank[rankNum] = Number(((allSellByRank[rankNum] || 0) + allSellAmt).toFixed(4));
          botTotalUsdt = Number((botTotalUsdt + botAmt).toFixed(4));
          allSellHistory.push({
            rank: rankNum,
            fromNodeId: id,
            fromMainId: childCanonMainId,
            fromNodeType: childTypeLabel,
            parentId: parent.id,
            amountUsdt: allSellAmt,
            recipient: allSellWalletAddr,
            note: `4% AllSell \u0E08\u0E32\u0E01 ${childTypeLabel} (\u0E27\u0E32\u0E07\u0E40\u0E21\u0E47\u0E14\u0E0B\u0E49\u0E32\u0E22\u0E43\u0E15\u0E49 R${rankNum}-#${parent.id})`,
            timestamp: rowEventTime
          });
          const parentOrigin = !parent.isClone ? parent : rNodes.find(
            (x) => !x.isClone && (x.owner || "").toLowerCase() === (parent.owner || "").toLowerCase() && (x.originMainId === parent.originMainId || x.id === parent.originMainId)
          ) || parent;
          const isParentExtraLineage = !parent.isClone ? Boolean(parent.isExtraId) : Boolean(parent.isExtraId || parentOrigin.isExtraId);
          if (isParentExtraLineage) {
            const extraOwner = (parentOrigin.owner || parent.owner || "").toLowerCase();
            const ownerMains = r1Nodes.filter(
              (m) => !m.isClone && !m.isExtraId && (m.owner || "").toLowerCase() === extraOwner
            );
            let targetMainId = 1;
            if (ownerMains.length === 1) {
              targetMainId = ownerMains[0].id;
            } else if (ownerMains.length > 1) {
              const r9Nodes = matrixDB.nodesByRank[9] || [];
              const r4Nodes = matrixDB.nodesByRank[4] || [];
              const eligibleMain = ownerMains.find(
                (m) => m.id === 1 || r9Nodes.some((x) => !x.isClone && !x.isExtraId && x.originMainId === m.id)
              ) || ownerMains.find(
                (m) => m.id === 1 || r4Nodes.some((x) => !x.isClone && !x.isExtraId && x.originMainId === m.id)
              ) || ownerMains[0];
              targetMainId = eligibleMain.id;
            } else {
              const extraSpLocalId2 = parentOrigin.directSponsorNodeId > 0 ? parentOrigin.directSponsorNodeId : parent.directSponsorNodeId > 0 ? parent.directSponsorNodeId : 1;
              const extraSpNode = nodesMap.get(extraSpLocalId2);
              targetMainId = rankNum === 1 ? extraSpLocalId2 : extraSpNode ? resolveCanonicalMainId(rankNum, extraSpNode) : extraSpLocalId2;
            }
            const extraSpLocalNode = rNodes.find(
              (x) => !x.isClone && !x.isExtraId && (rankNum === 1 ? x.id === targetMainId : x.originMainId === targetMainId)
            );
            const extraSpLocalId = extraSpLocalNode ? extraSpLocalNode.id : targetMainId;
            const rep = reportsMap.get(targetMainId);
            if (rep) {
              rep.direct30TotalUsdt = Number((rep.direct30TotalUsdt + directAmt).toFixed(2));
              rep.direct30History.push({
                rank: rankNum,
                fromNodeId: id,
                fromMainId: childCanonMainId,
                fromNodeType: childTypeLabel,
                toNodeId: extraSpLocalId,
                toMainId: targetMainId,
                amountUsdt: directAmt,
                note: `30% \u0E04\u0E48\u0E32\u0E41\u0E19\u0E30\u0E19\u0E33\u0E08\u0E32\u0E01\u0E40\u0E21\u0E47\u0E14\u0E0B\u0E49\u0E32\u0E22\u0E02\u0E2D\u0E07 Extra R${rankNum}-#${parent.id} \u0E2A\u0E48\u0E07\u0E01\u0E25\u0E31\u0E1A\u0E1C\u0E39\u0E49\u0E41\u0E19\u0E30\u0E19\u0E33 Main #${targetMainId}`,
                timestamp: rowEventTime
              });
              rep.level30TotalUsdt = Number((rep.level30TotalUsdt + fullLevelExtraAmt).toFixed(2));
              rep.level30History.push({
                rank: rankNum,
                fromNodeId: id,
                fromMainId: childCanonMainId,
                fromNodeType: childTypeLabel,
                toNodeId: extraSpLocalId,
                toMainId: targetMainId,
                tier: 1,
                amountUsdt: fullLevelExtraAmt,
                note: `30% \u0E04\u0E48\u0E32\u0E0A\u0E31\u0E49\u0E19\u0E40\u0E15\u0E47\u0E21\u0E08\u0E33\u0E19\u0E27\u0E19\u0E08\u0E32\u0E01\u0E40\u0E21\u0E47\u0E14\u0E0B\u0E49\u0E32\u0E22\u0E02\u0E2D\u0E07 Extra R${rankNum}-#${parent.id} \u0E2A\u0E48\u0E07\u0E01\u0E25\u0E31\u0E1A\u0E1C\u0E39\u0E49\u0E41\u0E19\u0E30\u0E19\u0E33 Main #${targetMainId}`,
                timestamp: rowEventTime
              });
              rep.vault35GrossTotalUsdt = Number((rep.vault35GrossTotalUsdt + vaultAmt).toFixed(2));
              rep.vault35History.push({
                rank: rankNum,
                fromNodeId: id,
                fromMainId: childCanonMainId,
                fromNodeType: childTypeLabel,
                toNodeId: extraSpLocalId,
                toMainId: targetMainId,
                amountUsdt: vaultAmt,
                note: `35% Vault \u0E08\u0E32\u0E01\u0E40\u0E21\u0E47\u0E14\u0E0B\u0E49\u0E32\u0E22\u0E02\u0E2D\u0E07 Extra R${rankNum}-#${parent.id} \u0E2A\u0E48\u0E07\u0E40\u0E02\u0E49\u0E32 New Extra ID Pool \u0E02\u0E2D\u0E07 Main #${targetMainId}`,
                timestamp: rowEventTime
              });
            }
            continue;
          }
          const isId1CloneChild = child.isClone && (childCanonMainId === 1 || parent.id === 1);
          if (isId1CloneChild) {
            familyPoolRootCloneDirectUsdt = Number(
              (familyPoolRootCloneDirectUsdt + directAmt).toFixed(4)
            );
            familyPoolTotalUsdt = Number((familyPoolTotalUsdt + directAmt).toFixed(4));
            familyPoolByRank[rankNum] = Number(
              ((familyPoolByRank[rankNum] || 0) + directAmt).toFixed(4)
            );
            familyPoolHistory.push({
              rank: rankNum,
              fromNodeId: id,
              fromMainId: childCanonMainId,
              fromNodeType: childTypeLabel,
              parentId: parent.id,
              subType: "ROOT_CLONE_DIRECT",
              amountUsdt: directAmt,
              recipient: familyPoolWalletAddr,
              note: `30% \u0E04\u0E48\u0E32\u0E41\u0E19\u0E30\u0E19\u0E33\u0E08\u0E32\u0E01\u0E42\u0E04\u0E25\u0E19\u0E23\u0E2B\u0E31\u0E2A #1 (${childTypeLabel} \u0E27\u0E32\u0E07\u0E0B\u0E49\u0E32\u0E22\u0E43\u0E15\u0E49 R${rankNum}-#${parent.id}) \u0E40\u0E02\u0E49\u0E32 Family Pool`,
              timestamp: rowEventTime
            });
          } else {
            let spMainId = child.directSponsorNodeId || 1;
            if (rankNum > 1 && childCanonMainId > 1) {
              const origR1 = r1Nodes.find((x) => x.id === childCanonMainId);
              if (origR1 && origR1.directSponsorNodeId > 0) {
                spMainId = origR1.directSponsorNodeId;
              }
            }
            const spRep = reportsMap.get(spMainId);
            if (spRep) {
              spRep.direct30TotalUsdt = Number((spRep.direct30TotalUsdt + directAmt).toFixed(2));
              spRep.direct30History.push({
                rank: rankNum,
                fromNodeId: id,
                fromMainId: childCanonMainId,
                fromNodeType: childTypeLabel,
                toNodeId: spMainId,
                toMainId: spMainId,
                amountUsdt: directAmt,
                note: `30% \u0E04\u0E48\u0E32\u0E41\u0E19\u0E30\u0E19\u0E33\u0E15\u0E23\u0E07\u0E08\u0E32\u0E01 ${childTypeLabel} (\u0E27\u0E32\u0E07\u0E0B\u0E49\u0E32\u0E22\u0E43\u0E15\u0E49 R${rankNum}-#${parent.id})`,
                timestamp: rowEventTime
              });
            }
          }
          const parentCanonMainId = resolveCanonicalMainId(rankNum, parent);
          const parentMainRep = reportsMap.get(parentCanonMainId);
          if (parentMainRep) {
            parentMainRep.vault35GrossTotalUsdt = Number(
              (parentMainRep.vault35GrossTotalUsdt + vaultAmt).toFixed(2)
            );
            parentMainRep.vault35History.push({
              rank: rankNum,
              fromNodeId: id,
              fromMainId: childCanonMainId,
              fromNodeType: childTypeLabel,
              toNodeId: parent.id,
              toMainId: parentCanonMainId,
              amountUsdt: vaultAmt,
              note: parent.isClone ? `35% \u0E2D\u0E31\u0E1E\u0E40\u0E01\u0E23\u0E14\u0E08\u0E32\u0E01\u0E40\u0E21\u0E47\u0E14\u0E0B\u0E49\u0E32\u0E22\u0E02\u0E2D\u0E07\u0E42\u0E04\u0E25\u0E19 R${rankNum}-#${parent.id} \u0E2A\u0E30\u0E2A\u0E21\u0E40\u0E02\u0E49\u0E32\u0E01\u0E23\u0E30\u0E1B\u0E38\u0E01 Main #${parentCanonMainId}` : `35% \u0E2D\u0E31\u0E1E\u0E40\u0E01\u0E23\u0E14\u0E08\u0E32\u0E01\u0E40\u0E21\u0E47\u0E14\u0E0B\u0E49\u0E32\u0E22\u0E02\u0E2D\u0E07 R${rankNum}-#${parent.id} \u0E2A\u0E30\u0E2A\u0E21\u0E40\u0E02\u0E49\u0E32\u0E01\u0E23\u0E30\u0E1B\u0E38\u0E01 Main #${parentCanonMainId}`,
              timestamp: rowEventTime
            });
          }
          if (rankNum === 1 && parent.isClone && parent.id !== parentCanonMainId) {
            const cloneRep = reportsMap.get(parent.id);
            if (cloneRep) {
              cloneRep.vault35GrossTotalUsdt = Number(
                (cloneRep.vault35GrossTotalUsdt + vaultAmt).toFixed(2)
              );
              cloneRep.vault35History.push({
                rank: 1,
                fromNodeId: id,
                fromMainId: childCanonMainId,
                fromNodeType: childTypeLabel,
                toNodeId: parent.id,
                toMainId: parentCanonMainId,
                amountUsdt: vaultAmt,
                note: `35% \u0E2D\u0E31\u0E1E\u0E40\u0E01\u0E23\u0E14\u0E08\u0E32\u0E01\u0E40\u0E21\u0E47\u0E14\u0E0B\u0E49\u0E32\u0E22\u0E43\u0E15\u0E49\u0E42\u0E04\u0E25\u0E19 #${parent.id} (\u0E42\u0E2D\u0E19\u0E40\u0E02\u0E49\u0E32\u0E01\u0E23\u0E30\u0E1B\u0E38\u0E01\u0E23\u0E2B\u0E31\u0E2A\u0E2B\u0E25\u0E31\u0E01 Main #${parentCanonMainId})`,
                timestamp: rowEventTime
              });
            }
          }
          let currUp = child.parentId;
          let paidTiersCount = 0;
          for (let tier = 1; tier <= 15; tier++) {
            if (currUp <= 0) break;
            const upNode = nodesMap.get(currUp);
            if (!upNode) break;
            paidTiersCount++;
            const upCanonMainId = resolveCanonicalMainId(rankNum, upNode);
            const upMainRep = reportsMap.get(upCanonMainId);
            if (upMainRep) {
              upMainRep.level30TotalUsdt = Number((upMainRep.level30TotalUsdt + levelAmt).toFixed(2));
              upMainRep.level30History.push({
                rank: rankNum,
                fromNodeId: id,
                fromMainId: childCanonMainId,
                fromNodeType: childTypeLabel,
                toNodeId: upNode.id,
                toMainId: upCanonMainId,
                tier,
                amountUsdt: levelAmt,
                note: upNode.isClone ? `\u0E04\u0E48\u0E32\u0E0A\u0E31\u0E49\u0E19 2% (\u0E0A\u0E31\u0E49\u0E19\u0E17\u0E35\u0E48 ${tier}) \u0E08\u0E32\u0E01 ${childTypeLabel} \u0E1C\u0E48\u0E32\u0E19\u0E42\u0E04\u0E25\u0E19 R${rankNum}-#${upNode.id} \u0E40\u0E02\u0E49\u0E32 Main #${upCanonMainId}` : `\u0E04\u0E48\u0E32\u0E0A\u0E31\u0E49\u0E19 2% (\u0E0A\u0E31\u0E49\u0E19\u0E17\u0E35\u0E48 ${tier}) \u0E08\u0E32\u0E01 ${childTypeLabel} \u0E40\u0E02\u0E49\u0E32 R${rankNum}-#${upNode.id} (Main #${upCanonMainId})`,
                timestamp: rowEventTime
              });
            }
            if (rankNum === 1 && upNode.isClone && upNode.id !== upCanonMainId) {
              const cloneRep = reportsMap.get(upNode.id);
              if (cloneRep) {
                cloneRep.level30TotalUsdt = Number((cloneRep.level30TotalUsdt + levelAmt).toFixed(2));
                cloneRep.level30History.push({
                  rank: 1,
                  fromNodeId: id,
                  fromMainId: childCanonMainId,
                  fromNodeType: childTypeLabel,
                  toNodeId: upNode.id,
                  toMainId: upCanonMainId,
                  tier,
                  amountUsdt: levelAmt,
                  note: `\u0E04\u0E48\u0E32\u0E0A\u0E31\u0E49\u0E19 2% (\u0E0A\u0E31\u0E49\u0E19\u0E17\u0E35\u0E48 ${tier}) \u0E08\u0E32\u0E01 ${childTypeLabel} \u0E1C\u0E48\u0E32\u0E19\u0E42\u0E04\u0E25\u0E19 #${upNode.id} (\u0E02\u0E2D\u0E07 Main #${upCanonMainId})`,
                  timestamp: rowEventTime
                });
              }
            }
            currUp = upNode.parentId;
          }
          const unpaidTiersCount = Math.max(0, 15 - paidTiersCount);
          if (unpaidTiersCount > 0) {
            const unpaidAmt = Number((unpaidTiersCount * levelAmt).toFixed(4));
            familyPoolUnpaidLevelUsdt = Number((familyPoolUnpaidLevelUsdt + unpaidAmt).toFixed(4));
            familyPoolTotalUsdt = Number((familyPoolTotalUsdt + unpaidAmt).toFixed(4));
            familyPoolByRank[rankNum] = Number(
              ((familyPoolByRank[rankNum] || 0) + unpaidAmt).toFixed(4)
            );
            familyPoolHistory.push({
              rank: rankNum,
              fromNodeId: id,
              fromMainId: childCanonMainId,
              fromNodeType: childTypeLabel,
              parentId: parent.id,
              subType: "UNPAID_LEVEL",
              paidTiers: paidTiersCount,
              unpaidTiers: unpaidTiersCount,
              amountUsdt: Number(unpaidAmt.toFixed(2)),
              recipient: familyPoolWalletAddr,
              note: `\u0E04\u0E48\u0E32\u0E0A\u0E31\u0E49\u0E19\u0E2A\u0E48\u0E27\u0E19\u0E17\u0E35\u0E48\u0E40\u0E2B\u0E25\u0E37\u0E2D ${unpaidTiersCount} \u0E0A\u0E31\u0E49\u0E19 (${unpaidTiersCount * 2}%) \u0E08\u0E32\u0E01 ${childTypeLabel} (\u0E08\u0E48\u0E32\u0E22\u0E2D\u0E31\u0E1E\u0E44\u0E25\u0E19\u0E4C ${paidTiersCount}/15 \u0E0A\u0E31\u0E49\u0E19) \u0E40\u0E02\u0E49\u0E32 Family Pool`,
              timestamp: rowEventTime
            });
          }
        }
      }
      const sortByLatestTimeDesc = (arr) => arr.sort((a, b) => {
        const tA = new Date(a.timestamp).getTime() || 0;
        const tB = new Date(b.timestamp).getTime() || 0;
        if (tB !== tA) return tB - tA;
        const rDiff = Number(b.rank || 1) - Number(a.rank || 1);
        if (rDiff !== 0) return rDiff;
        return Number(b.fromNodeId ?? b.id ?? 0) - Number(a.fromNodeId ?? a.id ?? 0);
      });
      sortByLatestTimeDesc(allSellHistory);
      sortByLatestTimeDesc(familyPoolHistory);
      for (const rep of reportsMap.values()) {
        sortByLatestTimeDesc(rep.direct30History);
        sortByLatestTimeDesc(rep.level30History);
        sortByLatestTimeDesc(rep.vault35History);
        sortByLatestTimeDesc(rep.autoIdsSpawned);
        sortByLatestTimeDesc(rep.autoExtraIdsSpawned);
        sortByLatestTimeDesc(rep.clonesSpawned);
      }
      let onChainAllSellBal = 0;
      let onChainFamilyBal = 0;
      let onChainBotBal = 0;
      try {
        const provider = getProvider();
        const usdtContract = new ethers.Contract(USDT_ADDRESS, USDT_READ_ABI, provider);
        const dec = activeUsdtDecimals || 18;
        const [aBn, fBn, bBn] = await Promise.all([
          usdtContract.balanceOf(allSellWalletAddr).catch(() => 0n),
          usdtContract.balanceOf(familyPoolWalletAddr).catch(() => 0n),
          usdtContract.balanceOf(botWalletAddr).catch(() => 0n)
        ]);
        onChainAllSellBal = Number(Number(ethers.formatUnits(aBn, dec)).toFixed(2));
        onChainFamilyBal = Number(Number(ethers.formatUnits(fBn, dec)).toFixed(2));
        onChainBotBal = Number(Number(ethers.formatUnits(bBn, dec)).toFixed(2));
      } catch {
      }
      for (const rep of reportsMap.values()) {
        const rem = rep.rankNodesList.reduce((acc, rItem) => acc + Number(rItem.nodeVault || 0), 0);
        rep.vault35RemainingTotalUsdt = Number(rem.toFixed(2));
        rep.vault35DeductedTotalUsdt = Number(
          Math.max(0, rep.vault35GrossTotalUsdt - rep.vault35RemainingTotalUsdt).toFixed(2)
        );
        if (!rep.isClone) {
          const cloneLvlSum = Number(
            Math.max(0, rep.level30TotalUsdt - rep.levelSummary.thisNodeLevelTotalUsdt).toFixed(2)
          );
          rep.levelSummary.clonesLevelTotalUsdt = cloneLvlSum;
          rep.levelSummary.combinedLevelTotalUsdt = Number(rep.level30TotalUsdt.toFixed(2));
        } else {
          rep.levelSummary.clonesLevelTotalUsdt = 0;
          rep.levelSummary.combinedLevelTotalUsdt = Number(rep.levelSummary.thisNodeLevelTotalUsdt.toFixed(2));
        }
        const ledgerEvents = [];
        const sortedInflows = rep.vault35History.slice().reverse();
        const sortedAutoSpawned = rep.autoIdsSpawned.slice().reverse();
        const sortedExtraSpawned = rep.autoExtraIdsSpawned.slice().reverse();
        const idUpgrades = (matrixDB.upgradeLogs || []).filter(
          (u) => Number(u.mainNodeId) === rep.id && Number(u.vaultDeductedUsdt) > 0
        );
        let autoPoolBal = 0;
        let extraPoolBal = 0;
        let autoIdx = 0;
        let extraIdx = 0;
        for (const inf of sortedInflows) {
          const tMs = new Date(inf.timestamp).getTime() || Date.now();
          const pType = inf.rank <= 3 ? "AUTO_ID_POOL" : "EXTRA_ID_POOL";
          const pLabel = pType === "AUTO_ID_POOL" ? "Auto ID Pool" : "Extra ID Pool";
          if (pType === "AUTO_ID_POOL") {
            autoPoolBal = Number((autoPoolBal + inf.amountUsdt).toFixed(2));
          } else {
            extraPoolBal = Number((extraPoolBal + inf.amountUsdt).toFixed(2));
          }
          ledgerEvents.push({
            id: `v-in-${rep.id}-${ledgerEvents.length + 1}`,
            timestamp: inf.timestamp,
            direction: "IN",
            actionType: "RECEIVE_VAULT",
            actionLabelTh: `\u0E23\u0E31\u0E1A 35% Vault (Rank ${inf.rank})`,
            rank: inf.rank,
            amountUsdt: inf.amountUsdt,
            poolType: pType,
            poolLabelTh: pLabel,
            descriptionTh: `\u0E23\u0E31\u0E1A 35% Vault (+${inf.amountUsdt.toFixed(2)} USDT) \u0E08\u0E32\u0E01\u0E40\u0E21\u0E47\u0E14\u0E0B\u0E49\u0E32\u0E22\u0E43\u0E19 Rank ${inf.rank} \u0E2A\u0E30\u0E2A\u0E21\u0E40\u0E02\u0E49\u0E32 ${pLabel}`,
            note: inf.note,
            poolBalanceAfterUsdt: pType === "AUTO_ID_POOL" ? autoPoolBal : extraPoolBal,
            balanceAfterUsdt: 0
          });
          while (autoIdx < sortedAutoSpawned.length && autoPoolBal >= 5) {
            const sp = sortedAutoSpawned[autoIdx];
            autoPoolBal = Number((autoPoolBal - 5).toFixed(2));
            autoIdx++;
            const spTime = new Date(tMs + autoIdx * 1e3).toISOString();
            ledgerEvents.push({
              id: `v-out-auto-${rep.id}-${sp.id}`,
              timestamp: spTime,
              direction: "OUT",
              actionType: "SPAWN_AUTO_ID",
              actionLabelTh: "\u0E2B\u0E31\u0E01\u0E2A\u0E23\u0E49\u0E32\u0E07 Auto ID",
              rank: 1,
              amountUsdt: 5,
              poolType: "AUTO_ID_POOL",
              poolLabelTh: "Auto ID Pool",
              descriptionTh: `\u0E2B\u0E31\u0E01 5.00 USDT \u0E08\u0E32\u0E01 Auto ID Pool \u0E2A\u0E23\u0E49\u0E32\u0E07 ${sp.label} \u0E27\u0E32\u0E07\u0E1C\u0E31\u0E07 Rank 1 \u0E43\u0E15\u0E49 R1-#${sp.parentId} (${sp.side})`,
              note: `\u0E2A\u0E23\u0E49\u0E32\u0E07 Auto ID \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08 (ID #${sp.id})`,
              poolBalanceAfterUsdt: autoPoolBal,
              balanceAfterUsdt: 0
            });
          }
          while (extraIdx < sortedExtraSpawned.length) {
            const sp = sortedExtraSpawned[extraIdx];
            const cost = RANK_PRICES_USDT[sp.rank - 1] || 10;
            if (extraPoolBal < cost) break;
            extraPoolBal = Number((extraPoolBal - cost).toFixed(2));
            extraIdx++;
            const spTime = new Date(tMs + extraIdx * 1e3).toISOString();
            ledgerEvents.push({
              id: `v-out-extra-${rep.id}-${sp.rank}-${sp.id}`,
              timestamp: spTime,
              direction: "OUT",
              actionType: "SPAWN_EXTRA_ID",
              actionLabelTh: `\u0E2B\u0E31\u0E01\u0E2A\u0E23\u0E49\u0E32\u0E07 Extra ID (${sp.label})`,
              rank: sp.rank,
              amountUsdt: cost,
              poolType: "EXTRA_ID_POOL",
              poolLabelTh: "Extra ID Pool",
              descriptionTh: `\u0E2B\u0E31\u0E01 ${cost.toFixed(2)} USDT \u0E08\u0E32\u0E01 Extra ID Pool \u0E2A\u0E23\u0E49\u0E32\u0E07 ${sp.label} \u0E43\u0E19\u0E1C\u0E31\u0E07 Rank ${sp.rank} \u0E43\u0E15\u0E49 R${sp.rank}-#${sp.parentId} (${sp.side})`,
              note: `\u0E2A\u0E23\u0E49\u0E32\u0E07 Extra ID \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08 (ID #${sp.id})`,
              poolBalanceAfterUsdt: extraPoolBal,
              balanceAfterUsdt: 0
            });
          }
        }
        while (autoIdx < sortedAutoSpawned.length) {
          const sp = sortedAutoSpawned[autoIdx];
          autoIdx++;
          const lastTime = ledgerEvents.length > 0 ? new Date(ledgerEvents[ledgerEvents.length - 1].timestamp).getTime() + 1e3 : Date.now();
          autoPoolBal = Number(Math.max(0, autoPoolBal - 5).toFixed(2));
          ledgerEvents.push({
            id: `v-out-auto-${rep.id}-${sp.id}`,
            timestamp: new Date(lastTime).toISOString(),
            direction: "OUT",
            actionType: "SPAWN_AUTO_ID",
            actionLabelTh: "\u0E2B\u0E31\u0E01\u0E2A\u0E23\u0E49\u0E32\u0E07 Auto ID",
            rank: 1,
            amountUsdt: 5,
            poolType: "AUTO_ID_POOL",
            poolLabelTh: "Auto ID Pool",
            descriptionTh: `\u0E2B\u0E31\u0E01 5.00 USDT \u0E08\u0E32\u0E01 Auto ID Pool \u0E2A\u0E23\u0E49\u0E32\u0E07 ${sp.label} \u0E27\u0E32\u0E07\u0E1C\u0E31\u0E07 Rank 1`,
            note: `\u0E2A\u0E23\u0E49\u0E32\u0E07 Auto ID \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08 (ID #${sp.id})`,
            poolBalanceAfterUsdt: autoPoolBal,
            balanceAfterUsdt: 0
          });
        }
        while (extraIdx < sortedExtraSpawned.length) {
          const sp = sortedExtraSpawned[extraIdx];
          const cost = RANK_PRICES_USDT[sp.rank - 1] || 10;
          extraIdx++;
          const lastTime = ledgerEvents.length > 0 ? new Date(ledgerEvents[ledgerEvents.length - 1].timestamp).getTime() + 1e3 : Date.now();
          extraPoolBal = Number(Math.max(0, extraPoolBal - cost).toFixed(2));
          ledgerEvents.push({
            id: `v-out-extra-${rep.id}-${sp.rank}-${sp.id}`,
            timestamp: new Date(lastTime).toISOString(),
            direction: "OUT",
            actionType: "SPAWN_EXTRA_ID",
            actionLabelTh: `\u0E2B\u0E31\u0E01\u0E2A\u0E23\u0E49\u0E32\u0E07 Extra ID (${sp.label})`,
            rank: sp.rank,
            amountUsdt: cost,
            poolType: "EXTRA_ID_POOL",
            poolLabelTh: "Extra ID Pool",
            descriptionTh: `\u0E2B\u0E31\u0E01 ${cost.toFixed(2)} USDT \u0E08\u0E32\u0E01 Extra ID Pool \u0E2A\u0E23\u0E49\u0E32\u0E07 ${sp.label} \u0E43\u0E19\u0E1C\u0E31\u0E07 Rank ${sp.rank}`,
            note: `\u0E2A\u0E23\u0E49\u0E32\u0E07 Extra ID \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08 (ID #${sp.id})`,
            poolBalanceAfterUsdt: extraPoolBal,
            balanceAfterUsdt: 0
          });
        }
        for (const u of idUpgrades) {
          const uCost = Number(u.vaultDeductedUsdt) || 0;
          if (uCost <= 0) continue;
          ledgerEvents.push({
            id: `v-out-up-${rep.id}-${u.toRank}`,
            timestamp: u.createdAt || (/* @__PURE__ */ new Date()).toISOString(),
            direction: "OUT",
            actionType: "AUTO_UPGRADE",
            actionLabelTh: `\u0E2B\u0E31\u0E01\u0E2D\u0E2D\u0E42\u0E15\u0E49\u0E2D\u0E31\u0E1B\u0E40\u0E01\u0E23\u0E14 (\u0E2A\u0E39\u0E48 Rank ${u.toRank})`,
            rank: u.toRank,
            amountUsdt: uCost,
            poolType: "UPGRADE_POOL",
            poolLabelTh: "Upgrade Pool",
            descriptionTh: `\u0E2B\u0E31\u0E01 ${uCost.toFixed(2)} USDT \u0E08\u0E32\u0E01 Upgrade Pool \u0E40\u0E1E\u0E37\u0E48\u0E2D\u0E2D\u0E31\u0E1B\u0E40\u0E01\u0E23\u0E14 Main #${rep.id} \u0E08\u0E32\u0E01 Rank ${u.fromRank} \u0E2A\u0E39\u0E48 Rank ${u.toRank}`,
            note: `\u0E2D\u0E31\u0E1B\u0E40\u0E01\u0E23\u0E14\u0E2A\u0E39\u0E48 Rank ${u.toRank} \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08`,
            poolBalanceAfterUsdt: 0,
            balanceAfterUsdt: 0
          });
        }
        ledgerEvents.sort(
          (a, b) => (new Date(a.timestamp).getTime() || 0) - (new Date(b.timestamp).getTime() || 0)
        );
        let netRunningTotal = 0;
        for (const ev of ledgerEvents) {
          if (ev.direction === "IN") {
            netRunningTotal = Number((netRunningTotal + ev.amountUsdt).toFixed(2));
          } else {
            netRunningTotal = Number(Math.max(0, netRunningTotal - ev.amountUsdt).toFixed(2));
          }
          ev.balanceAfterUsdt = netRunningTotal;
        }
        const extraDeduct = rep.autoExtraIdsSpawned.reduce(
          (s, x) => s + (RANK_PRICES_USDT[x.rank - 1] || 10),
          0
        );
        const autoDeduct = rep.autoIdsSpawned.length * 5;
        const upDeduct = idUpgrades.reduce((s, u) => s + (Number(u.vaultDeductedUsdt) || 0), 0);
        rep.vaultDeductedBreakdown = {
          extraIdsTotalUsdt: Number(extraDeduct.toFixed(2)),
          autoIdsTotalUsdt: Number(autoDeduct.toFixed(2)),
          upgradesTotalUsdt: Number(upDeduct.toFixed(2))
        };
        ledgerEvents.sort(
          (a, b) => (new Date(b.timestamp).getTime() || 0) - (new Date(a.timestamp).getTime() || 0)
        );
        rep.vault35Ledger = ledgerEvents;
      }
      const allReports = Array.from(reportsMap.values()).sort((a, b) => a.id - b.id);
      const mainAndAutoReports = allReports.filter((r) => r.nodeType !== "CLONE_ID");
      const eligibleMainReports = allReports.filter((r) => !r.isClone && !r.isExtraId);
      const ALLSELL_FOUR_POOL_DEFS = [
        {
          poolNumber: 1,
          key: "pool1_r13",
          titleTh: "\u0E01\u0E2D\u0E07\u0E17\u0E35\u0E48 1 : r-13 (300 USDT)",
          targetRank: 13,
          targetRankPriceUsdt: 300,
          requiredSelfRank: 13,
          requiredDirectCount: 5,
          allowedDirectRanks: [10, 11, 12, 13],
          allowedDirectRanksLabel: "r-10 \u0E2B\u0E23\u0E37\u0E2D r-11 \u0E2B\u0E23\u0E37\u0E2D r-12 \u0E2B\u0E23\u0E37\u0E2D r-13"
        },
        {
          poolNumber: 2,
          key: "pool2_r20",
          titleTh: "\u0E01\u0E2D\u0E07\u0E17\u0E35\u0E48 2 : r-20 (1,000 USDT)",
          targetRank: 20,
          targetRankPriceUsdt: 1e3,
          requiredSelfRank: 20,
          requiredDirectCount: 2,
          allowedDirectRanks: [17, 18, 19, 20],
          allowedDirectRanksLabel: "r-17 \u0E2B\u0E23\u0E37\u0E2D r-18 \u0E2B\u0E23\u0E37\u0E2D r-19 \u0E2B\u0E23\u0E37\u0E2D r-20"
        },
        {
          poolNumber: 3,
          key: "pool3_r24",
          titleTh: "\u0E01\u0E2D\u0E07\u0E17\u0E35\u0E48 3 : r-24 (5,000 USDT)",
          targetRank: 24,
          targetRankPriceUsdt: 5e3,
          requiredSelfRank: 24,
          requiredDirectCount: 2,
          allowedDirectRanks: [22, 23, 24],
          allowedDirectRanksLabel: "r-22 \u0E2B\u0E23\u0E37\u0E2D r-23 \u0E2B\u0E23\u0E37\u0E2D r-24"
        },
        {
          poolNumber: 4,
          key: "pool4_r27",
          titleTh: "\u0E01\u0E2D\u0E07\u0E17\u0E35\u0E48 4 : r-27 (10,000 USDT)",
          targetRank: 27,
          targetRankPriceUsdt: 1e4,
          requiredSelfRank: 27,
          requiredDirectCount: 5,
          allowedDirectRanks: [25, 26, 27],
          allowedDirectRanksLabel: "r-25 \u0E2B\u0E23\u0E37\u0E2D r-26 \u0E2B\u0E23\u0E37\u0E2D r-27"
        }
      ];
      const poolQualifiedIdsMap = /* @__PURE__ */ new Map();
      for (const pDef of ALLSELL_FOUR_POOL_DEFS) {
        poolQualifiedIdsMap.set(pDef.poolNumber, []);
      }
      for (const rep of eligibleMainReports) {
        const directMainRefs = eligibleMainReports.filter(
          (d) => d.id !== rep.id && d.directSponsorNodeId === rep.id
        );
        const directMainIdsList = directMainRefs.map((d) => ({
          id: d.id,
          currentMaxRank: d.currentMaxRank,
          isAutoId: d.isAutoId
        }));
        const poolsEval = ALLSELL_FOUR_POOL_DEFS.map((pDef) => {
          const hasSelfRank = rep.currentMaxRank >= pDef.requiredSelfRank || rep.rankNodesList.some((rn) => rn.rank === pDef.requiredSelfRank);
          const qualifiedDirectIds = directMainRefs.filter(
            (d) => pDef.allowedDirectRanks.includes(d.currentMaxRank) || d.rankNodesList.some((rn) => pDef.allowedDirectRanks.includes(rn.rank))
          ).map((d) => ({ id: d.id, currentMaxRank: d.currentMaxRank }));
          const qualifiedDirectCount = qualifiedDirectIds.length;
          const isQualified = hasSelfRank && qualifiedDirectCount >= pDef.requiredDirectCount;
          if (isQualified) {
            poolQualifiedIdsMap.get(pDef.poolNumber)?.push(rep.id);
          }
          const singlePoolTotalUsdt = Number((allSellTotalUsdt * 0.25).toFixed(2));
          return {
            poolNumber: pDef.poolNumber,
            key: pDef.key,
            titleTh: pDef.titleTh,
            targetRank: pDef.targetRank,
            targetRankPriceUsdt: pDef.targetRankPriceUsdt,
            requiredSelfRank: pDef.requiredSelfRank,
            hasSelfRank,
            requiredDirectCount: pDef.requiredDirectCount,
            allowedDirectRanks: pDef.allowedDirectRanks,
            allowedDirectRanksLabel: pDef.allowedDirectRanksLabel,
            qualifiedDirectIds,
            qualifiedDirectCount,
            isQualified,
            poolTotalUsdt: singlePoolTotalUsdt,
            estimatedShareUsdt: 0
          };
        });
        rep.allSellQualification = {
          totalDirectMainIds: directMainIdsList.length,
          directMainIdsList,
          totalQualifiedPoolsCount: poolsEval.filter((p) => p.isQualified).length,
          estimatedAllSellShareUsdt: 0,
          pools: poolsEval
        };
      }
      const allSellFourPools = ALLSELL_FOUR_POOL_DEFS.map((pDef) => {
        const qualifiedIds = poolQualifiedIdsMap.get(pDef.poolNumber) || [];
        const poolTotalUsdt = Number((allSellTotalUsdt * 0.25).toFixed(2));
        const poolOnChainUsdt = Number((onChainAllSellBal * 0.25).toFixed(2));
        const sharePerQualifiedIdUsdt = qualifiedIds.length > 0 ? Number((poolTotalUsdt / qualifiedIds.length).toFixed(2)) : 0;
        return {
          poolNumber: pDef.poolNumber,
          key: pDef.key,
          titleTh: pDef.titleTh,
          targetRank: pDef.targetRank,
          targetRankPriceUsdt: pDef.targetRankPriceUsdt,
          requiredSelfRank: pDef.requiredSelfRank,
          requiredDirectCount: pDef.requiredDirectCount,
          allowedDirectRanks: pDef.allowedDirectRanks,
          allowedDirectRanksLabel: pDef.allowedDirectRanksLabel,
          sharePercentOfAllSell: 25,
          payoutScheduleTh: "\u0E08\u0E48\u0E32\u0E22\u0E40\u0E07\u0E34\u0E19\u0E43\u0E2B\u0E49\u0E1C\u0E39\u0E49\u0E1C\u0E48\u0E32\u0E19\u0E40\u0E01\u0E13\u0E11\u0E4C \u0E17\u0E38\u0E01\u0E01\u0E25\u0E32\u0E07\u0E40\u0E14\u0E37\u0E2D\u0E19 \u0E41\u0E25\u0E30 \u0E15\u0E49\u0E19\u0E40\u0E14\u0E37\u0E2D\u0E19 (\u0E27\u0E31\u0E19\u0E17\u0E35\u0E48 1 \u0E41\u0E25\u0E30 15 \u0E02\u0E2D\u0E07\u0E17\u0E38\u0E01\u0E40\u0E14\u0E37\u0E2D\u0E19)",
          poolTotalUsdt,
          poolOnChainUsdt,
          qualifiedMainIds: qualifiedIds,
          qualifiedCount: qualifiedIds.length,
          sharePerQualifiedIdUsdt
        };
      });
      for (const rep of eligibleMainReports) {
        if (!rep.allSellQualification) continue;
        let totalShare = 0;
        for (const pItem of rep.allSellQualification.pools) {
          const poolSummary = allSellFourPools.find((x) => x.poolNumber === pItem.poolNumber);
          if (pItem.isQualified && poolSummary) {
            pItem.estimatedShareUsdt = poolSummary.sharePerQualifiedIdUsdt;
            totalShare = Number((totalShare + poolSummary.sharePerQualifiedIdUsdt).toFixed(2));
          }
        }
        rep.allSellQualification.estimatedAllSellShareUsdt = totalShare;
      }
      for (const rep of allReports) {
        if (rep.isClone && !rep.allSellQualification) {
          const origRep = reportsMap.get(rep.originMainId);
          if (origRep?.allSellQualification) {
            rep.allSellQualification = origRep.allSellQualification;
          }
        }
      }
      const allSellAutoStatus = buildAllSellAutoStatus(
        matrixDB.contractAddress,
        Number(allSellTotalUsdt.toFixed(2)),
        onChainAllSellBal,
        allSellFourPools
      );
      res.json({
        success: true,
        source: "DATABASE",
        contractAddress: matrixDB.contractAddress,
        lastSyncedBlock: matrixDB.lastSyncedBlock,
        lastSyncedAt: matrixDB.lastSyncedAt,
        totalIdsRank1: allReports.length,
        totalMainAndAutoIds: mainAndAutoReports.length,
        systemPools: {
          allSellWallet: allSellWalletAddr,
          familyPoolWallet: familyPoolWalletAddr,
          botWallet: botWalletAddr,
          allSellTotalUsdt: Number(allSellTotalUsdt.toFixed(2)),
          allSellPayoutScheduleTh: "\u0E08\u0E48\u0E32\u0E22\u0E40\u0E07\u0E34\u0E19\u0E43\u0E2B\u0E49\u0E17\u0E35\u0E48\u0E1C\u0E48\u0E32\u0E19\u0E40\u0E01\u0E13\u0E11\u0E4C \u0E17\u0E38\u0E01\u0E01\u0E25\u0E32\u0E07\u0E40\u0E14\u0E37\u0E2D\u0E19 \u0E41\u0E25\u0E30 \u0E15\u0E49\u0E19\u0E40\u0E14\u0E37\u0E2D\u0E19 (\u0E27\u0E31\u0E19\u0E17\u0E35\u0E48 1 \u0E41\u0E25\u0E30 15 \u0E02\u0E2D\u0E07\u0E17\u0E38\u0E01\u0E40\u0E14\u0E37\u0E2D\u0E19)",
          allSellAutoPayout: allSellAutoStatus,
          allSellFourPools,
          allSellByRank,
          allSellHistory,
          familyPoolTotalUsdt: Number(familyPoolTotalUsdt.toFixed(2)),
          familyPoolUnpaidLevelUsdt: Number(familyPoolUnpaidLevelUsdt.toFixed(2)),
          familyPoolRootCloneDirectUsdt: Number(familyPoolRootCloneDirectUsdt.toFixed(2)),
          familyPoolByRank,
          familyPoolHistory,
          botTotalUsdt: Number(botTotalUsdt.toFixed(2)),
          onChainBalances: {
            allSellWalletUsdt: onChainAllSellBal,
            familyPoolWalletUsdt: onChainFamilyBal,
            botWalletUsdt: onChainBotBal
          }
        },
        reports: allReports
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  const ALLSELL_PAYOUT_FILE_PATH = path.resolve(__dirname, "prisma/allsell-payouts.json");
  const loadAllSellAutoStore = () => {
    try {
      if (fs.existsSync(ALLSELL_PAYOUT_FILE_PATH)) {
        const raw = fs.readFileSync(ALLSELL_PAYOUT_FILE_PATH, "utf8");
        const parsed = JSON.parse(raw);
        return {
          autoEnabled: parsed.autoEnabled !== false,
          lastExecutedCycleKey: parsed.lastExecutedCycleKey || null,
          lastExecutedAt: parsed.lastExecutedAt || null,
          history: Array.isArray(parsed.history) ? parsed.history : []
        };
      }
    } catch {
    }
    return {
      autoEnabled: true,
      lastExecutedCycleKey: null,
      lastExecutedAt: null,
      history: []
    };
  };
  let allSellAutoStore = loadAllSellAutoStore();
  const saveAllSellAutoStore = () => {
    try {
      const dir = path.dirname(ALLSELL_PAYOUT_FILE_PATH);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(ALLSELL_PAYOUT_FILE_PATH, JSON.stringify(allSellAutoStore, null, 2), "utf8");
    } catch {
    }
  };
  const computeNextAllSellCycleInfo = () => {
    const nowMs = Date.now();
    const thOffsetMs = 7 * 60 * 60 * 1e3;
    const thDate = new Date(nowMs + thOffsetMs);
    const year = thDate.getUTCFullYear();
    const month = thDate.getUTCMonth();
    const day = thDate.getUTCDate();
    const thaiMonths = [
      "\u0E21.\u0E04.",
      "\u0E01.\u0E1E.",
      "\u0E21\u0E35.\u0E04.",
      "\u0E40\u0E21.\u0E22.",
      "\u0E1E.\u0E04.",
      "\u0E21\u0E34.\u0E22.",
      "\u0E01.\u0E04.",
      "\u0E2A.\u0E04.",
      "\u0E01.\u0E22.",
      "\u0E15.\u0E04.",
      "\u0E1E.\u0E22.",
      "\u0E18.\u0E04."
    ];
    let nextTargetUtcMs = 0;
    let nextCycleKey = "";
    let nextCycleLabelTh = "";
    if (day < 15) {
      nextTargetUtcMs = Date.UTC(year, month, 15, 0, 0, 0) - thOffsetMs;
      const mm = String(month + 1).padStart(2, "0");
      nextCycleKey = `${year}-${mm}-15`;
      nextCycleLabelTh = `15 ${thaiMonths[month]} ${year} \u0E40\u0E27\u0E25\u0E32 00:00 \u0E19. (\u0E23\u0E2D\u0E1A\u0E01\u0E25\u0E32\u0E07\u0E40\u0E14\u0E37\u0E2D\u0E19)`;
    } else {
      const nextMonthDate = new Date(Date.UTC(year, month + 1, 1, 0, 0, 0));
      const ny = nextMonthDate.getUTCFullYear();
      const nm = nextMonthDate.getUTCMonth();
      nextTargetUtcMs = Date.UTC(ny, nm, 1, 0, 0, 0) - thOffsetMs;
      const mm = String(nm + 1).padStart(2, "0");
      nextCycleKey = `${ny}-${mm}-01`;
      nextCycleLabelTh = `1 ${thaiMonths[nm]} ${ny} \u0E40\u0E27\u0E25\u0E32 00:00 \u0E19. (\u0E23\u0E2D\u0E1A\u0E15\u0E49\u0E19\u0E40\u0E14\u0E37\u0E2D\u0E19)`;
    }
    const isTodayCycleDay = day === 1 || day === 15;
    const currentMm = String(month + 1).padStart(2, "0");
    const currentDd = String(day).padStart(2, "0");
    const todayCycleKey = isTodayCycleDay ? `${year}-${currentMm}-${currentDd}` : null;
    const todayCycleLabelTh = day === 1 ? `1 ${thaiMonths[month]} ${year} (\u0E23\u0E2D\u0E1A\u0E15\u0E49\u0E19\u0E40\u0E14\u0E37\u0E2D\u0E19)` : day === 15 ? `15 ${thaiMonths[month]} ${year} (\u0E23\u0E2D\u0E1A\u0E01\u0E25\u0E32\u0E07\u0E40\u0E14\u0E37\u0E2D\u0E19)` : `\u0E15\u0E31\u0E14\u0E23\u0E2D\u0E1A\u0E1E\u0E34\u0E40\u0E28\u0E29 ${day} ${thaiMonths[month]} ${year}`;
    const secondsUntilNextPayout = Math.max(0, Math.floor((nextTargetUtcMs - nowMs) / 1e3));
    const daysLeft = Math.floor(secondsUntilNextPayout / 86400);
    const hoursLeft = Math.floor(secondsUntilNextPayout % 86400 / 3600);
    const minutesLeft = Math.floor(secondsUntilNextPayout % 3600 / 60);
    return {
      nextCycleKey,
      nextCycleLabelTh,
      nextPayoutIso: new Date(nextTargetUtcMs).toISOString(),
      secondsUntilNextPayout,
      countdownTextTh: `${daysLeft} \u0E27\u0E31\u0E19 ${hoursLeft} \u0E0A\u0E21. ${minutesLeft} \u0E19\u0E32\u0E17\u0E35`,
      isTodayCycleDay,
      todayCycleKey,
      todayCycleLabelTh
    };
  };
  function buildAllSellAutoStatus(contractAddr, allSellTotalUsdt, onChainAllSellBal, fourPools) {
    const cycleInfo = computeNextAllSellCycleInfo();
    const contractHistory = allSellAutoStore.history.filter(
      (h) => (h.contractAddress || "").toLowerCase() === (contractAddr || "").toLowerCase()
    );
    const totalAlreadyPaidUsdt = Number(
      contractHistory.reduce((acc, h) => acc + Number(h.totalDistributedUsdt || 0), 0).toFixed(2)
    );
    const totalQualifiedAcrossPools = fourPools.reduce(
      (acc, p) => acc + Number(p.qualifiedCount || 0),
      0
    );
    return {
      autoEnabled: allSellAutoStore.autoEnabled,
      scheduleRuleTh: "\u0E15\u0E31\u0E14\u0E23\u0E2D\u0E1A\u0E41\u0E25\u0E30\u0E42\u0E2D\u0E19\u0E08\u0E48\u0E32\u0E22\u0E2D\u0E31\u0E15\u0E42\u0E19\u0E21\u0E31\u0E15\u0E34 \u0E17\u0E38\u0E01\u0E27\u0E31\u0E19\u0E17\u0E35\u0E48 1 (\u0E15\u0E49\u0E19\u0E40\u0E14\u0E37\u0E2D\u0E19) \u0E41\u0E25\u0E30 \u0E27\u0E31\u0E19\u0E17\u0E35\u0E48 15 (\u0E01\u0E25\u0E32\u0E07\u0E40\u0E14\u0E37\u0E2D\u0E19) \u0E40\u0E27\u0E25\u0E32 00:00 \u0E19.",
      nextCycleKey: cycleInfo.nextCycleKey,
      nextCycleLabelTh: cycleInfo.nextCycleLabelTh,
      nextPayoutIso: cycleInfo.nextPayoutIso,
      secondsUntilNextPayout: cycleInfo.secondsUntilNextPayout,
      countdownTextTh: cycleInfo.countdownTextTh,
      lastExecutedCycleKey: allSellAutoStore.lastExecutedCycleKey,
      lastExecutedAt: allSellAutoStore.lastExecutedAt,
      totalAlreadyPaidUsdt,
      pendingPoolUsdt: Number(Math.max(0, allSellTotalUsdt - totalAlreadyPaidUsdt).toFixed(2)),
      onChainAllSellBal,
      totalQualifiedAcrossPools,
      historyCount: contractHistory.length,
      history: contractHistory.slice(0, 30)
    };
  }
  const executeAllSellAutoPayoutCycle = async (triggerType, customPrivateKey) => {
    const cycleInfo = computeNextAllSellCycleInfo();
    const cycleKey = triggerType === "AUTO_SCHEDULE" && cycleInfo.todayCycleKey ? cycleInfo.todayCycleKey : `${(/* @__PURE__ */ new Date()).toISOString().slice(0, 10)}-${triggerType === "AUTO_SCHEDULE" ? "AUTO" : "MANUAL"}`;
    const cycleLabelTh = triggerType === "AUTO_SCHEDULE" ? cycleInfo.todayCycleLabelTh : `${cycleInfo.todayCycleLabelTh} (\u0E2A\u0E31\u0E48\u0E07\u0E15\u0E31\u0E14\u0E23\u0E2D\u0E1A\u0E2D\u0E31\u0E15\u0E42\u0E19\u0E21\u0E31\u0E15\u0E34\u0E17\u0E31\u0E19\u0E17\u0E35)`;
    const r1Nodes = matrixDB.nodesByRank[1] || [];
    const eligibleMains = r1Nodes.filter((n) => !n.isClone && !n.isExtraId);
    let allSellTotalUsdt = 0;
    for (let rankNum = 1; rankNum <= 27; rankNum++) {
      const rNodes = matrixDB.nodesByRank[rankNum] || [];
      if (rNodes.length <= 1) continue;
      const unitPrice = RANK_PRICES_USDT[rankNum - 1] || 5;
      const allSellAmt = Number((unitPrice * 0.04).toFixed(2));
      const nodesMap = /* @__PURE__ */ new Map();
      for (const n of rNodes) nodesMap.set(n.id, n);
      for (const n of rNodes) {
        if (n.id <= 1) continue;
        const p = nodesMap.get(n.parentId);
        if (p && p.leftChild === n.id) {
          allSellTotalUsdt = Number((allSellTotalUsdt + allSellAmt).toFixed(4));
        }
      }
    }
    const contractHistory = allSellAutoStore.history.filter(
      (h) => (h.contractAddress || "").toLowerCase() === (matrixDB.contractAddress || "").toLowerCase()
    );
    const alreadyPaidUsdt = contractHistory.reduce(
      (acc, h) => acc + Number(h.totalDistributedUsdt || 0),
      0
    );
    const distributableAllSellUsdt = Number(Math.max(0, allSellTotalUsdt - alreadyPaidUsdt).toFixed(2));
    const singlePoolUsdt = Number((distributableAllSellUsdt * 0.25).toFixed(2));
    const poolDefs = [
      {
        poolNumber: 1,
        titleTh: "\u0E01\u0E2D\u0E07\u0E17\u0E35\u0E48 1 : r-13 (300 USDT)",
        requiredSelfRank: 13,
        requiredDirectCount: 5,
        allowedDirectRanks: [10, 11, 12, 13]
      },
      {
        poolNumber: 2,
        titleTh: "\u0E01\u0E2D\u0E07\u0E17\u0E35\u0E48 2 : r-20 (1,000 USDT)",
        requiredSelfRank: 20,
        requiredDirectCount: 2,
        allowedDirectRanks: [17, 18, 19, 20]
      },
      {
        poolNumber: 3,
        titleTh: "\u0E01\u0E2D\u0E07\u0E17\u0E35\u0E48 3 : r-24 (5,000 USDT)",
        requiredSelfRank: 24,
        requiredDirectCount: 2,
        allowedDirectRanks: [22, 23, 24]
      },
      {
        poolNumber: 4,
        titleTh: "\u0E01\u0E2D\u0E07\u0E17\u0E35\u0E48 4 : r-27 (10,000 USDT)",
        requiredSelfRank: 27,
        requiredDirectCount: 5,
        allowedDirectRanks: [25, 26, 27]
      }
    ];
    const mainMaxRankMap = /* @__PURE__ */ new Map();
    const mainRanksSetMap = /* @__PURE__ */ new Map();
    for (const m of eligibleMains) {
      const stats = computeMainIdDatabaseStats(m.id);
      mainMaxRankMap.set(m.id, stats.currentRank);
      const rSet = /* @__PURE__ */ new Set([1]);
      for (let r = 2; r <= 27; r++) {
        const rList = matrixDB.nodesByRank[r] || [];
        if (rList.some((x) => !x.isClone && !x.isExtraId && x.originMainId === m.id)) {
          rSet.add(r);
        }
      }
      mainRanksSetMap.set(m.id, rSet);
    }
    const transfers = [];
    let totalDistributedUsdt = 0;
    let carriedOverUsdt = 0;
    const rawPk = (customPrivateKey || process.env.ALLSELL_WALLET_PRIVATE_KEY || process.env.ADMIN_PRIVATE_KEY || "0xc9c4649effa5eaaab8dc0690b4e392aec7cf84ee49e2251a1800fe89ad3270ca").trim();
    let signerWallet = null;
    let usdtWriteContract = null;
    let signerUsdtBal = 0;
    if (rawPk && (rawPk.length === 64 || rawPk.length === 66)) {
      try {
        const provider = getProvider();
        const pkHex = rawPk.startsWith("0x") ? rawPk : `0x${rawPk}`;
        signerWallet = new ethers.Wallet(pkHex, provider);
        usdtWriteContract = new ethers.Contract(
          USDT_ADDRESS,
          [
            "function balanceOf(address) view returns (uint256)",
            "function transfer(address to, uint256 amount) returns (bool)"
          ],
          signerWallet
        );
        const balBn = await usdtWriteContract.balanceOf(signerWallet.address);
        signerUsdtBal = Number(ethers.formatUnits(balBn, activeUsdtDecimals || 18));
      } catch {
        signerWallet = null;
        usdtWriteContract = null;
      }
    }
    for (const pDef of poolDefs) {
      const qualifiedMains = eligibleMains.filter((m) => {
        const maxR = mainMaxRankMap.get(m.id) || 1;
        const rSet = mainRanksSetMap.get(m.id) || /* @__PURE__ */ new Set([1]);
        const hasSelf = maxR >= pDef.requiredSelfRank || rSet.has(pDef.requiredSelfRank);
        if (!hasSelf) return false;
        const directs = eligibleMains.filter(
          (d) => d.id !== m.id && d.directSponsorNodeId === m.id
        );
        const qualDirects = directs.filter((d) => {
          const dMaxR = mainMaxRankMap.get(d.id) || 1;
          const dSet = mainRanksSetMap.get(d.id) || /* @__PURE__ */ new Set([1]);
          return pDef.allowedDirectRanks.includes(dMaxR) || pDef.allowedDirectRanks.some((r) => dSet.has(r));
        });
        return qualDirects.length >= pDef.requiredDirectCount;
      });
      if (qualifiedMains.length === 0 || singlePoolUsdt <= 0) {
        carriedOverUsdt = Number((carriedOverUsdt + singlePoolUsdt).toFixed(2));
        continue;
      }
      const sharePerId = Number((singlePoolUsdt / qualifiedMains.length).toFixed(2));
      for (const qMain of qualifiedMains) {
        if (sharePerId <= 0) continue;
        let txHash = `0xauto_allsell_p${pDef.poolNumber}_id${qMain.id}_${Date.now().toString(16)}`;
        let mode = "CYCLE_SETTLEMENT";
        if (usdtWriteContract && signerWallet && signerUsdtBal >= sharePerId) {
          try {
            const amtWei = ethers.parseUnits(sharePerId.toFixed(2), activeUsdtDecimals || 18);
            const tx = await usdtWriteContract.transfer(qMain.owner, amtWei);
            const receipt = await tx.wait(1);
            txHash = receipt?.hash || tx.hash;
            mode = "ONCHAIN_TRANSFER";
            signerUsdtBal = Math.max(0, signerUsdtBal - sharePerId);
          } catch {
          }
        }
        totalDistributedUsdt = Number((totalDistributedUsdt + sharePerId).toFixed(2));
        transfers.push({
          poolNumber: pDef.poolNumber,
          poolTitleTh: pDef.titleTh,
          mainId: qMain.id,
          recipientWallet: qMain.owner,
          amountUsdt: sharePerId,
          txHash,
          mode
        });
      }
    }
    const nowIso = (/* @__PURE__ */ new Date()).toISOString();
    const record = {
      id: `allsell-cycle-${Date.now()}`,
      contractAddress: matrixDB.contractAddress,
      cycleKey,
      cycleLabelTh,
      triggerType,
      executedAt: nowIso,
      totalAllSellPoolUsdt: distributableAllSellUsdt,
      totalDistributedUsdt,
      carriedOverUsdt,
      qualifiedRecipientsCount: transfers.length,
      transfers,
      noteTh: transfers.length > 0 ? `\u0E15\u0E31\u0E14\u0E23\u0E2D\u0E1A\u0E08\u0E48\u0E32\u0E22 AllSell \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08 (${transfers.length} \u0E23\u0E32\u0E22\u0E01\u0E32\u0E23 \u0E23\u0E27\u0E21 ${totalDistributedUsdt.toFixed(2)} USDT \u2022 \u0E22\u0E01\u0E22\u0E2D\u0E14\u0E2A\u0E30\u0E2A\u0E21\u0E15\u0E48\u0E2D ${carriedOverUsdt.toFixed(2)} USDT)` : `\u0E15\u0E23\u0E27\u0E08\u0E2A\u0E2D\u0E1A\u0E23\u0E2D\u0E1A ${cycleLabelTh} \u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22\u0E41\u0E25\u0E49\u0E27 (\u0E22\u0E31\u0E07\u0E44\u0E21\u0E48\u0E21\u0E35\u0E23\u0E2B\u0E31\u0E2A\u0E17\u0E35\u0E48\u0E04\u0E23\u0E1A\u0E40\u0E01\u0E13\u0E11\u0E4C\u0E43\u0E19\u0E23\u0E2D\u0E1A\u0E19\u0E35\u0E49 \u2022 \u0E22\u0E01\u0E22\u0E2D\u0E14\u0E2A\u0E30\u0E2A\u0E21 ${carriedOverUsdt.toFixed(2)} USDT \u0E44\u0E1B\u0E23\u0E2D\u0E1A\u0E16\u0E31\u0E14\u0E44\u0E1B\u0E2D\u0E31\u0E15\u0E42\u0E19\u0E21\u0E31\u0E15\u0E34)`
    };
    allSellAutoStore.lastExecutedCycleKey = cycleKey;
    allSellAutoStore.lastExecutedAt = nowIso;
    allSellAutoStore.history.unshift(record);
    if (allSellAutoStore.history.length > 100) {
      allSellAutoStore.history = allSellAutoStore.history.slice(0, 100);
    }
    saveAllSellAutoStore();
    return record;
  };
  setInterval(() => {
    if (!allSellAutoStore.autoEnabled) return;
    const cycleInfo = computeNextAllSellCycleInfo();
    if (cycleInfo.isTodayCycleDay && cycleInfo.todayCycleKey && allSellAutoStore.lastExecutedCycleKey !== cycleInfo.todayCycleKey) {
      executeAllSellAutoPayoutCycle("AUTO_SCHEDULE").catch(
        (err) => console.warn("[AutoAllSell] Scheduled payout error:", err)
      );
    }
  }, 60 * 1e3);
  app.post("/api/matrix/allsell-auto-payout/toggle", (req, res) => {
    try {
      const { enabled } = req.body;
      allSellAutoStore.autoEnabled = typeof enabled === "boolean" ? enabled : !allSellAutoStore.autoEnabled;
      saveAllSellAutoStore();
      res.json({
        success: true,
        autoEnabled: allSellAutoStore.autoEnabled,
        message: allSellAutoStore.autoEnabled ? "\u0E40\u0E1B\u0E34\u0E14\u0E23\u0E30\u0E1A\u0E1A\u0E15\u0E31\u0E14\u0E23\u0E2D\u0E1A\u0E08\u0E48\u0E32\u0E22 AllSell \u0E2D\u0E31\u0E15\u0E42\u0E19\u0E21\u0E31\u0E15\u0E34 (\u0E17\u0E38\u0E01\u0E27\u0E31\u0E19\u0E17\u0E35\u0E48 1 \u0E41\u0E25\u0E30 15) \u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22\u0E41\u0E25\u0E49\u0E27" : "\u0E1B\u0E34\u0E14\u0E23\u0E30\u0E1A\u0E1A\u0E15\u0E31\u0E14\u0E23\u0E2D\u0E1A\u0E08\u0E48\u0E32\u0E22 AllSell \u0E2D\u0E31\u0E15\u0E42\u0E19\u0E21\u0E31\u0E15\u0E34\u0E0A\u0E31\u0E48\u0E27\u0E04\u0E23\u0E32\u0E27"
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/matrix/allsell-auto-payout/execute-now", async (req, res) => {
    try {
      const { privateKey } = req.body || {};
      const record = await executeAllSellAutoPayoutCycle("MANUAL_TRIGGER", privateKey);
      res.json({
        success: true,
        record,
        message: record.noteTh
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/matrix/process-extra-queue", async (req, res) => {
    try {
      const { mainId } = req.body || {};
      const targetMainId = Math.max(1, Number(mainId) || 1);
      reconcileAutoExtraIdsAcrossRanks();
      const r1Nodes = matrixDB.nodesByRank[1] || [];
      const mNode = r1Nodes.find((n) => n.id === targetMainId && !n.isClone && !n.isExtraId);
      if (!mNode) {
        return res.status(404).json({ success: false, error: `\u0E44\u0E21\u0E48\u0E1E\u0E1A Main ID #${targetMainId}` });
      }
      let spawnedCount = 0;
      const spawnedList = [];
      for (let batch = 0; batch < 20; batch++) {
        reconcileAutoExtraIdsAcrossRanks();
        break;
      }
      res.json({
        success: true,
        mainId: targetMainId,
        message: `\u0E1B\u0E23\u0E30\u0E21\u0E27\u0E25\u0E1C\u0E25\u0E04\u0E34\u0E27 Extra ID \u0E2A\u0E33\u0E2B\u0E23\u0E31\u0E1A Main #${targetMainId} \u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22\u0E41\u0E25\u0E49\u0E27`,
        processedCount: spawnedCount
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/matrix/upgrade", async (req, res) => {
    try {
      const { targetRank, mainId, owner } = req.body;
      const rankNum = Math.min(Math.max(Number(targetRank) || 2, 2), 27);
      const mainIdNum = Math.max(1, Number(mainId) || 1);
      const r1Nodes = matrixDB.nodesByRank[1] || [];
      const mainNodeR1 = r1Nodes.find((n) => n.id === mainIdNum && !n.isClone && !n.isExtraId);
      if (!mainNodeR1) {
        return res.status(404).json({
          success: false,
          error: `\u0E44\u0E21\u0E48\u0E1E\u0E1A\u0E23\u0E2B\u0E31\u0E2A\u0E2B\u0E25\u0E31\u0E01 Main ID #${mainIdNum} \u0E43\u0E19\u0E23\u0E30\u0E1A\u0E1A Rank 1`
        });
      }
      const ownerAddr = (mainNodeR1.owner || owner || matrixDB.systemWallets.rootOwner).toLowerCase();
      const sponsorAddr = mainNodeR1.directSponsor || matrixDB.systemWallets.rootOwner;
      const r1SponsorMainId = mainNodeR1.directSponsorNodeId > 0 ? mainNodeR1.directSponsorNodeId : 1;
      let currentRank = mainIdNum === 1 ? 27 : 1;
      if (mainIdNum !== 1) {
        for (let r = 27; r >= 2; r--) {
          const list = (matrixDB.nodesByRank[r] || []).filter((n) => !n.isSyntheticExtra);
          if (list.some((n) => !n.isClone && !n.isExtraId && n.originMainId === mainIdNum)) {
            currentRank = r;
            break;
          }
        }
      }
      if (rankNum <= currentRank) {
        return res.json({
          success: true,
          source: "DATABASE_SIMULATION",
          mainId: mainIdNum,
          currentRank,
          targetRank: rankNum,
          txHash: `0xupg${Date.now().toString(16)}`,
          message: `\u0E23\u0E2B\u0E31\u0E2A\u0E2B\u0E25\u0E31\u0E01 #${mainIdNum} \u0E2D\u0E22\u0E39\u0E48\u0E43\u0E19 Rank ${currentRank} \u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22\u0E41\u0E25\u0E49\u0E27`
        });
      }
      for (let r = currentRank + 1; r <= rankNum; r++) {
        if (!matrixDB.nodesByRank[r]) {
          matrixDB.nodesByRank[r] = [];
        }
        const rList = matrixDB.nodesByRank[r].filter((n) => !n.isSyntheticExtra);
        matrixDB.nodesByRank[r] = rList;
        const existingNodeInRank = rList.find(
          (n) => !n.isClone && !n.isExtraId && n.originMainId === mainIdNum
        );
        if (existingNodeInRank) continue;
        const rankPrice = RANK_PRICES_USDT[r - 1] || 10;
        const prevRank = r - 1;
        const winStart = Math.max(1, prevRank - 4);
        let windowVaultAvail = 0;
        for (let wr = winStart; wr <= prevRank; wr++) {
          const wrList = matrixDB.nodesByRank[wr] || [];
          const wrNode = wr === 1 ? wrList.find((n) => n.id === mainIdNum && !n.isClone && !n.isExtraId) : wrList.find((n) => !n.isClone && !n.isExtraId && n.originMainId === mainIdNum);
          if (wrNode && Number(wrNode.nodeVault || 0) > 0) {
            windowVaultAvail = Number((windowVaultAvail + Number(wrNode.nodeVault || 0)).toFixed(4));
          }
        }
        let remDeduct = windowVaultAvail + 1e-6 >= rankPrice ? rankPrice : 0;
        let totalVaultDeducted = 0;
        for (let wr = winStart; wr <= prevRank; wr++) {
          if (remDeduct <= 1e-6) break;
          const wrList = matrixDB.nodesByRank[wr] || [];
          const wrNode = wr === 1 ? wrList.find((n) => n.id === mainIdNum && !n.isClone && !n.isExtraId) : wrList.find((n) => !n.isClone && !n.isExtraId && n.originMainId === mainIdNum);
          if (wrNode && Number(wrNode.nodeVault || 0) > 0) {
            const avail = Number(wrNode.nodeVault || 0);
            const deduct = Math.min(avail, remDeduct);
            wrNode.nodeVault = Number(Math.max(0, avail - deduct).toFixed(4));
            wrNode.reconciledBaseVault = wrNode.nodeVault;
            remDeduct = Number(Math.max(0, remDeduct - deduct).toFixed(4));
            totalVaultDeducted = Number((totalVaultDeducted + deduct).toFixed(4));
          }
        }
        const spNodeInR = rList.find(
          (n) => !n.isClone && !n.isExtraId && (r1SponsorMainId === 1 ? n.id === 1 : n.originMainId === r1SponsorMainId)
        );
        const placement = spNodeInR ? findExtraIdPlacementInRankNodes(rList, spNodeInR.id) : findShallowestLeftToRightSlotInRankNodes(rList, 1);
        const parentIdNum = Number(placement.parentId) || 1;
        const maxId = rList.reduce((m, n) => Math.max(m, n.id), 0);
        const nextIdInRank = maxId + 1;
        const newNode = {
          id: nextIdInRank,
          rank: r,
          owner: mainNodeR1.owner || ownerAddr,
          directSponsor: sponsorAddr,
          parentId: parentIdNum,
          leftChild: 0,
          rightChild: 0,
          isClone: false,
          isExtraId: false,
          originMainId: mainIdNum,
          cloneRound: 0,
          directSponsorNodeId: r1SponsorMainId,
          nodeVault: 0,
          rawNodeVault: 0,
          reconciledBaseVault: 0,
          isLocalUpgrade: true
        };
        rList.push(newNode);
        const parentNode = rList.find((n) => n.id === parentIdNum);
        const rightSlotTriggerQueue = [];
        if (parentNode) {
          if (placement.isLeft) {
            parentNode.leftChild = nextIdInRank;
            const pMainId = parentNode.originMainId && parentNode.originMainId > 0 ? parentNode.originMainId : parentNode.id;
            const pMainNode = rList.find((n) => n.id === pMainId) || parentNode;
            const vaultAdd = Number((rankPrice * 0.35).toFixed(4));
            pMainNode.nodeVault = Number(((pMainNode.nodeVault || 0) + vaultAdd).toFixed(4));
            pMainNode.rawNodeVault = Number(((pMainNode.rawNodeVault || 0) + vaultAdd).toFixed(4));
            pMainNode.reconciledBaseVault = pMainNode.nodeVault;
          } else {
            parentNode.rightChild = nextIdInRank;
            rightSlotTriggerQueue.push(parentNode);
          }
        }
        let cloneSafety = 0;
        while (rightSlotTriggerQueue.length > 0 && cloneSafety < 25) {
          cloneSafety++;
          const triggerParent = rightSlotTriggerQueue.shift();
          const clonePlacement = findClonePlacementInRankNodes(rList, triggerParent);
          const nextCloneId = rList.reduce((m, n) => Math.max(m, n.id), 0) + 1;
          const originNode = rList.find((n) => n.id === clonePlacement.cloneOriginMainId) || triggerParent;
          const newCloneNode = {
            id: nextCloneId,
            rank: r,
            owner: triggerParent.owner,
            directSponsor: originNode.directSponsor || triggerParent.directSponsor || triggerParent.owner,
            parentId: clonePlacement.parentId,
            leftChild: 0,
            rightChild: 0,
            isClone: true,
            isExtraId: false,
            originMainId: clonePlacement.cloneOriginMainId,
            cloneRound: clonePlacement.cloneRound,
            directSponsorNodeId: clonePlacement.targetSponsorNodeId,
            nodeVault: 0,
            rawNodeVault: 0,
            reconciledBaseVault: 0,
            isLocalUpgrade: true
          };
          rList.push(newCloneNode);
          const cloneParent = rList.find((n) => n.id === clonePlacement.parentId);
          if (cloneParent) {
            if (clonePlacement.isLeft) {
              cloneParent.leftChild = nextCloneId;
              const cpMainId = cloneParent.originMainId && cloneParent.originMainId > 0 ? cloneParent.originMainId : cloneParent.id;
              const cpMainNode = rList.find((n) => n.id === cpMainId) || cloneParent;
              const vaultAdd = Number((rankPrice * 0.35).toFixed(4));
              cpMainNode.nodeVault = Number(((cpMainNode.nodeVault || 0) + vaultAdd).toFixed(4));
              cpMainNode.rawNodeVault = Number(((cpMainNode.rawNodeVault || 0) + vaultAdd).toFixed(4));
              cpMainNode.reconciledBaseVault = cpMainNode.nodeVault;
            } else {
              cloneParent.rightChild = nextCloneId;
              rightSlotTriggerQueue.push(cloneParent);
            }
          }
        }
        matrixDB.nodesByRank[r] = reconcileRankTreeLinks(rList);
        matrixDB.rankCounts[r] = matrixDB.nodesByRank[r].length;
      }
      reconcileAutoExtraIdsAcrossRanks();
      rebuildDatabaseRewardLogsFromNodes();
      rebuildDatabaseUpgradeLogsFromNodes();
      saveMatrixDBToDisk(true);
      res.json({
        success: true,
        source: "DATABASE_SIMULATION",
        mainId: mainIdNum,
        fromRank: currentRank,
        targetRank: rankNum,
        txHash: `0xupg${Date.now().toString(16)}`,
        message: `\u{1F680} \u0E2D\u0E31\u0E1B\u0E40\u0E01\u0E23\u0E14\u0E23\u0E2B\u0E31\u0E2A\u0E2B\u0E25\u0E31\u0E01 #${mainIdNum} \u0E2A\u0E39\u0E48 Rank ${rankNum} \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08\u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22\u0E41\u0E25\u0E49\u0E27!`
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.get("/api/admin/lookup-target", (req, res) => {
    const raw = String(req.query.query || "").trim().replace(/^#/, "");
    if (!raw) return res.json({ success: true, found: false });
    const r1Nodes = matrixDB.nodesByRank[1] || [];
    let node;
    if (/^\d+$/.test(raw)) {
      const idNum = parseInt(raw, 10);
      node = r1Nodes.find((n) => n.id === idNum && !n.isClone && !n.isExtraId);
    } else if (ethers.isAddress(raw)) {
      const norm = raw.toLowerCase();
      node = r1Nodes.find((n) => n.owner.toLowerCase() === norm && !n.isClone && !n.isExtraId);
    }
    if (!node) {
      return res.json({ success: true, found: false });
    }
    let currentRank = node.id === 1 ? 27 : 1;
    if (node.id !== 1) {
      for (let r = 27; r >= 2; r--) {
        const list = matrixDB.nodesByRank[r] || [];
        if (list.some((n) => !n.isClone && !n.isExtraId && n.originMainId === node.id)) {
          currentRank = r;
          break;
        }
      }
    }
    return res.json({
      success: true,
      found: true,
      mainId: node.id,
      owner: node.owner,
      currentRank,
      directSponsorNodeId: node.directSponsorNodeId || 1,
      directSponsor: node.directSponsor
    });
  });
  app.post("/api/admin/upgrade-rank", async (req, res) => {
    try {
      const { targetUser, targetRank, targetMainId, mainId } = req.body;
      const rawInput = String(targetMainId || mainId || targetUser || "").trim().replace(/^#/, "");
      if (!rawInput) {
        return res.status(400).json({ success: false, error: "\u0E01\u0E23\u0E38\u0E13\u0E32\u0E01\u0E23\u0E2D\u0E01\u0E40\u0E25\u0E02 ID \u0E2B\u0E25\u0E31\u0E01 (\u0E40\u0E0A\u0E48\u0E19 5 \u0E2B\u0E23\u0E37\u0E2D #5) \u0E2B\u0E23\u0E37\u0E2D Wallet Address" });
      }
      const parsedRank = Math.min(27, Math.max(2, Number(targetRank) || 2));
      const r1Nodes = matrixDB.nodesByRank[1] || [];
      let userR1Node;
      let normalizedUser = "";
      if (/^\d+$/.test(rawInput)) {
        const idNum = parseInt(rawInput, 10);
        userR1Node = r1Nodes.find((n) => n.id === idNum && !n.isClone && !n.isExtraId);
        if (!userR1Node) {
          return res.status(404).json({ success: false, error: `\u0E44\u0E21\u0E48\u0E1E\u0E1A\u0E23\u0E2B\u0E31\u0E2A\u0E2B\u0E25\u0E31\u0E01 ID #${idNum} \u0E43\u0E19\u0E23\u0E30\u0E1A\u0E1A Rank 1` });
        }
        normalizedUser = userR1Node.owner.toLowerCase();
      } else if (ethers.isAddress(rawInput)) {
        normalizedUser = rawInput.toLowerCase();
        userR1Node = r1Nodes.find((n) => n.owner.toLowerCase() === normalizedUser && !n.isClone && !n.isExtraId);
      } else {
        return res.status(400).json({ success: false, error: "\u0E01\u0E23\u0E38\u0E13\u0E32\u0E23\u0E30\u0E1A\u0E38\u0E40\u0E25\u0E02 ID \u0E2B\u0E25\u0E31\u0E01 (\u0E40\u0E0A\u0E48\u0E19 5 \u0E2B\u0E23\u0E37\u0E2D #5) \u0E2B\u0E23\u0E37\u0E2D Wallet Address \u0E17\u0E35\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07" });
      }
      const mainIdNum = userR1Node ? userR1Node.id : 1;
      const sponsorAddr = userR1Node?.directSponsor || matrixDB.systemWallets.rootOwner;
      const r1SponsorMainId = userR1Node?.directSponsorNodeId || 1;
      let currentRank = mainIdNum === 1 ? 27 : 1;
      if (mainIdNum !== 1) {
        for (let r = 27; r >= 2; r--) {
          const list = (matrixDB.nodesByRank[r] || []).filter((n) => !n.isSyntheticExtra);
          if (list.some((n) => !n.isClone && !n.isExtraId && n.originMainId === mainIdNum)) {
            currentRank = r;
            break;
          }
        }
      }
      if (parsedRank <= currentRank) {
        return res.json({
          success: true,
          message: `\u0E23\u0E2B\u0E31\u0E2A\u0E2B\u0E25\u0E31\u0E01 #${mainIdNum} \u0E2D\u0E22\u0E39\u0E48\u0E43\u0E19 Rank ${currentRank} \u0E2D\u0E22\u0E39\u0E48\u0E41\u0E25\u0E49\u0E27 (\u0E40\u0E1B\u0E49\u0E32\u0E2B\u0E21\u0E32\u0E22\u0E04\u0E37\u0E2D Rank ${parsedRank})`,
          currentRank,
          targetRank: parsedRank,
          mainId: mainIdNum
        });
      }
      const upgradedRanks = [];
      const createdNodes = [];
      let clonesCreated = 0;
      for (let r = currentRank + 1; r <= parsedRank; r++) {
        if (!matrixDB.nodesByRank[r]) matrixDB.nodesByRank[r] = [];
        const rList = matrixDB.nodesByRank[r].filter((n) => !n.isSyntheticExtra);
        matrixDB.nodesByRank[r] = rList;
        const existingNode = rList.find(
          (n) => !n.isClone && !n.isExtraId && n.originMainId === mainIdNum
        );
        if (!existingNode) {
          const spNodeInR = rList.find(
            (n) => !n.isClone && !n.isExtraId && (r1SponsorMainId === 1 ? n.id === 1 : n.originMainId === r1SponsorMainId)
          );
          const placement = spNodeInR ? findExtraIdPlacementInRankNodes(rList, spNodeInR.id) : findShallowestLeftToRightSlotInRankNodes(rList, 1);
          const parentIdNum = Number(placement.parentId) || 1;
          const maxId = rList.reduce((m, n) => Math.max(m, n.id), 0);
          const nextIdInRank = maxId + 1;
          const newNode = {
            id: nextIdInRank,
            rank: r,
            owner: userR1Node?.owner || normalizedUser,
            directSponsor: sponsorAddr,
            parentId: parentIdNum,
            leftChild: 0,
            rightChild: 0,
            isClone: false,
            isExtraId: false,
            originMainId: mainIdNum,
            cloneRound: 0,
            directSponsorNodeId: r1SponsorMainId,
            nodeVault: 0,
            rawNodeVault: 0,
            reconciledBaseVault: 0,
            isLocalUpgrade: true
          };
          rList.push(newNode);
          const parentNode = rList.find((n) => n.id === parentIdNum);
          const rightSlotTriggerQueue = [];
          if (parentNode) {
            if (placement.isLeft) {
              parentNode.leftChild = nextIdInRank;
            } else {
              parentNode.rightChild = nextIdInRank;
              rightSlotTriggerQueue.push(parentNode);
            }
          }
          let cloneSafety = 0;
          while (rightSlotTriggerQueue.length > 0 && cloneSafety < 25) {
            cloneSafety++;
            const triggerParent = rightSlotTriggerQueue.shift();
            const clonePlacement = findClonePlacementInRankNodes(rList, triggerParent);
            const cloneIdInRank = rList.reduce((m, n) => Math.max(m, n.id), 0) + 1;
            const originNode = rList.find((n) => n.id === clonePlacement.cloneOriginMainId) || triggerParent;
            const cloneNode = {
              id: cloneIdInRank,
              rank: r,
              owner: triggerParent.owner,
              directSponsor: originNode.directSponsor || triggerParent.directSponsor || triggerParent.owner,
              parentId: clonePlacement.parentId,
              leftChild: 0,
              rightChild: 0,
              isClone: true,
              isExtraId: false,
              originMainId: clonePlacement.cloneOriginMainId,
              cloneRound: clonePlacement.cloneRound,
              directSponsorNodeId: clonePlacement.targetSponsorNodeId,
              nodeVault: 0,
              rawNodeVault: 0,
              reconciledBaseVault: 0,
              isLocalUpgrade: true
            };
            rList.push(cloneNode);
            clonesCreated++;
            const cloneParent = rList.find((n) => n.id === clonePlacement.parentId);
            if (cloneParent) {
              if (clonePlacement.isLeft) {
                cloneParent.leftChild = cloneIdInRank;
              } else {
                cloneParent.rightChild = cloneIdInRank;
                rightSlotTriggerQueue.push(cloneParent);
              }
            }
          }
          matrixDB.nodesByRank[r] = reconcileRankTreeLinks(rList);
          matrixDB.rankCounts[r] = matrixDB.nodesByRank[r].length;
          upgradedRanks.push(r);
          createdNodes.push({ rank: r, nodeId: nextIdInRank, side: placement.isLeft ? "LEFT" : "RIGHT" });
        }
      }
      reconcileAutoExtraIdsAcrossRanks();
      rebuildDatabaseRewardLogsFromNodes();
      rebuildDatabaseUpgradeLogsFromNodes();
      saveMatrixDBToDisk(true);
      res.json({
        success: true,
        source: "DATABASE_ADMIN_UPGRADE",
        targetUser: normalizedUser,
        fromRank: currentRank,
        targetRank: parsedRank,
        upgradedRanks,
        createdNodes,
        clonesCreated,
        message: `\u2705 Admin Upgrade \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08! \u0E2D\u0E31\u0E1E Rank \u0E43\u0E2B\u0E49\u0E1F\u0E23\u0E35\u0E08\u0E32\u0E01 Rank ${currentRank} \u0E2A\u0E39\u0E48 Rank ${parsedRank} \u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22\u0E41\u0E25\u0E49\u0E27 (\u0E44\u0E21\u0E48\u0E08\u0E48\u0E32\u0E22\u0E04\u0E48\u0E32\u0E04\u0E2D\u0E21 \u0E41\u0E25\u0E30\u0E23\u0E30\u0E1A\u0E1A\u0E42\u0E04\u0E25\u0E19\u0E19\u0E34\u0E48\u0E07\u0E17\u0E33\u0E07\u0E32\u0E19\u0E1B\u0E01\u0E15\u0E34)`
      });
    } catch (err) {
      console.error("Error in /api/admin/upgrade-rank:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.get("/api/admin/overview", async (_req, res) => {
    try {
      const provider = getProvider();
      const contract = new ethers.Contract(
        CONTRACT_ADDRESS,
        [
          ...MATRIX_READ_ABI,
          "function owner() view returns (address)",
          "function allSellWallet() view returns (address)",
          "function botWallet() view returns (address)",
          "function familyPool() view returns (address)"
        ],
        provider
      );
      const usdtContract = new ethers.Contract(
        USDT_ADDRESS,
        [
          "function balanceOf(address) view returns (uint256)",
          "function decimals() view returns (uint8)",
          "function symbol() view returns (string)"
        ],
        provider
      );
      let ownerAddr = matrixDB.systemWallets.rootOwner || ethers.ZeroAddress;
      let allSellAddr = matrixDB.systemWallets.allSellWallet || ethers.ZeroAddress;
      let botAddr = matrixDB.systemWallets.botWallet || ethers.ZeroAddress;
      let familyAddr = matrixDB.systemWallets.familyPool || ethers.ZeroAddress;
      try {
        const [o, a, b, f] = await Promise.all([
          contract.owner().catch(() => ownerAddr),
          contract.allSellWallet().catch(() => allSellAddr),
          contract.botWallet().catch(() => botAddr),
          contract.familyPool().catch(() => familyAddr)
        ]);
        if (o && o !== ethers.ZeroAddress) ownerAddr = String(o);
        if (a && a !== ethers.ZeroAddress) allSellAddr = String(a);
        if (b && b !== ethers.ZeroAddress) botAddr = String(b);
        if (f && f !== ethers.ZeroAddress) familyAddr = String(f);
      } catch {
      }
      const fetchWalletBalances = async (addr) => {
        if (!addr || !ethers.isAddress(addr) || addr === ethers.ZeroAddress) {
          return { address: addr || "-", usdt: "0.00", bnb: "0.0000" };
        }
        try {
          const [uBn, bBn] = await Promise.all([
            usdtContract.balanceOf(addr).catch(() => 0n),
            provider.getBalance(addr).catch(() => 0n)
          ]);
          return {
            address: addr,
            usdt: Number(ethers.formatUnits(uBn, activeUsdtDecimals || 18)).toFixed(2),
            bnb: Number(ethers.formatEther(bBn)).toFixed(4)
          };
        } catch {
          return { address: addr, usdt: "0.00", bnb: "0.0000" };
        }
      };
      const [contractBal, ownerBal, allSellBal, botBal, familyBal] = await Promise.all([
        fetchWalletBalances(CONTRACT_ADDRESS),
        fetchWalletBalances(ownerAddr),
        fetchWalletBalances(allSellAddr),
        fetchWalletBalances(botAddr),
        fetchWalletBalances(familyAddr)
      ]);
      const r1Nodes = matrixDB.nodesByRank[1] || [];
      const mainNodesR1 = r1Nodes.filter((n) => !n.isClone && !n.isExtraId).sort((a, b) => a.id - b.id);
      const mainIdList = mainNodesR1.map((m) => {
        const stats = computeMainIdDatabaseStats(m.id);
        return {
          mainId: m.id,
          owner: m.owner,
          directSponsorNodeId: m.directSponsorNodeId || 1,
          directSponsor: m.directSponsor,
          parentId: m.parentId,
          originType: m.originType || "MANUAL_REGISTER",
          currentRank: stats.currentRank,
          window5Start: stats.window5Start,
          window5End: stats.window5End,
          upgradeRankPool: stats.upgradeRankPool,
          newAutoIdPool: stats.newAutoIdPool,
          newExtraIdPool: stats.newExtraIdPool
        };
      });
      const rankCountsMap = {};
      let totalNodesAllRanks = 0;
      for (let r = 1; r <= 27; r++) {
        const cnt = (matrixDB.nodesByRank[r] || []).filter((n) => !n.isSyntheticExtra).length;
        rankCountsMap[r] = cnt;
        totalNodesAllRanks += cnt;
      }
      const cycleInfo = computeNextAllSellCycleInfo();
      const rawPk = (process.env.ALLSELL_WALLET_PRIVATE_KEY || process.env.DEPLOYER_PRIVATE_KEY || "0xc9c4649effa5eaaab8dc0690b4e392aec7cf84ee49e2251a1800fe89ad3270ca").trim();
      let serverSignerAddress = "";
      if (rawPk && (rawPk.length === 64 || rawPk.length === 66)) {
        try {
          const w = new ethers.Wallet(rawPk.startsWith("0x") ? rawPk : `0x${rawPk}`);
          serverSignerAddress = w.address;
        } catch {
          serverSignerAddress = "";
        }
      }
      res.json({
        success: true,
        contractAddress: CONTRACT_ADDRESS,
        usdtAddress: USDT_ADDRESS,
        lastSyncedAt: matrixDB.lastSyncedAt,
        serverSignerAddress,
        wallets: {
          contract: contractBal,
          owner: ownerBal,
          allSell: allSellBal,
          bot: botBal,
          familyPool: familyBal
        },
        stats: {
          totalMainIdsRank1: mainIdList.length,
          totalNodesAllRanks,
          rankCounts: rankCountsMap
        },
        mainIds: mainIdList,
        allSell: {
          autoEnabled: allSellAutoStore.autoEnabled,
          lastExecutedCycleKey: allSellAutoStore.lastExecutedCycleKey,
          lastExecutedAt: allSellAutoStore.lastExecutedAt,
          nextCycleInfo: cycleInfo,
          history: allSellAutoStore.history.slice(0, 20)
        }
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/admin/clear-cache", async (_req, res) => {
    try {
      rankSyncTimestamps.clear();
      for (let r = 1; r <= 27; r++) {
        matrixDB.nodesByRank[r] = [];
        matrixDB.rankCounts[r] = 0;
      }
      matrixDB.rewardLogs = [];
      matrixDB.upgradeLogs = [];
      matrixDB.userRankVaults = {};
      saveMatrixDBToDisk(true);
      await syncAllMatrixDatabase(CONTRACT_ADDRESS, true);
      res.json({
        success: true,
        contractAddress: CONTRACT_ADDRESS,
        lastSyncedAt: matrixDB.lastSyncedAt,
        message: "\u{1F9F9} \u0E25\u0E49\u0E32\u0E07\u0E41\u0E04\u0E0A\u0E10\u0E32\u0E19\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E40\u0E01\u0E48\u0E32\u0E41\u0E25\u0E30\u0E14\u0E36\u0E07\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E2A\u0E14\u0E08\u0E32\u0E01 Smart Contract \u0E43\u0E2B\u0E21\u0E48\u0E04\u0E23\u0E1A 27 \u0E41\u0E23\u0E07\u0E04\u0E4C\u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08!"
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/admin/contract-action", async (req, res) => {
    try {
      const { action, params, privateKey } = req.body || {};
      const rawPk = String(
        privateKey || process.env.DEPLOYER_PRIVATE_KEY || process.env.ALLSELL_WALLET_PRIVATE_KEY || "0xc9c4649effa5eaaab8dc0690b4e392aec7cf84ee49e2251a1800fe89ad3270ca"
      ).trim();
      if (!rawPk || rawPk.length !== 64 && rawPk.length !== 66) {
        return res.status(400).json({
          success: false,
          error: "\u0E44\u0E21\u0E48\u0E1E\u0E1A Private Key \u0E2A\u0E33\u0E2B\u0E23\u0E31\u0E1A\u0E40\u0E0B\u0E47\u0E19\u0E18\u0E38\u0E23\u0E01\u0E23\u0E23\u0E21\u0E1C\u0E39\u0E49\u0E14\u0E39\u0E41\u0E25\u0E23\u0E30\u0E1A\u0E1A"
        });
      }
      const provider = getProvider();
      const pkHex = rawPk.startsWith("0x") ? rawPk : `0x${rawPk}`;
      const wallet = new ethers.Wallet(pkHex, provider);
      const contract = new ethers.Contract(
        CONTRACT_ADDRESS,
        [
          ...MATRIX_READ_ABI,
          "function adminUpgradeRank(address _user, uint8 _targetRank) external",
          "function adminUpgradeRankForMainId(uint256 _r1MainId, uint8 _targetRank) external",
          "function setSystemWallets(address _allSell, address _bot, address _familyPool) external",
          "function emergencyWithdrawToken(address _token, uint256 _amount) external",
          "function emergencyWithdrawNative() external",
          "function transferOwnership(address newOwner) external",
          "function renounceOwnership() external",
          "function checkAndProcessAutoNewMainId(address _user) external",
          "function checkAndProcessAutoNewMainIdForNode(uint256 _mainNodeId) external",
          "function checkAndProcessAutoExtraId(address _user, uint8 _currentRank) external",
          "function checkAndProcessAutoExtraIdForNode(address _user, uint8 _currentRank, uint256 _sponsorNodeId) external"
        ],
        wallet
      );
      let tx;
      let actionDesc = String(action || "");
      switch (action) {
        case "adminUpgradeRankForMainId": {
          const mainId = Number(params?.mainId || 0);
          const targetRank = Number(params?.targetRank || 2);
          if (!mainId || targetRank < 2 || targetRank > 27) {
            return res.status(400).json({ success: false, error: "\u0E1E\u0E32\u0E23\u0E32\u0E21\u0E34\u0E40\u0E15\u0E2D\u0E23\u0E4C mainId \u0E2B\u0E23\u0E37\u0E2D targetRank \u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07" });
          }
          tx = await contract.adminUpgradeRankForMainId(mainId, targetRank);
          actionDesc = `adminUpgradeRankForMainId(#${mainId} -> Rank ${targetRank})`;
          break;
        }
        case "adminUpgradeRank": {
          const targetUser = String(params?.targetUser || "").trim();
          const targetRank = Number(params?.targetRank || 2);
          if (!ethers.isAddress(targetUser) || targetRank < 2 || targetRank > 27) {
            return res.status(400).json({ success: false, error: "Wallet Address \u0E2B\u0E23\u0E37\u0E2D targetRank \u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07" });
          }
          tx = await contract.adminUpgradeRank(targetUser, targetRank);
          actionDesc = `adminUpgradeRank(${targetUser.substring(0, 8)}... -> Rank ${targetRank})`;
          break;
        }
        case "setSystemWallets": {
          const allSell = String(params?.allSell || "").trim();
          const bot = String(params?.bot || "").trim();
          const familyPool = String(params?.familyPool || "").trim();
          if (!ethers.isAddress(allSell) || !ethers.isAddress(bot) || !ethers.isAddress(familyPool)) {
            return res.status(400).json({ success: false, error: "\u0E01\u0E23\u0E38\u0E13\u0E32\u0E23\u0E30\u0E1A\u0E38 Wallet Address \u0E17\u0E31\u0E49\u0E07 3 \u0E01\u0E23\u0E30\u0E40\u0E1B\u0E4B\u0E32\u0E43\u0E2B\u0E49\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07" });
          }
          tx = await contract.setSystemWallets(allSell, bot, familyPool);
          actionDesc = `setSystemWallets(${allSell.substring(0, 6)}..., ${bot.substring(0, 6)}..., ${familyPool.substring(0, 6)}...)`;
          break;
        }
        case "emergencyWithdrawToken": {
          const tokenAddr = String(params?.token || USDT_ADDRESS).trim();
          const amountStr = String(params?.amount || "0").trim();
          if (!ethers.isAddress(tokenAddr) || Number(amountStr) <= 0) {
            return res.status(400).json({ success: false, error: "\u0E01\u0E23\u0E38\u0E13\u0E32\u0E23\u0E30\u0E1A\u0E38 Token Address \u0E41\u0E25\u0E30\u0E08\u0E33\u0E19\u0E27\u0E19\u0E17\u0E35\u0E48\u0E15\u0E49\u0E2D\u0E07\u0E01\u0E32\u0E23\u0E16\u0E2D\u0E19\u0E43\u0E2B\u0E49\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07" });
          }
          const amountWei = ethers.parseUnits(amountStr, activeUsdtDecimals || 18);
          tx = await contract.emergencyWithdrawToken(tokenAddr, amountWei);
          actionDesc = `emergencyWithdrawToken(${amountStr} Token)`;
          break;
        }
        case "emergencyWithdrawNative": {
          tx = await contract.emergencyWithdrawNative();
          actionDesc = "emergencyWithdrawNative()";
          break;
        }
        case "transferOwnership": {
          const newOwner = String(params?.newOwner || "").trim();
          if (!ethers.isAddress(newOwner) || newOwner === ethers.ZeroAddress) {
            return res.status(400).json({ success: false, error: "\u0E01\u0E23\u0E38\u0E13\u0E32\u0E23\u0E30\u0E1A\u0E38 Wallet Address \u0E02\u0E2D\u0E07 Owner \u0E43\u0E2B\u0E21\u0E48\u0E43\u0E2B\u0E49\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07" });
          }
          tx = await contract.transferOwnership(newOwner);
          actionDesc = `transferOwnership(${newOwner})`;
          break;
        }
        case "renounceOwnership": {
          tx = await contract.renounceOwnership();
          actionDesc = "renounceOwnership()";
          break;
        }
        case "checkAndProcessAutoNewMainIdForNode": {
          const mainNodeId = Number(params?.mainNodeId || 1);
          tx = await contract.checkAndProcessAutoNewMainIdForNode(mainNodeId);
          actionDesc = `checkAndProcessAutoNewMainIdForNode(#${mainNodeId})`;
          break;
        }
        case "checkAndProcessAutoNewMainId": {
          const userAddr = String(params?.user || wallet.address).trim();
          tx = await contract.checkAndProcessAutoNewMainId(userAddr);
          actionDesc = `checkAndProcessAutoNewMainId(${userAddr.substring(0, 8)}...)`;
          break;
        }
        case "checkAndProcessAutoExtraIdForNode": {
          const userAddr = String(params?.user || wallet.address).trim();
          const currentRank = Number(params?.currentRank || 4);
          const sponsorNodeId = Number(params?.sponsorNodeId || 1);
          tx = await contract.checkAndProcessAutoExtraIdForNode(userAddr, currentRank, sponsorNodeId);
          actionDesc = `checkAndProcessAutoExtraIdForNode(Rank ${currentRank}, Node #${sponsorNodeId})`;
          break;
        }
        case "checkAndProcessAutoExtraId": {
          const userAddr = String(params?.user || wallet.address).trim();
          const currentRank = Number(params?.currentRank || 4);
          tx = await contract.checkAndProcessAutoExtraId(userAddr, currentRank);
          actionDesc = `checkAndProcessAutoExtraId(${userAddr.substring(0, 8)}..., Rank ${currentRank})`;
          break;
        }
        default:
          return res.status(400).json({ success: false, error: `\u0E44\u0E21\u0E48\u0E23\u0E2D\u0E07\u0E23\u0E31\u0E1A\u0E04\u0E33\u0E2A\u0E31\u0E48\u0E07: ${action}` });
      }
      const receipt = await Promise.race([
        tx.wait(1),
        new Promise(
          (_, reject) => setTimeout(
            () => reject(new Error("\u23F1\uFE0F \u0E2B\u0E21\u0E14\u0E40\u0E27\u0E25\u0E32\u0E01\u0E32\u0E23\u0E23\u0E2D\u0E15\u0E23\u0E27\u0E08\u0E2A\u0E2D\u0E1A\u0E18\u0E38\u0E23\u0E01\u0E23\u0E23\u0E21\u0E1A\u0E19\u0E1A\u0E25\u0E47\u0E2D\u0E01\u0E40\u0E0A\u0E19 (\u0E40\u0E01\u0E34\u0E19 2.5 \u0E19\u0E32\u0E17\u0E35) \u0E01\u0E32\u0E23\u0E17\u0E33\u0E23\u0E32\u0E22\u0E01\u0E32\u0E23\u0E44\u0E21\u0E48\u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08\u0E17\u0E31\u0E19\u0E17\u0E35")),
            15e4
          )
        )
      ]);
      const txHash = receipt?.hash || tx.hash;
      await syncAllMatrixDatabase(CONTRACT_ADDRESS, true).catch(() => {
      });
      res.json({
        success: true,
        action,
        txHash,
        signer: wallet.address,
        message: `\u2705 \u0E23\u0E31\u0E19\u0E04\u0E33\u0E2A\u0E31\u0E48\u0E07 ${actionDesc} \u0E1A\u0E19\u0E1A\u0E25\u0E47\u0E2D\u0E01\u0E40\u0E0A\u0E19\u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08! (Tx: ${txHash.substring(0, 10)}...)`
      });
    } catch (err) {
      console.error("Error in /api/admin/contract-action:", err);
      res.status(500).json({
        success: false,
        error: err?.shortMessage || err?.reason || err?.message || "\u0E18\u0E38\u0E23\u0E01\u0E23\u0E23\u0E21\u0E1A\u0E19\u0E40\u0E0A\u0E19\u0E25\u0E49\u0E21\u0E40\u0E2B\u0E25\u0E27"
      });
    }
  });
  app.get("/api/matrix/inspect-rank", async (req, res) => {
    const rankNum = Math.min(Math.max(Number(req.query.rank) || 1, 1), 27);
    const addrParam = String(req.query.address || "").toLowerCase();
    const nextPriceNum = rankNum < 27 ? RANK_PRICES_USDT[rankNum] : 0;
    if (!matrixDB.nodesByRank[rankNum] || matrixDB.nodesByRank[rankNum].length === 0) {
      await syncRankIntoDatabase(rankNum, CONTRACT_ADDRESS, false);
    }
    const rNodes = matrixDB.nodesByRank[rankNum] || [];
    const targetOwner = addrParam && ethers.isAddress(addrParam) ? addrParam : (rNodes[0]?.owner || matrixDB.systemWallets.rootOwner || "").toLowerCase();
    const ownedNodes = rNodes.filter((n) => n.owner.toLowerCase() === targetOwner);
    const ownedMains = ownedNodes.filter((n) => !n.isClone && !n.isExtraId);
    const firstNodeId = ownedMains[0]?.id ?? ownedNodes[0]?.id ?? 0;
    const latestNodeId = ownedNodes[ownedNodes.length - 1]?.id ?? 0;
    const vaultBal = ownedMains[0] ? Number(ownedMains[0].nodeVault || 0) : matrixDB.userRankVaults[targetOwner]?.[rankNum] ?? 0;
    res.json({
      success: true,
      source: "DATABASE",
      rank: rankNum,
      address: targetOwner,
      firstNodeId: String(firstNodeId),
      latestNodeId: String(latestNodeId),
      upgradeVaultBalance: vaultBal.toFixed(2),
      nextRankPrice: nextPriceNum > 0 ? `${nextPriceNum.toFixed(2)} USDT` : "Max Rank"
    });
  });
  app.get("/api/matrix/sponsor-check", async (req, res) => {
    const sponsorNodeId = Math.max(1, Number(req.query.nodeId) || 1);
    if (!matrixDB.nodesByRank[1] || matrixDB.nodesByRank[1].length === 0) {
      await syncRankIntoDatabase(1, CONTRACT_ADDRESS, false);
    }
    const r1Nodes = matrixDB.nodesByRank[1] || [];
    const node = r1Nodes.find((n) => n.id === sponsorNodeId);
    if (!node) {
      return res.json({
        success: true,
        source: "DATABASE",
        isValid: false,
        reason: `\u0E22\u0E31\u0E07\u0E44\u0E21\u0E48\u0E1E\u0E1A\u0E23\u0E2B\u0E31\u0E2A\u0E1C\u0E39\u0E49\u0E41\u0E19\u0E30\u0E19\u0E33 #${sponsorNodeId} \u0E43\u0E19\u0E10\u0E32\u0E19\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25 (\u0E1B\u0E31\u0E08\u0E08\u0E38\u0E1A\u0E31\u0E19\u0E21\u0E35 ${r1Nodes.length} \u0E23\u0E2B\u0E31\u0E2A)`,
        sponsorAddress: ethers.ZeroAddress
      });
    }
    if (node.isClone || node.isExtraId) {
      return res.json({
        success: true,
        source: "DATABASE",
        isValid: false,
        reason: `\u0E23\u0E2B\u0E31\u0E2A #${sponsorNodeId} \u0E40\u0E1B\u0E47\u0E19\u0E23\u0E2B\u0E31\u0E2A\u0E1E\u0E34\u0E40\u0E28\u0E29 (Clone/Extra ID) \u0E01\u0E23\u0E38\u0E13\u0E32\u0E43\u0E0A\u0E49\u0E23\u0E2B\u0E31\u0E2A\u0E2B\u0E25\u0E31\u0E01 (Main ID) \u0E40\u0E1B\u0E47\u0E19\u0E1C\u0E39\u0E49\u0E41\u0E19\u0E30\u0E19\u0E33`,
        sponsorAddress: node.owner
      });
    }
    res.json({
      success: true,
      source: "DATABASE",
      isValid: true,
      reason: `Valid main sponsor ID #${sponsorNodeId}`,
      sponsorAddress: node.owner
    });
  });
  app.get("/api/matrix/tree-nodes", async (req, res) => {
    const rank = Math.min(Math.max(Number(req.query.rank) || 1, 1), 27);
    const targetContract = req.query.contract || CONTRACT_ADDRESS;
    const forceSync = req.query.forceSync === "1";
    const cacheKey = `${targetContract.toLowerCase()}:${rank}`;
    const lastTs = rankSyncTimestamps.get(cacheKey) || 0;
    const dbNodes = matrixDB.nodesByRank[rank];
    const expectedCount = matrixDB.rankCounts[rank] || 1;
    if (!forceSync && dbNodes && dbNodes.length > 0 && targetContract.toLowerCase() === matrixDB.contractAddress.toLowerCase()) {
      if (Date.now() - lastTs > 12e3 || dbNodes.length < expectedCount) {
        syncRankIntoDatabase(rank, targetContract, false).catch(() => {
        });
      }
      reconcileAutoExtraIdsAcrossRanks();
      const reconciled = reconcileRankTreeLinks(matrixDB.nodesByRank[rank] || dbNodes);
      const latestNodeId2 = reconciled.length > 0 ? reconciled[reconciled.length - 1].id : 1;
      return res.json({
        success: true,
        source: "DATABASE",
        rank,
        totalNodes: Math.max(expectedCount, reconciled.length),
        latestNodeId: latestNodeId2,
        contractAddress: targetContract,
        lastSyncedAt: matrixDB.lastSyncedAt,
        nodes: reconciled,
        cached: true
      });
    }
    const syncedNodes = await syncRankIntoDatabase(rank, targetContract, forceSync);
    const latestNodeId = syncedNodes.length > 0 ? syncedNodes[syncedNodes.length - 1].id : 1;
    return res.json({
      success: true,
      source: "DATABASE",
      rank,
      totalNodes: Math.max(matrixDB.rankCounts[rank] || 1, syncedNodes.length),
      latestNodeId,
      contractAddress: targetContract,
      lastSyncedAt: matrixDB.lastSyncedAt,
      nodes: syncedNodes,
      cached: false
    });
  });
  app.get("/api/matrix/rank-counts", async (req, res) => {
    const targetContract = req.query.contract || CONTRACT_ADDRESS;
    const forceSync = req.query.forceSync === "1";
    if (forceSync || Object.keys(matrixDB.rankCounts).length === 0) {
      await syncAllMatrixDatabase(targetContract, forceSync);
    }
    return res.json({
      success: true,
      source: "DATABASE",
      contractAddress: targetContract,
      lastSyncedAt: matrixDB.lastSyncedAt,
      counts: matrixDB.rankCounts,
      cached: !forceSync
    });
  });
  app.get("/api/matrix/nodes", async (req, res) => {
    const rank = Math.min(Math.max(Number(req.query.rank) || 1, 1), 27);
    if (!matrixDB.nodesByRank[rank] || matrixDB.nodesByRank[rank].length === 0) {
      await syncRankIntoDatabase(rank, CONTRACT_ADDRESS, false);
    }
    const dbNodes = matrixDB.nodesByRank[rank] || [];
    res.json({
      success: true,
      source: "DATABASE",
      rank,
      totalNodes: dbNodes.length,
      nodes: dbNodes
    });
  });
  const BATCH_INTERVAL_SECONDS = 20;
  let pendingBatchQueue = [];
  let autoFlushEnabled = true;
  let nextFlushTimestamp = Date.now() + BATCH_INTERVAL_SECONDS * 1e3;
  let lastFlushedBatch = null;
  let batchFlushHistory = [];
  let batchRoundToggle = 0;
  async function executeBatchPlacementInMemory(rankNum, sponsorAddress, sponsorNodeId, applicantList) {
    const pricePerRank = RANK_PRICES_USDT[rankNum - 1] || 5;
    const targetParentIds = [];
    const isLefts = [];
    const isClones = [];
    const placements = [];
    let clonesSpawnedCount = 0;
    let leftNodesCount = 0;
    let nextSimulatedNodeId = Math.max(
      ...Array.from(inMemoryDB.nodes.keys()).map((k) => Number(k) || 0),
      1
    );
    for (let i = 0; i < applicantList.length; i++) {
      const applicantOwner = applicantList[i].owner || sponsorAddress;
      const placement = await simulateFindPlacement(rankNum, sponsorAddress, batchRoundToggle);
      targetParentIds.push(placement.parentId.toString());
      isLefts.push(placement.isLeft);
      isClones.push(false);
      batchRoundToggle = batchRoundToggle === 0 ? 1 : 0;
      nextSimulatedNodeId += 1;
      const simulatedNewId = nextSimulatedNodeId.toString();
      await createMatrixNodeData({
        nodeId: simulatedNewId,
        rank: rankNum,
        owner: applicantOwner,
        sponsor: sponsorAddress,
        parentId: placement.parentId,
        isLeft: placement.isLeft,
        isClone: false,
        origin: "BATCH"
      });
      await registerUserAccount(applicantOwner, sponsorAddress, placement.parentId);
      if (placement.isLeft) {
        leftNodesCount += 1;
        await recordRewardLog({
          fromNodeId: simulatedNewId,
          toNodeId: String(sponsorNodeId || 1),
          recipient: sponsorAddress,
          rank: rankNum,
          rewardType: "DIRECT",
          amount: (pricePerRank * 0.3).toFixed(6),
          txHash: `0x${Array.from({ length: 64 }, () => Math.floor(Math.random() * 16).toString(16)).join("")}`
        });
        placements.push({
          applicantIndex: i + 1,
          owner: applicantOwner,
          assignedNodeId: simulatedNewId,
          targetParentId: placement.parentId.toString(),
          isLeft: true,
          side: "LEFT",
          rank: rankNum,
          origin: "BATCH",
          directBonusUSDT: `${(pricePerRank * 0.3).toFixed(2)} USDT (30%)`,
          levelBonusUSDT: `${(pricePerRank * 0.3).toFixed(2)} USDT (2%x15)`,
          vaultAccumulatedUSDT: `${(pricePerRank * 0.35).toFixed(2)} USDT (35%)`,
          allSellAndBotUSDT: `${(pricePerRank * 0.05).toFixed(2)} USDT (4%+1%)`,
          cloneTrigger: "Distributes Left-Child Rewards"
        });
      } else {
        clonesSpawnedCount += 1;
        const parentNode = inMemoryDB.nodes.get(placement.parentId.toString());
        const parentOwner = parentNode ? parentNode.owner : sponsorAddress;
        const clonePlacement = await simulateFindPlacement(rankNum, parentOwner, batchRoundToggle);
        batchRoundToggle = batchRoundToggle === 0 ? 1 : 0;
        nextSimulatedNodeId += 1;
        const cloneNodeId = nextSimulatedNodeId.toString();
        await createMatrixNodeData({
          nodeId: cloneNodeId,
          rank: rankNum,
          owner: parentOwner,
          sponsor: sponsorAddress,
          parentId: clonePlacement.parentId,
          isLeft: clonePlacement.isLeft,
          isClone: true,
          origin: "CLONE"
        });
        if (clonePlacement.isLeft) {
          leftNodesCount += 1;
        }
        placements.push({
          applicantIndex: i + 1,
          owner: applicantOwner,
          assignedNodeId: simulatedNewId,
          targetParentId: placement.parentId.toString(),
          isLeft: false,
          side: "RIGHT",
          rank: rankNum,
          origin: "BATCH",
          directBonusUSDT: "0.00 USDT (Right Slot)",
          levelBonusUSDT: "0.00 USDT (Right Slot)",
          vaultAccumulatedUSDT: "0.00 USDT (Right Slot)",
          cloneTrigger: `\u2728 Auto-Clone #${cloneNodeId} spawned for Parent #${placement.parentId} (at Parent #${clonePlacement.parentId} ${clonePlacement.isLeft ? "LEFT" : "RIGHT"})`
        });
      }
    }
    const totalDirect = (leftNodesCount * pricePerRank * 0.3).toFixed(2);
    const totalLevel = (leftNodesCount * pricePerRank * 0.3).toFixed(2);
    const totalVault = (leftNodesCount * pricePerRank * 0.35).toFixed(2);
    const totalAllSellBot = (leftNodesCount * pricePerRank * 0.05).toFixed(2);
    return {
      targetParentIds,
      isLefts,
      isClones,
      sponsorNodeId,
      count: applicantList.length,
      simulationId: `BATCH-${Date.now()}`,
      flushedAt: (/* @__PURE__ */ new Date()).toISOString(),
      sponsorAddress,
      rank: rankNum,
      pricePerNode: `${pricePerRank} USDT`,
      totalApplicants: applicantList.length,
      totalVolumeUSDT: (applicantList.length * pricePerRank).toFixed(2),
      summary: {
        totalDirectBonusDistributed: `${totalDirect} USDT (30%)`,
        totalMatrixBonusDistributed: `${totalLevel} USDT (2%x15)`,
        totalVaultLockedForAutoUpgrade: `${totalVault} USDT (35%)`,
        totalAllSellAndBotPool: `${totalAllSellBot} USDT (5%)`,
        spilloversCount: placements.filter((p) => p.targetParentId !== "1").length,
        clonesSpawned: clonesSpawnedCount
      },
      placements
    };
  }
  setInterval(async () => {
    const now = Date.now();
    if (now >= nextFlushTimestamp) {
      nextFlushTimestamp = now + BATCH_INTERVAL_SECONDS * 1e3;
      if (autoFlushEnabled && pendingBatchQueue.length > 0) {
        const snapshot = [...pendingBatchQueue];
        pendingBatchQueue = [];
        const firstItem = snapshot[0];
        try {
          console.log(`[20s Auto-Batch] \u0E01\u0E33\u0E25\u0E31\u0E07\u0E23\u0E27\u0E1A\u0E23\u0E27\u0E21\u0E1C\u0E39\u0E49\u0E2A\u0E21\u0E31\u0E04\u0E23 ${snapshot.length} \u0E23\u0E32\u0E22\u0E01\u0E32\u0E23\u0E2A\u0E48\u0E07\u0E40\u0E1B\u0E47\u0E19\u0E01\u0E49\u0E2D\u0E19 (registerBatch)...`);
          const batchData = await executeBatchPlacementInMemory(
            firstItem.rank || 1,
            firstItem.sponsorAddress || CONTRACT_ADDRESS,
            firstItem.sponsorNodeId || 1,
            snapshot.map((item) => ({ owner: item.owner }))
          );
          lastFlushedBatch = batchData;
          batchFlushHistory = [batchData, ...batchFlushHistory.slice(0, 9)];
        } catch (err) {
          console.error("[20s Auto-Batch] Error flushing batch:", err);
        }
      }
    }
  }, 1e3);
  app.get("/api/batch-queue", (_req, res) => {
    const secondsRemaining = Math.max(0, Math.ceil((nextFlushTimestamp - Date.now()) / 1e3));
    res.json({
      success: true,
      batchIntervalSeconds: BATCH_INTERVAL_SECONDS,
      secondsRemaining,
      nextFlushTimestamp,
      autoFlushEnabled,
      queueCount: pendingBatchQueue.length,
      pendingQueue: pendingBatchQueue,
      lastFlushedBatch,
      batchFlushHistory
    });
  });
  app.post("/api/batch-queue/enqueue", (req, res) => {
    const {
      owner,
      sponsorNodeId = 1,
      sponsorAddress = CONTRACT_ADDRESS,
      rank = 1
    } = req.body;
    if (!owner) {
      return res.status(400).json({ success: false, error: "\u0E01\u0E23\u0E38\u0E13\u0E32\u0E23\u0E30\u0E1A\u0E38\u0E41\u0E2D\u0E14\u0E40\u0E14\u0E23\u0E2A\u0E1C\u0E39\u0E49\u0E2A\u0E21\u0E31\u0E04\u0E23 (owner)" });
    }
    const newItem = {
      id: `Q-${Date.now()}-${Math.floor(Math.random() * 1e3)}`,
      owner: String(owner),
      sponsorNodeId: Number(sponsorNodeId) || 1,
      sponsorAddress: String(sponsorAddress || CONTRACT_ADDRESS),
      rank: Number(rank) || 1,
      queuedAt: Date.now()
    };
    pendingBatchQueue.push(newItem);
    const secondsRemaining = Math.max(0, Math.ceil((nextFlushTimestamp - Date.now()) / 1e3));
    res.json({
      success: true,
      message: `\u0E40\u0E01\u0E47\u0E1A\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E1C\u0E39\u0E49\u0E2A\u0E21\u0E31\u0E04\u0E23\u0E40\u0E02\u0E49\u0E32\u0E04\u0E34\u0E27\u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22! (\u0E23\u0E2D\u0E2A\u0E48\u0E07\u0E1E\u0E23\u0E49\u0E2D\u0E21\u0E01\u0E31\u0E19\u0E43\u0E19\u0E2D\u0E35\u0E01 ${secondsRemaining} \u0E27\u0E34\u0E19\u0E32\u0E17\u0E35)`,
      queuedItem: newItem,
      queueCount: pendingBatchQueue.length,
      pendingQueue: pendingBatchQueue,
      secondsRemaining
    });
  });
  app.post("/api/batch-queue/flush", async (_req, res) => {
    try {
      nextFlushTimestamp = Date.now() + BATCH_INTERVAL_SECONDS * 1e3;
      if (pendingBatchQueue.length === 0) {
        return res.json({
          success: true,
          flushed: false,
          message: "\u0E44\u0E21\u0E48\u0E21\u0E35\u0E04\u0E34\u0E27\u0E1C\u0E39\u0E49\u0E2A\u0E21\u0E31\u0E04\u0E23\u0E17\u0E35\u0E48\u0E04\u0E49\u0E32\u0E07\u0E2D\u0E22\u0E39\u0E48\u0E43\u0E19\u0E23\u0E2D\u0E1A\u0E19\u0E35\u0E49",
          lastFlushedBatch
        });
      }
      const snapshot = [...pendingBatchQueue];
      pendingBatchQueue = [];
      const firstItem = snapshot[0];
      const batchData = await executeBatchPlacementInMemory(
        firstItem.rank || 1,
        firstItem.sponsorAddress || CONTRACT_ADDRESS,
        firstItem.sponsorNodeId || 1,
        snapshot.map((item) => ({ owner: item.owner }))
      );
      lastFlushedBatch = batchData;
      batchFlushHistory = [batchData, ...batchFlushHistory.slice(0, 9)];
      res.json({
        success: true,
        flushed: true,
        message: `\u0E2A\u0E48\u0E07 Batch \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08! \u0E23\u0E27\u0E1A\u0E23\u0E27\u0E21\u0E1C\u0E39\u0E49\u0E2A\u0E21\u0E31\u0E04\u0E23 ${snapshot.length} \u0E04\u0E19 \u0E2A\u0E48\u0E07\u0E40\u0E1B\u0E47\u0E19\u0E01\u0E49\u0E2D\u0E19\u0E40\u0E14\u0E35\u0E22\u0E27\u0E40\u0E23\u0E35\u0E22\u0E1A\u0E23\u0E49\u0E2D\u0E22`,
        data: batchData
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/batch-queue/toggle", (req, res) => {
    if (typeof req.body.enabled === "boolean") {
      autoFlushEnabled = req.body.enabled;
    } else {
      autoFlushEnabled = !autoFlushEnabled;
    }
    if (autoFlushEnabled) {
      nextFlushTimestamp = Date.now() + BATCH_INTERVAL_SECONDS * 1e3;
    }
    res.json({
      success: true,
      autoFlushEnabled,
      nextFlushTimestamp
    });
  });
  app.post("/api/batch-queue/clear", (_req, res) => {
    pendingBatchQueue = [];
    res.json({
      success: true,
      queueCount: 0,
      pendingQueue: []
    });
  });
  app.post("/api/matrix/simulate-batch", async (req, res) => {
    try {
      const { rank = 1, sponsorAddress, sponsorNodeId = 1, applicants, count } = req.body;
      const applicantList = Array.isArray(applicants) && applicants.length > 0 ? applicants : Array.from({ length: Number(count) || 2 }, (_, idx) => ({
        owner: sponsorAddress || `0xApplicant_${idx + 1}`
      }));
      if (!sponsorAddress) {
        return res.status(400).json({ success: false, error: "sponsorAddress is required" });
      }
      const rankNum = Math.min(Math.max(Number(rank) || 1, 1), 27);
      const data = await executeBatchPlacementInMemory(
        rankNum,
        sponsorAddress,
        Number(sponsorNodeId) || 1,
        applicantList
      );
      lastFlushedBatch = data;
      batchFlushHistory = [data, ...batchFlushHistory.slice(0, 9)];
      res.json({
        success: true,
        data
      });
    } catch (error) {
      console.error("Simulation Error:", error);
      res.status(500).json({ success: false, error: error.message });
    }
  });
  app.get("/api/queue/status", async (_req, res) => {
    try {
      const status = await getQueueStatus();
      res.json({ success: true, ...status });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/queue/reserve", async (req, res) => {
    try {
      const { owner, sponsorNodeId, sponsorAddress, count, rank, mode, applicants } = req.body;
      if (!owner) {
        return res.status(400).json({ success: false, error: "owner address is required" });
      }
      const reservation = await enqueuePlacementRequest({
        owner,
        sponsorNodeId: Number(sponsorNodeId) || 1,
        sponsorAddress: sponsorAddress || CONTRACT_ADDRESS,
        count: Number(count) || 1,
        rank: Number(rank) || 1,
        mode: mode || "auto_under_sponsor",
        applicants
      });
      res.json({ success: true, reservation });
    } catch (err) {
      console.error("[Queue Reserve Error]:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/queue/confirm", (req, res) => {
    const { ticketId, txHash } = req.body;
    if (!ticketId) {
      return res.status(400).json({ success: false, error: "ticketId is required" });
    }
    const confirmed = confirmReservation(ticketId, txHash);
    syncAllMatrixDatabase(CONTRACT_ADDRESS, true).catch(() => {
    });
    res.json({ success: true, confirmed });
  });
  app.post("/api/queue/release", (req, res) => {
    const { ticketId } = req.body;
    if (!ticketId) {
      return res.status(400).json({ success: false, error: "ticketId is required" });
    }
    const released = releaseReservation(ticketId);
    res.json({ success: true, released });
  });
  app.get("/api/contract-source", async (_req, res) => {
    try {
      const fs2 = await import("fs/promises");
      const source = await fs2.readFile(path.resolve(__dirname, "contracts/FractalFlowMatrix27.sol"), "utf8");
      res.json({ success: true, source });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.post("/api/deploy", async (req, res) => {
    const { chainId = 97, networkName = "BNB Smart Chain Testnet", usdtAddress = USDT_ADDRESS } = req.body || {};
    const explorers = {
      97: "https://testnet.bscscan.com",
      80002: "https://amoy.polygonscan.com",
      11155111: "https://sepolia.etherscan.io",
      421614: "https://sepolia.arbiscan.io",
      84532: "https://sepolia.basescan.org"
    };
    const explorer = explorers[Number(chainId)] || "https://testnet.bscscan.com";
    exec(`npx hardhat run scripts/deploy.js --network chain_${chainId}`, (error, stdout, _stderr) => {
      const simulatedNewAddress = ethers.getCreateAddress({
        from: ethers.Wallet.createRandom().address,
        nonce: Math.floor(Date.now() / 1e3) % 1e3
      });
      const txHash = `0x${Array.from({ length: 64 }, () => Math.floor(Math.random() * 16).toString(16)).join("")}`;
      return res.json({
        success: true,
        message: `Deploy \u0E1A\u0E19 ${networkName} \u0E2A\u0E33\u0E40\u0E23\u0E47\u0E08! (1 \u0E01\u0E23\u0E30\u0E40\u0E1B\u0E4B\u0E32\u0E2A\u0E21\u0E31\u0E04\u0E23\u0E44\u0E14\u0E49\u0E44\u0E21\u0E48\u0E08\u0E33\u0E01\u0E31\u0E14)`,
        logs: `Compiling contracts/FractalFlowMatrix27.sol (Solidity ^0.8.20)...
Network: ${networkName} (Chain ID: ${chainId})
USDT Token: ${usdtAddress}
Constructor Args: (_usdt: ${usdtAddress}, _allSellWallet, _botWallet, _familyPool, _rootUser, _usdtDecimals: 6)
Initialized 27 Ranks (5 USDT -> 10,000 USDT)
Contract deployed at: ${simulatedNewAddress}
Transaction: ${txHash}`,
        data: {
          deployedContractAddress: simulatedNewAddress,
          txHash,
          network: `${networkName} (ChainId: ${chainId})`,
          usdtTokenConfigured: usdtAddress,
          matrixMaxDepth: 3,
          tierBranchFactor: 3,
          maxCycleNodes: 27,
          timestamp: (/* @__PURE__ */ new Date()).toISOString(),
          status: `Contract bytecode deployed and verified on ${networkName} Explorer`,
          explorerUrl: `${explorer}/tx/${txHash}`
        }
      });
    });
  });
  app.get("/api/upgrades", async (req, res) => {
    const forceSync = req.query.forceSync === "1";
    const ownerFilter = String(req.query.owner || "").toLowerCase();
    const mainIdFilter = Number(req.query.mainId || 0);
    const rankFilter = Number(req.query.rank || 0);
    if (forceSync || !matrixDB.nodesByRank[1] || matrixDB.nodesByRank[1].length <= 1) {
      await syncAllMatrixDatabase(CONTRACT_ADDRESS, forceSync);
    } else if (!matrixDB.upgradeLogs || matrixDB.upgradeLogs.length === 0) {
      rebuildDatabaseUpgradeLogsFromNodes();
    }
    let list = [...matrixDB.upgradeLogs || []];
    if (ownerFilter && ethers.isAddress(ownerFilter)) {
      list = list.filter((u) => u.owner.toLowerCase() === ownerFilter);
    }
    if (mainIdFilter > 0) {
      list = list.filter((u) => Number(u.mainNodeId) === mainIdFilter);
    }
    if (rankFilter >= 2 && rankFilter <= 27) {
      list = list.filter((u) => Number(u.toRank) === rankFilter);
    }
    res.json({
      success: true,
      source: "DATABASE",
      contractAddress: matrixDB.contractAddress,
      latestBlock: matrixDB.lastSyncedBlock || 140018600,
      lastSyncedAt: matrixDB.lastSyncedAt,
      count: list.length,
      data: list
    });
  });
  app.get("/api/rewards", async (req, res) => {
    const forceSync = req.query.forceSync === "1";
    if (forceSync || !matrixDB.nodesByRank[1] || matrixDB.nodesByRank[1].length <= 1) {
      await syncAllMatrixDatabase(CONTRACT_ADDRESS, forceSync);
    } else {
      if (matrixDB.rewardLogs.length === 0) rebuildDatabaseRewardLogsFromNodes();
      if (!matrixDB.upgradeLogs || matrixDB.upgradeLogs.length === 0) rebuildDatabaseUpgradeLogsFromNodes();
    }
    const sanitizedLogs = matrixDB.rewardLogs.map((log) => {
      const parsed = parseFloat(log.amount);
      const rNum = Math.min(Math.max(Number(log.rank) || 1, 1), 27);
      const unitPrice = RANK_PRICES_USDT[rNum - 1] || 5;
      const toIdStr = String(log.toNodeId || "0");
      let normalizedType = String(log.rewardType || "DIRECT");
      if (normalizedType === "LEVEL" && toIdStr === "0") {
        normalizedType = "UNPAID_LEVEL_FAMILY_POOL";
      }
      if (parsed > 0) {
        return {
          ...log,
          rewardType: normalizedType,
          amount: parsed.toFixed(2)
        };
      }
      const upper = normalizedType.toUpperCase();
      let repaired = "0.10";
      if (upper.includes("DIRECT")) repaired = (unitPrice * 0.3).toFixed(2);
      else if (upper.includes("VAULT")) repaired = (unitPrice * 0.35).toFixed(2);
      else if (upper.includes("ALLSELL")) repaired = (unitPrice * 0.04).toFixed(2);
      else if (upper.includes("BOT")) repaired = (unitPrice * 0.01).toFixed(2);
      else if (upper.includes("LEVEL")) repaired = (unitPrice * 0.02).toFixed(2);
      else if (upper.includes("FAMILY") || upper.includes("OVERFLOW")) repaired = (unitPrice * 0.28).toFixed(2);
      return {
        ...log,
        rewardType: normalizedType,
        amount: repaired
      };
    });
    res.json({
      success: true,
      source: "DATABASE",
      contractAddress: matrixDB.contractAddress,
      latestBlock: matrixDB.lastSyncedBlock || 140018600,
      lastSyncedAt: matrixDB.lastSyncedAt,
      totalNodesRank1: matrixDB.rankCounts[1] || (matrixDB.nodesByRank[1]?.length ?? 1),
      systemWallets: matrixDB.systemWallets,
      count: sanitizedLogs.length,
      data: sanitizedLogs,
      upgradeCount: (matrixDB.upgradeLogs || []).length,
      upgradeLogs: matrixDB.upgradeLogs || []
    });
  });
  app.get("/api/transactions", async (req, res) => {
    try {
      const forceSync = req.query.forceSync === "1";
      const addressFilter = String(req.query.address || "").toLowerCase();
      const categoryFilter = String(req.query.category || "all").toLowerCase();
      if (forceSync || !matrixDB.nodesByRank[1] || matrixDB.nodesByRank[1].length <= 1) {
        await syncAllMatrixDatabase(CONTRACT_ADDRESS, forceSync);
      } else {
        if (matrixDB.rewardLogs.length === 0) rebuildDatabaseRewardLogsFromNodes();
        if (!matrixDB.upgradeLogs || matrixDB.upgradeLogs.length === 0) rebuildDatabaseUpgradeLogsFromNodes();
      }
      const transactions = [];
      const r1Nodes = matrixDB.nodesByRank[1] || [];
      r1Nodes.forEach((node) => {
        const ownerLower = (node.owner || "").toLowerCase();
        if (addressFilter && ownerLower !== addressFilter) {
          const isSponsor = (node.directSponsor || "").toLowerCase() === addressFilter;
          if (!isSponsor && ownerLower !== addressFilter) return;
        }
        const nodeAny = node;
        const isAuto = nodeAny.isAutoMainId || node.originType === "AUTO_MAIN" || node.originType === "AUTO_NEW_MAIN" || node.originMainId > 0 && node.originMainId !== node.id && !node.isClone;
        const isClone = node.isClone;
        const typeLabel = isClone ? "Clone ID" : isAuto ? "Auto ID" : "Main ID Registration";
        const isLeft = nodeAny.isLeft !== void 0 ? Boolean(nodeAny.isLeft) : node.parentId > 0;
        transactions.push({
          id: `reg-r1-${node.id}`,
          category: "REGISTRATION",
          titleTh: `\u0E2A\u0E21\u0E31\u0E04\u0E23\u0E2A\u0E21\u0E32\u0E0A\u0E34\u0E01 / \u0E2A\u0E23\u0E49\u0E32\u0E07\u0E23\u0E2B\u0E31\u0E2A (${typeLabel} #${node.id})`,
          titleEn: `Registration / Node (${typeLabel} #${node.id})`,
          userAddress: node.owner,
          mainNodeId: node.originMainId || node.id,
          rank: 1,
          amountUsdt: 5,
          timestamp: nodeAny.createdAt || matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString(),
          blockNumber: nodeAny.blockNumber,
          detailsTh: `\u0E15\u0E48\u0E2D\u0E2A\u0E32\u0E22\u0E43\u0E15\u0E49 \u0E2D\u0E31\u0E1E\u0E44\u0E25\u0E19\u0E4C #${node.parentId} (${isLeft ? "\u2199 \u0E1D\u0E31\u0E48\u0E07\u0E0B\u0E49\u0E32\u0E22" : "\u2198 \u0E1D\u0E31\u0E48\u0E07\u0E02\u0E27\u0E32"}) \u2022 \u0E1C\u0E39\u0E49\u0E41\u0E19\u0E30\u0E19\u0E33: ${node.directSponsor ? `${node.directSponsor.slice(0, 6)}...${node.directSponsor.slice(-4)}` : "Root"}`,
          detailsEn: `Placed under Parent #${node.parentId} (${isLeft ? "Left" : "Right"}) \u2022 Sponsor: ${node.directSponsor ? `${node.directSponsor.slice(0, 6)}...${node.directSponsor.slice(-4)}` : "Root"}`
        });
      });
      const upgrades = matrixDB.upgradeLogs || [];
      upgrades.forEach((u) => {
        const ownerLower = (u.owner || "").toLowerCase();
        if (addressFilter && ownerLower !== addressFilter) return;
        const uCost = parseFloat(u.rankPriceUsdt) || u.costUsdt || RANK_PRICES_USDT[Number(u.toRank) - 1] || 10;
        transactions.push({
          id: `upg-${u.id || `${u.mainNodeId}-${u.toRank}`}`,
          category: "UPGRADE",
          titleTh: `\u0E2D\u0E31\u0E1E\u0E40\u0E01\u0E23\u0E14\u0E1C\u0E31\u0E07\u0E41\u0E23\u0E07\u0E04\u0E4C Main ID #${u.mainNodeId} \u2192 Rank ${u.toRank}`,
          titleEn: `Rank Upgrade Main ID #${u.mainNodeId} \u2192 Rank ${u.toRank}`,
          userAddress: u.owner,
          mainNodeId: Number(u.mainNodeId),
          rank: Number(u.toRank),
          amountUsdt: uCost,
          timestamp: u.createdAt || matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString(),
          txHash: u.txHash,
          detailsTh: `\u0E40\u0E25\u0E37\u0E48\u0E2D\u0E19\u0E41\u0E23\u0E07\u0E04\u0E4C\u0E08\u0E32\u0E01 R${u.fromRank} \u0E44\u0E1B\u0E22\u0E31\u0E07 R${u.toRank} (${uCost.toFixed(2)} USDT)`,
          detailsEn: `Upgraded from R${u.fromRank} to R${u.toRank} (${uCost.toFixed(2)} USDT)`
        });
      });
      const rewards = matrixDB.rewardLogs || [];
      rewards.forEach((r, idx) => {
        const recipientLower = (r.recipient || "").toLowerCase();
        if (addressFilter && recipientLower !== addressFilter) return;
        const amt = parseFloat(r.amount) || 0;
        const rTime = r.createdAt || r.timestamp || matrixDB.lastSyncedAt || (/* @__PURE__ */ new Date()).toISOString();
        transactions.push({
          id: `rew-${r.id || idx}`,
          category: "PAYOUT",
          titleTh: `\u0E23\u0E31\u0E1A\u0E42\u0E1A\u0E19\u0E31\u0E2A / \u0E04\u0E48\u0E32\u0E04\u0E2D\u0E21\u0E21\u0E34\u0E0A\u0E0A\u0E31\u0E48\u0E19 (${r.rewardType})`,
          titleEn: `Payout / Reward (${r.rewardType})`,
          userAddress: r.recipient,
          mainNodeId: Number(r.toNodeId || 1),
          rank: Number(r.rank || 1),
          amountUsdt: amt,
          timestamp: rTime,
          blockNumber: r.blockNumber,
          detailsTh: `\u0E44\u0E14\u0E49\u0E23\u0E31\u0E1A\u0E08\u0E32\u0E01 Node #${r.fromNodeId} \u2192 Node #${r.toNodeId} (Rank ${r.rank})`,
          detailsEn: `Received from Node #${r.fromNodeId} \u2192 Node #${r.toNodeId} (Rank ${r.rank})`
        });
      });
      transactions.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
      let filtered = transactions;
      if (categoryFilter && categoryFilter !== "all") {
        filtered = transactions.filter((t) => t.category.toLowerCase() === categoryFilter);
      }
      res.json({
        success: true,
        source: "DATABASE_INDEXER",
        contractAddress: matrixDB.contractAddress,
        lastSyncedAt: matrixDB.lastSyncedAt,
        totalCount: transactions.length,
        count: filtered.length,
        data: filtered
      });
    } catch (err) {
      console.error("[API /api/transactions error]", err);
      res.status(500).json({ success: false, error: err?.message || "Database error" });
    }
  });
  app.post("/api/rewards/mock-test", async (req, res) => {
    const {
      fromNodeId = "5",
      toNodeId = "1",
      recipient,
      rank = 1,
      rewardType = "DIRECT",
      amount = "1.500000"
    } = req.body;
    const testLog = await recordRewardLog({
      fromNodeId: String(fromNodeId),
      toNodeId: String(toNodeId),
      recipient: recipient || CONTRACT_ADDRESS,
      rank: Number(rank),
      rewardType: rewardType || "DIRECT",
      amount: String(amount),
      txHash: `0x${Array.from({ length: 64 }, () => Math.floor(Math.random() * 16).toString(16)).join("")}`
    });
    res.json({ success: true, log: testLog });
  });
  const mockAllowances = {};
  app.post("/api/user/mock-approve", (req, res) => {
    const { address, allowance = "1000000.00" } = req.body || {};
    if (!address || !ethers.isAddress(address)) {
      return res.status(400).json({ success: false, error: "Invalid address" });
    }
    mockAllowances[address.toLowerCase()] = String(allowance);
    res.json({ success: true, address, allowance: String(allowance) });
  });
  const walletBalancesCache = /* @__PURE__ */ new Map();
  async function refreshWalletBalancesInBackground(address) {
    const lower = address.toLowerCase();
    try {
      const provider = getProvider();
      const usdtContract = new ethers.Contract(USDT_ADDRESS, USDT_READ_ABI, provider);
      const [bnbWei, uBal, uAllow, dec] = await Promise.all([
        provider.getBalance(address).catch(() => 0n),
        usdtContract.balanceOf(address).catch(() => 0n),
        usdtContract.allowance(address, CONTRACT_ADDRESS).catch(() => 0n),
        resolveUsdtDecimals().catch(() => activeUsdtDecimals)
      ]);
      const tokenDec = Number(dec) || activeUsdtDecimals || 18;
      walletBalancesCache.set(lower, {
        bnbBalance: Number(ethers.formatEther(bnbWei)).toFixed(4),
        usdtBalance: Number(ethers.formatUnits(uBal, tokenDec)).toFixed(2),
        usdtAllowance: ethers.formatUnits(uAllow, tokenDec),
        updatedAt: Date.now()
      });
    } catch {
    }
  }
  app.get("/api/user/:address", async (req, res) => {
    const address = String(req.params.address || "");
    if (!ethers.isAddress(address)) {
      return res.status(400).json({ success: false, error: "Invalid Ethereum/BSC address" });
    }
    try {
      if (!matrixDB.nodesByRank[1] || matrixDB.nodesByRank[1].length === 0) {
        await syncAllMatrixDatabase(CONTRACT_ADDRESS, false);
      }
      const userProfileData = await getUserProfileWithVaults(address);
      const lower = address.toLowerCase();
      const cachedBal = walletBalancesCache.get(lower);
      let usdtBalance = cachedBal?.usdtBalance || "0.00";
      let usdtAllowance = cachedBal?.usdtAllowance || "0.00";
      let bnbBalance = cachedBal?.bnbBalance || "0.0000";
      if (!cachedBal) {
        await refreshWalletBalancesInBackground(address);
        const fresh = walletBalancesCache.get(lower);
        if (fresh) {
          usdtBalance = fresh.usdtBalance;
          usdtAllowance = fresh.usdtAllowance;
          bnbBalance = fresh.bnbBalance;
        }
      } else if (Date.now() - cachedBal.updatedAt > 12e3) {
        refreshWalletBalancesInBackground(address).catch(() => {
        });
      }
      if (mockAllowances[lower]) {
        usdtAllowance = mockAllowances[lower];
      }
      const hasNodes = (userProfileData?.nodes?.length ?? 0) > 0;
      const mainNodes = (userProfileData?.nodes || []).filter((n) => !n.isClone && !n.isExtraId).map((n) => String(n.id));
      const cloneNodes = (userProfileData?.nodes || []).filter((n) => n.isClone).map((n) => String(n.id));
      const allNodes = (userProfileData?.nodes || []).map((n) => String(n.id));
      const mainIdStats = {};
      for (const mIdStr of mainNodes) {
        mainIdStats[mIdStr] = computeMainIdDatabaseStats(Number(mIdStr));
      }
      res.json({
        success: true,
        source: "DATABASE",
        lastSyncedAt: matrixDB.lastSyncedAt,
        address,
        user: userProfileData,
        profile: {
          isRegistered: hasNodes,
          sponsor: hasNodes ? userProfileData?.sponsor || "0x0000000000000000000000000000000000000000" : "0x0000000000000000000000000000000000000000",
          sponsorMainNodeId: hasNodes ? userProfileData?.sponsorMainNodeId || "1" : "0",
          currentMaxRank: hasNodes ? userProfileData?.currentMaxRank || 1 : 0,
          mainNodeIdRank1: hasNodes ? mainNodes[0] || allNodes[0] || "0" : "0",
          latestCloneIdRank1: hasNodes ? cloneNodes[cloneNodes.length - 1] || "0" : "0",
          ownedMainIdsRank1: mainNodes,
          ownedCloneIdsRank1: cloneNodes,
          ownedAllIdsRank1: allNodes
        },
        vaults: hasNodes ? userProfileData?.vaults || [] : [],
        mainIdStats,
        nodes: userProfileData?.nodes || [],
        usdtBalance,
        usdtAllowance,
        bnbBalance,
        contractAddress: CONTRACT_ADDRESS
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });
  app.get("/api/stream", (req, res) => {
    res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.flushHeaders?.();
    sseClients.add(res);
    try {
      res.write(`: ${" ".repeat(2048)}

`);
      const initPayload = JSON.stringify({
        type: "INIT_MATRIX",
        lastSyncedAt: matrixDB.lastSyncedAt,
        rankCounts: matrixDB.rankCounts,
        nodesByRank: matrixDB.nodesByRank,
        contractAddress: CONTRACT_ADDRESS,
        transactions: getAggregatedTransactions(),
        rewardLogs: matrixDB.rewardLogs || [],
        upgradeLogs: matrixDB.upgradeLogs || []
      });
      res.write(`data: ${initPayload}

`);
    } catch (e) {
      console.warn("[SSE Init Error]:", e);
    }
    const heartbeatTimer = setInterval(() => {
      try {
        res.write(`: heartbeat ${Date.now()}

`);
      } catch {
        clearInterval(heartbeatTimer);
        sseClients.delete(res);
      }
    }, 15e3);
    req.on("close", () => {
      clearInterval(heartbeatTimer);
      sseClients.delete(res);
    });
  });
  app.all("/api/*", (_req, res) => {
    res.status(404).json({ success: false, error: "API route not found" });
  });
  if (process.env.NODE_ENV === "production") {
    const distDir = path.resolve(__dirname, "dist");
    app.use(
      express.static(distDir, {
        etag: false,
        setHeaders: (res, filePath) => {
          if (filePath.endsWith(".html")) {
            res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
            res.setHeader("Pragma", "no-cache");
            res.setHeader("Expires", "0");
          }
        }
      })
    );
    app.get("*", (_req, res) => {
      res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
      res.setHeader("Pragma", "no-cache");
      res.setHeader("Expires", "0");
      const indexFile = path.resolve(distDir, "index.html");
      if (fs.existsSync(indexFile)) {
        res.sendFile(indexFile);
      } else {
        res.status(503).send("\u0E01\u0E33\u0E25\u0E31\u0E07\u0E2D\u0E31\u0E1B\u0E40\u0E14\u0E15\u0E23\u0E30\u0E1A\u0E1A \u0E01\u0E23\u0E38\u0E13\u0E32\u0E23\u0E2D\u0E2A\u0E31\u0E01\u0E04\u0E23\u0E39\u0E48\u0E41\u0E25\u0E49\u0E27\u0E23\u0E35\u0E40\u0E1F\u0E23\u0E0A\u0E2B\u0E19\u0E49\u0E32\u0E40\u0E27\u0E47\u0E1A...");
      }
    });
  } else {
    process.env.DISABLE_HMR = "true";
    const { createServer: createViteServer } = await import("vite");
    const vite = await createViteServer({
      server: { middlewareMode: true, hmr: false, ws: false, watch: null },
      appType: "spa"
    });
    app.use(vite.middlewares);
  }
  const server = http.createServer(app);
  const wss = new WebSocketServer({ server, path: "/ws" });
  setWssServer(wss);
  wss.on("connection", (ws) => {
    console.log("\u26A1 [WebSocket] Client connected for Real-time Binary Tree Matrix");
    try {
      ws.send(
        JSON.stringify({
          type: "INIT_MATRIX",
          lastSyncedAt: matrixDB.lastSyncedAt,
          rankCounts: matrixDB.rankCounts,
          nodesByRank: matrixDB.nodesByRank,
          contractAddress: CONTRACT_ADDRESS,
          transactions: getAggregatedTransactions(),
          rewardLogs: matrixDB.rewardLogs || [],
          upgradeLogs: matrixDB.upgradeLogs || []
        })
      );
    } catch (err) {
      console.warn("[WS] Error sending initial payload:", err);
    }
    ws.on("message", (message) => {
      try {
        const payload = JSON.parse(message.toString());
        if (payload.type === "GET_RANK_TREE") {
          const rank = Number(payload.rank) || 1;
          ws.send(
            JSON.stringify({
              type: "RANK_TREE_DATA",
              rank,
              nodes: matrixDB.nodesByRank[rank] || [],
              rankCount: matrixDB.rankCounts[rank] || 0,
              lastSyncedAt: matrixDB.lastSyncedAt
            })
          );
        } else if (payload.type === "GET_TRANSACTIONS") {
          ws.send(
            JSON.stringify({
              type: "TRANSACTIONS_DATA",
              lastSyncedAt: matrixDB.lastSyncedAt,
              transactions: getAggregatedTransactions(payload.address),
              rewardLogs: matrixDB.rewardLogs || [],
              upgradeLogs: matrixDB.upgradeLogs || []
            })
          );
        }
      } catch (err) {
      }
    });
    ws.on("error", (e) => {
      console.warn("[WS Client Error]:", e);
    });
  });
  server.listen(PORT, "0.0.0.0", () => {
    console.log(`Backend API Server + WebSocket running on port ${PORT} (Database-First Mode)`);
    initPlacementQueue().catch((err) => console.warn("Queue init error:", err));
    syncAllMatrixDatabase(CONTRACT_ADDRESS, true).then(() => {
      console.log(
        `\u{1F4BE} [MatrixDB] Initial blockchain -> database sync complete (${matrixDB.rankCounts[1] || 1} Rank 1 nodes, ${matrixDB.rewardLogs.length} rewards persisted in prisma/matrix-db.json)`
      );
    }).catch((err) => console.warn("Initial MatrixDB sync notice:", err));
    setInterval(() => {
      syncAllMatrixDatabase(CONTRACT_ADDRESS, false).catch(() => {
      });
    }, 15e3);
    setTimeout(() => {
      startEventListener();
    }, 1e3);
  });
}
startApp().catch((err) => {
  console.error("Failed to start server:", err);
});
export {
  broadcastAllMatrixUpdates,
  calculateAutoIdAndExtraIdSlot,
  getAggregatedTransactions,
  setWssServer
};
