const LEGACY_CONTRACT_ADDRESSES = new Set([
  '0x104d65034b00f0db59497036208f3b798b4c10cc',
  '0x0742e9581217cc8cd6de2aa80b7ce9ad881deee3',
  '0xc5cf23d0cead13a08b3b3d968a052b956a64fbc6',
  '0xe231fe72e1c2ab2617d8972c4ea15bc9122e514f',
  '0x45405293ecce73f1a8c39ca8c0cc2248d6b9ab77',
  '0xf955de5c1a403c9d286538a872c17837aebdd8ca',
  '0xc71a9a677d9ef61a9133d6e6545306a11bd3ee5a',
  '0x87e9f1b9a63eb65c91fca718c5af1cee4eec6209',
  '0x675cdb85d9a843ac3e87b42beb7a606da79882e8',
  '0xae3736ecd23dfb6c49b76ef8548391c1b6fff7b9',
  '0x1700326ce4d4507a5dd88eb50156758581b6f4cb',
  '0x5f0a48aa94dcfa820a12cb8e6c46e9cce807d341',
  '0x48acb86c42a389efda47cc1b16938eea73f1f892',
  '0x82d18c8b7d6b1e3823fb52723435a3b4fb558622',
  '0xf09a03df9d6f89049dbdbcc5bfdb3d2be3bc5f10',
  '0x82d07e805f670943a3dbf74ab777eb89189c7c27',
  '0xa8fce96d490856338384a8a38a297b0789b0a8f1',
  '0x154f3654088bb55ddabba62850cbcfdea6046e3c',
  '0x36ff96c21e285fe557e6a3185d2b5467b46edc01',
  '0x2246c327b711c7ba82ad35af42371253aa88d6e6',
  '0xcf8d2348894ab77dcd1529c869e526924e2a7c3d',
  '0xa285257430fbff1bb63c0a74958874a5bf159ccd',
  '0x2df74a86df9d32c735755e1f448a98f4cda456c5',
  '0x01fed3686a284dcac8ef7277c4657a2aa12fab54',
  '0xda7acd2ca8967e8015e0931e652d4b7ee5e1c436',
  '0x07953ba884e2d05fa543ef7eaec426c8de6d58ff',
  '0x7fa05f1aeb74209612aea56472d9a2e2e9d44073',
  '0x003ef4aaab33e517b2f565320df37e88b2db7a69',
  '0x47ff540a2e04aa36cdc4a4ec6e4feadac1a71b66',
  '0xc115f39897811d83be2613e9722753cfb78d492b',
  '0x191d0c2cd0496e05aa4f91273125ddccc6645df4',
  '0x36932dca76922f8f05513710db89238258e61378',
  '0xee087e0318a8ac20229892bbc63d2fe43809234d',
  '0x982a87f726cc1044a546fa853a425e6f445bca3d',
  '0x3b69efbd1f615cd31e326497555bfa56a194f935',
  '0xda9da3b56b75f5d59ac0b17b402e79dd3dddffba',
  '0x05cf73c0b412e17fac31305936d25a7fa1622771',
  '0xed264d4cf339ddc19a0d28206a5f3aed0e1ebb3e',
  '0x9ed96af9128f0791fd883dee7ade2c728d2cec11',
  '0xcd581c30aceba9d6c81baaea5b9f88d8aaa54a42',
  '0xffc4336bb0bb6da36254466a7d0c5e9566ac5349',
  '0x98c2b26e052c63e3f520c75428cea3cb5e0649e0',
]);

const LEGACY_USDT_ADDRESSES = new Set([
  '0x95856be9c16afc9c147c034cd6a514d730bd90ba',
]);

const envContractAddr = import.meta.env?.VITE_CONTRACT_ADDRESS?.trim();
export const DEFAULT_CONTRACT_ADDRESS =
  envContractAddr && !LEGACY_CONTRACT_ADDRESSES.has(envContractAddr.toLowerCase())
    ? envContractAddr
    : '0x742D5249758F73683e95e5404d801f2Ac6755781';
export const DEFAULT_USDT_ADDRESS =
  import.meta.env?.VITE_USDT_ADDRESS || '0x05f24E1F598DF1aE8d0B424ce978f8ea33bB752b';

export const ADMIN_WALLET_ADDRESSES = new Set([
  '0x622e20082e63c7220c5af0a77cbc437a98505311', // Root User / Main ID #1
  '0xbefeb9871a20656aef7e8d218f751e9c23faf710', // Deployer / Contract Owner / AllSell Signer
  '0xbefedbb942a73957f6123ba0b3aa27b4a081f710', // System AllSell Wallet
]);

export function isAuthorizedAdminWallet(
  account?: string | null,
  profile?: { mainNodeIdRank1?: string | number; ownedMainIdsRank1?: (number | string)[] } | null,
  contractOwner?: string | null
): boolean {
  if (!account) return false;
  const norm = account.trim().toLowerCase();
  if (ADMIN_WALLET_ADDRESSES.has(norm)) return true;
  if (contractOwner && contractOwner.trim().toLowerCase() === norm) return true;
  if (profile) {
    if (String(profile.mainNodeIdRank1) === '1') return true;
    if (
      Array.isArray(profile.ownedMainIdsRank1) &&
      profile.ownedMainIdsRank1.some((id) => String(id) === '1')
    ) {
      return true;
    }
  }
  return false;
}

export const CONTRACT_ADDRESS = DEFAULT_CONTRACT_ADDRESS;
export const USDT_ADDRESS = DEFAULT_USDT_ADDRESS;
export const BSC_TESTNET_RPC =
  import.meta.env?.VITE_BSC_TESTNET_RPC || 'https://bsc-testnet-rpc.publicnode.com';
export const BSC_TESTNET_CHAIN_ID = Number(import.meta.env?.VITE_CHAIN_ID || 97);
export const BSC_TESTNET_CHAIN_ID_HEX = '0x61';

export function getDynamicContractAddress(): string {
  if (typeof window !== 'undefined') {
    const prevBuildDefault = localStorage.getItem('F27_BUILD_DEFAULT_CONTRACT');
    if (prevBuildDefault !== DEFAULT_CONTRACT_ADDRESS.toLowerCase()) {
      localStorage.setItem('F27_BUILD_DEFAULT_CONTRACT', DEFAULT_CONTRACT_ADDRESS.toLowerCase());
      localStorage.setItem('F27_ACTIVE_CONTRACT_ADDRESS', DEFAULT_CONTRACT_ADDRESS);
      return DEFAULT_CONTRACT_ADDRESS;
    }
    const custom = localStorage.getItem('F27_ACTIVE_CONTRACT_ADDRESS');
    if (custom && custom.startsWith('0x') && custom.length === 42) {
      if (LEGACY_CONTRACT_ADDRESSES.has(custom.toLowerCase())) {
        localStorage.setItem('F27_ACTIVE_CONTRACT_ADDRESS', DEFAULT_CONTRACT_ADDRESS);
        return DEFAULT_CONTRACT_ADDRESS;
      }
      return custom;
    }
  }
  return DEFAULT_CONTRACT_ADDRESS;
}

export function getDynamicUsdtAddress(): string {
  if (typeof window !== 'undefined') {
    const custom = localStorage.getItem('F27_ACTIVE_USDT_ADDRESS');
    if (custom && custom.startsWith('0x') && custom.length === 42) {
      if (LEGACY_USDT_ADDRESSES.has(custom.toLowerCase())) {
        localStorage.setItem('F27_ACTIVE_USDT_ADDRESS', DEFAULT_USDT_ADDRESS);
        return DEFAULT_USDT_ADDRESS;
      }
      return custom;
    }
  }
  return DEFAULT_USDT_ADDRESS;
}

export function setDynamicContractConfig(contractAddr: string, usdtAddr?: string) {
  if (typeof window !== 'undefined') {
    if (contractAddr) localStorage.setItem('F27_ACTIVE_CONTRACT_ADDRESS', contractAddr);
    if (usdtAddr) localStorage.setItem('F27_ACTIVE_USDT_ADDRESS', usdtAddr);
    window.dispatchEvent(new CustomEvent('matrix:config:updated', {
      detail: { contractAddress: contractAddr, usdtAddress: usdtAddr }
    }));
  }
}

export function clearDynamicContractConfig() {
  if (typeof window !== 'undefined') {
    localStorage.removeItem('F27_ACTIVE_CONTRACT_ADDRESS');
    localStorage.removeItem('F27_ACTIVE_USDT_ADDRESS');
    window.dispatchEvent(new CustomEvent('matrix:config:updated', {
      detail: { contractAddress: DEFAULT_CONTRACT_ADDRESS, usdtAddress: DEFAULT_USDT_ADDRESS }
    }));
  }
}

export const MAX_RANK = 27;
export const MAX_LEVELS = 15;

export const RANK_PRICES_USDT: number[] = [
  5, 10, 20, 30, 40, 50, 60, 70, 80, 90,
  100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
  2000, 3000, 4000, 5000, 6000, 8000, 10000,
];

export interface NetworkConfig {
  chainId: number;
  chainIdHex: string;
  name: string;
  currencySymbol: string;
  rpcUrl: string;
  rpcUrls: string[];
  blockExplorerUrl: string;
  defaultUsdtAddress: string;
  defaultContractAddress?: string;
}

export const SUPPORTED_NETWORKS: Record<number, NetworkConfig> = {
  97: {
    chainId: 97,
    chainIdHex: '0x61',
    name: 'BNB Smart Chain Testnet',
    currencySymbol: 'tBNB',
    rpcUrl: 'https://bsc-testnet-rpc.publicnode.com',
    rpcUrls: [
      'https://bsc-testnet-rpc.publicnode.com',
      'https://bsc-testnet.publicnode.com',
      'https://bsc-testnet.bnbchain.org',
      'https://data-seed-prebsc-1-s2.binance.org:8545/',
      'https://data-seed-prebsc-1-s1.binance.org:8545/',
    ],
    blockExplorerUrl: 'https://testnet.bscscan.com',
    defaultUsdtAddress: '0x05f24E1F598DF1aE8d0B424ce978f8ea33bB752b',
    defaultContractAddress: DEFAULT_CONTRACT_ADDRESS,
  },
  80002: {
    chainId: 80002,
    chainIdHex: '0x13882',
    name: 'Polygon Amoy Testnet',
    currencySymbol: 'POL',
    rpcUrl: 'https://rpc-amoy.polygon.technology',
    rpcUrls: ['https://rpc-amoy.polygon.technology'],
    blockExplorerUrl: 'https://amoy.polygonscan.com',
    defaultUsdtAddress: '0x41e94Eb019c0762f9Bfcf9Fb1E58725BfB0e7582',
  },
  11155111: {
    chainId: 11155111,
    chainIdHex: '0xaa36a7',
    name: 'Ethereum Sepolia Testnet',
    currencySymbol: 'SepoliaETH',
    rpcUrl: 'https://rpc.ankr.com/eth_sepolia',
    rpcUrls: ['https://rpc.ankr.com/eth_sepolia', 'https://ethereum-sepolia-rpc.publicnode.com'],
    blockExplorerUrl: 'https://sepolia.etherscan.io',
    defaultUsdtAddress: '0x7169D38820dfd117C3FA1f22a697dBA58d90BA06',
  },
  421614: {
    chainId: 421614,
    chainIdHex: '0x66eee',
    name: 'Arbitrum Sepolia',
    currencySymbol: 'ETH',
    rpcUrl: 'https://sepolia-rollup.arbitrum.io/rpc',
    rpcUrls: ['https://sepolia-rollup.arbitrum.io/rpc'],
    blockExplorerUrl: 'https://sepolia.arbiscan.io',
    defaultUsdtAddress: '0x75faf114eafb1BDbe2F03C67A90dE8F9e812543A',
  },
  84532: {
    chainId: 84532,
    chainIdHex: '0x14a34',
    name: 'Base Sepolia',
    currencySymbol: 'ETH',
    rpcUrl: 'https://sepolia.base.org',
    rpcUrls: ['https://sepolia.base.org'],
    blockExplorerUrl: 'https://sepolia.basescan.org',
    defaultUsdtAddress: '0x036CbD53842c5426634e7929541eC2318f3dCF7e',
  },
};

export const BSC_TESTNET_PARAMS = {
  chainId: BSC_TESTNET_CHAIN_ID_HEX,
  chainName: 'BNB Smart Chain Testnet',
  nativeCurrency: {
    name: 'Binance Testnet BNB',
    symbol: 'tBNB',
    decimals: 18,
  },
  rpcUrls: [
    'https://bsc-testnet-rpc.publicnode.com',
    'https://bsc-testnet.publicnode.com',
    'https://bsc-testnet.bnbchain.org',
    'https://data-seed-prebsc-1-s2.binance.org:8545/',
    'https://data-seed-prebsc-1-s1.binance.org:8545/',
  ],
  blockExplorerUrls: ['https://testnet.bscscan.com/'],
};

export const MATRIX_ABI = [
  // View / Constants
  'function usdt() external view returns (address)',
  'function MAX_RANK() external view returns (uint8)',
  'function MAX_LEVELS() external view returns (uint8)',
  'function allSellWallet() external view returns (address)',
  'function botWallet() external view returns (address)',
  'function familyPool() external view returns (address)',
  'function owner() external view returns (address)',
  'function rankPrice(uint8) external view returns (uint256)',
  'function totalNodesInRank(uint8) external view returns (uint256)',
  'function sponsorRoundToggle(uint8, uint256) external view returns (uint8)',
  'function placementRoundToggle(uint8) external view returns (uint8)',
  'function rankNodes(uint8, uint256) external view returns (uint256 id, address owner, address directSponsor, uint256 parentId, uint256 leftChild, uint256 rightChild, bool isClone, bool isExtraId)',
  'function checkValidSponsorId(uint256 _sponsorNodeId) external view returns (bool isValid, string reason, address sponsorAddress)',
  'function previewPlacement(uint8 _rank, address _sponsor) external view returns (uint256 targetParentId, bool isLeft, uint8 activeRound)',
  'function getAccumulatedVault(address _user, uint8 _targetRank) external view returns (uint256 totalVault)',
  'function getAccumulatedVaultForMainId(uint256 _r1MainId) external view returns (uint256 totalVault)',
  'function getMainIdCurrentRank(uint256 _r1MainId) external view returns (uint8)',
  'function getMainIdNodeInRank(uint256 _r1MainId, uint8 _rank) external view returns (uint256)',
  'function getMainIdThreePools(address _user, uint256 _mainNodeIdRank1) external view returns (uint8 currentRank, uint8 window5Start, uint8 window5End, uint256 upgradeRankPool, uint256 newAutoIdPool, uint256 newExtraIdPool)',
  'function getUserProfile(address _user) external view returns (bool isRegistered, address sponsor, uint256 sponsorMainNodeId, uint8 currentMaxRank, uint256 mainNodeIdRank1, uint256 latestCloneIdRank1)',
  'function getUserRankInfo(address _user, uint8 _rank) external view returns (uint256 firstNodeId, uint256 latestNodeId, uint256 upgradeVaultBalance, uint256 nextRankPrice)',
  'function nodeUpgradeVault(uint8, uint256) external view returns (uint256)',
  'function nodeOriginMainId(uint8, uint256) external view returns (uint256)',
  'function nodeToR1MainId(uint8, uint256) external view returns (uint256)',
  'function mainIdCurrentMaxRank(uint256) external view returns (uint8)',
  'function mainIdNodeInRank(uint256, uint8) external view returns (uint256)',
  'function mainNodeLatestCloneId(uint8, uint256) external view returns (uint256)',
  'function nodeDirectSponsorNodeId(uint8, uint256) external view returns (uint256)',
  'function nodeCloneRound(uint8, uint256) external view returns (uint8)',
  'function nodeCloneTotalCount(uint8, uint256) external view returns (uint256)',
  'function getNodeCloneRound(uint8, uint256) external view returns (uint8)',
  'function getNodeCloneTotalCount(uint8, uint256) external view returns (uint256)',
  'function getNodeVault(uint8 _rank, uint256 _nodeId) external view returns (uint256)',

  // State-changing Functions
  'function register(uint256 _sponsorNodeId) external',
  'function registerWithPayouts(uint256 _sponsorNodeId, uint256 _targetParentId, bool _isLeft, address[] _recipients, uint256[] _amounts, uint256 _vaultTargetNodeId, uint256 _vaultAmount) external',
  'function registerBatch(uint256 _sponsorNodeId, uint256 _count, uint256[] _targetParentIds, bool[] _isLefts, bool[] _isClones) external',
  'function registerBatchWithPayouts(uint256 _sponsorNodeId, uint256 _count, uint256[] _targetParentIds, bool[] _isLefts, bool[] _isClones, address[] _recipients, uint256[] _amounts, uint256 _vaultTargetNodeId, uint256 _vaultAmount) external',
  'function upgradeRank(uint8 _targetRank) external',
  'function upgradeRankForMainId(uint256 _r1MainId, uint8 _targetRank) external',
  'function upgradeRankWithPayouts(uint256 _r1MainId, uint8 _targetRank, uint256 _targetParentId, bool _isLeft, address[] _recipients, uint256[] _amounts, uint256 _vaultTargetNodeId, uint256 _vaultAmount) external',
  'function checkAndProcessAutoNewMainId(address _user) external',
  'function checkAndProcessAutoNewMainIdForNode(uint256 _mainNodeId) external',
  'function checkAndProcessAutoExtraId(address _user, uint8 _currentRank) external',
  'function checkAndProcessAutoExtraIdForNode(address _user, uint8 _currentRank, uint256 _sponsorNodeId) external',
  'function getPendingCloneCount() external view returns (uint256)',
  'function processCloneQueue(uint256 _maxIterations) external',
  'function processPendingExtraIdQueue(uint256 _r1MainId, uint8 _maxBatch) external returns (uint8 processed)',
  'function adminUpgradeRank(address _user, uint8 _targetRank) external',
  'function adminUpgradeRankForMainId(uint256 _r1MainId, uint8 _targetRank) external',
  'function setSystemWallets(address _allSell, address _bot, address _familyPool) external',
  'function emergencyWithdrawToken(address _token, uint256 _amount) external',
  'function emergencyWithdrawNative() external',
  'function transferOwnership(address newOwner) external',
  'function renounceOwnership() external',

  // Events
  'event UserRegistered(address indexed user, address indexed sponsor, uint256 indexed sponsorNodeId, uint256 mainNodeId)',
  'event BatchRegistered(address indexed user, uint256 count, uint256 totalCost)',
  'event NodeCreated(uint8 indexed rank, uint256 indexed nodeId, address indexed owner, address directSponsor, uint256 parentId, bool isLeft, bool isClone, bool isExtraId)',
  'event RewardDistributed(uint256 indexed fromNodeId, uint256 indexed toNodeId, address indexed recipient, uint8 rank, string rewardType, uint256 amount)',
  'event RankUpgraded(address indexed user, uint8 fromRank, uint8 toRank, uint256 newNodeId, bool isAutoUpgrade)',
  'event AutoNewMainIdCreated(address indexed user, uint256 count, uint256 vaultDiff)',
  'event AutoExtraIdCreated(address indexed user, uint8 indexed rank, uint256 nodeId)',
];

export const CONTRACT_ABI = MATRIX_ABI;

export const USDT_ABI = [
  'function approve(address spender, uint256 amount) external returns (bool)',
  'function allowance(address owner, address spender) external view returns (uint256)',
  'function balanceOf(address account) external view returns (uint256)',
  'function decimals() external view returns (uint8)',
  'function symbol() external view returns (string)',
];

export interface RankDefinition {
  rank: number;
  nameEn: string;
  nameTh: string;
  priceUSDT: number;
  directBonusPercent: number; // 30%
  levelBonusPercent: number; // 30% (2% x 15 levels)
  vaultPercent: number; // 35%
  allSellPercent: number; // 4%
  botPercent: number; // 1%
  tierGroup: 'Starter (1-10)' | 'Growth (11-20)' | 'Whale (21-27)';
}

export const RANK_DEFINITIONS: RankDefinition[] = RANK_PRICES_USDT.map((price, idx) => {
  const rank = idx + 1;
  const tierGroup = rank <= 10 ? 'Starter (1-10)' : rank <= 20 ? 'Growth (11-20)' : 'Whale (21-27)';
  return {
    rank,
    nameEn: `Rank ${rank} Matrix`,
    nameTh: `แรงค์ ${rank} (${price.toLocaleString()} USDT)`,
    priceUSDT: price,
    directBonusPercent: 30,
    levelBonusPercent: 30,
    vaultPercent: 35,
    allSellPercent: 4,
    botPercent: 1,
    tierGroup,
  };
});
