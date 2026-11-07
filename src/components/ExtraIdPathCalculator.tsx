import React, { useMemo, useState } from 'react';
import { RANK_PRICES_USDT } from '../contracts/config';
import { Calculator, Sparkles, ArrowRight, Layers, CheckCircle2, RotateCcw, Zap, Bot } from 'lucide-react';

export interface SimRawNode {
  id: number;
  rank: number;
  owner: string;
  parentId: number;
  leftChild: number;
  rightChild: number;
  isClone: boolean;
  isExtraId: boolean;
  directSponsor: string;
  originMainId: number;
  directSponsorNodeId: number;
  cloneRound?: number;
  totalClonesSpawned?: number;
  nodeVault: number;
  originType?: string;
}

export interface ExtraIdGenerationEvent {
  stepRank: number;
  ownerMainId: number;
  spawnedInRank: number;
  rankPriceUsdt: number;
  newNodeId: number;
  parentId: number;
  side: 'LEFT' | 'RIGHT';
  poolBeforeUsdt: number;
  deductedUsdt: number;
  poolAfterUsdt: number;
  triggeredByNote: string;
}

export interface ChainReactionEvent {
  type: 'CLONE' | 'AUTO_ID' | 'VAULT_GAIN' | 'EXTRA_LEFT_BONUS';
  rank: number;
  nodeId: number;
  ownerMainId: number;
  parentId: number;
  side: 'LEFT' | 'RIGHT';
  descriptionTh: string;
}

export type GeneratedItemCategory = 'EXTRA_ID' | 'AUTO_ID' | 'CLONE';

export interface UnifiedGeneratedItem {
  id: string;
  category: GeneratedItemCategory;
  categoryLabel: string;
  stepRank: number;
  ownerMainId: number;
  spawnedInRank: number;
  newNodeId: number;
  nodeCode: string;
  parentId: number;
  side: 'LEFT' | 'RIGHT';
  rankPriceUsdt: number;
  costOrDeduction: string;
  descriptionTh: string;
}

export interface UpgradePathStepResult {
  stepNumber: number;
  upgradeRank: number;
  rankPriceUsdt: number;
  upgradingMainId: number;
  newNodeIdInRank: number;
  placedUnderParentId: number;
  placedUnderParentMainId: number;
  placedSide: 'LEFT' | 'RIGHT';
  vaultAddedToParentUsdt: number;
  recipientMainId: number;
  window5RangeTarget: [number, number];
  window5RangeSponsor: [number, number];
  targetPaidFromVaultUsdt: number;
  targetPaidFromWalletUsdt: number;
  targetUpgradePoolBeforeUsdt: number;
  sponsorUpgradePoolFinalUsdt: number;
  sponsorExtraPoolBeforeUsdt: number;
  sponsorExtraPoolAfterGainUsdt: number;
  sponsorExtraPoolFinalUsdt: number;
  sponsorAutoPoolFinalUsdt: number;
  targetUpgradePoolFinalUsdt: number;
  targetExtraPoolBeforeUsdt: number;
  targetExtraPoolAfterGainUsdt: number;
  targetExtraPoolFinalUsdt: number;
  targetAutoPoolFinalUsdt: number;
  extraIdsGenerated: ExtraIdGenerationEvent[];
  chainReactions: ChainReactionEvent[];
}

export interface UpgradePathCalculationResult {
  targetMainId: number;
  sponsorMainId: number;
  currentOnChainRank: number;
  startSimulatedRank: number;
  prospectiveRank: number;
  steps: UpgradePathStepResult[];
  allExtraIdEvents: ExtraIdGenerationEvent[];
  sponsorExtraIdEvents: ExtraIdGenerationEvent[];
  targetExtraIdEvents: ExtraIdGenerationEvent[];
  allAutoIdEvents: ChainReactionEvent[];
  sponsorAutoIdEvents: ChainReactionEvent[];
  targetAutoIdEvents: ChainReactionEvent[];
  targetInitialUpgradePoolUsdt: number;
  sponsorInitialUpgradePoolUsdt: number;
  targetTotalRankCostUsdt: number;
  targetPaidFromVaultUsdt: number;
  targetPaidFromWalletUsdt: number;
  targetProspectiveRankPriceUsdt: number;
  finalSponsorUpgradePoolUsdt: number;
  finalTargetUpgradePoolUsdt: number;
  targetFinalRank: number;
  sponsorFinalRank: number;
  targetWindow5Range: [number, number];
  sponsorWindow5Range: [number, number];
  targetNextRankPriceUsdt: number;
  sponsorNextRankPriceUsdt: number;
  finalSponsorAutoPoolUsdt: number;
  finalSponsorExtraPoolUsdt: number;
  finalTargetAutoPoolUsdt: number;
  finalTargetExtraPoolUsdt: number;
}

function getFiveRankWindow(currentRank: number): [number, number] {
  if (currentRank <= 0) return [0, 0];
  const start = currentRank > 4 ? currentRank - 4 : 1;
  return [start, currentRank];
}

/**
 * Pure calculator function that takes a target Main ID and a prospective rank,
 * simulates the exact FractalFlowMatrix27 binary placement, 5-rank sliding window,
 * 35% left-branch vault accumulation, 100% right-branch rebirth clone cascade,
 * and R27 -> R2 Extra ID generation loop, returning the complete path and events.
 */
export function calculateMatrixUpgradePath(
  multiRankSnapshots: Map<number, Map<number, SimRawNode>>,
  targetMainIdInput: number,
  prospectiveRankInput: number,
  perIdReportsMap?: Map<number, any>
): UpgradePathCalculationResult {
  const targetMainId = Math.max(1, Math.floor(targetMainIdInput || 2));
  const prospectiveRank = Math.min(27, Math.max(2, Math.floor(prospectiveRankInput || 10)));

  // Deep-clone all 27 rank maps so simulation never mutates live UI state
  const simRanks = new Map<number, Map<number, SimRawNode>>();
  for (let r = 1; r <= 27; r++) {
    const srcMap = multiRankSnapshots.get(r);
    const clonedMap = new Map<number, SimRawNode>();
    if (srcMap) {
      for (const [k, v] of srcMap.entries()) {
        clonedMap.set(k, { ...v });
      }
    }
    if (clonedMap.size === 0) {
      clonedMap.set(1, {
        id: 1,
        rank: r,
        owner: '0x622E20082e63c7220c5AF0A77Cbc437a98505311',
        parentId: 0,
        leftChild: 0,
        rightChild: 0,
        isClone: false,
        isExtraId: false,
        directSponsor: '0x0000000000000000000000000000000000000000',
        originMainId: 1,
        directSponsorNodeId: 1,
        cloneRound: 0,
        totalClonesSpawned: 0,
        nodeVault: 0,
        originType: 'MANUAL_REGISTER',
      });
    }
    simRanks.set(r, clonedMap);
  }

  const r1Map = simRanks.get(1)!;
  if (!r1Map.has(targetMainId)) {
    // If the user enters a Main ID not yet registered on-chain, create a virtual Main ID sponsored by #1
    const root1 = r1Map.get(1)!;
    r1Map.set(targetMainId, {
      id: targetMainId,
      rank: 1,
      owner: `0xSimMain${targetMainId}`,
      parentId: 1,
      leftChild: 0,
      rightChild: 0,
      isClone: false,
      isExtraId: false,
      directSponsor: root1.owner,
      originMainId: targetMainId,
      directSponsorNodeId: 1,
      cloneRound: 0,
      totalClonesSpawned: 0,
      nodeVault: 0,
      originType: 'MANUAL_REGISTER',
    });
  }

  const getCorrespondingNodeInRank = (r1Main: number, rank: number): number => {
    if (rank === 1) return r1Main;
    const snapR = simRanks.get(rank);
    if (!snapR) return 0;
    for (const n of snapR.values()) {
      if (!n.isClone && !n.isExtraId && n.originMainId === r1Main) {
        return n.id;
      }
    }
    const r1Node = r1Map.get(r1Main);
    if (!r1Node) return 0;
    const ownerLow = r1Node.owner.toLowerCase();
    const r1MainsOfOwner = Array.from(r1Map.values())
      .filter((x) => !x.isClone && !x.isExtraId && x.owner.toLowerCase() === ownerLow)
      .sort((a, b) => a.id - b.id);
    const idx = r1MainsOfOwner.findIndex((x) => x.id === r1Main);
    if (idx < 0) return 0;
    const rMainsOfOwner = Array.from(snapR.values())
      .filter((x) => !x.isClone && !x.isExtraId && x.owner.toLowerCase() === ownerLow)
      .sort((a, b) => a.id - b.id);
    return idx < rMainsOfOwner.length ? rMainsOfOwner[idx].id : 0;
  };

  const resolveR1MainForNode = (rank: number, nodeId: number): number => {
    const snapR = simRanks.get(rank);
    const n = snapR?.get(nodeId);
    if (!n) return 1;
    if (rank === 1 && !n.isClone && !n.isExtraId) return n.id;
    if (n.isExtraId) {
      return n.directSponsorNodeId > 0 ? n.directSponsorNodeId : 1;
    }
    const orig = n.originMainId > 0 ? n.originMainId : n.id;
    if (rank === 1) return orig;
    if (r1Map.has(orig) && !r1Map.get(orig)!.isClone) {
      const candOwner = r1Map.get(orig)!.owner.toLowerCase();
      if (candOwner === n.owner.toLowerCase()) return orig;
    }
    const ownerLow = n.owner.toLowerCase();
    const rMains = Array.from(snapR!.values())
      .filter((x) => !x.isClone && !x.isExtraId && x.owner.toLowerCase() === ownerLow)
      .sort((a, b) => a.id - b.id);
    const idx = rMains.findIndex((x) => x.id === orig || x.id === n.id);
    const r1Mains = Array.from(r1Map.values())
      .filter((x) => !x.isClone && !x.isExtraId && x.owner.toLowerCase() === ownerLow)
      .sort((a, b) => a.id - b.id);
    if (idx >= 0 && idx < r1Mains.length) return r1Mains[idx].id;
    return r1Mains[0]?.id || 1;
  };

  const getMainCurrentRank = (r1Main: number): number => {
    if (r1Main === 1) return 27;
    let maxR = 1;
    for (let r = 2; r <= 27; r++) {
      if (getCorrespondingNodeInRank(r1Main, r) > 0) {
        maxR = r;
      }
    }
    return maxR;
  };

  // Track Extra ID left-child 35% bonus per R1 Main ID (`mainIdExtraPoolBonus`)
  const extraLeftBonusByMain = new Map<number, number>();
  for (let r = 27; r >= 1; r--) {
    const snapR = simRanks.get(r)!;
    const price = RANK_PRICES_USDT[r - 1] || 10;
    for (const sn of snapR.values()) {
      const originNode = !sn.isClone
        ? sn
        : Array.from(snapR.values()).find(
            (x) => !x.isClone && x.owner.toLowerCase() === sn.owner.toLowerCase() && x.id === sn.originMainId
          ) || sn;
      const isExtraLineage = !sn.isClone ? sn.isExtraId : sn.isExtraId || originNode.isExtraId;
      if (isExtraLineage && sn.leftChild > 0) {
        const spMain = originNode.directSponsorNodeId > 0 ? originNode.directSponsorNodeId : 1;
        extraLeftBonusByMain.set(
          spMain,
          Number(((extraLeftBonusByMain.get(spMain) || 0) + price * 0.35).toFixed(4))
        );
      }
    }
  }
  // Subtract already-deducted Extra ID left-child bonus on-chain
  // กฎเหล็ก Smart Contract: ต้นทุน Extra ID (totalExtraCost) จะถูกหักออกจากยอดสะสม R4..27 ก่อนเสมอ (deductedFromR4To27)
  // ส่วนที่เกินจาก R4..27 เท่านั้นจึงจะนำมาหักออกจาก grossBonus (เม็ดซ้าย Extra ID)
  for (const r1Node of r1Map.values()) {
    if (r1Node.isClone || r1Node.isExtraId) continue;
    const mId = r1Node.id;

    // หากมีรายงานสถิติจาก Backend (perIdReportsMap) ให้ใช้ค่า authentic ที่ตรวจสอบตรงกับเชน/DB ทันที
    const rep = perIdReportsMap?.get(mId);
    if (rep && typeof rep.extraLeftVaultBonus === 'number') {
      extraLeftBonusByMain.set(mId, Number(rep.extraLeftVaultBonus.toFixed(4)));
      continue;
    }

    const grossBonus = extraLeftBonusByMain.get(mId) || 0;
    let totalExtraCost = 0;
    for (let r = 2; r <= 27; r++) {
      const snapR = simRanks.get(r)!;
      const price = RANK_PRICES_USDT[r - 1] || 10;
      for (const sn of snapR.values()) {
        if (sn.isExtraId && !sn.isClone) {
          const spMain = sn.directSponsorNodeId > 0 ? sn.directSponsorNodeId : 1;
          if (spMain === mId) totalExtraCost += price;
        }
      }
    }

    const curRank = getMainCurrentRank(mId);
    const [w5Start] = getFiveRankWindow(curRank);
    let grossR4To27 = 0;
    let remR4To27 = 0;
    for (let r = 4; r <= 27; r++) {
      if (curRank === 27 || r < w5Start) {
        const snapR = simRanks.get(r)!;
        const nId = getCorrespondingNodeInRank(mId, r);
        if (nId > 0) {
          const n = snapR.get(nId);
          remR4To27 = Number((remR4To27 + (n?.nodeVault || 0)).toFixed(4));
          const rUnitVault = Number(((RANK_PRICES_USDT[r - 1] || 30) * 0.35).toFixed(4));
          for (const sn of snapR.values()) {
            const isSameMain =
              sn.id === nId ||
              (sn.isClone && (sn.originMainId === nId || sn.originMainId === mId));
            if (isSameMain && sn.leftChild > 0) {
              grossR4To27 = Number((grossR4To27 + rUnitVault).toFixed(4));
            }
          }
        }
      }
    }

    const deductedFromR4To27 = Math.max(0, Number((grossR4To27 - remR4To27).toFixed(4)));
    const remainingCostForBonus = Math.max(0, Number((totalExtraCost - deductedFromR4To27).toFixed(4)));
    const deductedFromBonus = Math.min(grossBonus, remainingCostForBonus);
    extraLeftBonusByMain.set(mId, Math.max(0, Number((grossBonus - deductedFromBonus).toFixed(4))));
  }

  const calcAutoIdPool = (r1Main: number): number => {
    const curRank = getMainCurrentRank(r1Main);
    const [w5Start] = getFiveRankWindow(curRank);
    let total = 0;
    for (let r = 1; r <= 3; r++) {
      if (r < w5Start) {
        const nId = getCorrespondingNodeInRank(r1Main, r);
        if (nId > 0) {
          const n = simRanks.get(r)?.get(nId);
          total = Number((total + (n?.nodeVault || 0)).toFixed(4));
        }
      }
    }
    return total;
  };

  const calcUpgradeRankPool = (r1Main: number): number => {
    const curRank = getMainCurrentRank(r1Main);
    if (curRank >= 27) return 0;
    const [w5Start, w5End] = getFiveRankWindow(curRank);
    let total = 0;
    for (let r = w5Start; r <= w5End; r++) {
      const nId = getCorrespondingNodeInRank(r1Main, r);
      if (nId > 0) {
        const n = simRanks.get(r)?.get(nId);
        total = Number((total + (n?.nodeVault || 0)).toFixed(4));
      }
    }
    return total;
  };

  const calcExtraIdPool = (r1Main: number): number => {
    const curRank = getMainCurrentRank(r1Main);
    const [w5Start] = getFiveRankWindow(curRank);
    let total = extraLeftBonusByMain.get(r1Main) || 0;
    for (let r = 4; r <= 27; r++) {
      if (curRank === 27 || r < w5Start) {
        const nId = getCorrespondingNodeInRank(r1Main, r);
        if (nId > 0) {
          const n = simRanks.get(r)?.get(nId);
          total = Number((total + (n?.nodeVault || 0)).toFixed(4));
        }
      }
    }
    return total;
  };

  const deductVaultRange = (r1Main: number, startRank: number, endRank: number, amount: number) => {
    let rem = amount;
    for (let r = startRank; r <= endRank && rem > 1e-6; r++) {
      const nId = getCorrespondingNodeInRank(r1Main, r);
      if (nId <= 0) continue;
      const n = simRanks.get(r)?.get(nId);
      if (!n || n.nodeVault <= 0) continue;
      const deduct = Math.min(n.nodeVault, rem);
      n.nodeVault = Math.max(0, Number((n.nodeVault - deduct).toFixed(4)));
      rem = Number((rem - deduct).toFixed(4));
    }
  };

  const deductExtraPool = (r1Main: number, price: number) => {
    let rem = price;
    const bonus = extraLeftBonusByMain.get(r1Main) || 0;
    if (bonus > 0) {
      const bDeduct = Math.min(bonus, rem);
      extraLeftBonusByMain.set(r1Main, Math.max(0, Number((bonus - bDeduct).toFixed(4))));
      rem = Number((rem - bDeduct).toFixed(4));
    }
    if (rem > 1e-6) {
      const curRank = getMainCurrentRank(r1Main);
      const [w5Start] = getFiveRankWindow(curRank);
      const endR = curRank === 27 ? 27 : w5Start > 4 ? w5Start - 1 : 4;
      deductVaultRange(r1Main, 4, endR, rem);
    }
  };

  const findFirstAvailableUnder = (rank: number, startNodeId: number): { parentId: number; isLeft: boolean } => {
    const snapR = simRanks.get(rank)!;
    const rootId = startNodeId > 0 && snapR.has(startNodeId) ? startNodeId : 1;
    const queue: number[] = [rootId];
    const visited = new Set<number>();
    while (queue.length > 0) {
      const curId = queue.shift()!;
      if (visited.has(curId)) continue;
      visited.add(curId);
      const node = snapR.get(curId);
      if (!node) continue;
      if (node.leftChild === 0) return { parentId: curId, isLeft: true };
      if (node.rightChild === 0) return { parentId: curId, isLeft: false };
      queue.push(node.leftChild, node.rightChild);
    }
    return { parentId: rootId, isLeft: true };
  };

  const findPlacementSpot = (
    rank: number,
    sponsorNodeId: number,
    isExtra: boolean
  ): { parentId: number; isLeft: boolean } => {
    const snapR = simRanks.get(rank)!;
    let primaryNodeId = sponsorNodeId > 0 && snapR.has(sponsorNodeId) ? sponsorNodeId : 1;
    const primaryNode = snapR.get(primaryNodeId)!;
    const originTag = primaryNode.originMainId > 0 ? primaryNode.originMainId : primaryNodeId;
    if (!isExtra && rank === 1 && snapR.has(originTag)) {
      primaryNodeId = originTag;
    }
    const pNode = snapR.get(primaryNodeId)!;
    if (pNode.leftChild === 0) return { parentId: primaryNodeId, isLeft: true };
    if (pNode.rightChild === 0) return { parentId: primaryNodeId, isLeft: false };

    const sortedIds = Array.from(snapR.keys()).sort((a, b) => a - b);
    for (const id of sortedIds) {
      const c = snapR.get(id)!;
      if (c.isClone && (c.originMainId === originTag || c.originMainId === primaryNodeId)) {
        if (c.leftChild === 0) return { parentId: id, isLeft: true };
        if (c.rightChild === 0) return { parentId: id, isLeft: false };
      }
    }
    return findFirstAvailableUnder(rank, primaryNodeId);
  };

  const insertSimNode = (
    rank: number,
    owner: string,
    directSponsor: string,
    parentId: number,
    isLeft: boolean,
    isClone: boolean,
    isExtraId: boolean,
    originMainId: number,
    directSponsorNodeId: number,
    originType: string
  ): SimRawNode => {
    const snapR = simRanks.get(rank)!;
    const maxId = snapR.size > 0 ? Math.max(...Array.from(snapR.keys())) : 0;
    const newId = maxId + 1;
    const parentNode = snapR.get(parentId)!;
    if (isLeft) parentNode.leftChild = newId;
    else parentNode.rightChild = newId;

    const newNode: SimRawNode = {
      id: newId,
      rank,
      owner,
      parentId,
      leftChild: 0,
      rightChild: 0,
      isClone,
      isExtraId,
      directSponsor,
      originMainId: originMainId > 0 ? originMainId : newId,
      directSponsorNodeId,
      nodeVault: 0,
      originType,
    };
    snapR.set(newId, newNode);
    return newNode;
  };

  const targetR1Node = r1Map.get(targetMainId)!;
  const sponsorMainId =
    targetR1Node.directSponsorNodeId > 0 && targetR1Node.directSponsorNodeId !== targetMainId
      ? targetR1Node.directSponsorNodeId
      : 1;

  const currentOnChainRank = getMainCurrentRank(targetMainId);
  const startSimulatedRank = currentOnChainRank < prospectiveRank ? currentOnChainRank + 1 : prospectiveRank + 1;

  const steps: UpgradePathStepResult[] = [];
  const allExtraIdEvents: ExtraIdGenerationEvent[] = [];
  const allAutoIdEvents: ChainReactionEvent[] = [];

  // Recursive helper for processing left-child rewards, right-child clones, Auto IDs, and Extra IDs
  const processNodePlacementCascade = (
    stepUpgradeRank: number,
    rank: number,
    newNode: SimRawNode,
    parentId: number,
    isLeft: boolean,
    stepExtraEvents: ExtraIdGenerationEvent[],
    stepChainEvents: ChainReactionEvent[],
    depth = 0
  ) => {
    if (depth > 25) return;
    const snapR = simRanks.get(rank)!;
    const parentNode = snapR.get(parentId);
    if (!parentNode) return;
    const price = RANK_PRICES_USDT[rank - 1] || 5;

    if (isLeft) {
      const parentOriginNode = !parentNode.isClone
        ? parentNode
        : Array.from(snapR.values()).find(
            (x) =>
              !x.isClone &&
              x.owner.toLowerCase() === parentNode.owner.toLowerCase() &&
              x.id === parentNode.originMainId
          ) || parentNode;
      const isParentExtraLineage = parentNode.isExtraId || parentOriginNode.isExtraId;

      if (isParentExtraLineage) {
        const spMain = parentOriginNode.directSponsorNodeId > 0 ? parentOriginNode.directSponsorNodeId : 1;
        const vaultBonus = Number((price * 0.35).toFixed(4));
        extraLeftBonusByMain.set(
          spMain,
          Number(((extraLeftBonusByMain.get(spMain) || 0) + vaultBonus).toFixed(4))
        );
        stepChainEvents.push({
          type: 'EXTRA_LEFT_BONUS',
          rank,
          nodeId: newNode.id,
          ownerMainId: spMain,
          parentId,
          side: 'LEFT',
          descriptionTh: `เม็ดซ้ายของ Extra ID (R${rank}-#${parentId}) ส่ง Vault +${vaultBonus.toFixed(2)} U เข้ากระปุก Extra ID ของ Main #${spMain}`,
        });
        checkAndSpawnExtraIds(stepUpgradeRank, spMain, stepExtraEvents, stepChainEvents, depth + 1);
        return;
      }

      const parentR1Main = resolveR1MainForNode(rank, parentId);
      const vaultTargetNodeId = rank === 1 ? parentR1Main : getCorrespondingNodeInRank(parentR1Main, rank);
      const targetVaultNode = snapR.get(vaultTargetNodeId || parentNode.originMainId || parentId);
      const vaultAdded = Number((price * 0.35).toFixed(4));
      if (targetVaultNode) {
        targetVaultNode.nodeVault = Number((targetVaultNode.nodeVault + vaultAdded).toFixed(4));
      }

      if (depth > 0) {
        stepChainEvents.push({
          type: 'VAULT_GAIN',
          rank,
          nodeId: newNode.id,
          ownerMainId: parentR1Main,
          parentId,
          side: 'LEFT',
          descriptionTh: `R${rank}-#${newNode.id} ตกซ้ายของ R${rank}-#${parentId} → Main #${parentR1Main} ได้รับ Vault +${vaultAdded.toFixed(2)} U ใน R${rank}`,
        });
      }

      checkAndSpawnAutoMainIds(stepUpgradeRank, parentR1Main, stepExtraEvents, stepChainEvents, depth + 1);
      if (rank >= 4) {
        checkAndSpawnExtraIds(stepUpgradeRank, parentR1Main, stepExtraEvents, stepChainEvents, depth + 1);
      }
    } else {
      // Right child triggers 100% Rebirth Clone of parent's origin node
      const originR1Main = resolveR1MainForNode(rank, parentNode.id);
      const localCorrespId =
        !parentNode.isClone
          ? parentNode.id
          : getCorrespondingNodeInRank(originR1Main, rank) || parentNode.originMainId || parentNode.id;
      const originNode = snapR.get(localCorrespId) || parentNode;

      const curClonesCount = originNode.totalClonesSpawned || 0;
      const roundIdx = curClonesCount % 2; // 0 => Round 1 (Sponsor), 1 => Round 2 (Global BFS under Root #1)
      originNode.totalClonesSpawned = curClonesCount + 1;

      let sponsorTargetInRank = 1;
      let lockedR1SponsorId = originNode.directSponsorNodeId || 1;
      let lockedR1SponsorAddr = originNode.directSponsor;
      if (originNode.isExtraId) {
        const spMain = originNode.directSponsorNodeId > 0 ? originNode.directSponsorNodeId : 1;
        lockedR1SponsorId = spMain;
        sponsorTargetInRank = getCorrespondingNodeInRank(spMain, rank) || 1;
      } else {
        const r1OrigNode = r1Map.get(originR1Main);
        const r1SpId = r1OrigNode?.directSponsorNodeId || 1;
        lockedR1SponsorId = originR1Main === 1 ? 0 : r1SpId;
        if (r1OrigNode?.directSponsor) {
          lockedR1SponsorAddr = r1OrigNode.directSponsor;
        }
        sponsorTargetInRank = getCorrespondingNodeInRank(r1SpId, rank) || 1;
      }

      const cloneSpot =
        roundIdx === 0
          ? findPlacementSpot(rank, sponsorTargetInRank, false)
          : findFirstAvailableUnder(rank, 1);

      const cloneNode = insertSimNode(
        rank,
        originNode.owner,
        lockedR1SponsorAddr,
        cloneSpot.parentId,
        cloneSpot.isLeft,
        true,
        false,
        rank === 1 || originNode.isExtraId ? originNode.id : originR1Main,
        lockedR1SponsorId,
        'CLONE'
      );

      stepChainEvents.push({
        type: 'CLONE',
        rank,
        nodeId: cloneNode.id,
        ownerMainId: originR1Main,
        parentId: cloneSpot.parentId,
        side: cloneSpot.isLeft ? 'LEFT' : 'RIGHT',
        descriptionTh: `R${rank}-#${parentId} ครบซ้าย-ขวา → เด้งโคลน R${rank}-#${cloneNode.id} (ของ Main #${originR1Main} • รอบ ${roundIdx + 1}) ไปวางใต้ R${rank}-#${cloneSpot.parentId} (${cloneSpot.isLeft ? 'ซ้าย' : 'ขวา'})`,
      });

      processNodePlacementCascade(
        stepUpgradeRank,
        rank,
        cloneNode,
        cloneSpot.parentId,
        cloneSpot.isLeft,
        stepExtraEvents,
        stepChainEvents,
        depth + 1
      );
    }
  };

  const checkAndSpawnAutoMainIds = (
    stepUpgradeRank: number,
    r1Main: number,
    stepExtraEvents: ExtraIdGenerationEvent[],
    stepChainEvents: ChainReactionEvent[],
    depth: number
  ) => {
    if (depth > 25) return;
    const curRank = getMainCurrentRank(r1Main);
    const [w5Start] = getFiveRankWindow(curRank);
    if (w5Start <= 1) return;

    const autoPool = calcAutoIdPool(r1Main);
    if (autoPool + 1e-6 < 5) return;

    const count = Math.floor((autoPool + 1e-6) / 5);
    const endR = w5Start > 3 ? 3 : w5Start - 1;
    deductVaultRange(r1Main, 1, endR, count * 5);

    const mNode = r1Map.get(r1Main)!;
    for (let i = 0; i < count; i++) {
      const spot = findPlacementSpot(1, r1Main, false);
      const autoNode = insertSimNode(
        1,
        mNode.owner,
        mNode.directSponsor,
        spot.parentId,
        spot.isLeft,
        false,
        false,
        0,
        r1Main,
        'AUTO_NEW_MAIN'
      );
      const autoEv: ChainReactionEvent = {
        type: 'AUTO_ID',
        rank: 1,
        nodeId: autoNode.id,
        ownerMainId: r1Main,
        parentId: spot.parentId,
        side: spot.isLeft ? 'LEFT' : 'RIGHT',
        descriptionTh: `กระปุก Auto ID ของ Main #${r1Main} ครบ 5.00 U → แตก Auto ID R1-#${autoNode.id} วางใต้ R1-#${spot.parentId} (${spot.isLeft ? 'ซ้าย' : 'ขวา'})`,
      };
      stepChainEvents.push(autoEv);
      allAutoIdEvents.push(autoEv);
      processNodePlacementCascade(
        stepUpgradeRank,
        1,
        autoNode,
        spot.parentId,
        spot.isLeft,
        stepExtraEvents,
        stepChainEvents,
        depth + 1
      );
    }
  };

  const checkAndSpawnExtraIds = (
    stepUpgradeRank: number,
    r1Main: number,
    stepExtraEvents: ExtraIdGenerationEvent[],
    stepChainEvents: ChainReactionEvent[],
    depth: number
  ) => {
    if (depth > 25) return;
    const mNode = r1Map.get(r1Main);
    if (!mNode) return;

    for (let pass = 0; pass < 10; pass++) {
      let spawned = false;
      for (let targetRank = 27; targetRank >= 2; targetRank--) {
        const poolBefore = calcExtraIdPool(r1Main);
        const price = RANK_PRICES_USDT[targetRank - 1] || 10;
        if (poolBefore + 1e-6 < price) continue;
        const sponsorNodeInTarget = getCorrespondingNodeInRank(r1Main, targetRank);
        if (sponsorNodeInTarget <= 0) continue;

        deductExtraPool(r1Main, price);
        const poolAfter = calcExtraIdPool(r1Main);

        const spot = findPlacementSpot(targetRank, sponsorNodeInTarget, true);
        const extraNode = insertSimNode(
          targetRank,
          mNode.owner,
          mNode.owner,
          spot.parentId,
          spot.isLeft,
          false,
          true,
          0,
          r1Main,
          'AUTO_EXTRA'
        );

        const ev: ExtraIdGenerationEvent = {
          stepRank: stepUpgradeRank,
          ownerMainId: r1Main,
          spawnedInRank: targetRank,
          rankPriceUsdt: price,
          newNodeId: extraNode.id,
          parentId: spot.parentId,
          side: spot.isLeft ? 'LEFT' : 'RIGHT',
          poolBeforeUsdt: poolBefore,
          deductedUsdt: price,
          poolAfterUsdt: poolAfter,
          triggeredByNote: `ลูป R27→R2 พบยอดสะสม ${poolBefore.toFixed(2)} U >= ${price.toFixed(2)} U (R${targetRank})`,
        };
        stepExtraEvents.push(ev);
        allExtraIdEvents.push(ev);
        spawned = true;

        processNodePlacementCascade(
          stepUpgradeRank,
          targetRank,
          extraNode,
          spot.parentId,
          spot.isLeft,
          stepExtraEvents,
          stepChainEvents,
          depth + 1
        );
      }
      if (!spawned) break;
    }
  };

  const initialTargetUpgradePool = calcUpgradeRankPool(targetMainId);
  const initialSponsorUpgradePool = calcUpgradeRankPool(sponsorMainId);
  let totalTargetCost = 0;
  let totalTargetPaidFromVault = 0;
  let totalTargetPaidFromWallet = 0;

  for (let r = startSimulatedRank; r <= prospectiveRank; r++) {
    const rankPrice = RANK_PRICES_USDT[r - 1] || 5;
    totalTargetCost = Number((totalTargetCost + rankPrice).toFixed(4));

    // Smart Contract rule: Deduct available vault in 5-rank window BEFORE upgrade towards rankPrice
    const targetUpgradePoolBefore = calcUpgradeRankPool(targetMainId);
    const paidFromVault = Number(Math.min(targetUpgradePoolBefore, rankPrice).toFixed(4));
    const paidFromWallet = Number(Math.max(0, rankPrice - paidFromVault).toFixed(4));
    if (targetUpgradePoolBefore > 0) {
      const [wStart, wEnd] = getFiveRankWindow(getMainCurrentRank(targetMainId));
      deductVaultRange(targetMainId, wStart, wEnd, targetUpgradePoolBefore);
    }
    totalTargetPaidFromVault = Number((totalTargetPaidFromVault + paidFromVault).toFixed(4));
    totalTargetPaidFromWallet = Number((totalTargetPaidFromWallet + paidFromWallet).toFixed(4));

    const sponsorExtraBefore = calcExtraIdPool(sponsorMainId);
    const targetExtraBefore = calcExtraIdPool(targetMainId);

    const sponsorNodeInRank = getCorrespondingNodeInRank(sponsorMainId, r) || 1;
    const spot = findPlacementSpot(r, sponsorNodeInRank, false);
    const parentNodeBefore = simRanks.get(r)!.get(spot.parentId)!;
    const parentR1Main = resolveR1MainForNode(r, spot.parentId);

    const newUpgradedNode = insertSimNode(
      r,
      targetR1Node.owner,
      targetR1Node.directSponsor,
      spot.parentId,
      spot.isLeft,
      false,
      false,
      targetMainId,
      sponsorMainId,
      'MANUAL_REGISTER'
    );

    const vaultAddedToParent = spot.isLeft ? Number((rankPrice * 0.35).toFixed(4)) : 0;
    const sponsorExtraAfterGain =
      spot.isLeft && parentR1Main === sponsorMainId && r >= 4
        ? Number((sponsorExtraBefore + vaultAddedToParent).toFixed(4))
        : sponsorExtraBefore;
    const targetExtraAfterGain =
      spot.isLeft && parentR1Main === targetMainId && r >= 4
        ? Number((targetExtraBefore + vaultAddedToParent).toFixed(4))
        : targetExtraBefore;

    const stepExtraEvents: ExtraIdGenerationEvent[] = [];
    const stepChainEvents: ChainReactionEvent[] = [];

    processNodePlacementCascade(
      r,
      r,
      newUpgradedNode,
      spot.parentId,
      spot.isLeft,
      stepExtraEvents,
      stepChainEvents,
      0
    );

    // Also check if targetMainId's own 5-rank window shift unlocked Auto ID or Extra ID for targetMainId!
    checkAndSpawnAutoMainIds(r, targetMainId, stepExtraEvents, stepChainEvents, 1);
    checkAndSpawnExtraIds(r, targetMainId, stepExtraEvents, stepChainEvents, 1);

    steps.push({
      stepNumber: steps.length + 1,
      upgradeRank: r,
      rankPriceUsdt: rankPrice,
      upgradingMainId: targetMainId,
      newNodeIdInRank: newUpgradedNode.id,
      placedUnderParentId: parentNodeBefore.id,
      placedUnderParentMainId: parentR1Main,
      placedSide: spot.isLeft ? 'LEFT' : 'RIGHT',
      vaultAddedToParentUsdt: vaultAddedToParent,
      recipientMainId: parentR1Main,
      window5RangeTarget: getFiveRankWindow(getMainCurrentRank(targetMainId)),
      window5RangeSponsor: getFiveRankWindow(getMainCurrentRank(sponsorMainId)),
      targetPaidFromVaultUsdt: paidFromVault,
      targetPaidFromWalletUsdt: paidFromWallet,
      targetUpgradePoolBeforeUsdt: targetUpgradePoolBefore,
      sponsorUpgradePoolFinalUsdt: calcUpgradeRankPool(sponsorMainId),
      sponsorExtraPoolBeforeUsdt: sponsorExtraBefore,
      sponsorExtraPoolAfterGainUsdt: sponsorExtraAfterGain,
      sponsorExtraPoolFinalUsdt: calcExtraIdPool(sponsorMainId),
      sponsorAutoPoolFinalUsdt: calcAutoIdPool(sponsorMainId),
      targetUpgradePoolFinalUsdt: calcUpgradeRankPool(targetMainId),
      targetExtraPoolBeforeUsdt: targetExtraBefore,
      targetExtraPoolAfterGainUsdt: targetExtraAfterGain,
      targetExtraPoolFinalUsdt: calcExtraIdPool(targetMainId),
      targetAutoPoolFinalUsdt: calcAutoIdPool(targetMainId),
      extraIdsGenerated: stepExtraEvents,
      chainReactions: stepChainEvents,
    });
  }

  const targetFinalRank = getMainCurrentRank(targetMainId);
  const sponsorFinalRank = getMainCurrentRank(sponsorMainId);

  return {
    targetMainId,
    sponsorMainId,
    currentOnChainRank,
    startSimulatedRank,
    prospectiveRank,
    steps,
    allExtraIdEvents,
    sponsorExtraIdEvents: allExtraIdEvents.filter((e) => e.ownerMainId === sponsorMainId),
    targetExtraIdEvents: allExtraIdEvents.filter((e) => e.ownerMainId === targetMainId),
    allAutoIdEvents,
    sponsorAutoIdEvents: allAutoIdEvents.filter((e) => e.ownerMainId === sponsorMainId),
    targetAutoIdEvents: allAutoIdEvents.filter((e) => e.ownerMainId === targetMainId),
    targetInitialUpgradePoolUsdt: initialTargetUpgradePool,
    sponsorInitialUpgradePoolUsdt: initialSponsorUpgradePool,
    targetTotalRankCostUsdt: totalTargetCost,
    targetPaidFromVaultUsdt: totalTargetPaidFromVault,
    targetPaidFromWalletUsdt: totalTargetPaidFromWallet,
    targetProspectiveRankPriceUsdt: RANK_PRICES_USDT[prospectiveRank - 1] || 0,
    finalSponsorUpgradePoolUsdt: calcUpgradeRankPool(sponsorMainId),
    finalTargetUpgradePoolUsdt: calcUpgradeRankPool(targetMainId),
    targetFinalRank,
    sponsorFinalRank,
    targetWindow5Range: getFiveRankWindow(targetFinalRank),
    sponsorWindow5Range: getFiveRankWindow(sponsorFinalRank),
    targetNextRankPriceUsdt: targetFinalRank < 27 ? (RANK_PRICES_USDT[targetFinalRank] || 0) : 0,
    sponsorNextRankPriceUsdt: sponsorFinalRank < 27 ? (RANK_PRICES_USDT[sponsorFinalRank] || 0) : 0,
    finalSponsorAutoPoolUsdt: calcAutoIdPool(sponsorMainId),
    finalSponsorExtraPoolUsdt: calcExtraIdPool(sponsorMainId),
    finalTargetAutoPoolUsdt: calcAutoIdPool(targetMainId),
    finalTargetExtraPoolUsdt: calcExtraIdPool(targetMainId),
  };
}

interface ExtraIdPathCalculatorProps {
  multiRankSnapshots: Map<number, Map<number, SimRawNode>>;
  defaultMainId?: number;
  defaultProspectiveRank?: number;
  onSelectRankAndNode?: (rank: number, nodeId: number) => void;
  perIdReportsMap?: Map<number, any>;
}

export const ExtraIdPathCalculator: React.FC<ExtraIdPathCalculatorProps> = ({
  multiRankSnapshots,
  defaultMainId = 2,
  defaultProspectiveRank = 10,
  onSelectRankAndNode,
  perIdReportsMap,
}) => {
  const availableMainIds = useMemo(() => {
    const r1 = multiRankSnapshots.get(1);
    if (!r1) return [2, 3];
    const list = Array.from(r1.values())
      .filter((n) => !n.isClone && !n.isExtraId && n.id > 1)
      .map((n) => n.id)
      .sort((a, b) => a - b);
    return list.length > 0 ? list : [2];
  }, [multiRankSnapshots]);

  const [targetMainId, setTargetMainId] = useState<number>(defaultMainId);
  const [prospectiveRank, setProspectiveRank] = useState<number>(defaultProspectiveRank);
  const [typeFilter, setTypeFilter] = useState<'ALL' | 'EXTRA_ID' | 'AUTO_ID' | 'CLONE'>('ALL');
  const [ownerFilter, setOwnerFilter] = useState<'ALL' | 'SPONSOR' | 'TARGET'>('ALL');

  const calculation = useMemo(
    () => calculateMatrixUpgradePath(multiRankSnapshots, targetMainId, prospectiveRank, perIdReportsMap),
    [multiRankSnapshots, targetMainId, prospectiveRank, perIdReportsMap]
  );

  const handleSelectMainId = (newId: number) => {
    const safeId = Math.max(1, Math.floor(newId || 2));
    setTargetMainId(safeId);
    // Automatically set prospectiveRank to at least currentOnChainRank + 1 if needed
    let maxR = 1;
    for (let r = 2; r <= 27; r++) {
      const snap = multiRankSnapshots.get(r);
      if (snap && Array.from(snap.values()).some((n) => !n.isClone && !n.isExtraId && n.originMainId === safeId)) {
        maxR = r;
      }
    }
    if (prospectiveRank <= maxR && maxR < 27) {
      setProspectiveRank(maxR + 1);
    }
  };

  // Compile all generated identities (Extra ID, Auto ID, Clone) across simulated steps
  const allGeneratedItems = useMemo<UnifiedGeneratedItem[]>(() => {
    const items: UnifiedGeneratedItem[] = [];

    calculation.steps.forEach((step) => {
      // 1. Extra IDs in this step
      step.extraIdsGenerated.forEach((ex, idx) => {
        items.push({
          id: `step-${step.upgradeRank}-extra-${idx}-${ex.newNodeId}`,
          category: 'EXTRA_ID',
          categoryLabel: 'Extra ID',
          stepRank: step.upgradeRank,
          ownerMainId: ex.ownerMainId,
          spawnedInRank: ex.spawnedInRank,
          newNodeId: ex.newNodeId,
          nodeCode: `Extra R${ex.spawnedInRank}-#${ex.newNodeId}`,
          parentId: ex.parentId,
          side: ex.side,
          rankPriceUsdt: ex.rankPriceUsdt,
          costOrDeduction: `หัก ${ex.deductedUsdt.toFixed(2)} U จาก Extra Pool (เหลือ ${ex.poolAfterUsdt.toFixed(2)} U)`,
          descriptionTh: ex.triggeredByNote,
        });
      });

      // 2. Auto IDs and Clones from chain reactions
      step.chainReactions.forEach((cr, idx) => {
        if (cr.type === 'AUTO_ID') {
          items.push({
            id: `step-${step.upgradeRank}-auto-${idx}-${cr.nodeId}`,
            category: 'AUTO_ID',
            categoryLabel: 'Auto ID',
            stepRank: step.upgradeRank,
            ownerMainId: cr.ownerMainId,
            spawnedInRank: 1,
            newNodeId: cr.nodeId,
            nodeCode: `Auto R1-#${cr.nodeId}`,
            parentId: cr.parentId,
            side: cr.side,
            rankPriceUsdt: 5,
            costOrDeduction: 'หัก 5.00 U จาก Auto Pool (R1-3)',
            descriptionTh: cr.descriptionTh,
          });
        } else if (cr.type === 'CLONE') {
          const clonePrice = RANK_PRICES_USDT[cr.rank - 1] || 0;
          items.push({
            id: `step-${step.upgradeRank}-clone-${idx}-${cr.nodeId}`,
            category: 'CLONE',
            categoryLabel: 'โคลนนิ่ง',
            stepRank: step.upgradeRank,
            ownerMainId: cr.ownerMainId,
            spawnedInRank: cr.rank,
            newNodeId: cr.nodeId,
            nodeCode: `Clone R${cr.rank}-#${cr.nodeId}`,
            parentId: cr.parentId,
            side: cr.side,
            rankPriceUsdt: clonePrice,
            costOrDeduction: 'ฟรี (เด้งโคลน 100% Rebirth ขาขวา)',
            descriptionTh: cr.descriptionTh,
          });
        }
      });
    });

    return items;
  }, [calculation.steps]);

  const extraCount = useMemo(
    () => allGeneratedItems.filter((i) => i.category === 'EXTRA_ID').length,
    [allGeneratedItems]
  );
  const autoCount = useMemo(
    () => allGeneratedItems.filter((i) => i.category === 'AUTO_ID').length,
    [allGeneratedItems]
  );
  const cloneCount = useMemo(
    () => allGeneratedItems.filter((i) => i.category === 'CLONE').length,
    [allGeneratedItems]
  );

  const sponsorTotalCount = useMemo(
    () => allGeneratedItems.filter((i) => i.ownerMainId === calculation.sponsorMainId).length,
    [allGeneratedItems, calculation.sponsorMainId]
  );
  const targetTotalCount = useMemo(
    () => allGeneratedItems.filter((i) => i.ownerMainId === calculation.targetMainId).length,
    [allGeneratedItems, calculation.targetMainId]
  );

  const filteredItems = useMemo(() => {
    return allGeneratedItems.filter((item) => {
      const matchType = typeFilter === 'ALL' || item.category === typeFilter;
      const matchOwner =
        ownerFilter === 'ALL' ||
        (ownerFilter === 'SPONSOR' && item.ownerMainId === calculation.sponsorMainId) ||
        (ownerFilter === 'TARGET' && item.ownerMainId === calculation.targetMainId);
      return matchType && matchOwner;
    });
  }, [allGeneratedItems, typeFilter, ownerFilter, calculation.sponsorMainId, calculation.targetMainId]);

  return (
    <div className="rounded-xl bg-slate-950/95 border border-slate-800 p-4 sm:p-5 space-y-4 text-slate-100 shadow-xl">
      {/* Header & Description */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-800 pb-3.5">
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <Calculator className="w-5 h-5 text-amber-400 shrink-0" />
            <h3 className="text-sm sm:text-base font-bold text-white tracking-tight">
              เครื่องคำนวณเส้นทางอัพเกรดและการเกิด Extra ID ล่วงหน้า (Matrix Path & Extra ID Simulator)
            </h3>
          </div>
          <p className="text-xs text-slate-400">
            คำนวณเส้นทางการวางสายไบนารี่ ยอดสะสมเข้ากระปุก 35% ลูป R27→R2 สำหรับแตก Extra ID และปฏิกิริยาโคลนต่อเนื่องตามกฎ Smart Contract ปัจจุบัน
          </p>
        </div>

        <button
          type="button"
          onClick={() => {
            setTargetMainId(2);
            setProspectiveRank(10);
            setTypeFilter('ALL');
            setOwnerFilter('ALL');
          }}
          className="px-3 py-1.5 rounded-lg bg-slate-900 hover:bg-slate-800 border border-slate-700 text-xs font-medium text-slate-300 flex items-center gap-1.5 self-start sm:self-auto cursor-pointer transition"
        >
          <RotateCcw className="w-3.5 h-3.5 text-slate-400" />
          <span>รีเซ็ตค่าเริ่มต้น (ID #2 → R10)</span>
        </button>
      </div>

      {/* Input Controls Bar */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 bg-slate-900/70 p-3.5 rounded-xl border border-slate-800/90">
        {/* 1. Target Main ID Selector */}
        <div className="space-y-1.5">
          <label className="block text-xs font-semibold text-slate-300">
            1. เลือกรหัสหลักที่ต้องการอัพเกรด (Target Main ID)
          </label>
          <div className="flex items-center gap-2">
            <select
              value={availableMainIds.includes(targetMainId) ? targetMainId : 'custom'}
              onChange={(e) => {
                if (e.target.value !== 'custom') {
                  handleSelectMainId(Number(e.target.value));
                }
              }}
              className="flex-1 bg-slate-950 border border-slate-700 focus:border-amber-400 rounded-lg px-3 py-2 text-xs font-mono text-white outline-none"
            >
              {availableMainIds.map((id) => (
                <option key={`calc-main-${id}`} value={id}>
                  Main ID #{id}
                </option>
              ))}
              {!availableMainIds.includes(targetMainId) && (
                <option value="custom">Main ID #{targetMainId} (กำหนดเอง)</option>
              )}
            </select>
            <input
              type="number"
              min={2}
              max={9999}
              value={targetMainId}
              onChange={(e) => handleSelectMainId(Number(e.target.value))}
              className="w-20 bg-slate-950 border border-slate-700 focus:border-amber-400 rounded-lg px-2.5 py-2 text-xs font-mono text-amber-300 font-bold text-center outline-none"
              title="กรอกหมายเลข Main ID"
            />
          </div>
          <div className="text-[11px] text-slate-400 font-mono">
            อยู่บนเชนปัจจุบัน: <strong className="text-emerald-400">Rank {calculation.currentOnChainRank}</strong> · ผู้แนะนำตรง:{' '}
            <strong className="text-amber-300">Main #{calculation.sponsorMainId}</strong>
          </div>
        </div>

        {/* 2. Prospective Rank Selector */}
        <div className="space-y-1.5">
          <label className="block text-xs font-semibold text-slate-300">
            2. เลือก Rank เป้าหมายที่ต้องการคำนวณ (Prospective Rank)
          </label>
          <div className="flex items-center gap-2">
            <select
              value={prospectiveRank}
              onChange={(e) => setProspectiveRank(Number(e.target.value))}
              className="flex-1 bg-slate-950 border border-slate-700 focus:border-amber-400 rounded-lg px-3 py-2 text-xs font-mono text-white outline-none"
            >
              {RANK_PRICES_USDT.map((price, idx) => {
                const r = idx + 1;
                if (r < 2) return null;
                const isAlreadyActive = r <= calculation.currentOnChainRank;
                return (
                  <option key={`calc-rank-${r}`} value={r}>
                    Rank {r} ({price.toLocaleString()} USDT){isAlreadyActive ? ' — อัพแล้วบนเชน' : ''}
                  </option>
                );
              })}
            </select>
          </div>
          <div className="flex items-center gap-1.5 flex-wrap pt-0.5">
            {[
              Math.min(27, calculation.currentOnChainRank + 1),
              Math.min(27, calculation.currentOnChainRank + 2),
              Math.min(27, calculation.currentOnChainRank + 3),
              15,
              20,
              27,
            ]
              .filter((v, i, arr) => v > calculation.currentOnChainRank && arr.indexOf(v) === i)
              .slice(0, 5)
              .map((quickRank) => (
                <button
                  key={`quick-rank-${quickRank}`}
                  type="button"
                  onClick={() => setProspectiveRank(quickRank)}
                  className={`px-2 py-0.5 rounded text-[11px] font-mono font-semibold cursor-pointer transition ${
                    prospectiveRank === quickRank
                      ? 'bg-amber-500 text-slate-950'
                      : 'bg-slate-800 hover:bg-slate-700 text-slate-300'
                  }`}
                >
                  R{quickRank}
                </button>
              ))}
          </div>
        </div>

        {/* 3. Summary KPI Box */}
        <div className="bg-slate-950 border border-slate-800 rounded-lg p-3 flex flex-col justify-between space-y-2">
          <div className="flex items-center justify-between text-xs">
            <span className="text-slate-400">ช่วงการจำลองอัพเกรด:</span>
            <span className="font-mono font-bold text-white">
              {calculation.steps.length > 0
                ? `R${calculation.startSimulatedRank} → R${calculation.prospectiveRank} (${calculation.steps.length} ขั้น)`
                : `อยู่ R${calculation.currentOnChainRank} แล้ว`}
            </span>
          </div>
          <div className="grid grid-cols-2 gap-2 pt-1 border-t border-slate-800/80 font-mono text-xs">
            <div>
              <div className="text-[10.5px] text-slate-400">Extra ID เกิดที่ Main #{calculation.sponsorMainId}:</div>
              <div className="text-sm font-extrabold text-fuchsia-400">
                {calculation.sponsorExtraIdEvents.length} รหัส
                {calculation.sponsorExtraIdEvents.length > 0 && (
                  <span className="text-[11px] text-fuchsia-300 ml-1">
                    ({calculation.sponsorExtraIdEvents.map((e) => `R${e.spawnedInRank}`).join(', ')})
                  </span>
                )}
              </div>
            </div>
            <div>
              <div className="text-[10.5px] text-slate-400">Extra ID เกิดที่ Main #{calculation.targetMainId}:</div>
              <div className="text-sm font-extrabold text-amber-400">
                {calculation.targetExtraIdEvents.length} รหัส
                {calculation.targetExtraIdEvents.length > 0 && (
                  <span className="text-[11px] text-amber-300 ml-1">
                    ({calculation.targetExtraIdEvents.map((e) => `R${e.spawnedInRank}`).join(', ')})
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Warning if selected rank is already reached */}
      {calculation.steps.length === 0 ? (
        <div className="rounded-lg bg-slate-900 border border-slate-800 p-4 text-center space-y-2">
          <div className="text-xs sm:text-sm font-semibold text-amber-300">
            Main ID #{calculation.targetMainId} อยู่ใน Rank {calculation.currentOnChainRank} บนบล็อกเชนเรียบร้อยแล้ว
          </div>
          <p className="text-xs text-slate-400">
            กรุณาเลือก Rank เป้าหมายตั้งแต่ <strong className="text-white">Rank {Math.min(27, calculation.currentOnChainRank + 1)}</strong> ขึ้นไป เพื่อคำนวณเส้นทางอัพเกรดและการเกิด Extra ID ล่วงหน้า
          </p>
        </div>
      ) : (
        <>
          {/* Quick Pool Summary Cards (35% Upgrade Pool, Auto ID Pool, Extra ID Pool) */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-3 p-3 rounded-xl bg-slate-900/60 border border-slate-800 text-xs">
            {/* 35% Upgrade Rank Pool Summary */}
            <div className="rounded-lg bg-slate-950 border border-cyan-500/30 p-2.5 space-y-1.5">
              <div className="flex items-center justify-between text-cyan-300 font-bold">
                <span>🏆 สรุปยอด 35% อัพ rank (Upgrade Pool)</span>
                <span className="text-[10px] text-slate-400 font-normal">กรอบ 5 Rank สไลด์</span>
              </div>
              <div className="space-y-2 font-mono text-[11px] pt-1.5 border-t border-slate-800/80">
                {/* Sponsor Main ID */}
                <div className="space-y-0.5">
                  <div className="flex items-center justify-between">
                    <span className="text-slate-300">Main #{calculation.sponsorMainId} (Sponsor):</span>
                    {calculation.sponsorFinalRank >= 27 ? (
                      <span className="text-amber-300 font-bold">0.00 U (Max Rank R27 👑)</span>
                    ) : (
                      <span className="text-cyan-300 font-bold">
                        สะสม {calculation.finalSponsorUpgradePoolUsdt.toFixed(2)} / {calculation.sponsorNextRankPriceUsdt.toLocaleString()} U
                      </span>
                    )}
                  </div>
                  <div className="flex items-center justify-between text-[10px] text-slate-500">
                    <span>กรอบ 5 Rank: R{calculation.sponsorWindow5Range[0]}–R{calculation.sponsorWindow5Range[1]}</span>
                    <span className="text-slate-400">
                      {calculation.sponsorFinalRank >= 27 ? 'ยอด 35% เข้า Extra ID Pool ทั้งหมด' : ''}
                    </span>
                  </div>
                </div>

                {/* Target Main ID */}
                <div className="space-y-1 pt-1.5 border-t border-slate-800/60">
                  <div className="flex items-center justify-between">
                    <span className="text-slate-200 font-bold">Main #{calculation.targetMainId} (Target):</span>
                    <span className="text-cyan-300 font-bold">
                      {calculation.targetFinalRank >= 27
                        ? '0.00 U (Max Rank R27 👑)'
                        : `สะสมคงเหลือ ${calculation.finalTargetUpgradePoolUsdt.toFixed(2)} U`}
                    </span>
                  </div>

                  {/* Target Rank Cost & Source Breakdown */}
                  <div className="bg-slate-900/80 rounded p-1.5 space-y-1 text-[10.5px]">
                    <div className="flex items-center justify-between text-slate-300">
                      <span>ค่าอัพเกรด (R{calculation.startSimulatedRank}{calculation.startSimulatedRank !== calculation.prospectiveRank ? `–R${calculation.prospectiveRank}` : ''}):</span>
                      <span className="text-amber-300 font-bold">{calculation.targetTotalRankCostUsdt.toLocaleString()} U</span>
                    </div>
                    <div className="flex items-center justify-between text-slate-400 text-[10px]">
                      <span>• หักจากกระปุก 35%:</span>
                      <span className={calculation.targetPaidFromVaultUsdt > 0 ? 'text-emerald-400 font-bold' : 'text-slate-500'}>
                        {calculation.targetPaidFromVaultUsdt.toFixed(2)} U
                      </span>
                    </div>
                    <div className="flex items-center justify-between text-slate-400 text-[10px]">
                      <span>• จ่ายจากกระเป๋า (Wallet):</span>
                      <span className={calculation.targetPaidFromWalletUsdt > 0 ? 'text-rose-300 font-bold' : 'text-emerald-400'}>
                        {calculation.targetPaidFromWalletUsdt.toLocaleString()} U
                      </span>
                    </div>
                  </div>

                  {/* Next Rank progress after simulation */}
                  {calculation.targetFinalRank < 27 && (
                    <div className="pt-0.5 space-y-0.5 text-[10px]">
                      <div className="flex items-center justify-between text-slate-400">
                        <span>สะสมสำหรับ Rank {calculation.targetFinalRank + 1} ต่อไป:</span>
                        <span className="text-cyan-300 font-bold">
                          {calculation.finalTargetUpgradePoolUsdt.toFixed(2)} / {calculation.targetNextRankPriceUsdt.toLocaleString()} U
                        </span>
                      </div>
                      <div className="flex items-center justify-between text-slate-500">
                        <span>กรอบ 5 Rank ใหม่: R{calculation.targetWindow5Range[0]}–R{calculation.targetWindow5Range[1]}</span>
                        <span className={calculation.finalTargetUpgradePoolUsdt >= calculation.targetNextRankPriceUsdt ? 'text-emerald-400 font-bold' : 'text-slate-400'}>
                          {calculation.finalTargetUpgradePoolUsdt >= calculation.targetNextRankPriceUsdt
                            ? `(พร้อมอัพ R${calculation.targetFinalRank + 1}! 🚀)`
                            : `(ขาดอีก ${(calculation.targetNextRankPriceUsdt - calculation.finalTargetUpgradePoolUsdt).toFixed(2)} U)`}
                        </span>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>

            {/* Auto ID Pool Summary */}
            <div className="rounded-lg bg-slate-950 border border-amber-500/30 p-2.5 space-y-1">
              <div className="flex items-center justify-between text-amber-300 font-bold">
                <span>🤖 สรุปยอด Auto ID Pool (Rank 1-3)</span>
                <span className="text-[10px] text-slate-400 font-normal">เกณฑ์: ครบ 5.00 U แตก 1 Auto ID</span>
              </div>
              <div className="space-y-1 font-mono text-[11px] pt-1 border-t border-slate-800/80">
                <div className="flex items-center justify-between">
                  <span className="text-slate-300">Main #{calculation.sponsorMainId} (Sponsor):</span>
                  <span className="text-amber-300 font-bold">
                    สะสม {calculation.finalSponsorAutoPoolUsdt.toFixed(2)} / 5.00 U
                    {calculation.sponsorAutoIdEvents.length > 0 ? (
                      <span className="text-emerald-400 ml-1">(แตกใหม่ +{calculation.sponsorAutoIdEvents.length} รหัส)</span>
                    ) : (
                      <span className="text-slate-400 font-normal text-[10px] ml-1">
                        {calculation.finalSponsorAutoPoolUsdt >= 5
                          ? ''
                          : `(ขาดอีก ${(5.0 - calculation.finalSponsorAutoPoolUsdt).toFixed(2)} U)`}
                      </span>
                    )}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-slate-300">Main #{calculation.targetMainId} (Target):</span>
                  <span className="text-amber-300 font-bold">
                    สะสม {calculation.finalTargetAutoPoolUsdt.toFixed(2)} / 5.00 U
                    {calculation.targetAutoIdEvents.length > 0 ? (
                      <span className="text-emerald-400 ml-1">(แตกใหม่ +{calculation.targetAutoIdEvents.length} รหัส)</span>
                    ) : (
                      <span className="text-slate-400 font-normal text-[10px] ml-1">
                        {calculation.finalTargetAutoPoolUsdt >= 5
                          ? ''
                          : `(ขาดอีก ${(5.0 - calculation.finalTargetAutoPoolUsdt).toFixed(2)} U)`}
                      </span>
                    )}
                  </span>
                </div>
              </div>
            </div>

            {/* Extra ID Pool Summary */}
            <div className="rounded-lg bg-slate-950 border border-fuchsia-500/30 p-2.5 space-y-1">
              <div className="flex items-center justify-between text-fuchsia-300 font-bold">
                <span>⚡ สรุปยอด Extra ID Pool (Rank 4-27)</span>
                <span className="text-[10px] text-slate-400 font-normal">เกณฑ์: ลูป R27→R2 แตก Extra ID</span>
              </div>
              <div className="space-y-1 font-mono text-[11px] pt-1 border-t border-slate-800/80">
                <div className="flex items-center justify-between">
                  <span className="text-slate-300">Main #{calculation.sponsorMainId} (Sponsor):</span>
                  <span className="text-fuchsia-300 font-bold">
                    คงเหลือ {calculation.finalSponsorExtraPoolUsdt.toFixed(2)} U
                    {calculation.sponsorExtraIdEvents.length > 0 && (
                      <span className="text-emerald-400 ml-1">
                        (แตก +{calculation.sponsorExtraIdEvents.length} รหัส: {calculation.sponsorExtraIdEvents.map((e) => `R${e.spawnedInRank}`).join(', ')})
                      </span>
                    )}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-slate-300">Main #{calculation.targetMainId} (Target):</span>
                  <span className="text-fuchsia-300 font-bold">
                    คงเหลือ {calculation.finalTargetExtraPoolUsdt.toFixed(2)} U
                    {calculation.targetExtraIdEvents.length > 0 && (
                      <span className="text-emerald-400 ml-1">
                        (แตก +{calculation.targetExtraIdEvents.length} รหัส: {calculation.targetExtraIdEvents.map((e) => `R${e.spawnedInRank}`).join(', ')})
                      </span>
                    )}
                  </span>
                </div>
              </div>
            </div>
          </div>

          {/* Table 1: Step-by-Step Upgrade Path & Resulting Extra ID / Clone Cascade */}
          <div className="space-y-2">
            <div className="flex items-center justify-between flex-wrap gap-2">
              <div className="flex items-center gap-2 text-xs sm:text-sm font-bold text-slate-200">
                <Layers className="w-4 h-4 text-cyan-400" />
                <span>
                  ตารางที่ 1: ลำดับการต่อสายไบนารี่ ยอดสะสมกระปุก และการเกิด Extra ID ทีละขั้น (R{calculation.startSimulatedRank} → R{calculation.prospectiveRank})
                </span>
              </div>
              <div className="text-[11px] text-slate-400 font-mono">
                คลิกที่หมายเลขโหนดเพื่อสลับไปดูผังจริงของ Rank นั้น
              </div>
            </div>

            <div className="overflow-x-auto rounded-xl border border-slate-800 bg-slate-900/40">
              <table className="w-full text-left border-collapse text-xs">
                <thead>
                  <tr className="border-b border-slate-800 bg-slate-900/90 text-slate-300 font-semibold">
                    <th className="py-2.5 px-3 whitespace-nowrap">ขั้นที่ / Rank</th>
                    <th className="py-2.5 px-3 whitespace-nowrap">ตำแหน่งต่อสาย (Path)</th>
                    <th className="py-2.5 px-3 whitespace-nowrap">35% Vault ที่เกิดขึ้น</th>
                    <th className="py-2.5 px-3 min-w-[190px]">Extra ID ที่เกิดในขั้นนี้ (R27→R2)</th>
                    <th className="py-2.5 px-3 min-w-[190px]">Auto ID ที่เกิดในขั้นนี้ (R1)</th>
                    <th className="py-2.5 px-3 min-w-[230px]">โคลนนิ่ง ที่เกิดในขั้นนี้ (R1 → R27)</th>
                    <th className="py-2.5 px-3 whitespace-nowrap min-w-[160px]">คงเหลือ 3 กระปุก (Up / Auto / Extra)</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/80 font-mono">
                  {calculation.steps.map((step) => {
                    const autoEvents = step.chainReactions.filter((cr) => cr.type === 'AUTO_ID');
                    const cloneEvents = step.chainReactions.filter((cr) => cr.type === 'CLONE');
                    const vaultGainEvents = step.chainReactions.filter(
                      (cr) => cr.type === 'VAULT_GAIN' || cr.type === 'EXTRA_LEFT_BONUS'
                    );

                    return (
                      <tr key={`sim-step-${step.upgradeRank}`} className="hover:bg-slate-900/60 align-top transition-colors">
                        {/* Col 1: Rank & Price */}
                        <td className="py-3 px-3 whitespace-nowrap">
                          <div className="font-bold text-white flex items-center gap-1.5">
                            <span>Rank {step.upgradeRank}</span>
                            <span className="text-slate-400">·</span>
                            <span className="text-amber-300">{step.rankPriceUsdt.toLocaleString()} U</span>
                          </div>
                          <div className="text-[10px] mt-0.5 space-y-0.5">
                            {step.targetPaidFromVaultUsdt > 0 && (
                              <div className="text-emerald-400 font-bold">
                                หักกระปุก 35%: -{step.targetPaidFromVaultUsdt.toFixed(2)} U
                              </div>
                            )}
                            <div className="text-slate-400">
                              จ่ายจากกระเป๋า:{' '}
                              <strong className={step.targetPaidFromWalletUsdt > 0 ? 'text-rose-300' : 'text-emerald-400'}>
                                {step.targetPaidFromWalletUsdt.toLocaleString(undefined, {
                                  minimumFractionDigits: step.targetPaidFromWalletUsdt % 1 !== 0 ? 2 : 0,
                                  maximumFractionDigits: 2,
                                })}{' '}
                                U
                              </strong>
                            </div>
                          </div>
                          <div className="text-[10.5px] text-slate-400 mt-0.5">
                            Main #{step.upgradingMainId} → โหนดใหม่{' '}
                            <strong className="text-cyan-300">
                              R{step.upgradeRank}-#{step.newNodeIdInRank}
                            </strong>
                          </div>
                          <div className="text-[10px] text-slate-500 mt-0.5">
                            กรอบ 5 Rank ของ #{step.upgradingMainId}: R{step.window5RangeTarget[0]}–R{step.window5RangeTarget[1]}
                          </div>
                        </td>

                        {/* Col 2: Placement Path */}
                        <td className="py-3 px-3 whitespace-nowrap">
                          <div className="flex items-center gap-1 text-slate-200">
                            <span>ต่อใต้</span>
                            <button
                              type="button"
                              onClick={() => onSelectRankAndNode?.(step.upgradeRank, step.placedUnderParentId)}
                              className="font-bold text-indigo-300 hover:text-indigo-200 underline cursor-pointer"
                            >
                              R{step.upgradeRank}-#{step.placedUnderParentId}
                            </button>
                            <span className="text-slate-400">(Main #{step.placedUnderParentMainId})</span>
                          </div>
                          <div className="mt-1">
                            <span
                              className={`text-[11px] font-bold ${
                                step.placedSide === 'LEFT' ? 'text-emerald-400' : 'text-purple-400'
                              }`}
                            >
                              {step.placedSide === 'LEFT' ? '↙ ฝั่งซ้าย (Left • คำนวณ 35% Vault)' : '↘ ฝั่งขวา (Right • เด้งโคลน 100%)'}
                            </span>
                          </div>
                        </td>

                        {/* Col 3: Vault Flow */}
                        <td className="py-3 px-3 whitespace-nowrap">
                          {step.vaultAddedToParentUsdt > 0 ? (
                            <div className="space-y-0.5">
                              <div className="text-emerald-400 font-bold">
                                +{step.vaultAddedToParentUsdt.toFixed(2)} USDT → Main #{step.recipientMainId}
                              </div>
                              {step.recipientMainId === calculation.sponsorMainId && step.upgradeRank >= 4 && (
                                <div className="text-[10.5px] text-slate-300">
                                  Extra Pool #{calculation.sponsorMainId}: {step.sponsorExtraPoolBeforeUsdt.toFixed(2)} +{' '}
                                  {step.vaultAddedToParentUsdt.toFixed(2)} ={' '}
                                  <strong className="text-amber-300">
                                    {step.sponsorExtraPoolAfterGainUsdt.toFixed(2)} U
                                  </strong>
                                </div>
                              )}
                            </div>
                          ) : (
                            <span className="text-slate-500">0.00 U (ฝั่งขวาเด้งโคลน)</span>
                          )}
                        </td>

                        {/* Col 4: Extra IDs Spawned (R27 → R2) */}
                        <td className="py-3 px-3">
                          {step.extraIdsGenerated.length === 0 ? (
                            <span className="text-slate-500 text-[11px]">ไม่เกิด Extra ID ในขั้นนี้</span>
                          ) : (
                            <div className="space-y-1.5">
                              {step.extraIdsGenerated.map((ex, idx) => (
                                <div
                                  key={`step-${step.upgradeRank}-extra-${idx}`}
                                  className="rounded-lg bg-slate-950/90 border border-fuchsia-500/40 p-2 space-y-0.5"
                                >
                                  <div className="flex items-center justify-between gap-2">
                                    <span className="font-bold text-fuchsia-300">
                                      ⚡ Main #{ex.ownerMainId} เกิด Extra ID ที่ Rank {ex.spawnedInRank}
                                    </span>
                                    <span className="text-rose-300 font-bold">-{ex.deductedUsdt.toFixed(2)} U</span>
                                  </div>
                                  <div className="text-[11px] text-slate-300">
                                    โหนดใหม่:{' '}
                                    <strong className="text-white">
                                      R{ex.spawnedInRank}-#{ex.newNodeId}
                                    </strong>{' '}
                                    วางใต้{' '}
                                    <button
                                      type="button"
                                      onClick={() => onSelectRankAndNode?.(ex.spawnedInRank, ex.parentId)}
                                      className="text-indigo-300 hover:underline font-bold cursor-pointer"
                                    >
                                      R{ex.spawnedInRank}-#{ex.parentId}
                                    </button>{' '}
                                    ({ex.side === 'LEFT' ? '↙ซ้าย' : '↘ขวา'})
                                  </div>
                                  <div className="text-[10px] text-slate-400">
                                    {ex.triggeredByNote} → คงเหลือ {ex.poolAfterUsdt.toFixed(2)} U
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </td>

                        {/* Col 5: Auto ID ที่เกิดในขั้นนี้ (R1) */}
                        <td className="py-3 px-3">
                          {autoEvents.length === 0 ? (
                            <span className="text-slate-500 text-[11px]">ไม่เกิด Auto ID ในขั้นนี้</span>
                          ) : (
                            <div className="space-y-1.5">
                              {autoEvents.map((ae, idx) => (
                                <div
                                  key={`step-${step.upgradeRank}-auto-${idx}`}
                                  className="rounded-lg bg-slate-950/90 border border-amber-500/40 p-2 space-y-0.5"
                                >
                                  <div className="flex items-center justify-between gap-2">
                                    <span className="font-bold text-amber-300">
                                      🤖 Main #{ae.ownerMainId} เกิด Auto ID ที่ Rank 1
                                    </span>
                                    <span className="text-rose-300 font-bold">-5.00 U</span>
                                  </div>
                                  <div className="text-[11px] text-slate-300">
                                    โหนดใหม่:{' '}
                                    <strong className="text-white">
                                      R1-#{ae.nodeId}
                                    </strong>{' '}
                                    วางใต้{' '}
                                    <button
                                      type="button"
                                      onClick={() => onSelectRankAndNode?.(1, ae.parentId)}
                                      className="text-indigo-300 hover:underline font-bold cursor-pointer"
                                    >
                                      R1-#{ae.parentId}
                                    </button>{' '}
                                    ({ae.side === 'LEFT' ? '↙ซ้าย' : '↘ขวา'})
                                  </div>
                                  <div className="text-[10px] text-slate-400 leading-relaxed">
                                    {ae.descriptionTh}
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </td>

                        {/* Col 6: โคลนนิ่ง ที่เกิดในขั้นนี้ (R1 → R27) */}
                        <td className="py-3 px-3">
                          {cloneEvents.length === 0 && vaultGainEvents.length === 0 ? (
                            <span className="text-slate-500 text-[11px]">ไม่เกิดโคลนในขั้นนี้</span>
                          ) : (
                            <div className="space-y-1.5">
                              {cloneEvents.map((ce, idx) => (
                                <div
                                  key={`step-${step.upgradeRank}-clone-${idx}`}
                                  className="rounded-lg bg-slate-950/90 border border-emerald-500/40 p-2 space-y-0.5"
                                >
                                  <div className="flex items-center justify-between gap-2">
                                    <span className="font-bold text-emerald-400">
                                      ✨ โคลน Rank {ce.rank}: R{ce.rank}-#{ce.nodeId}
                                    </span>
                                    <span className="text-slate-400 text-[10px] font-bold">
                                      (Main #{ce.ownerMainId})
                                    </span>
                                  </div>
                                  <div className="text-[11px] text-slate-300">
                                    วางใต้{' '}
                                    <button
                                      type="button"
                                      onClick={() => onSelectRankAndNode?.(ce.rank, ce.parentId)}
                                      className="text-indigo-300 hover:underline font-bold cursor-pointer"
                                    >
                                      R{ce.rank}-#{ce.parentId}
                                    </button>{' '}
                                    ({ce.side === 'LEFT' ? '↙ซ้าย' : '↘ขวา'})
                                  </div>
                                  <div className="text-[10px] text-slate-400 leading-relaxed">
                                    {ce.descriptionTh}
                                  </div>
                                </div>
                              ))}

                              {/* Cascading Vault Gains from these Clones / Auto IDs */}
                              {vaultGainEvents.length > 0 && (
                                <div className="space-y-1 pt-1 border-t border-slate-800/60 text-[10.5px]">
                                  {vaultGainEvents.map((vg, vIdx) => (
                                    <div key={`step-${step.upgradeRank}-vg-${vIdx}`} className="text-cyan-300 leading-relaxed">
                                      <span className="font-bold text-amber-300">💰 ปฏิกิริยากระปุก:</span> {vg.descriptionTh}
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}
                        </td>

                        {/* Col 7: Remaining 3 Pool Balances (Up / Auto / Extra) */}
                        <td className="py-3 px-3 whitespace-nowrap text-[11px] space-y-1.5">
                          <div className="rounded bg-slate-950/80 border border-slate-800/80 p-1.5 space-y-1">
                            <div className="text-[10px] text-slate-400 font-bold">Main #{calculation.sponsorMainId} (Sponsor)</div>
                            <div className="text-cyan-300 font-bold">
                              Up: {step.sponsorUpgradePoolFinalUsdt.toFixed(2)} U
                            </div>
                            <div className="flex items-center gap-1.5 text-[10px]">
                              <span className="text-amber-300 font-bold">Auto: {step.sponsorAutoPoolFinalUsdt.toFixed(2)} U</span>
                              <span className="text-slate-600">·</span>
                              <span className="text-fuchsia-300 font-bold">Extra: {step.sponsorExtraPoolFinalUsdt.toFixed(2)} U</span>
                            </div>
                          </div>
                          <div className="rounded bg-slate-950/80 border border-slate-800/80 p-1.5 space-y-1">
                            <div className="text-[10px] text-slate-400 font-bold">Main #{calculation.targetMainId} (Target)</div>
                            <div className="text-cyan-300 font-bold">
                              Up: {step.targetUpgradePoolFinalUsdt.toFixed(2)} U
                            </div>
                            <div className="flex items-center gap-1.5 text-[10px]">
                              <span className="text-amber-300 font-bold">Auto: {step.targetAutoPoolFinalUsdt.toFixed(2)} U</span>
                              <span className="text-slate-600">·</span>
                              <span className="text-fuchsia-300 font-bold">Extra: {step.targetExtraPoolFinalUsdt.toFixed(2)} U</span>
                            </div>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Table 2: Filtered Summary of All Generated Sub-Identities (Extra ID, Auto ID, Clone) */}
          <div className="space-y-3 pt-3 border-t border-slate-800/80">
            <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-2.5">
              <div className="space-y-0.5">
                <div className="flex items-center gap-2 text-xs sm:text-sm font-bold text-slate-200">
                  <Sparkles className="w-4 h-4 text-fuchsia-400" />
                  <span>
                    ตารางที่ 2: สรุปรายการ Extra ID, Auto ID และโคลนนิ่ง ที่จะเกิดทั้งหมด ({filteredItems.length} จาก {allGeneratedItems.length} รหัส)
                  </span>
                </div>
                <div className="text-[11px] text-slate-400">
                  รวบรวมรหัสใหม่ทุกประเภทที่เกิดขึ้นตามปฏิกิริยาลูกโซ่: ⚡ Extra ID (R27→R2), 🤖 Auto ID (R1), ✨ โคลนนิ่ง (R1→R27)
                </div>
              </div>

              {/* Interactive Filter Controls: Category & Owner */}
              <div className="flex flex-wrap items-center gap-2">
                {/* 1. Category Filter */}
                <div className="flex items-center gap-1 p-1 bg-slate-900 rounded-lg border border-slate-800">
                  {[
                    { key: 'ALL', label: `ทั้งหมด (${allGeneratedItems.length})` },
                    { key: 'EXTRA_ID', label: `⚡ Extra (${extraCount})` },
                    { key: 'AUTO_ID', label: `🤖 Auto (${autoCount})` },
                    { key: 'CLONE', label: `✨ โคลน (${cloneCount})` },
                  ].map((tab) => (
                    <button
                      key={`filter-cat-${tab.key}`}
                      type="button"
                      onClick={() => setTypeFilter(tab.key as any)}
                      className={`px-2.5 py-1 rounded-md text-xs font-medium transition cursor-pointer ${
                        typeFilter === tab.key
                          ? 'bg-fuchsia-600 text-white shadow-sm'
                          : 'text-slate-400 hover:text-white'
                      }`}
                    >
                      {tab.label}
                    </button>
                  ))}
                </div>

                {/* 2. Owner Filter */}
                <div className="flex items-center gap-1 p-1 bg-slate-900 rounded-lg border border-slate-800">
                  {[
                    { key: 'ALL', label: `ทุกคน (${allGeneratedItems.length})` },
                    {
                      key: 'SPONSOR',
                      label: `Main #${calculation.sponsorMainId} (${sponsorTotalCount})`,
                    },
                    {
                      key: 'TARGET',
                      label: `Main #${calculation.targetMainId} (${targetTotalCount})`,
                    },
                  ].map((tab) => (
                    <button
                      key={`filter-owner-${tab.key}`}
                      type="button"
                      onClick={() => setOwnerFilter(tab.key as any)}
                      className={`px-2.5 py-1 rounded-md text-xs font-medium transition cursor-pointer ${
                        ownerFilter === tab.key
                          ? 'bg-indigo-600 text-white shadow-sm'
                          : 'text-slate-400 hover:text-white'
                      }`}
                    >
                      {tab.label}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {filteredItems.length === 0 ? (
              <div className="rounded-lg bg-slate-900/50 border border-slate-800 p-4 text-center text-xs text-slate-400">
                ไม่มีรายการรหัสใหม่ (Extra ID / Auto ID / โคลนนิ่ง) ที่เกิดสำหรับตัวกรองนี้ในช่วง Rank {calculation.startSimulatedRank} → {calculation.prospectiveRank}
              </div>
            ) : (
              <div className="overflow-x-auto rounded-xl border border-slate-800 bg-slate-900/40">
                <table className="w-full text-left border-collapse text-xs font-mono">
                  <thead>
                    <tr className="border-b border-slate-800 bg-slate-900/90 text-slate-300 font-sans font-semibold">
                      <th className="py-2.5 px-3 whitespace-nowrap">ลำดับ</th>
                      <th className="py-2.5 px-3 whitespace-nowrap">ประเภท</th>
                      <th className="py-2.5 px-3 whitespace-nowrap">เกิดเมื่ออัพถึง</th>
                      <th className="py-2.5 px-3 whitespace-nowrap">เจ้าของรหัส</th>
                      <th className="py-2.5 px-3 whitespace-nowrap">เกิดที่ Rank (ราคา)</th>
                      <th className="py-2.5 px-3 whitespace-nowrap">รหัสโหนดใหม่</th>
                      <th className="py-2.5 px-3 whitespace-nowrap">วางต่อใต้อัพไลน์</th>
                      <th className="py-2.5 px-3">รายละเอียด / ยอดหัก / ที่มา</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/70">
                    {filteredItems.map((item, i) => (
                      <tr key={`summary-item-${item.id}`} className="hover:bg-slate-900/60 transition-colors">
                        {/* 1. ลำดับ */}
                        <td className="py-2.5 px-3 text-slate-400">#{i + 1}</td>

                        {/* 2. ประเภท (Badge) */}
                        <td className="py-2.5 px-3 whitespace-nowrap">
                          {item.category === 'EXTRA_ID' && (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10.5px] font-bold bg-fuchsia-950/80 border border-fuchsia-500/50 text-fuchsia-300">
                              <Zap className="w-3 h-3 text-fuchsia-400" />
                              Extra ID
                            </span>
                          )}
                          {item.category === 'AUTO_ID' && (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10.5px] font-bold bg-amber-950/80 border border-amber-500/50 text-amber-300">
                              <Bot className="w-3 h-3 text-amber-400" />
                              Auto ID
                            </span>
                          )}
                          {item.category === 'CLONE' && (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10.5px] font-bold bg-cyan-950/80 border border-cyan-500/50 text-cyan-300">
                              <Sparkles className="w-3 h-3 text-cyan-400" />
                              โคลนนิ่ง
                            </span>
                          )}
                        </td>

                        {/* 3. เกิดเมื่ออัพถึง */}
                        <td className="py-2.5 px-3 text-amber-300 font-bold whitespace-nowrap">
                          เมื่อ Main #{calculation.targetMainId} ขึ้น R{item.stepRank}
                        </td>

                        {/* 4. เจ้าของรหัส */}
                        <td className="py-2.5 px-3 whitespace-nowrap">
                          <span
                            className={`font-bold ${
                              item.ownerMainId === calculation.sponsorMainId ? 'text-fuchsia-300' : 'text-cyan-300'
                            }`}
                          >
                            Main ID #{item.ownerMainId}
                          </span>
                          <span className="text-[10px] text-slate-400 font-normal ml-1">
                            ({item.ownerMainId === calculation.sponsorMainId ? 'Sponsor' : 'Target'})
                          </span>
                        </td>

                        {/* 5. เกิดที่ Rank (ราคา) */}
                        <td className="py-2.5 px-3 font-bold text-white whitespace-nowrap">
                          Rank {item.spawnedInRank}{' '}
                          <span className="text-slate-400 font-normal">
                            ({item.rankPriceUsdt.toLocaleString()} U)
                          </span>
                        </td>

                        {/* 6. รหัสโหนดใหม่ */}
                        <td className="py-2.5 px-3 whitespace-nowrap">
                          <button
                            type="button"
                            onClick={() => onSelectRankAndNode?.(item.spawnedInRank, item.newNodeId)}
                            className={`font-bold hover:underline cursor-pointer ${
                              item.category === 'EXTRA_ID'
                                ? 'text-fuchsia-300'
                                : item.category === 'AUTO_ID'
                                ? 'text-amber-300'
                                : 'text-cyan-300'
                            }`}
                            title={`คลิกเพื่อดูผัง Rank ${item.spawnedInRank} ที่โหนด #${item.newNodeId}`}
                          >
                            {item.nodeCode}
                          </button>
                        </td>

                        {/* 7. วางต่อใต้อัพไลน์ */}
                        <td className="py-2.5 px-3 whitespace-nowrap">
                          <div className="space-y-0.5">
                            <div>
                              <span className="text-slate-400 mr-1">ใต้</span>
                              <button
                                type="button"
                                onClick={() => onSelectRankAndNode?.(item.spawnedInRank, item.parentId)}
                                className="text-indigo-300 hover:underline font-bold cursor-pointer"
                                title={`คลิกเพื่อดูผัง Rank ${item.spawnedInRank} ที่โหนด #${item.parentId}`}
                              >
                                R{item.spawnedInRank}-#{item.parentId}
                              </button>
                            </div>
                            <div
                              className={`text-[10.5px] font-bold ${
                                item.side === 'LEFT' ? 'text-emerald-400' : 'text-purple-400'
                              }`}
                            >
                              {item.side === 'LEFT' ? '↙ ฝั่งซ้าย' : '↘ ฝั่งขวา'}
                            </div>
                          </div>
                        </td>

                        {/* 8. รายละเอียด / ยอดหัก / ที่มา */}
                        <td className="py-2.5 px-3">
                          <div className="space-y-0.5 min-w-[200px] max-w-sm">
                            <div className="text-[11px] font-bold text-slate-200">
                              {item.costOrDeduction}
                            </div>
                            <div className="text-[10.5px] text-slate-400 font-sans leading-relaxed">
                              {item.descriptionTh}
                            </div>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
};
