/**
 * Utility for calculating the 3 Vault Pools strictly scoped to a specific Main ID (Idหลักนั้นๆ):
 * 
 * คำนิยามคำว่าไอดีหลัก:
 * หลักการสำคัญ: ระบบคำนวณแยกตาม "Idหลักนั้นๆ" โดยเฉพาะ ไม่ใช่ยอดรวมทั้งกระเป๋า
 * เนื่องจาก 1 กระเป๋าเงินสามารถถือครองได้หลายรหัสหลัก (ทั้งจากการสมัครตรง และจาก Auto New Main ID)
 *
 * คำนิยาม 5 rank ย้อนหลัง:
 * สถานะ rank ปัจจุบันของ idหลักนั้น ถอยหลัง 5 rank [Math.max(1, currentRank - 4) ... currentRank]
 * ยกตัวอย่าง ถ้าสถานะไอดี #6 อยู่ rank 6 ก็ให้รวมยอด Vault (35%) rank 2-6
 * 
 * 1. Upgrade Rank Pool:
 *    สูตร: ยอดสะสม Vault (35%) ของ 5 rank ย้อนหลัง
 *    คำนิยาม 5 rank ย้อนหลัง: สถานะ rank ปัจจุบันของ idหลักนั้น ถอยหลัง 5 rank [Math.max(1, currentRank - 4) ... currentRank]
 *    การใช้งาน: ยอดสะสมในกรอบ 5 rank ย้อนหลังนี้จะถูกนำมาใช้ปลดล็อกอัปเกรดสู่แรงค์ถัดไปโดยอัตโนมัติ
 * 
 * 2. New Auto ID Pool:
 *    สูตร: ยอดสะสม Vault (35%) ใน Rank 1 ถึง 3 ลบด้วย 5 rank ย้อนหลังของรหัสหลักนั้น
 *    คำนิยาม 5 rank ย้อนหลัง: สถานะ rank ปัจจุบันของ idหลักนั้น ถอยหลัง 5 rank
 *    การปลดล็อก: เมื่อสะสมครบ 5.00 USDT ปลดล็อกสิทธิ์สร้าง Auto New Main ID ใหม่ทันที (เงินแยก ไม่ใช้เหรียญในกระเป๋า)
 * 
 * 3. New Extra ID Pool:
 *    สูตร: 
 *    - สถานะ rank ปัจจุบันของ idหลักนั้น ต่ำกว่า 27:
 *      ยอดสะสม Vault (35%) ใน Rank 4 ถึง 27 ลบด้วย 5 rank ย้อนหลังของรหัสหลักนั้น
 *    - สถานะ rank ปัจจุบันของ idหลักนั้น เท่ากับ 27:
 *      ยอดสะสม Vault (35%) ใน Rank 4 ถึง 27
 *    คำนิยาม 5 rank ย้อนหลัง: สถานะ rank ปัจจุบันของ idหลักนั้น ถอยหลัง 5 rank
 *    การเกิด: สะสมครบตาม ราคา Rank เป้าหมาย ปลดล็อกสร้าง Auto Extra ID ใน Rank 2-27
 */

export interface VaultPoolDeductions {
  upgradesUsed?: number;   // ยอดที่นำไปใช้อัปเกรด
  autoIdsUsed?: number;    // ยอดที่นำไปใช้สร้าง Auto ID (5.00 USDT / รหัส)
  extraIdsUsed?: number;   // ยอดที่นำไปใช้สร้าง Extra ID
}

export interface MainIdVaultBreakdown {
  mainId: number;
  currentRank: number;
  window5Start: number;
  window5End: number;
  window5Ranks: number[];
  upgradeRankPool: number;
  upgradeRankPoolGross?: number;
  upgradeRankPoolDeducted?: number;
  newAutoIdPool: number;
  newAutoIdPoolGross?: number;
  newAutoIdPoolDeducted?: number;
  newExtraIdPool: number;
  newExtraIdPoolGross?: number;
  newExtraIdPoolDeducted?: number;
  oldCarryoverVault: number;
  extraLeftVaultBonus: number;
  vaultByRank: Record<number, number>;
  totalAccumulatedVault: number;
  windowDescriptionTh: string;
  autoIdPoolDescriptionTh: string;
  extraIdPoolDescriptionTh: string;
}

export function calculateMainIdVaultPools(
  mainId: number,
  currentRank: number,
  vaultByRank: Record<number, number> = {},
  extraLeftVaultBonus: number = 0,
  isExtraOrCloneOfExtra: boolean = false,
  deductions?: VaultPoolDeductions
): MainIdVaultBreakdown {
  const safeRank = Math.max(1, Math.min(27, mainId === 1 ? 27 : (currentRank || 1)));
  const window5Start = Math.max(1, safeRank - 4);
  const window5End = safeRank;

  const window5Ranks: number[] = [];
  for (let r = window5Start; r <= window5End; r++) {
    window5Ranks.push(r);
  }

  // 1. Upgrade Rank Pool: ยอดสะสม Vault (35%) ของ 5 rank ย้อนหลัง
  // เงื่อนไข:
  // - เฉพาะไอดี ที่ ไม่ใช่ EXTRA ID, โคลนนิ่ง ของ EXTRA ID (ถ้าเป็น Extra หรือ Clone ของ Extra ยอดจะเป็น 0)
  // - rank < 27: รวมยอด UPGRADE_VAULT 5 Rank ย้อนหลัง ลบด้วยยอดที่นำไปใช้อัปเกรด (หากมี)
  // - rank == 27: ไม่ต้องหายอดละเป็น 0
  let upgradeRankPoolGross = 0;
  if (!isExtraOrCloneOfExtra && safeRank < 27) {
    for (const r of window5Ranks) {
      upgradeRankPoolGross += Number(vaultByRank[r] || 0);
    }
  }
  const upgradeRankPoolDeducted = Math.min(upgradeRankPoolGross, Number(deductions?.upgradesUsed || 0));
  const upgradeRankPool = Math.max(0, upgradeRankPoolGross - upgradeRankPoolDeducted);

  // 2. New Auto ID Pool:
  // เงื่อนไข:
  // - เฉพาะไอดีที่ "ไม่ใช่ EXTRA ID" และ "ไม่ใช่ โคลนนิ่งของ EXTRA ID"
  // - กรณี rank <= 8: กรอบ 5 Rank ย้อนหลัง [currentRank - 4 ... currentRank] ลบด้วยยอดสะสม Rank 1-3 (คือเอาเฉพาะ Rank 1..3 ที่ r < window5Start)
  // - กรณี rank >= 9: ยอดสะสม Rank 1-3 ทั้งหมด (เพราะหลุดกรอบ 5 Rank ย้อนหลังครบ 100%)
  // - หากมีการนำยอดไปใช้สร้าง Auto ID (5 USDT / รหัส) ให้หักลบยอดออกด้วย
  let newAutoIdPoolGross = 0;
  if (!isExtraOrCloneOfExtra) {
    for (let r = 1; r <= 3; r++) {
      if (safeRank >= 9 || r < window5Start) {
        newAutoIdPoolGross += Number(vaultByRank[r] || 0);
      }
    }
  }
  const newAutoIdPoolDeducted = Math.min(newAutoIdPoolGross, Number(deductions?.autoIdsUsed || 0));
  const newAutoIdPool = Math.max(0, newAutoIdPoolGross - newAutoIdPoolDeducted);

  // 3. New Extra ID Pool:
  // เงื่อนไข:
  // 1. รายได้เกิดจากไอดี Extra ID (โบนัสเม็ดซ้าย Extra ID)
  // 2. กรณี rank < 27: รวมยอดสะสม UPGRADE_VAULT (35%) ของ rank 4 ถึง 27 ลบด้วย Upgrade Pool (อยู่นอก 5 rank ย้อนหลัง)
  //    กรณี rank == 27: รวมยอดสะสม UPGRADE_VAULT (35%) ของ rank 4 ถึง 27 ทั้งหมด
  // - เฉพาะไอดีที่ ไม่ใช่ EXTRA ID และ ไม่ใช่ โคลนนิ่งของ EXTRA ID
  // - หากมีการนำยอดไปใช้สร้าง Extra ID ให้หักลบยอดออกด้วย
  let r4To27OutsideWindow = 0;
  if (!isExtraOrCloneOfExtra) {
    if (safeRank === 27) {
      for (let r = 4; r <= 27; r++) {
        r4To27OutsideWindow += Number(vaultByRank[r] || 0);
      }
    } else {
      for (let r = 4; r <= 27; r++) {
        if (r < window5Start || r > window5End) {
          r4To27OutsideWindow += Number(vaultByRank[r] || 0);
        }
      }
    }
  }

  const safeExtraBonus = isExtraOrCloneOfExtra ? 0 : Math.max(0, Number(extraLeftVaultBonus || 0));
  const hasExtraPoolEligibility =
    !isExtraOrCloneOfExtra &&
    (safeRank === 27 || window5Start > 4 || r4To27OutsideWindow > 0 || safeExtraBonus > 0);

  const newExtraIdPoolGross = Number(
    (r4To27OutsideWindow + safeExtraBonus).toFixed(4)
  );
  const newExtraIdPoolDeducted = Math.min(newExtraIdPoolGross, Number(deductions?.extraIdsUsed || 0));
  const newExtraIdPool = Math.max(0, newExtraIdPoolGross - newExtraIdPoolDeducted);

  const oldCarryoverVault = newExtraIdPool;

  let totalAccumulatedVault = safeExtraBonus;
  for (let r = 1; r <= 27; r++) {
    totalAccumulatedVault += Number(vaultByRank[r] || 0);
  }

  const windowDescriptionTh =
    window5Start === window5End
      ? `Rank ${window5Start}`
      : `Rank ${window5Start} - Rank ${window5End}`;

  const autoIdPoolDescriptionTh =
    window5Start <= 1
      ? 'ทุก Rank 1-3 อยู่ใน 5 rank ย้อนหลัง (ยังไม่มียอดที่หลุดกรอบ)'
      : window5Start >= 4
      ? 'Rank 1-3 ทั้งหมดหลุดจาก 5 rank ย้อนหลังแล้ว (สะสมเต็มจำนวนสำหรับออก Auto ID 5 USDT)'
      : `Rank 1 ถึง Rank ${window5Start - 1} ที่หลุดจาก 5 rank ย้อนหลัง`;

  const extraIdPoolDescriptionTh =
    safeRank === 27
      ? 'สถานะ Rank 27: รวมยอดเก่าคงเหลือสะสม + ยอด Vault ทั้งหมดใน Rank 4 ถึง 27 ก่อนสร้าง Extra ID'
      : window5Start <= 4 && !hasExtraPoolEligibility
      ? 'ยังไม่มี Rank 4-27 ที่หลุดจาก 5 rank ย้อนหลัง'
      : `รวมยอดเก่าคงเหลือสะสม (R4 ถึง R${Math.max(4, window5Start - 1)} ที่พ้นกรอบ 5 rank + เม็ดซ้าย Extra) ก่อนสร้าง Extra ID`;

  return {
    mainId,
    currentRank: safeRank,
    window5Start,
    window5End,
    window5Ranks,
    upgradeRankPool: Number(upgradeRankPool.toFixed(4)),
    upgradeRankPoolGross: Number(upgradeRankPoolGross.toFixed(4)),
    upgradeRankPoolDeducted: Number(upgradeRankPoolDeducted.toFixed(4)),
    newAutoIdPool: Number(newAutoIdPool.toFixed(4)),
    newAutoIdPoolGross: Number(newAutoIdPoolGross.toFixed(4)),
    newAutoIdPoolDeducted: Number(newAutoIdPoolDeducted.toFixed(4)),
    newExtraIdPool: Number(newExtraIdPool.toFixed(4)),
    newExtraIdPoolGross: Number(newExtraIdPoolGross.toFixed(4)),
    newExtraIdPoolDeducted: Number(newExtraIdPoolDeducted.toFixed(4)),
    oldCarryoverVault,
    extraLeftVaultBonus: Number(safeExtraBonus.toFixed(4)),
    vaultByRank,
    totalAccumulatedVault: Number(totalAccumulatedVault.toFixed(4)),
    windowDescriptionTh,
    autoIdPoolDescriptionTh,
    extraIdPoolDescriptionTh,
  };
}
