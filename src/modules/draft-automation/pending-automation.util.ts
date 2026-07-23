/**
 * "Tự động hoá sau khi lên Camp" — hợp đồng dữ liệu (MIRROR của mb-ads
 * src/modules/draft-campaign/pending-automation.util.ts). Sửa ở đây phải sửa cả bên kia.
 */

export type PendingAutomationKind = 'SCHEDULE' | 'RULE';
export type PendingAutomationLevel = 'CAMPAIGN' | 'ADSET';

export interface PendingBudgetPeriod {
  timeStart: string; // "YYYY-MM-DDTHH:mm" giờ tường (timezone TKQC)
  timeEnd: string;
  budgetValueType: 'ABSOLUTE' | 'MULTIPLIER';
  budgetValue: number;
}

export interface PendingAutomationEntry {
  uid: string;
  kind: PendingAutomationKind;
  level: PendingAutomationLevel; // gợi ý lúc cấu hình; cấp THẬT suy từ campaign_CBO lúc áp
  name?: string;
  periods?: PendingBudgetPeriod[];
  scheduleEnabled?: boolean;
  rule?: Record<string, any>;
  // BACKEND ghi lúc publish; FE KHÔNG gửi (strip lúc lưu):
  appliedRuleIds?: string[];
  appliedAdSetIds?: string[]; // adSet meta_id (ABO) hoặc '__CAMPAIGN__' (CBO) đã phủ
  /** Id HighDemandPeriod (SCHEDULE) đã tạo trên Meta — OWNERSHIP để retry xoá ĐÚNG lịch của
   *  mình (không đụng lịch tay cùng khung nhưng khác mức). */
  appliedMetaScheduleIds?: string[];
  appliedAt?: string;
  appliedError?: string;
}

export interface PendingAutomationConfig {
  version: number;
  entries: PendingAutomationEntry[];
}

export const PENDING_AUTOMATION_VERSION = 1;
export const CAMPAIGN_TARGET_SENTINEL = '__CAMPAIGN__';

const VALUE_TYPES = new Set(['ABSOLUTE', 'MULTIPLIER']);

const MIN_WINDOW_MS = 15 * 60 * 1000; // Meta: mỗi khung tối thiểu 15 phút

function normalizePeriods(raw: any): PendingBudgetPeriod[] {
  if (!Array.isArray(raw)) return [];
  const collected: Array<PendingBudgetPeriod & { _s: number; _e: number }> = [];
  for (const p of raw) {
    if (!p || typeof p !== 'object') continue;
    const timeStart = String(p.timeStart ?? '').trim();
    const timeEnd = String(p.timeEnd ?? '').trim();
    const budgetValueType = String(p.budgetValueType ?? '').trim();
    const budgetValue = Number(p.budgetValue);
    if (!timeStart || !timeEnd) continue;
    const startMs = Date.parse(timeStart);
    const endMs = Date.parse(timeEnd);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) continue;
    if (endMs - startMs < MIN_WINDOW_MS) continue; // ≥15 phút (cũng chặn end<=start)
    if (!VALUE_TYPES.has(budgetValueType)) continue;
    if (!Number.isFinite(budgetValue) || budgetValue <= 0) continue;
    if (budgetValueType === 'MULTIPLIER' && !(budgetValue > 1)) continue;
    collected.push({
      timeStart,
      timeEnd,
      budgetValueType: budgetValueType as 'ABSOLUTE' | 'MULTIPLIER',
      budgetValue,
      _s: startMs,
      _e: endMs,
    });
  }
  collected.sort((a, b) => a._s - b._s);
  const out: PendingBudgetPeriod[] = [];
  let lastEnd = -Infinity;
  for (const c of collected) {
    if (c._s < lastEnd) continue; // chồng chéo → bỏ
    out.push({
      timeStart: c.timeStart,
      timeEnd: c.timeEnd,
      budgetValueType: c.budgetValueType,
      budgetValue: c.budgetValue,
    });
    lastEnd = c._e;
  }
  return out;
}

/**
 * Chuẩn hoá + LỌC config: bỏ entry không thể vật chất hoá (SCHEDULE không có khung giờ; RULE
 * không có task HOẶC không có schedule → runner không nhặt). `keepApplied`=false (FE input)
 * strip trạng thái đã-áp; =true khi đọc từ DB.
 */
export function normalizePendingAutomation(
  raw: any,
  keepApplied = false,
): PendingAutomationConfig | null {
  if (!raw || typeof raw !== 'object') return null;
  const rawEntries = Array.isArray(raw.entries) ? raw.entries : [];
  const seen = new Set<string>();
  const entries: PendingAutomationEntry[] = [];
  let scheduleTaken = false; // mỗi scope chỉ 1 lịch (parity mb-ads) — bỏ SCHEDULE thứ 2 trở đi

  rawEntries.forEach((e: any, idx: number) => {
    if (!e || typeof e !== 'object') return;
    const kind: PendingAutomationKind = e.kind === 'RULE' ? 'RULE' : 'SCHEDULE';
    const level: PendingAutomationLevel =
      e.level === 'ADSET' ? 'ADSET' : 'CAMPAIGN';
    let uid =
      typeof e.uid === 'string' && e.uid.trim() ? e.uid.trim() : `entry-${idx}`;
    if (seen.has(uid)) uid = `${uid}-${idx}`;

    const entry: PendingAutomationEntry = { uid, kind, level };
    if (typeof e.name === 'string' && e.name.trim()) entry.name = e.name.trim();

    if (kind === 'SCHEDULE') {
      if (scheduleTaken) return;
      const periods = normalizePeriods(e.periods);
      if (periods.length === 0) return;
      entry.periods = periods;
      entry.scheduleEnabled = e.scheduleEnabled !== false;
      scheduleTaken = true;
    } else {
      const rule = e.rule;
      const tasks = rule && Array.isArray(rule.tasks) ? rule.tasks : [];
      const hasSchedule =
        !!rule &&
        typeof rule === 'object' &&
        !!rule.schedule &&
        typeof rule.schedule === 'object';
      if (!rule || typeof rule !== 'object' || tasks.length === 0 || !hasSchedule)
        return;
      entry.rule = rule;
    }

    if (keepApplied) {
      if (Array.isArray(e.appliedRuleIds) && e.appliedRuleIds.length) {
        entry.appliedRuleIds = e.appliedRuleIds.map((x: any) => String(x));
      }
      if (Array.isArray(e.appliedAdSetIds) && e.appliedAdSetIds.length) {
        entry.appliedAdSetIds = e.appliedAdSetIds.map((x: any) => String(x));
      }
      if (
        Array.isArray(e.appliedMetaScheduleIds) &&
        e.appliedMetaScheduleIds.length
      ) {
        entry.appliedMetaScheduleIds = e.appliedMetaScheduleIds.map((x: any) =>
          String(x),
        );
      }
      if (typeof e.appliedAt === 'string') entry.appliedAt = e.appliedAt;
      if (typeof e.appliedError === 'string') entry.appliedError = e.appliedError;
    }

    seen.add(uid);
    entries.push(entry);
  });

  if (entries.length === 0) return null;
  return { version: PENDING_AUTOMATION_VERSION, entries };
}
