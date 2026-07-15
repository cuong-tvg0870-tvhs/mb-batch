import { Logger } from '@nestjs/common';
import { AdSet, Campaign, HighDemandPeriod } from 'facebook-nodejs-business-sdk';
import { parseMetaError } from '../../common/utils';
import {
  ceilToQuarter,
  floorToQuarter,
  metaTimeToUnix,
  nextClockUnix,
  wallClockToUnix,
} from './campaign-rule-tz.util';

/**
 * Thực thi action BUDGET_SCHEDULE_BUMP: đẩy `budget_schedule_specs` lên Meta cho
 * campaign (CBO) hoặc ad set (ABO). Meta tự bơm ngân sách theo khung giờ rồi revert.
 *
 * Giả định FacebookAdsApi.init(...) ĐÃ được gọi (service làm 1 lần trước khi chạy).
 */

const logger = new Logger('CampaignRuleExecutor');

export interface BudgetPeriod {
  timeStart?: string; // "YYYY-MM-DDTHH:mm" (local)
  timeEnd?: string;
  budgetValueType?: 'ABSOLUTE' | 'MULTIPLIER' | string;
  budgetValue?: number;
}

export interface BudgetScheduleSpec {
  time_start: number;
  time_end: number;
  budget_value: number;
  budget_value_type: string;
}

export interface ExecResult {
  ok: boolean;
  error?: any;
  metaTraceId?: string;
  // Id các HighDemandPeriod Meta trả về (để sau tắt/xoá).
  scheduleIds: string[];
}

/**
 * periods (từ task.params) → budget_schedule_specs của Meta.
 * QUAN TRỌNG: Meta yêu cầu `budget_value` là SỐ NGUYÊN → luôn gửi ABSOLUTE.
 *   %  (MULTIPLIER 1.5)  → ABSOLUTE = round(ngân_sách_hằng_ngày × 1.5) theo ngân
 *                          sách THẬT của đối tượng (`targetBudget`, minor units).
 *   Số tiền (ABSOLUTE)   → round(budget_value).
 * Mốc giờ "YYYY-MM-DDTHH:mm" được diễn giải theo MÚI GIỜ TKQC (`tz`, IANA) — Meta
 * chạy budget schedule theo timezone tài khoản quảng cáo, không theo tz server.
 * Bỏ qua period thiếu thời gian hợp lệ, hoặc MULTIPLIER mà không biết targetBudget.
 */
export function buildSpecs(
  periods: any,
  targetBudget?: number | null,
  tz?: string | null,
): BudgetScheduleSpec[] {
  if (!Array.isArray(periods)) return [];
  const specs: BudgetScheduleSpec[] = [];
  for (const period of periods) {
    if (!period) continue;
    // Căn mốc 15' (Meta bắt buộc): start làm tròn LÊN, end làm tròn XUỐNG.
    const timeStart = ceilToQuarter(wallClockToUnix(String(period.timeStart), tz));
    const timeEnd = floorToQuarter(wallClockToUnix(String(period.timeEnd), tz));
    if (!Number.isFinite(timeStart) || !Number.isFinite(timeEnd)) continue;
    if (timeEnd <= timeStart) continue; // khung rỗng sau khi căn mốc → bỏ

    const type = period.budgetValueType || 'ABSOLUTE';
    const rawValue = Number(period.budgetValue);
    if (!Number.isFinite(rawValue)) continue;

    let budgetValue: number;
    if (type === 'MULTIPLIER') {
      if (!targetBudget || targetBudget <= 0) continue; // không quy đổi được → bỏ qua
      budgetValue = Math.round(targetBudget * rawValue);
    } else {
      budgetValue = Math.round(rawValue);
    }
    if (!(budgetValue > 0)) continue;

    specs.push({
      time_start: timeStart,
      time_end: timeEnd,
      budget_value: budgetValue,
      budget_value_type: 'ABSOLUTE',
    });
  }
  return specs;
}

// ============================================================================
//  CHẾ ĐỘ "CUỐN CHIẾU" (ROLLING): khung ĐỘNG tính theo thời điểm rule nổ, nối đuôi
//  nhau (không overlap). Khác FIXED (khung lịch cố định user cấu hình sẵn).
// ============================================================================

export interface RollingConfig {
  leadMinutes?: number; // khung bắt đầu = now + lead (mặc định 15')
  windowMode?: 'DURATION' | 'UNTIL_CLOCK' | string;
  durationHours?: number; // DURATION: mỗi khung dài X giờ
  untilClock?: string; // UNTIL_CLOCK: nối tới mốc "HH:mm" (tz TKQC), vd "08:30"
  increaseType?: 'ABSOLUTE' | 'MULTIPLIER' | string;
  increaseValue?: number; // % (multiplier) hoặc số tiền (minor units)
  maxChainHours?: number | null; // chốt an toàn: tổng chuỗi ≤ X giờ tính từ khung đầu
  hardEndAt?: string | null; // "YYYY-MM-DDTHH:mm" (tz TKQC) — dừng tuyệt đối
}

// Khung budget schedule đang có trên Meta (mốc đã chuẩn hoá về unix seconds).
export interface LiveWindow {
  id: string;
  time_start: number;
  time_end: number;
}

export interface RollingBuildResult {
  spec?: BudgetScheduleSpec;
  skipReason?: string; // lý do không tạo khung (để log/hiển thị)
}

/**
 * Dựng 1 khung "cuốn chiếu" kế tiếp:
 *   start = max(now + lead, coveredUntil)  → nối liền sau khung của mình (end-to-end,
 *           không tự-overlap); end = start + X giờ HOẶC tới mốc giờ untilClock.
 *   Chốt an toàn theo maxChainHours (từ khung ĐẦU của chuỗi) + hardEndAt.
 *   Tránh overlap khung của NGƯỜI KHÁC (foreign): đẩy start qua khung đang phủ + cắt
 *   end tại khung foreign kế tiếp. end ≤ start → không tạo (đã tới biên/bị chặn).
 * Trả về 0 hoặc 1 spec (Meta yêu cầu budget_value là số nguyên → luôn ABSOLUTE).
 */
export function buildRollingSpec(
  rolling: RollingConfig,
  opts: {
    nowUnix: number;
    tz?: string | null;
    targetBudget?: number | null; // minor units (đọc LIVE từ Meta lúc execute)
    coveredUntil: number; // unix — cuối vùng phủ của khung "của mình" (≥ nowUnix)
    ownedWindows: LiveWindow[];
    foreignWindows: LiveWindow[];
  },
): RollingBuildResult {
  const lead = Math.max(0, Math.round(rolling.leadMinutes ?? 15)) * 60;
  // Nối LIỀN sau vùng phủ hiện có (start = coveredUntil nếu còn ở tương lai) → không
  // hở khung. Chưa có phủ → bắt đầu ở now + lead (Meta cần khung bắt đầu ở tương lai).
  let start = opts.coveredUntil > opts.nowUnix ? opts.coveredUntil : opts.nowUnix + lead;
  // Meta bắt mốc khung rơi đúng 00/15/30/45 → căn start LÊN NGAY để DURATION (start+X
  // giờ) cũng tính từ mốc chuẩn. (Khi nối sau coveredUntil/khung foreign — vốn đã do
  // Meta enforce đúng mốc — ceil là no-op.)
  start = ceilToQuarter(start);

  let end: number;
  if (rolling.windowMode === 'UNTIL_CLOCK') {
    // Nối tới mốc giờ KẾ TIẾP tính theo NOW. Khi vùng phủ đã chạm mốc → DỪNG (không
    // kéo sang mốc của ngày hôm sau, tránh tạo khung ~24h ngoài ý muốn).
    const target = nextClockUnix(String(rolling.untilClock ?? ''), opts.tz, opts.nowUnix);
    if (!Number.isFinite(target)) return { skipReason: 'until_clock_invalid' };
    if (opts.coveredUntil >= target) return { skipReason: 'reached_clock' };
    end = target;
  } else {
    const hours = Number(rolling.durationHours);
    if (!(hours > 0)) return { skipReason: 'duration_invalid' };
    end = start + Math.round(hours * 3600);
  }

  // Chốt an toàn: tổng chuỗi (từ khung đầu của mình) ≤ maxChainHours; và ≤ hardEndAt.
  const chainStart = opts.ownedWindows.length
    ? Math.min(...opts.ownedWindows.map((w) => w.time_start))
    : start;
  if (rolling.maxChainHours && rolling.maxChainHours > 0) {
    end = Math.min(end, chainStart + Math.round(rolling.maxChainHours * 3600));
  }
  if (rolling.hardEndAt) {
    const hardEnd = wallClockToUnix(String(rolling.hardEndAt), opts.tz);
    if (Number.isFinite(hardEnd)) end = Math.min(end, hardEnd);
  }

  // Né khung của người khác (Meta chặn overlap): đẩy start qua khung foreign đang phủ,
  // rồi cắt end tại mốc bắt đầu của khung foreign gần nhất phía sau.
  const foreign = [...opts.foreignWindows].sort((a, b) => a.time_start - b.time_start);
  for (const f of foreign) {
    if (f.time_start <= start && start < f.time_end) start = f.time_end;
  }
  for (const f of foreign) {
    if (start < f.time_start && f.time_start < end) {
      end = f.time_start;
      break;
    }
  }

  // Căn mốc 15' LẦN CUỐI: end làm tròn XUỐNG (UNTIL_CLOCK/maxChainHours/hardEndAt/né
  // foreign có thể tạo phút lẻ), start ceil lại phòng hờ. Meta chặn nếu lệch mốc.
  start = ceilToQuarter(start);
  end = floorToQuarter(end);

  if (end <= start) return { skipReason: 'boundary_reached' };

  // Mức tăng: % (MULTIPLIER) quy đổi theo ngân sách THẬT (live) → số tiền tuyệt đối.
  const raw = Number(rolling.increaseValue);
  if (!Number.isFinite(raw)) return { skipReason: 'increase_invalid' };
  let budgetValue: number;
  if ((rolling.increaseType || 'MULTIPLIER') === 'MULTIPLIER') {
    if (!opts.targetBudget || opts.targetBudget <= 0) return { skipReason: 'no_budget' };
    budgetValue = Math.round(opts.targetBudget * raw);
  } else {
    budgetValue = Math.round(raw);
  }
  if (!(budgetValue > 0)) return { skipReason: 'budget_zero' };

  return {
    spec: {
      time_start: start,
      time_end: end,
      budget_value: budgetValue,
      budget_value_type: 'ABSOLUTE',
    },
  };
}

/** Đọc các budget schedule đang có trên Meta của 1 entity (rỗng nếu lỗi/không có). */
export async function fetchBudgetSchedules(
  level: 'CAMPAIGN' | 'ADSET' | string,
  entityId: string,
): Promise<LiveWindow[]> {
  try {
    const target = (level === 'CAMPAIGN' ? new Campaign(entityId) : new AdSet(entityId)) as unknown as {
      getBudgetSchedules: (fields: string[], params: object) => Promise<unknown[]>;
    };
    const cursor = await target.getBudgetSchedules(['id', 'time_start', 'time_end'], { limit: 100 });
    const rows = ((cursor as { _data?: any }[]) || []).map((s) => (s as { _data?: any })._data ?? s);
    return rows
      .filter((r) => r && r.id != null)
      .map((r) => ({
        id: String(r.id),
        time_start: metaTimeToUnix(r.time_start),
        time_end: metaTimeToUnix(r.time_end),
      }));
  } catch (e) {
    logger.warn(`Đọc budget schedules ${level} ${entityId} lỗi: ${parseMetaError(e).message}`);
    return [];
  }
}

/** Đọc ngân sách hằng ngày/trọn đời LIVE từ Meta (minor units). null nếu lỗi/không có. */
export async function fetchLiveBudget(
  level: 'CAMPAIGN' | 'ADSET' | string,
  entityId: string,
): Promise<{ dailyBudget: number | null; lifetimeBudget: number | null }> {
  try {
    const target = level === 'CAMPAIGN' ? new Campaign(entityId) : new AdSet(entityId);
    await (target as unknown as { read: (f: string[]) => Promise<unknown> }).read([
      'daily_budget',
      'lifetime_budget',
    ]);
    const data =
      (target as { _data?: any })._data ?? (target as unknown as Record<string, unknown>);
    const d = Number((data as any)?.daily_budget);
    const l = Number((data as any)?.lifetime_budget);
    return {
      dailyBudget: Number.isFinite(d) && d > 0 ? d : null,
      lifetimeBudget: Number.isFinite(l) && l > 0 ? l : null,
    };
  } catch (e) {
    logger.warn(`Đọc ngân sách live ${level} ${entityId} lỗi: ${parseMetaError(e).message}`);
    return { dailyBudget: null, lifetimeBudget: null };
  }
}

/** Xoá các budget schedule (HighDemandPeriod) theo id trên Meta. Best-effort. */
export async function deleteBudgetSchedules(
  ids: string[],
): Promise<{ removed: number; errors: string[] }> {
  let removed = 0;
  const errors: string[] = [];
  for (const id of ids) {
    try {
      await new HighDemandPeriod(id).delete([]);
      removed += 1;
    } catch (e) {
      errors.push(parseMetaError(e).message);
    }
  }
  return { removed, errors };
}

/**
 * Đẩy specs lên Meta theo 2 pha: validate_only rồi commit. Trả {ok,error,metaTraceId}.
 * Bắt mọi lỗi (rate-limit/permission/validate) → ok=false + error đã parse.
 */
export async function executeBudgetSchedule(
  level: 'CAMPAIGN' | 'ADSET' | string,
  entityId: string,
  specs: BudgetScheduleSpec[],
): Promise<ExecResult> {
  if (!specs || specs.length === 0) {
    return { ok: false, error: { message: 'Không có budget_schedule_specs để đẩy' }, scheduleIds: [] };
  }

  if (level !== 'CAMPAIGN' && level !== 'ADSET') {
    return {
      ok: false,
      error: { message: `Level ${level} không hỗ trợ BUDGET_SCHEDULE_BUMP` },
      scheduleIds: [],
    };
  }

  // Với entity ĐÃ TỒN TẠI phải TẠO từng budget schedule qua edge
  // /{id}/budget_schedules (SDK createBudgetSchedule). Set field budget_schedule_specs
  // qua update chỉ hợp lệ lúc CREATE campaign → nếu không sẽ "Invalid parameter".
  const scheduleIds: string[] = [];
  try {
    for (const spec of specs) {
      const params = {
        time_start: spec.time_start,
        time_end: spec.time_end,
        budget_value: spec.budget_value,
        budget_value_type: spec.budget_value_type,
      };
      const hdp =
        level === 'CAMPAIGN'
          ? await new Campaign(entityId).createBudgetSchedule([], params)
          : await new AdSet(entityId).createBudgetSchedule([], params);
      const sid = (hdp as { id?: string; _data?: { id?: string } })?.id ?? (hdp as { _data?: { id?: string } })?._data?.id;
      if (sid) scheduleIds.push(String(sid));
    }

    // Bật cờ tổng để Meta tick "Schedule budget increases" + thực sự áp dụng
    // (tạo khung qua edge không tự bật). Best-effort — lỗi không làm hỏng kết quả.
    if (scheduleIds.length > 0) {
      try {
        if (level === 'CAMPAIGN')
          await new Campaign(entityId).update([], { is_budget_schedule_enabled: true });
        else await new AdSet(entityId).update([], { is_budget_schedule_enabled: true });
      } catch (e) {
        logger.warn(
          `Bật is_budget_schedule_enabled ${level} ${entityId} lỗi: ${parseMetaError(e).message}`,
        );
      }
    }

    return { ok: true, scheduleIds };
  } catch (error) {
    const parsed = parseMetaError(error);
    const userMsg =
      (error as { response?: { error_user_msg?: string; error_user_title?: string } })?.response
        ?.error_user_msg ||
      (error as { response?: { error_user_title?: string } })?.response?.error_user_title ||
      parsed.message;
    logger.warn(
      `Đẩy budget_schedule ${level} ${entityId} thất bại: ${userMsg}` +
        (parsed.fbtrace_id ? ` (trace ${parsed.fbtrace_id})` : ''),
    );
    return {
      ok: false,
      error: {
        message: userMsg,
        code: parsed.code,
        subcode: parsed.subcode,
        type: parsed.type,
        blameFields: parsed.blameFields,
      },
      metaTraceId: parsed.fbtrace_id,
      // Có thể vài khung đã tạo được trước khi lỗi — vẫn lưu để tắt được.
      scheduleIds,
    };
  }
}
