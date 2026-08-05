import { Logger } from '@nestjs/common';
import { AdSet, Campaign, HighDemandPeriod } from 'facebook-nodejs-business-sdk';
import { parseMetaError } from '../../common/utils';
import {
  ceilToQuarter,
  floorToQuarter,
  metaTimeToUnix,
  nextClockUnix,
  QUARTER_SEC,
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

// Trần % tăng của Meta: tổng ngân sách trong khung ≤ 8× ngân sách gốc ⇒ mức TĂNG
// tối đa = +700%. Kẹp để không bị Meta từ chối (và chặn giá trị vô lý).
export const META_MAX_INCREASE_PCT = 700;

/**
 * Meta: "High demand periods must be at 3 hours in length or more." — khung ngắn hơn bị
 * từ chối (code 100 / subcode 3858094: "Vui lòng nhập một khoảng thời gian ít nhất là 3
 * giờ"). Xem developers.facebook.com/docs/marketing-api/reference/high-demand-period.
 */
export const META_MIN_HDP_SEC = 3 * 3600;

// Khung FIXED nằm trong QUÁ KHỨ bị Meta từ chối → kẹp start về "now + lead" trước khi
// gửi. 15' là mức tối thiểu an toàn (bằng lead mặc định của ROLLING) và cũng đúng mốc 15'.
const FIXED_MIN_LEAD_SEC = 15 * 60;

/**
 * Lead TỐI THIỂU cho khung ROLLING, kể cả khi nối tiếp khung cũ.
 *
 * VÌ SAO (lỗi prod 11:00 04/08/2026 — Meta: "Ngày bắt đầu của đợt cao điểm cho lịch ngân
 * sách đã qua rồi"): nhánh nối chuỗi lấy `start = coveredUntil` thẳng, không lead. Runner
 * chỉ nối khi `coveredUntil - now <= lead` nên `start` có thể chỉ cách hiện tại vài giây
 * (khung cũ hết đúng 11:00, tick nổ 10:5x) — và giữa lúc dựng spec với lúc Meta thực nhận
 * còn nhiều lời gọi Graph nối tiếp. Tới tay Meta thì `time_start` đã thành QUÁ KHỨ ⇒ bị
 * từ chối, mất trọn lượt bơm.
 *
 * Đánh đổi: kẹp start có thể để HỞ tối đa `lead` phút giữa khung cũ và khung mới. Chấp
 * nhận được — thà hở 15' còn hơn Meta từ chối cả khung 3+ giờ.
 */
const ROLLING_MIN_LEAD_SEC = 15 * 60;

/**
 * Quy đổi giá trị nội bộ → `budget_value` + `budget_value_type` mà Meta thực nhận.
 *
 * MẤU CHỐT: Meta hiểu `budget_value` là KHOẢN TĂNG THÊM (increase, cộng lên ngân sách
 * gốc), KHÔNG phải tổng ngân sách mới. (Đã kiểm chứng bằng chi tiêu thực + doc API
 * "Actual budget increase" + UI Meta "chi tiêu thêm …".)
 *   - ABSOLUTE  : rawValue là SỐ TIỀN cộng thêm (minor units) → round & gửi thẳng.
 *   - MULTIPLIER: convention nội bộ lưu HỆ SỐ thập phân (2.01 = ×2.01). Meta nhận % TĂNG
 *                 dạng SỐ NGUYÊN ⇒ gửi `budget_value = round((hệ_số − 1) × 100)` với
 *                 type='MULTIPLIER'. Meta tự áp % lên ngân sách LIVE và tự revert —
 *                 KHÔNG cần đọc ngân sách gốc, KHÔNG bị đóng băng thành tiền tươi.
 * Trả null nếu không phải mức tăng hợp lệ (≤ ×1 / ≤ 0đ).
 */
export function toMetaIncrease(
  type: string,
  rawValue: number,
): { budget_value: number; budget_value_type: 'ABSOLUTE' | 'MULTIPLIER' } | null {
  if (!Number.isFinite(rawValue)) return null;
  if (type === 'MULTIPLIER') {
    const pct = Math.round((rawValue - 1) * 100); // 2.01 → 101 (%), Meta +101% ⇒ ×2.01
    if (!(pct >= 1)) return null; // ≤ ×1 → không tăng
    return {
      budget_value: Math.min(pct, META_MAX_INCREASE_PCT),
      budget_value_type: 'MULTIPLIER',
    };
  }
  const abs = Math.round(rawValue);
  if (!(abs > 0)) return null;
  return { budget_value: abs, budget_value_type: 'ABSOLUTE' };
}

/**
 * periods (từ task.params) → budget_schedule_specs của Meta.
 * MULTIPLIER (%) gửi thẳng dạng % nguyên; ABSOLUTE gửi số tiền cộng thêm — xem
 * `toMetaIncrease`. `targetBudget` KHÔNG còn cần cho MULTIPLIER (Meta tự áp % lên
 * ngân sách live) — giữ tham số cho tương thích call-site.
 * Mốc giờ "YYYY-MM-DDTHH:mm" được diễn giải theo MÚI GIỜ TKQC (`tz`, IANA) — Meta
 * chạy budget schedule theo timezone tài khoản quảng cáo, không theo tz server.
 */
export function buildSpecs(
  periods: any,
  targetBudget?: number | null,
  tz?: string | null,
): BudgetScheduleSpec[] {
  if (!Array.isArray(periods)) return [];
  const specs: BudgetScheduleSpec[] = [];
  const nowUnix = Math.floor(Date.now() / 1000);
  const earliestStart = ceilToQuarter(nowUnix + FIXED_MIN_LEAD_SEC);
  for (const period of periods) {
    if (!period) continue;
    // Căn mốc 15' (Meta bắt buộc): start làm tròn LÊN, end làm tròn XUỐNG.
    let timeStart = ceilToQuarter(wallClockToUnix(String(period.timeStart), tz));
    const timeEnd = floorToQuarter(wallClockToUnix(String(period.timeEnd), tz));
    if (!Number.isFinite(timeStart) || !Number.isFinite(timeEnd)) continue;
    if (timeEnd <= timeStart) continue; // khung rỗng sau khi căn mốc → bỏ
    // Khung cấu hình sẵn có thể đã trôi vào QUÁ KHỨ khi rule nổ (hoặc lúc kích hoạt
    // nháp) — trước đây vẫn gửi thẳng lên Meta và bị từ chối. Kẹp về mốc gần nhất ở
    // tương lai rồi kiểm tra lại độ dài.
    if (timeStart < earliestStart) timeStart = earliestStart;
    // Meta yêu cầu khung ≥ 3 giờ → khung ngắn hơn (hoặc bị kẹp cụt) chắc chắn lỗi → bỏ.
    if (timeEnd - timeStart < META_MIN_HDP_SEC) continue;

    const type = period.budgetValueType || 'ABSOLUTE';
    const conv = toMetaIncrease(type, Number(period.budgetValue));
    if (!conv) continue;

    specs.push({
      time_start: timeStart,
      time_end: timeEnd,
      ...conv,
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
  // Độ dài khung còn lại (giây) khi bị bỏ vì ngắn hơn mức tối thiểu của Meta — để
  // runner ghi thông báo cụ thể "còn X phút".
  availableSec?: number;
}

/**
 * Mốc bắt đầu của CHUỖI KHUNG LIÊN TỤC đang phủ tới hiện tại — dùng để tính trần
 * `maxChainHours` cho đúng nghĩa "một đợt tăng liền mạch".
 *
 * BUG đã sửa: trước đây lấy min(time_start) của MỌI khung "của mình" còn nằm trên
 * Meta, KỂ CẢ khung đã HẾT HẠN từ nhiều ngày trước (Meta giữ khung hết hạn lại làm
 * lịch sử, không tự xoá). Hệ quả: trần 24h bị neo mãi vào khung ĐẦU TIÊN từng tạo →
 * sau 24h là rule "chết" vĩnh viễn, qua ngày mới cũng KHÔNG reset (dù chuỗi đã ĐỨT,
 * không còn khung nào phủ hiện tại) → mọi tick đạt điều kiện đều `boundary_reached`.
 *
 * Cách đúng: chỉ tính chuỗi khung NỐI LIỀN nhau phủ tới `now`. Không còn khung nào
 * còn hiệu lực (time_end > now) → chuỗi đã đứt → trả `fallback` (mốc khung mới) để
 * trần tính lại từ đầu. Khung hết hạn nhưng NỐI LIỀN với khung sống vẫn được gộp, để
 * trần vẫn chặn đúng cho một đợt chạy liên tục dài hơn maxChainHours.
 */
export function activeChainStart(
  ownedWindows: LiveWindow[],
  nowUnix: number,
  fallback: number,
): number {
  const sorted = [...ownedWindows].sort((a, b) => a.time_start - b.time_start);
  // Khung sống sớm nhất (còn phủ hiện tại/tương lai). Không có → chuỗi đã đứt.
  const firstLive = sorted.findIndex((w) => w.time_end > nowUnix);
  if (firstLive < 0) return fallback;
  let chainStart = sorted[firstLive].time_start;
  // Lùi qua khung phía trước: gộp nếu NỐI LIỀN (end chạm start, cho lệch ≤ 15' do căn
  // mốc); gặp khoảng hở → chuỗi trước đó không liên tục, dừng.
  for (let i = firstLive - 1; i >= 0; i--) {
    if (sorted[i].time_end + QUARTER_SEC >= chainStart) {
      chainStart = Math.min(chainStart, sorted[i].time_start);
    } else {
      break;
    }
  }
  return chainStart;
}

/**
 * Dựng 1 khung "cuốn chiếu" kế tiếp:
 *   start = max(coveredUntil, now + ROLLING_MIN_LEAD_SEC)  → nối liền sau khung của mình
 *           (end-to-end, không tự-overlap) nhưng LUÔN ở tương lai đủ xa để Meta không từ
 *           chối; end = start + X giờ HOẶC tới mốc giờ untilClock.
 *   `opts.nowUnix` PHẢI là đồng hồ TƯƠI tại thời điểm dựng spec, không phải mốc đầu tick.
 *   Chốt an toàn theo maxChainHours (từ khung ĐẦU của chuỗi) + hardEndAt.
 *   Tránh overlap khung của NGƯỜI KHÁC (foreign): CHỐT start (đẩy qua khung đang phủ)
 *   TRƯỚC rồi mới tính end, sau đó cắt end tại khung foreign kế tiếp. end ≤ start →
 *   không tạo (đã tới biên/bị chặn); khung < 3 giờ → không tạo (Meta từ chối).
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
  // KẸP về tương lai: `coveredUntil` có thể chỉ cách now vài giây (khung cũ vừa hết) →
  // Meta từ chối vì "time_start đã qua". Xem ROLLING_MIN_LEAD_SEC. Phải kẹp TRƯỚC vòng
  // né khung foreign bên dưới, để mốc sau khi kẹp vẫn được kiểm tra overlap.
  const minStart = opts.nowUnix + ROLLING_MIN_LEAD_SEC;
  if (start < minStart) start = minStart;
  // Meta bắt mốc khung rơi đúng 00/15/30/45 → căn start LÊN NGAY để DURATION (start+X
  // giờ) cũng tính từ mốc chuẩn. (Khi nối sau coveredUntil/khung foreign — vốn đã do
  // Meta enforce đúng mốc — ceil là no-op.)
  start = ceilToQuarter(start);

  // ---- CHỐT `start` TRƯỚC, tính `end` SAU ----
  // BUG đã sửa (khung 15' → Meta lỗi 3858094 "ít nhất 3 giờ"): trước đây `end` được tính
  // từ `start` CŨ rồi mới đẩy `start` qua khung của người khác (foreign) — start nhảy tới
  // nhưng end giữ nguyên nên khung bị cụt (vd camp có khung tay 20:00→00:00, rule nổ 21:00
  // → start 21:15/end 00:15, foreign đẩy start:=00:00 ⇒ khung còn 15 phút).
  // Né khung foreign NGAY tại đây (Meta chặn overlap). Lặp cho tới khi start không còn
  // rơi vào khung nào — vì đẩy qua khung này có thể rơi thẳng vào khung kề sát phía sau.
  // Mỗi vòng start tăng nghiêm ngặt và vượt hẳn 1 khung ⇒ tối đa foreign.length + 1 vòng.
  const foreign = [...opts.foreignWindows].sort((a, b) => a.time_start - b.time_start);
  for (let guard = 0; guard <= foreign.length; guard++) {
    const covering = foreign.find((f) => f.time_start <= start && start < f.time_end);
    if (!covering) break;
    start = ceilToQuarter(covering.time_end);
  }

  let end: number;
  if (rolling.windowMode === 'UNTIL_CLOCK') {
    // Nối tới mốc giờ KẾ TIẾP tính theo NOW. Khi vùng phủ đã chạm mốc → DỪNG (không
    // kéo sang mốc của ngày hôm sau, tránh tạo khung ~24h ngoài ý muốn).
    let target = nextClockUnix(String(rolling.untilClock ?? ''), opts.tz, opts.nowUnix);
    if (!Number.isFinite(target)) return { skipReason: 'until_clock_invalid' };
    if (opts.coveredUntil >= target) return { skipReason: 'reached_clock' };
    // `target` tính theo NOW nên KHÔNG liên quan tới `start`: tick nổ sát mốc (hoặc start
    // bị đẩy qua khung foreign) ⇒ khung ngắn hơn 3 giờ, Meta từ chối. Xử lý: NỐI TỚI MỐC
    // CỦA NGÀY KẾ TIẾP. Ít bất ngờ nhất vì user cấu hình "bơm cho tới HH:mm" — bỏ hẳn lượt
    // này mới là mất tác dụng; và khung dài vẫn bị maxChainHours/hardEndAt cắt ở dưới
    // (cắt xuống dưới 3h thì guard cuối cùng sẽ SKIP, không đẩy khung lố lên Meta).
    for (let guard = 0; guard < 2 && target - start < META_MIN_HDP_SEC; guard++) {
      target = nextClockUnix(String(rolling.untilClock ?? ''), opts.tz, target);
    }
    end = target;
  } else {
    const hours = Number(rolling.durationHours);
    if (!(hours > 0)) return { skipReason: 'duration_invalid' };
    end = start + Math.round(hours * 3600);
  }

  // Chốt an toàn: tổng chuỗi (từ khung đầu của mình) ≤ maxChainHours; và ≤ hardEndAt.
  // Mốc đầu chuỗi = khung đầu của chuỗi LIÊN TỤC còn hiệu lực (KHÔNG tính khung hết
  // hạn đã đứt chuỗi — nếu không trần 24h bị neo mãi vào khung cũ, xem activeChainStart).
  const chainStart = activeChainStart(opts.ownedWindows, opts.nowUnix, start);
  if (rolling.maxChainHours && rolling.maxChainHours > 0) {
    end = Math.min(end, chainStart + Math.round(rolling.maxChainHours * 3600));
  }
  if (rolling.hardEndAt) {
    const hardEnd = wallClockToUnix(String(rolling.hardEndAt), opts.tz);
    if (Number.isFinite(hardEnd)) end = Math.min(end, hardEnd);
  }

  // Né khung của người khác (Meta chặn overlap): start đã được đẩy qua khung foreign
  // đang phủ ở TRÊN (trước khi tính end) — ở đây chỉ còn cắt end tại mốc bắt đầu của
  // khung foreign gần nhất phía sau.
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

  // Ngưỡng 3 giờ của Meta — kiểm tra SAU CÙNG, vì cả 4 nguồn phía trên (maxChainHours,
  // hardEndAt, cắt tại khung foreign kế tiếp, floorToQuarter) đều có thể làm khung cụt.
  // Bỏ lượt này (SKIPPED, không phải lỗi): lượt sau vùng phủ/biên đổi thì nối được.
  if (end - start < META_MIN_HDP_SEC) {
    return { skipReason: 'below_min_duration_3h', availableSec: end - start };
  }

  // Mức tăng: MULTIPLIER (%) gửi thẳng dạng % nguyên; ABSOLUTE là số tiền cộng thêm.
  // Meta tự áp % lên ngân sách LIVE và tự revert → KHÔNG cần targetBudget cho MULTIPLIER.
  const conv = toMetaIncrease(
    rolling.increaseType || 'MULTIPLIER',
    Number(rolling.increaseValue),
  );
  if (!conv) return { skipReason: 'increase_invalid' };

  return {
    spec: {
      time_start: start,
      time_end: end,
      ...conv,
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

/**
 * Như fetchBudgetSchedules nhưng NÉM LỖI nếu đọc thất bại (không nuốt về []). Dùng khi cần
 * reconcile an toàn: đọc-lỗi mà coi như rỗng rồi tạo mới → dễ nhân đôi lịch. Caller nên
 * abort + cảnh báo nếu hàm này ném.
 */
export interface LiveSchedule extends LiveWindow {
  budget_value: number | null;
  budget_value_type: string | null;
}

export async function fetchBudgetSchedulesStrict(
  level: 'CAMPAIGN' | 'ADSET' | string,
  entityId: string,
): Promise<LiveSchedule[]> {
  const target = (level === 'CAMPAIGN'
    ? new Campaign(entityId)
    : new AdSet(entityId)) as unknown as {
    getBudgetSchedules: (fields: string[], params: object) => Promise<unknown[]>;
  };
  const cursor = await target.getBudgetSchedules(
    ['id', 'time_start', 'time_end', 'budget_value', 'budget_value_type'],
    { limit: 100 },
  );
  const rows = ((cursor as { _data?: any }[]) || []).map(
    (s) => (s as { _data?: any })._data ?? s,
  );
  return rows
    .filter((r) => r && r.id != null)
    .map((r) => ({
      id: String(r.id),
      time_start: metaTimeToUnix(r.time_start),
      time_end: metaTimeToUnix(r.time_end),
      budget_value:
        r.budget_value != null && Number.isFinite(Number(r.budget_value))
          ? Number(r.budget_value)
          : null,
      budget_value_type: r.budget_value_type != null ? String(r.budget_value_type) : null,
    }));
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

/**
 * Xoá các budget schedule (HighDemandPeriod) theo id trên Meta. Best-effort: KHÔNG throw,
 * trả kết quả từng id. `failedIds` = các id XOÁ KHÔNG THÀNH CÔNG (vẫn còn LIVE trên Meta) →
 * caller phải GIỮ ownership để retry sau còn dọn (không bỏ rơi lịch sót).
 */
export async function deleteBudgetSchedules(
  ids: string[],
): Promise<{ removed: number; errors: string[]; failedIds: string[] }> {
  let removed = 0;
  const errors: string[] = [];
  const failedIds: string[] = [];
  for (const id of ids) {
    try {
      await new HighDemandPeriod(id).delete([]);
      removed += 1;
    } catch (e) {
      errors.push(parseMetaError(e).message);
      failedIds.push(id);
    }
  }
  return { removed, errors, failedIds };
}

/**
 * Đẩy specs lên Meta theo 2 pha: validate_only rồi commit. Trả {ok,error,metaTraceId}.
 * Bắt mọi lỗi (rate-limit/permission/validate) → ok=false + error đã parse.
 */
export async function executeBudgetSchedule(
  level: 'CAMPAIGN' | 'ADSET' | string,
  entityId: string,
  specs: BudgetScheduleSpec[],
  // manageToggle: có TỰ bật `is_budget_schedule_enabled=true` sau khi tạo khung không.
  //   - true (mặc định): runner "gửi ngay" — tạo xong bật luôn (hành vi cũ).
  //   - false: activator pending-automation gọi — activator là NƠI DUY NHẤT đặt cờ (theo
  //     đúng scheduleEnabled), nên ở đây KHÔNG đụng cờ để tránh bật/tắt hai lần (bật nhầm
  //     lịch khác trên entity khi cấu hình mong muốn là TẮT).
  manageToggle = true,
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
    // CHỈ khi manageToggle=true (runner); activator tự đặt cờ theo scheduleEnabled.
    if (manageToggle && scheduleIds.length > 0) {
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
