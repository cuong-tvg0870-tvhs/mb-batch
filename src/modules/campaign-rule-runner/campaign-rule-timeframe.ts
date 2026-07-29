import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import { normalizeAccountTz } from './campaign-rule-tz.util';

dayjs.extend(utc);
dayjs.extend(timezone);

/**
 * ============================================================================
 *  TIMEFRAME của điều kiện rule "tăng ngân sách theo lịch" (BUDGET_SCHEDULE).
 * ----------------------------------------------------------------------------
 *  Engine cũ fetch insight LIVE cứng `date_preset=today` và BỎ QUA hoàn toàn
 *  `params.timeframe`/`leftTimeframe`/`rightTimeframe`. File này chuẩn hoá 1 giá
 *  trị timeframe → tham số Meta getInsights, để điều kiện đọc số theo ĐÚNG khung
 *  người dùng chọn (mở khoá so sánh "trend": hôm nay vs 3 ngày qua...).
 *
 *  HỢP ĐỒNG (đã chốt — KHÔNG đổi):
 *   - Allow-list = 12 giá trị ở TIMEFRAME_ALLOWLIST bên dưới.
 *   - timeframe thiếu/rỗng/không thuộc allow-list → coi như `today` (back-compat:
 *     mọi rule cũ chạy Y HỆT hành vi hiện tại — vì FE mặc định 'today', và các giá
 *     trị lạ như custom:<uuid>/custom:<range>/khung theo giờ đều rơi về today).
 *   - Mapping Meta: các preset chuẩn (yesterday, last_3d, last_7d, last_14d,
 *     last_30d, this_month, last_month, maximum, today) → truyền THẲNG `date_preset`.
 *     Riêng `today_and_yesterday` và `last_*_days_incl_today` → tính
 *     `time_range {since, until}` THEO TIMEZONE CỦA TKQC (Meta không có date_preset
 *     "gồm hôm nay" tương ứng cho các mốc này). until = hôm nay, since = hôm nay −
 *     (N−1) ngày; today_and_yesterday: since = hôm qua. Ngày format 'YYYY-MM-DD'.
 * ============================================================================
 */

/** 12 timeframe được engine hỗ trợ (mọi giá trị khác → today). */
export const TIMEFRAME_ALLOWLIST: readonly string[] = [
  'today',
  'yesterday',
  'today_and_yesterday',
  'last_3_days_incl_today',
  'last_7_days_incl_today',
  'last_3d',
  'last_7d',
  'last_14d',
  'last_30d',
  'this_month',
  'last_month',
  'maximum',
];

const ALLOWLIST_SET = new Set<string>(TIMEFRAME_ALLOWLIST);

/**
 * Các timeframe "gồm hôm nay" phải tính time_range động (Meta thiếu date_preset
 * tương ứng). Value = SỐ NGÀY của cửa sổ (tính cả hôm nay).
 *   today_and_yesterday    → 2 ngày (since = hôm qua, until = hôm nay)
 *   last_3_days_incl_today → 3 ngày (since = hôm nay − 2)
 *   last_7_days_incl_today → 7 ngày (since = hôm nay − 6)
 */
const INCL_TODAY_DAYS: Record<string, number> = {
  today_and_yesterday: 2,
  last_3_days_incl_today: 3,
  last_7_days_incl_today: 7,
};

/** Nhãn tiếng Việt (cho nhật ký EXPLAIN) — 12 timeframe. */
export const TIMEFRAME_LABEL_VI: Record<string, string> = {
  today: 'hôm nay',
  yesterday: 'hôm qua',
  today_and_yesterday: 'hôm nay & hôm qua',
  last_3_days_incl_today: '3 ngày gần nhất (gồm hôm nay)',
  last_7_days_incl_today: '7 ngày gần nhất (gồm hôm nay)',
  last_3d: '3 ngày qua',
  last_7d: '7 ngày qua',
  last_14d: '14 ngày qua',
  last_30d: '30 ngày qua',
  this_month: 'tháng này',
  last_month: 'tháng trước',
  maximum: 'toàn thời gian',
};

/**
 * Chuẩn hoá timeframe thô về 1 giá trị trong allow-list. Thiếu/rỗng/không thuộc
 * allow-list (kể cả custom:<uuid>, custom:<range>, khung theo giờ) → 'today'.
 */
export function normalizeTimeframe(raw: unknown): string {
  if (typeof raw !== 'string') return 'today';
  const tf = raw.trim().toLowerCase();
  return ALLOWLIST_SET.has(tf) ? tf : 'today';
}

/** Nhãn tiếng Việt của 1 timeframe (đã hoặc chưa normalize). */
export function timeframeLabelVi(tf: unknown): string {
  return TIMEFRAME_LABEL_VI[normalizeTimeframe(tf)];
}

export interface MetaInsightTimeParams {
  date_preset?: string;
  time_range?: { since: string; until: string };
}

/**
 * timeframe (đã hoặc chưa normalize) → tham số thời gian cho Meta getInsights.
 *   - "gồm hôm nay" (today_and_yesterday / last_*_days_incl_today) → time_range
 *     {since, until} tính theo múi giờ TKQC quanh `now`.
 *   - còn lại → truyền thẳng date_preset (Meta tự diễn giải theo tz TKQC).
 *
 * @param now       thời điểm tick (Date thật, UTC nội bộ).
 * @param timezone  timezone TKQC ĐÃ resolve ở tick (rule.timezone hoặc tz account).
 */
export function timeframeToMetaParams(
  timeframe: unknown,
  now: Date,
  timezone: string,
): MetaInsightTimeParams {
  const tf = normalizeTimeframe(timeframe);

  const days = INCL_TODAY_DAYS[tf];
  if (days != null) {
    const zone = normalizeAccountTz(timezone);
    // "Hôm nay" theo múi giờ TKQC (không phải theo UTC/server) → biên ngày khớp
    // cách Meta cắt ngày cho TKQC (tránh lệch 1 ngày ở tz âm/dương so với UTC).
    const today = dayjs(now).tz(zone);
    const until = today.format('YYYY-MM-DD');
    const since = today.subtract(days - 1, 'day').format('YYYY-MM-DD');
    return { time_range: { since, until } };
  }

  // Preset chuẩn (gồm 'today') → date_preset truyền thẳng.
  return { date_preset: tf };
}
