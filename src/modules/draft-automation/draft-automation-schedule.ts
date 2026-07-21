/**
 * Lịch dạng rule-builder cho DraftAutomation — CÙNG shape với `AutoLaunchRule.schedule`
 * để màn "Test content tự động" và "Scale bài hiệu quả" dùng chung đúng một bộ điều khiển
 * lịch trên UI (RuleScheduleSetting + lưới 7 ngày × 24 giờ).
 *
 * ⚠️ PARITY: file này có BẢN SAO byte-identical ở
 *    mb-ads/src/modules/draft-automation/draft-automation-schedule.ts
 * Sửa một bên phải sửa cả bên kia — mb-ads tính `nextRunAt` lúc lưu, mb-batch tính lại
 * sau mỗi lượt chạy; hai bên lệch công thức thì lịch trôi dần.
 *
 * KHÁC auto-launch: ở đó runner POLL mỗi 5' rồi hỏi "rule có tới hạn không" (dueSlots).
 * Ở đây DraftAutomation đã có sẵn cột `nextRunAt` và scheduler quét theo mốc đó, nên ta
 * tính TRƯỚC mốc kế tiếp thay vì poll — giữ nguyên máy móc đang chạy, không viết lại runner.
 */

export interface RuleScheduleJson {
  type: 'INTERVAL' | 'SPECIFIC';
  /** "15m" | "30m" | "60m" | "3h" | "6h" | "12h" | "24h" | "36h" | "72h" */
  interval?: string;
  /** { mon: ["09:00","17:15"], tue: [...] } — key viết thường 3 ký tự. */
  specificSlots?: Record<string, string[]>;
  useDateRange?: boolean;
  dateFrom?: string | null;
  dateTo?: string | null;
}

const DAY_KEYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'] as const;

/** "6h"/"15m"/"2d" → phút. Không parse được → 0 (caller tự fallback). */
export function parseIntervalMinutes(value: unknown): number {
  const m = String(value ?? '')
    .trim()
    .match(/^(\d+)\s*([mhd])$/i);
  if (!m) return 0;
  const n = Number(m[1]);
  const u = m[2].toLowerCase();
  return u === 'm' ? n : u === 'h' ? n * 60 : n * 1440;
}

/**
 * Chuẩn hoá JSON do client gửi lên. Allow-list chặt vì cột này được runner tin dùng:
 * chỉ giữ đúng các khoá đã khai, ép kiểu, loại slot sai định dạng.
 * Trả null khi không phải lịch hợp lệ → caller fallback cột phẳng cũ.
 */
export function normalizeRuleSchedule(raw: unknown): RuleScheduleJson | null {
  if (!raw || typeof raw !== 'object') return null;
  const r = raw as Record<string, unknown>;
  const type = r.type === 'SPECIFIC' ? 'SPECIFIC' : 'INTERVAL';

  const slots: Record<string, string[]> = {};
  if (r.specificSlots && typeof r.specificSlots === 'object') {
    for (const day of DAY_KEYS) {
      const list = (r.specificSlots as Record<string, unknown>)[day];
      if (!Array.isArray(list)) continue;
      const clean = Array.from(
        new Set(
          list
            .map((x) => String(x).trim())
            .filter((x) => /^([01]\d|2[0-3]):([0-5]\d)$/.test(x)),
        ),
      ).sort();
      if (clean.length) slots[day] = clean;
    }
  }

  // Lịch SPECIFIC mà không còn khung giờ nào hợp lệ = không bao giờ chạy. Coi như
  // không có lịch JSON để caller fallback cột phẳng, thay vì im lặng treo automation.
  if (type === 'SPECIFIC' && !Object.keys(slots).length) return null;

  const interval = parseIntervalMinutes(r.interval)
    ? String(r.interval)
    : undefined;
  if (type === 'INTERVAL' && !interval) return null;

  const asDate = (v: unknown) => {
    if (typeof v !== 'string' || !v.trim()) return null;
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  };

  return {
    type,
    ...(interval ? { interval } : {}),
    ...(Object.keys(slots).length ? { specificSlots: slots } : {}),
    useDateRange: r.useDateRange === true,
    dateFrom: asDate(r.dateFrom),
    dateTo: asDate(r.dateTo),
  };
}

/**
 * Giờ tường (wall-clock) của `at` theo múi giờ — dùng Intl, khỏi kéo thư viện tz.
 */
function partsIn(timeZone: string, at: Date) {
  const fmt = new Intl.DateTimeFormat('en-GB', {
    timeZone,
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  const p = Object.fromEntries(
    fmt.formatToParts(at).map((x) => [x.type, x.value]),
  ) as Record<string, string>;
  return {
    weekday: (p.weekday || '').slice(0, 3).toLowerCase(),
    year: Number(p.year),
    month: Number(p.month),
    day: Number(p.day),
    minutes: Number(p.hour) * 60 + Number(p.minute),
  };
}

/**
 * Lệch giữa giờ tường của múi giờ và UTC tại thời điểm `at`, tính bằng ms.
 * Giả định vùng KHÔNG có DST — các múi giờ hệ thống cho chọn (Asia/Ho_Chi_Minh,
 * Asia/Bangkok, Asia/Singapore, UTC) đều không đổi giờ, nên lệch là hằng số.
 */
function tzOffsetMs(timeZone: string, at: Date): number {
  const p = partsIn(timeZone, at);
  const asUtc = Date.UTC(
    p.year,
    p.month - 1,
    p.day,
    Math.floor(p.minutes / 60),
    p.minutes % 60,
    0,
    0,
  );
  // Bỏ phần giây/ms của `at` cho khớp độ phân giải phút của asUtc.
  const floored = Math.floor(at.getTime() / 60000) * 60000;
  return asUtc - floored;
}

/**
 * Mốc chạy kế tiếp theo lịch JSON, hoặc null nếu không xác định được / đã hết hạn
 * (dateTo đã qua). Caller coi null nghĩa là "ngừng lịch".
 *
 * SPECIFIC: quét sang tối đa 7 ngày để tìm khung giờ gần nhất SAU `from`.
 * INTERVAL: từ `from` cộng thẳng số phút.
 */
export function nextRunFromSchedule(
  schedule: RuleScheduleJson,
  from: Date = new Date(),
  timeZone = 'Asia/Ho_Chi_Minh',
): Date | null {
  // Khoảng ngày hiệu lực (tuỳ chọn): quá hạn thì dừng hẳn, chưa tới thì đợi tới mốc đầu.
  let base = from;
  if (schedule.useDateRange) {
    if (schedule.dateTo && from.getTime() > new Date(schedule.dateTo).getTime())
      return null;
    if (
      schedule.dateFrom &&
      from.getTime() < new Date(schedule.dateFrom).getTime()
    )
      base = new Date(schedule.dateFrom);
  }

  if (schedule.type === 'INTERVAL') {
    const minutes = parseIntervalMinutes(schedule.interval);
    if (!minutes) return null;
    return new Date(base.getTime() + minutes * 60_000);
  }

  const slots = schedule.specificSlots || {};
  const p = partsIn(timeZone, base);
  const todayIdx = DAY_KEYS.indexOf(p.weekday as (typeof DAY_KEYS)[number]);
  if (todayIdx < 0) return null;

  for (let ahead = 0; ahead <= 7; ahead++) {
    const dayKey = DAY_KEYS[(todayIdx + ahead) % 7];
    for (const hhmm of slots[dayKey] || []) {
      const [h, m] = hhmm.split(':').map(Number);
      const slotMin = h * 60 + m;
      // Hôm nay thì chỉ nhận khung giờ CÒN Ở PHÍA TRƯỚC, tránh chạy lại khung vừa qua.
      if (ahead === 0 && slotMin <= p.minutes) continue;

      const wallUtc = Date.UTC(p.year, p.month - 1, p.day + ahead, h, m, 0, 0);
      const at = new Date(wallUtc - tzOffsetMs(timeZone, base));
      if (at.getTime() > base.getTime()) {
        if (
          schedule.useDateRange &&
          schedule.dateTo &&
          at.getTime() > new Date(schedule.dateTo).getTime()
        )
          return null;
        return at;
      }
    }
  }
  return null;
}
