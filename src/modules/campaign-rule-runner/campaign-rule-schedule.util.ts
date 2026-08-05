import {
  DEFAULT_TIMEZONE,
  INTERVAL_MS,
  INTERVAL_TOLERANCE_MS,
  TICK_WINDOW_MS,
} from './campaign-rule-runner.constants';

/**
 * Tiện ích lịch cho runner. Thuần hàm (không state) để dễ test và tái dùng.
 *
 * Ý tưởng chung:
 * - INTERVAL: đến hạn nếu chưa từng chạy, hoặc đã trôi >= interval (trừ dung sai).
 *   Mốc `aligned` = now làm tròn xuống phút (dedupeKey chỉ chống chồng trong cùng tick;
 *   dãn cách giữa các lượt do lastRunAt quyết định).
 * - SPECIFIC: đến hạn nếu có slot "HH:MM" của weekday hiện tại HOẶC HÔM QUA (theo timezone
 *   rule) vừa đi qua trong cửa sổ tick [now - TICK_WINDOW, now]. Xét cả weekday hôm qua để
 *   slot sát nửa đêm (vd 23:58) vẫn được tick đầu ngày hôm sau bắt đúng — tick đó weekday
 *   đã sang ngày mới nên nếu chỉ tra weekday hiện tại sẽ tra SAI bucket và mất slot vĩnh
 *   viễn (bug cũ, xem chi tiết trong isRuleDue). Mốc `aligned` = đúng phút của slot (tính
 *   tuyệt đối theo ms, không phụ thuộc weekday), nên dedupeKey ổn định → mỗi slot chỉ chạy
 *   1 lần dù tick chồng lấn hay slot thuộc ngày hôm trước.
 */

export interface ScheduleLike {
  type?: string | null;
  interval?: string | null;
  specificSlots?: any;
  useDateRange?: boolean | null;
  dateFrom?: Date | null;
  dateTo?: Date | null;
}

export interface DuenessResult {
  due: boolean;
  aligned?: Date;
  /** true khi rule có date-range và now nằm ngoài [dateFrom, dateTo]. */
  outOfDateRange?: boolean;
}

/** Giờ/phút/thứ của một mốc thời gian theo timezone IANA (0=Sun..6=Sat). */
export interface ZonedTimeParts {
  weekday: number;
  hour: number;
  minute: number;
}

const WEEKDAY_INDEX: Record<string, number> = {
  Sun: 0,
  Mon: 1,
  Tue: 2,
  Wed: 3,
  Thu: 4,
  Fri: 5,
  Sat: 6,
};

const SLOT_KEYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

/**
 * Bóc weekday/hour/minute của `date` theo `timeZone` bằng Intl (không cần lib tz ngoài).
 * Lỗi timezone không hợp lệ → fallback DEFAULT_TIMEZONE.
 */
export function zonedTimeParts(date: Date, timeZone: string): ZonedTimeParts {
  const tz = timeZone || DEFAULT_TIMEZONE;
  let parts: Intl.DateTimeFormatPart[];
  try {
    parts = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hourCycle: 'h23',
      weekday: 'short',
      hour: '2-digit',
      minute: '2-digit',
    }).formatToParts(date);
  } catch {
    parts = new Intl.DateTimeFormat('en-US', {
      timeZone: DEFAULT_TIMEZONE,
      hourCycle: 'h23',
      weekday: 'short',
      hour: '2-digit',
      minute: '2-digit',
    }).formatToParts(date);
  }

  const get = (type: string) =>
    parts.find((p) => p.type === type)?.value ?? '';
  const weekday = WEEKDAY_INDEX[get('weekday')] ?? 0;
  // hourCycle 'h23' cho 00..23; một vài môi trường trả "24" lúc nửa đêm → chuẩn hóa về 0.
  let hour = Number(get('hour'));
  if (!Number.isFinite(hour) || hour === 24) hour = 0;
  const minute = Number(get('minute')) || 0;
  return { weekday, hour, minute };
}

/** Interval string → mili giây (null nếu không nhận diện được). */
export function intervalMs(interval?: string | null): number | null {
  if (!interval) return null;
  return INTERVAL_MS[interval] ?? null;
}

/** Làm tròn xuống phút — mốc aligned cho lịch INTERVAL. */
export function alignedNow(now: Date): Date {
  return new Date(Math.floor(now.getTime() / 60000) * 60000);
}

/** Khóa idempotency của một lượt chạy: `ruleId:accountId:<aligned ISO>`. */
export function dedupeKey(
  ruleId: string,
  accountId: string,
  aligned: Date,
): string {
  return `${ruleId}:${accountId}:${aligned.toISOString()}`;
}

// ---------------------------------------------------------------------------
//  THỬ LẠI SLOT khi lượt chạy chết vì lỗi TẠM THỜI (mạng/timeout/rate-limit)
//
//  Bối cảnh sự cố prod: slot 11:00 gặp "The request was made but no response was
//  received" lúc ĐỌC insight → mọi task FAILED nhưng run vẫn COMPLETED, mà cửa sổ
//  TICK_WINDOW_MS chỉ 5' nên tick sau slot không còn "đến hạn" → lượt bơm ngân sách
//  11:00 MẤT HẲN.
//
//  CHỈ thử lại khi lượt trước chết ở khâu ĐỌC insight — tức là CHƯA hề gọi Meta để
//  tạo khung ngân sách (không có tác dụng phụ nào) → chạy lại KHÔNG thể bơm 2 lần.
//  Lỗi SAU khi đã gọi Meta (executionError) TUYỆT ĐỐI không tự thử lại vì không biết
//  Meta đã nhận hay chưa. Xem CampaignRuleRunnerService.findRetryableRun.
// ---------------------------------------------------------------------------

/** Trần số lần THỬ LẠI của MỖI slot (không tính lượt đầu) → tối đa 3 lần chạm Meta. */
export const SLOT_RETRY_MAX_ATTEMPTS = 2;

/**
 * Chỉ thử lại trong vòng 30' kể từ mốc slot. Trễ hơn thì khung tăng ngân sách của slot
 * đó đã mất phần lớn ý nghĩa (và tick kế tiếp/slot sau sẽ lo) → thôi, không quét mãi.
 */
export const SLOT_RETRY_WINDOW_MS = 30 * 60 * 1000;

/**
 * dedupeKey của LẦN THỬ LẠI thứ `attempt` (>=1) cho cùng một slot. Suffix suy ra TẤT
 * ĐỊNH từ trạng thái DB (số lần đã thử) nên 2 replica cùng thử lại sẽ sinh CÙNG một
 * khóa → unique index vẫn chỉ cho 1 lượt chạy đi qua.
 */
export function retryDedupeKey(baseKey: string, attempt: number): string {
  return `${baseKey}#retry${attempt}`;
}

/**
 * Trả về danh sách "HH:MM" của weekday `weekdayIdx` (0=Sun) từ specificSlots.
 * specificSlots là Record<"sun".."sat", string[]>. Không có → mảng rỗng.
 */
function slotsForWeekday(specificSlots: any, weekdayIdx: number): string[] {
  if (!specificSlots || typeof specificSlots !== 'object') return [];
  const key = SLOT_KEYS[weekdayIdx];
  const list = specificSlots[key];
  return Array.isArray(list) ? list.filter((s) => typeof s === 'string') : [];
}

/** "HH:MM" → phút trong ngày (null nếu sai định dạng). */
function slotToMinutes(slot: string): number | null {
  const m = /^(\d{1,2}):(\d{2})$/.exec(slot.trim());
  if (!m) return null;
  const h = Number(m[1]);
  const min = Number(m[2]);
  if (h < 0 || h > 23 || min < 0 || min > 59) return null;
  return h * 60 + min;
}

/**
 * Đánh giá rule có đến hạn tại `now` không.
 *
 * @param timezone timezone ĐÃ resolve (rule.timezone hoặc tz account nếu "account").
 */
export function isRuleDue(
  schedule: ScheduleLike | null | undefined,
  lastRunAt: Date | null,
  now: Date,
  timezone: string,
): DuenessResult {
  if (!schedule) return { due: false };

  // Khoảng ngày hiệu lực (nếu bật): ngoài khoảng → không chạy.
  if (schedule.useDateRange) {
    if (schedule.dateFrom && now < new Date(schedule.dateFrom)) {
      return { due: false, outOfDateRange: true };
    }
    if (schedule.dateTo && now > new Date(schedule.dateTo)) {
      return { due: false, outOfDateRange: true };
    }
  }

  if (schedule.type === 'SPECIFIC') {
    const { weekday, hour, minute } = zonedTimeParts(now, timezone);
    const nowMinutes = hour * 60 + minute;
    const secondsIntoMinute = now.getSeconds() * 1000 + now.getMilliseconds();
    const windowMinutes = TICK_WINDOW_MS / 60000;
    const MINUTES_PER_DAY = 1440;
    const MINUTES_PER_WEEK = 7 * MINUTES_PER_DAY;

    // BUG CŨ: chỉ tra slotsForWeekday(weekday HIỆN TẠI) → slot cuối ngày (vd 23:58) bị
    // tick 00:xx hôm sau bỏ lỡ vĩnh viễn, vì lúc đó weekday đã sang ngày mới nên tra
    // SAI bucket (xem comment đầu file). Fix: quy về "phút trong TUẦN" (weekday*1440 +
    // phút trong ngày) và xét CẢ weekday hôm nay lẫn hôm qua — vì windowMinutes luôn
    // rất nhỏ so với 1440 nên chỉ slot ở sát ranh giới ngày mới cần xét chéo weekday;
    // slot ban ngày bình thường không thể lọt vào diff < windowMinutes của weekday kia.
    const weekMinutesNow = weekday * MINUTES_PER_DAY + nowMinutes;
    const candidateWeekdays = [weekday, (weekday + 6) % 7];

    let bestDiff: number | null = null;
    for (const wd of candidateWeekdays) {
      const slots = slotsForWeekday(schedule.specificSlots, wd);
      for (const slot of slots) {
        const slotMinutes = slotToMinutes(slot);
        if (slotMinutes == null) continue;
        const slotWeekMinutes = wd * MINUTES_PER_DAY + slotMinutes;
        // khoảng cách (phút) từ slot tới hiện tại, cuộn vòng theo tuần để so được
        // slot "hôm qua" (vd Chủ Nhật 23:58 so với tick Thứ Hai 00:02).
        const diff =
          (weekMinutesNow - slotWeekMinutes + MINUTES_PER_WEEK) %
          MINUTES_PER_WEEK;
        // slot vừa đi qua trong cửa sổ tick (không tính slot ở tương lai); nhiều slot
        // cùng lọt cửa sổ → lấy slot GẦN now nhất (diff nhỏ nhất), y hệt hành vi cũ.
        if (diff < windowMinutes && (bestDiff == null || diff < bestDiff)) {
          bestDiff = diff;
        }
      }
    }
    if (bestDiff == null) return { due: false };

    // aligned = đúng phút của slot (giây/ms bị zero), lùi lại `bestDiff` phút từ now
    // theo mốc TUYỆT ĐỐI (ms) — không phụ thuộc weekday nên vẫn đúng cả khi slot thuộc
    // NGÀY HÔM TRƯỚC. dedupeKey = ruleId:accountId:<aligned ISO> nhờ vậy vẫn TẤT ĐỊNH
    // (mỗi mốc phút tuyệt đối chỉ ứng với đúng 1 slot) và KHÔNG đụng độ slot khác.
    const alignedMs = now.getTime() - secondsIntoMinute - bestDiff * 60000;
    return { due: true, aligned: new Date(alignedMs) };
  }

  // Mặc định coi là INTERVAL.
  const ms = intervalMs(schedule.interval);
  if (ms == null) return { due: false };
  if (lastRunAt == null) {
    return { due: true, aligned: alignedNow(now) };
  }
  const elapsed = now.getTime() - new Date(lastRunAt).getTime();
  if (elapsed >= ms - INTERVAL_TOLERANCE_MS) {
    return { due: true, aligned: alignedNow(now) };
  }
  return { due: false };
}
