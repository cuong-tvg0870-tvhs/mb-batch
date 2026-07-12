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
 * - SPECIFIC: đến hạn nếu có slot "HH:MM" của weekday hiện tại (theo timezone rule) vừa
 *   đi qua trong cửa sổ tick [now - TICK_WINDOW, now]. Mốc `aligned` = đúng phút của slot,
 *   nên dedupeKey ổn định → mỗi slot chỉ chạy 1 lần dù tick chồng lấn.
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

    const slots = slotsForWeekday(schedule.specificSlots, weekday);
    let best: number | null = null;
    for (const slot of slots) {
      const slotMinutes = slotToMinutes(slot);
      if (slotMinutes == null) continue;
      const diff = nowMinutes - slotMinutes;
      // slot vừa đi qua trong cửa sổ tick (không tính slot ở tương lai).
      if (diff >= 0 && diff < windowMinutes) {
        if (best == null || slotMinutes > best) best = slotMinutes;
      }
    }
    if (best == null) return { due: false };

    // aligned = đúng phút của slot (giây/ms bị zero) → dedupeKey ổn định.
    const alignedMs =
      now.getTime() - (nowMinutes - best) * 60000 - secondsIntoMinute;
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
