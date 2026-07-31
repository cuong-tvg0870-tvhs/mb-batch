// COPY từ mb-ads/src/modules/auto-launch-rule/auto-launch-ad-schedule.util.ts — GIỮ PARITY,
// đổi logic phải sửa CẢ HAI repo. (Đổi import timezone sang util của mb-batch.)
import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import { normalizeAccountTz } from '../campaign-rule-runner/campaign-rule-tz.util';

dayjs.extend(utc);
dayjs.extend(timezone);

/**
 * LỊCH CHẠY QUẢNG CÁO cho camp do "Tự lên Camp" dựng (Meta `start_time`/`end_time` của NHÓM QC).
 * KHÁC hoàn toàn `schedule` của rule (khoảng rule được phép nổ) — cái này là lịch của QUẢNG CÁO.
 *
 * VÌ SAO CẦN: Meta tiêu HẾT ngân sách NGÀY trong phần ngày còn lại. Camp lên lúc 22h thì từ 22h→00h
 * đốt trọn budget ngày ⇒ rất phí. Nên cho chọn dời điểm bắt đầu sang đầu ngày hôm sau.
 *
 * 4 chế độ bắt đầu:
 *  - NOW      : chạy ngay khi lên camp.
 *  - NEXT_DAY : luôn dời sang HÔM SAU lúc `startTime`.
 *  - SMART    : lên camp SAU `deferAfter` → dời sang hôm sau lúc `startTime`; còn sớm → chạy ngay.
 *               (đúng ý: 22h thoả điều kiện thì để 01h hôm sau; 01h thoả thì chạy luôn)
 *  - FIXED    : mốc cứng theo giờ TKQC; đã qua → chạy ngay (không để Meta từ chối).
 *
 * Mọi mốc diễn giải theo MÚI GIỜ TKQC (Meta chạy theo tz tài khoản), trả về unix seconds.
 */
export type AdScheduleStartMode = 'NOW' | 'NEXT_DAY' | 'SMART' | 'FIXED';
export type AdScheduleEndMode = 'NONE' | 'AFTER_DAYS' | 'FIXED';

export interface AdScheduleConfig {
  startMode?: AdScheduleStartMode;
  /** "HH:MM" — giờ bắt đầu của ngày hôm sau (NEXT_DAY / SMART). Mặc định 00:00. */
  startTime?: string | null;
  /** "HH:MM" — SMART: lên camp sau mốc này thì dời sang hôm sau. Mặc định 18:00. */
  deferAfter?: string | null;
  /** "YYYY-MM-DDTHH:mm" theo giờ TKQC (FIXED). */
  startAt?: string | null;
  endMode?: AdScheduleEndMode;
  /** Số ngày chạy kể từ lúc bắt đầu (AFTER_DAYS). */
  endDays?: number | null;
  /** "YYYY-MM-DDTHH:mm" theo giờ TKQC (FIXED). */
  endAt?: string | null;
}

export interface ResolvedAdSchedule {
  /**
   * unix seconds cho mốc bắt đầu TƯƠNG LAI đã hẹn (SMART-dời/NEXT_DAY/FIXED-tương-lai).
   * `null` = CHẠY NGAY: builder BỎ start_time để Meta tự bắt đầu lúc PUBLISH THẬT.
   * Vì sao null thay vì now: nháp DRAFT_ONLY có thể publish tay vài GIỜ sau — ghim now lúc dựng
   * sẽ thành mốc quá khứ khi publish → Meta từ chối. Bỏ hẳn start_time vừa dọn mốc cũ của mẫu
   * (mẫu giữ start_time từ lúc tạo) vừa để Meta bắt đầu ngay khi thực sự đăng.
   */
  startUnix: number | null;
  /** unix seconds; null = không đặt hạn kết thúc. */
  endUnix: number | null;
}

const HHMM = /^(\d{1,2}):(\d{2})$/;

/** "HH:MM" → phút trong ngày; sai định dạng → fallback. */
function minutesOf(s: unknown, fallback: number): number {
  const m = HHMM.exec(String(s ?? '').trim());
  if (!m) return fallback;
  const h = Number(m[1]);
  const min = Number(m[2]);
  if (h > 23 || min > 59) return fallback;
  return h * 60 + min;
}

const pad = (n: number) => String(n).padStart(2, '0');
const wallOfMinutes = (day: dayjs.Dayjs, minutes: number) =>
  `${day.format('YYYY-MM-DD')}T${pad(Math.floor(minutes / 60))}:${pad(minutes % 60)}`;

/** Wall-clock "YYYY-MM-DDTHH:mm" (giờ TKQC) → unix seconds; sai định dạng → null. */
function wallToUnix(wall: unknown, tz: string): number | null {
  const s = String(wall ?? '').trim();
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(s)) return null;
  const d = dayjs.tz(s.slice(0, 16), tz);
  return d.isValid() ? d.unix() : null;
}

export function resolveAdSchedule(
  cfg: AdScheduleConfig | null | undefined,
  nowUnix: number,
  timeZone?: string | null,
): ResolvedAdSchedule {
  const tz = normalizeAccountTz(timeZone);
  const now = dayjs.unix(nowUnix).tz(tz);
  const startMode: AdScheduleStartMode = cfg?.startMode ?? 'NOW';
  const startMinutes = minutesOf(cfg?.startTime, 0); // mặc định 00:00
  const deferAfterMinutes = minutesOf(cfg?.deferAfter, 18 * 60); // mặc định 18:00
  const nextDayStart = () =>
    wallToUnix(wallOfMinutes(now.add(1, 'day'), startMinutes), tz) ?? nowUnix;

  // `null` = chạy ngay (bỏ start_time để Meta bắt đầu lúc publish); số = mốc tương lai đã hẹn.
  let startUnix: number | null;
  switch (startMode) {
    case 'NEXT_DAY':
      startUnix = nextDayStart();
      break;
    case 'SMART': {
      const nowMinutes = now.hour() * 60 + now.minute();
      // Còn sớm trong ngày → chạy NGAY (null); đã muộn → dời sang hôm sau cho trọn ngày ngân sách.
      startUnix = nowMinutes >= deferAfterMinutes ? nextDayStart() : null;
      break;
    }
    case 'FIXED':
      startUnix = wallToUnix(cfg?.startAt, tz);
      break;
    default: // NOW
      startUnix = null;
  }
  // Mốc KHÔNG còn ở tương lai (FIXED đã qua, hoặc lệch tính toán) → coi như chạy ngay (null) thay
  // vì ghim mốc quá khứ mà Meta sẽ từ chối. Dùng buffer 2' để tránh đua sát-giây lúc publish.
  if (startUnix !== null && startUnix < nowUnix + 120) startUnix = null;

  // Mốc để tính hạn "sau N ngày": nếu chạy ngay thì tính từ bây giờ.
  const effectiveStart = startUnix ?? nowUnix;
  let endUnix: number | null = null;
  switch (cfg?.endMode) {
    case 'AFTER_DAYS': {
      const days = Number(cfg?.endDays);
      if (Number.isFinite(days) && days > 0) {
        endUnix = dayjs
          .unix(effectiveStart)
          .tz(tz)
          .add(Math.floor(days), 'day')
          .unix();
      }
      break;
    }
    case 'FIXED':
      endUnix = wallToUnix(cfg?.endAt, tz);
      break;
    default:
      endUnix = null;
  }
  // Hạn kết thúc không hợp lệ (trước/bằng lúc bắt đầu hiệu lực) → bỏ, để camp chạy liên tục còn
  // hơn dựng ra một nhóm QC Meta từ chối.
  if (endUnix !== null && endUnix <= effectiveStart) endUnix = null;

  return { startUnix, endUnix };
}

/** Có cấu hình lịch chạy thật sự không (để runner biết có cần ghi đè mẫu hay chỉ dọn mốc cũ). */
export function hasAdSchedule(cfg: AdScheduleConfig | null | undefined): boolean {
  if (!cfg) return false;
  return (
    (!!cfg.startMode && cfg.startMode !== 'NOW') ||
    (!!cfg.endMode && cfg.endMode !== 'NONE')
  );
}

// COPY từ mb-ads/src/modules/draft-campaign/draft-campaign.service.ts sanitizeAdSetScheduleFromTemplate
// — GIỮ PARITY, đổi logic phải sửa CẢ HAI repo. (Chuyển từ private method thành function export.)
//
// Dọn LỊCH CHẠY của ad set clone từ mẫu. Mẫu giữ nguyên start_time/end_time từ lúc
// TẠO MẪU → thường đã QUÁ KHỨ, Meta từ chối ("start_time/end_time phải ở tương lai").
// Dùng chung cho auto-launch ("Scale bài hiệu quả") lẫn draft-automation ("Test content
// tự động").
//  - Có adSchedule (auto-launch đã hẹn giờ): TÔN TRỌNG mốc hẹn — startUnix/endUnix=null
//    nghĩa "chạy ngay/không đặt hạn" → bỏ mốc mẫu (trừ lifetime_budget bắt buộc có hạn).
//  - Không có adSchedule (draft-automation, hoặc auto-launch không hẹn): chỉ XOÁ những
//    mốc QUÁ KHỨ mà mẫu bê theo, giữ nguyên mốc tương lai hợp lệ nếu mẫu có.
export function sanitizeAdSetScheduleFromTemplate(
  adset: any,
  adSchedule?: { startUnix: number | null; endUnix: number | null } | null,
): void {
  if (!adset || typeof adset !== 'object') return;
  const nowUnix = Math.floor(Date.now() / 1000);
  // end_time/start_time có thể là epoch (string/number, đơn vị GIÂY) hoặc ISO string.
  const toUnix = (v: any): number | null => {
    if (v === null || v === undefined || v === '') return null;
    if (typeof v === 'number' && Number.isFinite(v)) return Math.floor(v);
    const s = String(v).trim();
    if (/^\d+$/.test(s)) return parseInt(s, 10);
    const ms = Date.parse(s);
    return Number.isNaN(ms) ? null : Math.floor(ms / 1000);
  };

  if (adSchedule) {
    // startUnix=null nghĩa "chạy ngay" → BỎ HẲN start_time (dọn mốc cũ mẫu, để Meta bắt
    // đầu lúc PUBLISH THẬT). startUnix có số = mốc tương lai đã hẹn → ghim.
    if (adSchedule.startUnix !== null) {
      adset.start_time = String(adSchedule.startUnix);
      adset.timezone_type = 'ADVERTISER';
    } else {
      delete adset.start_time;
    }
    if (adSchedule.endUnix !== null) {
      adset.end_time = String(adSchedule.endUnix);
      adset.has_end_date = true;
    } else {
      // Không đặt hạn: bỏ end_time cũ của mẫu (có thể đã qua) trừ khi ngân sách trọn đời
      // BẮT BUỘC phải có hạn — khi đó giữ nguyên mốc mẫu để không phá cấu hình.
      if (!adset.lifetime_budget) {
        delete adset.end_time;
        adset.has_end_date = false;
      }
    }
    return;
  }

  // Không có lịch hẹn: chỉ xử lý mốc QUÁ KHỨ, không đụng mốc tương lai hợp lệ.
  const startUnix = toUnix(adset.start_time);
  if (startUnix !== null && startUnix <= nowUnix) {
    // start_time quá khứ → Meta hiểu là "chạy ngay" khi bỏ trống → xoá.
    delete adset.start_time;
  }
  const endUnix = toUnix(adset.end_time);
  if (endUnix !== null && endUnix <= nowUnix) {
    if (!adset.lifetime_budget) {
      delete adset.end_time;
      adset.has_end_date = false;
    } else {
      // lifetime_budget BẮT BUỘC có end_time trong tương lai → không thể xoá. Mốc mẫu
      // đã quá khứ nên phải DỜI: chọn +30 ngày kể từ bây giờ (phương án an toàn nhất —
      // publish không lỗi, vẫn còn hạn để chi hết ngân sách trọn đời).
      adset.end_time = String(nowUnix + 30 * 24 * 60 * 60);
      adset.has_end_date = true;
    }
  }
}
