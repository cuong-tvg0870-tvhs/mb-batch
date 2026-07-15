import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';

dayjs.extend(utc);
dayjs.extend(timezone);

// Quy đổi thời gian cho lịch tăng ngân sách (budget schedule) THEO ĐÚNG múi giờ của
// TÀI KHOẢN QUẢNG CÁO. Meta chạy budget schedule theo timezone của TKQC, nên mốc giờ
// user chọn ("9:00") phải được diễn giải theo tz TKQC — KHÔNG theo tz của server.
// (Parity với mb-ads/src/modules/campaign-rule/campaign-rule-tz.util.ts.)

// Múi giờ mặc định khi TKQC chưa lưu timezone (giữ hành vi cũ cho tài khoản VN).
export const DEFAULT_ACCOUNT_TZ = 'Asia/Ho_Chi_Minh';

// Chuẩn hoá tên IANA; rỗng/không hợp lệ → mặc định (dayjs.tz ném lỗi với tên sai).
export function normalizeAccountTz(tz?: string | null): string {
  if (!tz) return DEFAULT_ACCOUNT_TZ;
  try {
    dayjs().tz(tz);
    return tz;
  } catch {
    return DEFAULT_ACCOUNT_TZ;
  }
}

// Wall-clock "YYYY-MM-DDTHH:mm" (giờ theo TKQC) → unix seconds.
export function wallClockToUnix(wall: string, tz?: string | null): number {
  return dayjs.tz(wall, normalizeAccountTz(tz)).unix();
}

// Mốc giờ "HH:mm" (theo tz TKQC) KẾ TIẾP sau thời điểm afterUnix → unix seconds.
// Dùng cho chế độ "nối khung tới 1 mốc giờ" (vd 08:30 sáng): nếu hôm nay đã qua mốc
// thì lấy mốc của ngày mai. NaN nếu chuỗi giờ không hợp lệ.
export function nextClockUnix(
  clock: string,
  tz: string | null | undefined,
  afterUnix: number,
): number {
  const zone = normalizeAccountTz(tz);
  const m = /^(\d{1,2}):(\d{2})$/.exec(clock ?? '');
  if (!m) return NaN;
  const h = Number(m[1]);
  const min = Number(m[2]);
  if (h > 23 || min > 59) return NaN;
  let d = dayjs
    .unix(afterUnix)
    .tz(zone)
    .hour(h)
    .minute(min)
    .second(0)
    .millisecond(0);
  if (d.unix() <= afterUnix) d = d.add(1, 'day');
  return d.unix();
}

// Meta YÊU CẦU mốc khung budget schedule rơi ĐÚNG bội số 15 phút (00/15/30/45 theo giờ
// TKQC) — lệch sẽ bị lỗi "Thời gian bạn nhập cho đợt cao điểm phải cách quãng 15 phút
// (0, 15, 30, 45)". Mọi múi giờ thực tế đều lệch UTC theo bội số 15' nên căn theo unix
// (bội số 900s) là đủ để mốc giờ ĐỊA PHƯƠNG cũng rơi đúng 00/15/30/45.
export const QUARTER_SEC = 15 * 60;
// start → làm tròn LÊN (giữ ở tương lai, không lùi vào khung/quá khứ).
export const ceilToQuarter = (unix: number): number =>
  Number.isFinite(unix) ? Math.ceil(unix / QUARTER_SEC) * QUARTER_SEC : unix;
// end → làm tròn XUỐNG (không lấn sang khung kế/vượt mốc chốt).
export const floorToQuarter = (unix: number): number =>
  Number.isFinite(unix) ? Math.floor(unix / QUARTER_SEC) * QUARTER_SEC : unix;

// Meta trả time_start/end dạng ISO ("…+0700") hoặc đôi khi unix → chuẩn hoá unix seconds.
export function metaTimeToUnix(v: string | number): number {
  if (typeof v === 'number') return Math.floor(v);
  if (/^\d+$/.test(String(v))) return parseInt(String(v), 10);
  return Math.floor(new Date(String(v)).getTime() / 1000);
}

// unix seconds / ISO (Meta trả) → wall-clock "YYYY-MM-DDTHH:mm" theo tz TKQC.
export function toAccountWallClock(isoOrUnix: string | number, tz?: string | null): string {
  const zone = normalizeAccountTz(tz);
  const d =
    typeof isoOrUnix === 'number' || /^\d+$/.test(String(isoOrUnix))
      ? dayjs.unix(Number(isoOrUnix))
      : dayjs(String(isoOrUnix));
  return d.tz(zone).format('YYYY-MM-DDTHH:mm');
}
