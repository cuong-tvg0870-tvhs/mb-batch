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

// unix seconds / ISO (Meta trả) → wall-clock "YYYY-MM-DDTHH:mm" theo tz TKQC.
export function toAccountWallClock(isoOrUnix: string | number, tz?: string | null): string {
  const zone = normalizeAccountTz(tz);
  const d =
    typeof isoOrUnix === 'number' || /^\d+$/.test(String(isoOrUnix))
      ? dayjs.unix(Number(isoOrUnix))
      : dayjs(String(isoOrUnix));
  return d.tz(zone).format('YYYY-MM-DDTHH:mm');
}
