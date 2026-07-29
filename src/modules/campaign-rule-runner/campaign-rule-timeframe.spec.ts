// Khẳng định hợp đồng TIMEFRAME của engine campaign-rule (budget-schedule):
//   - normalizeTimeframe: allow-list pass-through; thiếu/rỗng/lạ → today (back-compat).
//   - timeframeToMetaParams: preset chuẩn → date_preset; "gồm hôm nay" → time_range
//     tính theo MÚI GIỜ TKQC (case biên: đầu tháng, tz lệch ngày so với UTC).
import {
  TIMEFRAME_ALLOWLIST,
  normalizeTimeframe,
  timeframeLabelVi,
  timeframeToMetaParams,
} from './campaign-rule-timeframe';

describe('normalizeTimeframe', () => {
  it('giữ nguyên mọi giá trị trong allow-list (không phân biệt hoa/thường/space)', () => {
    for (const tf of TIMEFRAME_ALLOWLIST) {
      expect(normalizeTimeframe(tf)).toBe(tf);
      expect(normalizeTimeframe(`  ${tf.toUpperCase()}  `)).toBe(tf);
    }
  });

  it('thiếu/rỗng/không phải chuỗi → today (back-compat)', () => {
    expect(normalizeTimeframe(undefined)).toBe('today');
    expect(normalizeTimeframe(null)).toBe('today');
    expect(normalizeTimeframe('')).toBe('today');
    expect(normalizeTimeframe('   ')).toBe('today');
    expect(normalizeTimeframe(7 as unknown)).toBe('today');
  });

  it('giá trị ngoài allow-list (custom ref/range, khung giờ) → today', () => {
    expect(normalizeTimeframe('custom:0f9a1b2c-0000-0000-0000-000000000000')).toBe(
      'today',
    );
    expect(normalizeTimeframe('custom:2026-07-01:2026-07-15')).toBe('today');
    expect(normalizeTimeframe('previous_hour')).toBe('today');
    expect(normalizeTimeframe('last_90d')).toBe('today');
    expect(normalizeTimeframe('last_2_days')).toBe('today');
  });
});

describe('timeframeToMetaParams — preset chuẩn → date_preset', () => {
  const now = new Date('2026-07-29T05:00:00.000Z');
  const tz = 'Asia/Ho_Chi_Minh';

  for (const tf of [
    'today',
    'yesterday',
    'last_3d',
    'last_7d',
    'last_14d',
    'last_30d',
    'this_month',
    'last_month',
    'maximum',
  ]) {
    it(`${tf} → date_preset:${tf}`, () => {
      expect(timeframeToMetaParams(tf, now, tz)).toEqual({ date_preset: tf });
    });
  }

  it('giá trị lạ → date_preset:today', () => {
    expect(timeframeToMetaParams('khong_ton_tai', now, tz)).toEqual({
      date_preset: 'today',
    });
  });
});

describe('timeframeToMetaParams — "gồm hôm nay" → time_range theo tz TKQC', () => {
  it('today_and_yesterday: since=hôm qua, until=hôm nay', () => {
    const now = new Date('2026-07-29T05:00:00.000Z'); // 12:00 giờ VN 29/07
    expect(timeframeToMetaParams('today_and_yesterday', now, 'Asia/Ho_Chi_Minh')).toEqual({
      time_range: { since: '2026-07-28', until: '2026-07-29' },
    });
  });

  it('last_3_days_incl_today: cửa sổ 3 ngày kết thúc hôm nay', () => {
    const now = new Date('2026-07-29T05:00:00.000Z');
    expect(
      timeframeToMetaParams('last_3_days_incl_today', now, 'Asia/Ho_Chi_Minh'),
    ).toEqual({ time_range: { since: '2026-07-27', until: '2026-07-29' } });
  });

  it('last_7_days_incl_today: cửa sổ 7 ngày kết thúc hôm nay', () => {
    const now = new Date('2026-07-29T05:00:00.000Z');
    expect(
      timeframeToMetaParams('last_7_days_incl_today', now, 'Asia/Ho_Chi_Minh'),
    ).toEqual({ time_range: { since: '2026-07-23', until: '2026-07-29' } });
  });

  it('BIÊN ĐẦU THÁNG: cửa sổ bắc qua ranh giới tháng', () => {
    const now = new Date('2026-08-01T05:00:00.000Z'); // 12:00 VN 01/08
    expect(
      timeframeToMetaParams('last_3_days_incl_today', now, 'Asia/Ho_Chi_Minh'),
    ).toEqual({ time_range: { since: '2026-07-30', until: '2026-08-01' } });
  });

  it('TZ LỆCH NGÀY so với UTC (+07): 23:30 UTC 28/07 = 06:30 VN 29/07 → until 29/07', () => {
    const now = new Date('2026-07-28T23:30:00.000Z');
    expect(timeframeToMetaParams('today_and_yesterday', now, 'Asia/Ho_Chi_Minh')).toEqual({
      time_range: { since: '2026-07-28', until: '2026-07-29' },
    });
  });

  it('TZ ÂM (America/Los_Angeles −07): 05:00 UTC 29/07 = 22:00 PDT 28/07 → until 28/07', () => {
    const now = new Date('2026-07-29T05:00:00.000Z');
    expect(
      timeframeToMetaParams('last_3_days_incl_today', now, 'America/Los_Angeles'),
    ).toEqual({ time_range: { since: '2026-07-26', until: '2026-07-28' } });
  });

  it('tz rỗng/không hợp lệ → fallback tz mặc định (Asia/Ho_Chi_Minh)', () => {
    const now = new Date('2026-07-29T05:00:00.000Z');
    expect(timeframeToMetaParams('today_and_yesterday', now, '' as any)).toEqual({
      time_range: { since: '2026-07-28', until: '2026-07-29' },
    });
  });
});

describe('timeframeLabelVi', () => {
  it('trả nhãn tiếng Việt; giá trị lạ → nhãn của today', () => {
    expect(timeframeLabelVi('last_7d')).toBe('7 ngày qua');
    expect(timeframeLabelVi('maximum')).toBe('toàn thời gian');
    expect(timeframeLabelVi('xyz')).toBe('hôm nay');
  });
});
