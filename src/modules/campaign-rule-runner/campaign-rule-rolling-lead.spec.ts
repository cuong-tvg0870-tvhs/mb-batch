// Chốt fix lỗi prod 11:00 04/08/2026 — Meta từ chối khung tăng ngân sách với thông báo
// "Ngày bắt đầu của đợt cao điểm cho lịch ngân sách đã qua rồi. Vui lòng thử lại."
//
// Gốc: nhánh NỐI CHUỖI của buildRollingSpec lấy `start = coveredUntil` thẳng, không lead.
// Runner chỉ nối khi `coveredUntil - now <= lead` nên start có thể chỉ cách hiện tại vài
// giây (khung cũ hết đúng 11:00, tick nổ 10:5x) — trong khi từ lúc dựng spec tới lúc Meta
// thực nhận còn nhiều lời gọi Graph nối tiếp ⇒ tới tay Meta thì time_start đã QUÁ KHỨ.
//
// Đây là code tiêu tiền thật nên test khoá đúng phần bất biến: start LUÔN cách "now" tối
// thiểu 15 phút, và việc kẹp không được phá các bất biến còn lại (mốc 15', khung ≥ 3 giờ,
// không overlap khung của người khác).
jest.mock('facebook-nodejs-business-sdk', () => ({
  HighDemandPeriod: jest.fn(),
  Campaign: jest.fn(),
  AdSet: jest.fn(),
  FacebookAdsApi: { init: jest.fn() },
}));

import { buildRollingSpec } from './campaign-rule-executor';

const MIN_LEAD_SEC = 15 * 60;
const HOUR = 3600;
// Mốc tròn 15' để phép tính trong test tường minh: 2026-08-04T11:00:00Z.
const NOW = Math.floor(new Date('2026-08-04T11:00:00.000Z').getTime() / 1000);

const rolling = { windowMode: 'DURATION', durationHours: 4, increaseValue: 50 };

function build(coveredUntil: number, extra: Record<string, any> = {}) {
  return buildRollingSpec(
    { ...rolling, ...extra },
    {
      nowUnix: NOW,
      tz: 'UTC',
      targetBudget: 1_000_000,
      coveredUntil,
      ownedWindows: [],
      foreignWindows: [],
      ...(extra.opts ?? {}),
    },
  );
}

describe('buildRollingSpec — start luôn ở tương lai (≥ now + 15 phút)', () => {
  it('khung cũ vừa hết (coveredUntil = now + 30 giây) → KẸP start về now + 15 phút', () => {
    const { spec, skipReason } = build(NOW + 30);
    expect(skipReason).toBeUndefined();
    expect(spec!.time_start).toBe(NOW + MIN_LEAD_SEC);
    // Chấp nhận HỞ khung ~15' — thà hở còn hơn Meta từ chối cả khung 4 giờ.
    expect(spec!.time_start - (NOW + 30)).toBeGreaterThan(0);
  });

  it('coveredUntil đã ở quá khứ (không còn phủ) → start = now + lead, vẫn ở tương lai', () => {
    const { spec } = build(NOW - HOUR);
    expect(spec!.time_start).toBe(NOW + MIN_LEAD_SEC);
  });

  it('coveredUntil còn xa (now + 1 giờ) → GIỮ nối liền, KHÔNG kẹp (không tạo hở vô cớ)', () => {
    const { spec } = build(NOW + HOUR);
    expect(spec!.time_start).toBe(NOW + HOUR);
  });

  it('start bị kẹp vẫn phải rơi đúng mốc 15 phút của Meta', () => {
    // now lệch mốc (11:03:47) → sau khi kẹp phải được ceil lên mốc 15' kế tiếp.
    const odd = NOW + 3 * 60 + 47;
    const { spec } = buildRollingSpec(rolling, {
      nowUnix: odd,
      tz: 'UTC',
      targetBudget: 1_000_000,
      coveredUntil: odd + 10,
      ownedWindows: [],
      foreignWindows: [],
    });
    expect(spec!.time_start % 900).toBe(0);
    expect(spec!.time_start).toBeGreaterThanOrEqual(odd + MIN_LEAD_SEC);
  });

  it('độ dài khung tính TỪ start đã kẹp (đủ 4 giờ, không bị cụt)', () => {
    const { spec } = build(NOW + 30);
    expect(spec!.time_end - spec!.time_start).toBe(4 * HOUR);
  });

  it('kẹp xong vẫn NÉ khung của người khác: start rơi vào khung foreign → đẩy qua', () => {
    // foreign phủ [now+10', now+2h] → start kẹp (now+15') nằm trong khung này ⇒ phải đẩy
    // ra sau time_end của nó, không được overlap (Meta chặn).
    const foreign = [
      { id: 'f1', time_start: NOW + 10 * 60, time_end: NOW + 2 * HOUR },
    ];
    const { spec, skipReason } = buildRollingSpec(rolling, {
      nowUnix: NOW,
      tz: 'UTC',
      targetBudget: 1_000_000,
      coveredUntil: NOW + 30,
      ownedWindows: [],
      foreignWindows: foreign,
    });
    expect(skipReason).toBeUndefined();
    expect(spec!.time_start).toBeGreaterThanOrEqual(NOW + 2 * HOUR);
  });

  it('kẹp làm khung còn dưới 3 giờ (hardEndAt sát) → SKIP, KHÔNG đẩy khung lố lên Meta', () => {
    // hardEndAt = now + 3h05'; start kẹp về now+15' ⇒ còn 2h50' < 3h ⇒ Meta sẽ từ chối.
    const hardEndAt = new Date((NOW + 3 * HOUR + 5 * 60) * 1000)
      .toISOString()
      .slice(0, 16);
    const { spec, skipReason } = buildRollingSpec(
      { ...rolling, hardEndAt },
      {
        nowUnix: NOW,
        tz: 'UTC',
        targetBudget: 1_000_000,
        coveredUntil: NOW + 30,
        ownedWindows: [],
        foreignWindows: [],
      },
    );
    expect(spec).toBeUndefined();
    expect(skipReason).toBe('below_min_duration_3h');
  });
});
