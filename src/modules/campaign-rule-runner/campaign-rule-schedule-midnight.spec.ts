// Chốt fix bug "slot cuối ngày không bao giờ được kích hoạt" trong isRuleDue (nhánh
// SPECIFIC): trước đây slotsForWeekday tra theo weekday TẠI THỜI ĐIỂM TICK, nên slot sát
// nửa đêm (vd 23:58 Chủ Nhật) bị tick đầu ngày hôm sau (Thứ Hai 00:02) tra SAI bucket
// (weekday đã sang Thứ Hai) → mất slot vĩnh viễn mỗi tuần. Fix: quy về "phút trong tuần"
// và xét cả weekday hôm qua khi diff (đã cuộn vòng theo tuần) lọt cửa sổ tick.
//
// Đây là code tiêu tiền thật (tăng ngân sách qua Meta) nên tập trung xác nhận:
//   1. Slot cuối ngày ĐƯỢC bắt đúng weekday của NÓ (không phải weekday lúc tick).
//   2. Slot bình thường (giữa ngày) KHÔNG bị bắt 2 lần bởi cơ chế "xét thêm hôm qua".
//   3. dedupeKey tất định — 2 tick "cùng slot" (chồng lấn giây/ms) ra CÙNG khóa; slot
//      khác (khác ngày/khác giờ) ra khóa KHÁC (không đụng độ).
import {
  dedupeKey,
  isRuleDue,
  zonedTimeParts,
} from './campaign-rule-schedule.util';

const TZ = 'UTC'; // dùng UTC cho phép tính mốc thời gian trong test tường minh, dễ soát.
const RULE_ID = 'rule-1';
const ACCOUNT_ID = 'act_1';

// 2026-08-02 = Chủ Nhật, 2026-08-03 = Thứ Hai (xác nhận qua Intl trước khi viết test).
const SUNDAY_LATE_SLOT = '23:58';
const SPECIFIC_SLOTS = {
  sun: [SUNDAY_LATE_SLOT],
  mon: ['08:00'],
};

describe('isRuleDue SPECIFIC — slot cuối ngày bắc qua nửa đêm', () => {
  it('tick 00:02 Thứ Hai bắt ĐÚNG slot 23:58 Chủ Nhật (không phải slot Thứ Hai)', () => {
    const now = new Date('2026-08-03T00:02:00.000Z');
    // đảm bảo giả định weekday của test đúng như comment ở trên.
    expect(zonedTimeParts(now, TZ).weekday).toBe(1); // Mon

    const result = isRuleDue(
      { type: 'SPECIFIC', specificSlots: SPECIFIC_SLOTS },
      null,
      now,
      TZ,
    );

    expect(result.due).toBe(true);
    // aligned phải đúng mốc TUYỆT ĐỐI của slot 23:58 Chủ Nhật (ngày hôm trước), không
    // phải giờ hiện tại (00:02 Thứ Hai) và không phải slot 08:00 Thứ Hai.
    expect(result.aligned).toEqual(new Date('2026-08-02T23:58:00.000Z'));
  });

  it('tick 00:07 (tick kế tiếp theo lịch 2-59/5) đã ra khỏi cửa sổ 5 phút → không còn due', () => {
    // 00:07 cách slot 23:58 tới 9 phút > TICK_WINDOW_MS(5') nên KHÔNG được bắt lại — xác
    // nhận cơ chế mới không "quét mãi", chỉ mở đúng 1 cửa tick cho slot cuối ngày.
    const now = new Date('2026-08-03T00:07:00.000Z');
    const result = isRuleDue(
      { type: 'SPECIFIC', specificSlots: SPECIFIC_SLOTS },
      null,
      now,
      TZ,
    );
    expect(result.due).toBe(false);
  });

  it('slot giữa ngày bình thường (Thứ Hai 08:00) vẫn hoạt động như cũ, không bị ảnh hưởng', () => {
    const now = new Date('2026-08-03T08:02:00.000Z');
    const result = isRuleDue(
      { type: 'SPECIFIC', specificSlots: SPECIFIC_SLOTS },
      null,
      now,
      TZ,
    );
    expect(result.due).toBe(true);
    expect(result.aligned).toEqual(new Date('2026-08-03T08:00:00.000Z'));
  });

  it('KHÔNG double-fire: tick 08:02 Thứ Hai không bị việc "xét thêm hôm qua" kéo theo slot 23:58 Chủ Nhật', () => {
    const now = new Date('2026-08-03T08:02:00.000Z');
    const result = isRuleDue(
      { type: 'SPECIFIC', specificSlots: SPECIFIC_SLOTS },
      null,
      now,
      TZ,
    );
    // Chỉ có đúng 1 slot due (08:00 Thứ Hai) — nếu cơ chế lookback bị lỗi và bắt luôn
    // slot 23:58 hôm qua thì aligned sẽ lệch qua ngày Chủ Nhật, test trên đã chặn rồi;
    // ở đây xác nhận thêm aligned KHÔNG bằng mốc slot cuối tuần trước.
    expect(result.aligned).not.toEqual(new Date('2026-08-02T23:58:00.000Z'));
  });

  it('biên cửa sổ: diff đúng bằng TICK_WINDOW_MS (5 phút) thì KHÔNG due (chặn trên loại trừ)', () => {
    // slot 23:58 Chủ Nhật, tick đúng 5 phút sau = 00:03 Thứ Hai → diff=5, không < 5.
    const now = new Date('2026-08-03T00:03:00.000Z');
    const result = isRuleDue(
      { type: 'SPECIFIC', specificSlots: SPECIFIC_SLOTS },
      null,
      now,
      TZ,
    );
    expect(result.due).toBe(false);
  });

  it('dedupeKey TẤT ĐỊNH: 2 tick lệch giây/ms trong cùng cửa sổ của slot 23:58 CN ra CÙNG khóa', () => {
    const tickA = isRuleDue(
      { type: 'SPECIFIC', specificSlots: SPECIFIC_SLOTS },
      null,
      new Date('2026-08-03T00:02:00.100Z'),
      TZ,
    );
    const tickB = isRuleDue(
      { type: 'SPECIFIC', specificSlots: SPECIFIC_SLOTS },
      null,
      new Date('2026-08-03T00:02:59.900Z'),
      TZ,
    );
    expect(tickA.due && tickB.due).toBe(true);
    const keyA = dedupeKey(RULE_ID, ACCOUNT_ID, tickA.aligned as Date);
    const keyB = dedupeKey(RULE_ID, ACCOUNT_ID, tickB.aligned as Date);
    expect(keyA).toBe(keyB);
    expect(keyA).toBe(`${RULE_ID}:${ACCOUNT_ID}:2026-08-02T23:58:00.000Z`);
  });

  it('dedupeKey của slot cuối ngày Chủ Nhật KHÁC dedupeKey của slot Thứ Hai (cùng HH:MM khác ngày) — không đụng độ', () => {
    const slotsSameTime = { sun: ['08:00'], mon: ['08:00'] };

    const sundayTick = isRuleDue(
      { type: 'SPECIFIC', specificSlots: slotsSameTime },
      null,
      new Date('2026-08-02T08:02:00.000Z'),
      TZ,
    );
    const mondayTick = isRuleDue(
      { type: 'SPECIFIC', specificSlots: slotsSameTime },
      null,
      new Date('2026-08-03T08:02:00.000Z'),
      TZ,
    );

    expect(sundayTick.due && mondayTick.due).toBe(true);
    const keySun = dedupeKey(RULE_ID, ACCOUNT_ID, sundayTick.aligned as Date);
    const keyMon = dedupeKey(RULE_ID, ACCOUNT_ID, mondayTick.aligned as Date);
    expect(keySun).not.toBe(keyMon);
  });

  it('tuần bọc Thứ Bảy → Chủ Nhật cũng hoạt động (không chỉ riêng biên Chủ Nhật/Thứ Hai)', () => {
    // 2026-08-01 = Thứ Bảy. Slot 23:59 Thứ Bảy phải được tick 00:01 Chủ Nhật (2026-08-02) bắt.
    const now = new Date('2026-08-02T00:01:00.000Z');
    expect(zonedTimeParts(now, TZ).weekday).toBe(0); // Sun

    const result = isRuleDue(
      { type: 'SPECIFIC', specificSlots: { sat: ['23:59'] } },
      null,
      now,
      TZ,
    );
    expect(result.due).toBe(true);
    expect(result.aligned).toEqual(new Date('2026-08-01T23:59:00.000Z'));
  });

  it('không có slot nào khớp (kể cả sau khi xét thêm hôm qua) → due=false', () => {
    const now = new Date('2026-08-03T00:02:00.000Z');
    const result = isRuleDue(
      { type: 'SPECIFIC', specificSlots: { mon: ['08:00'] } }, // không có slot sun/23:58
      null,
      now,
      TZ,
    );
    expect(result.due).toBe(false);
  });
});
