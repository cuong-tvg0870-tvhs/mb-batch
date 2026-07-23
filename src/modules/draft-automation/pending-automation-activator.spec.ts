import {
  computeSchedulesToDelete,
  computeSpecsToCreate,
  scheduleKey,
} from './pending-automation-activator';

// Lịch LIVE trên Meta (đã chuẩn hoá unix + budget).
const live = (
  id: string,
  ts: number,
  te: number,
  bv: number | null,
  bt: string | null,
) => ({ id, time_start: ts, time_end: te, budget_value: bv, budget_value_type: bt });
// Spec mong muốn.
const spec = (ts: number, te: number, bv: number, bt: string) => ({
  time_start: ts,
  time_end: te,
  budget_value: bv,
  budget_value_type: bt,
});

describe('scheduleKey', () => {
  it('khớp khi time + budget + type giống hệt', () => {
    expect(scheduleKey(100, 200, 50, 'MULTIPLIER')).toBe(
      scheduleKey(100, 200, 50, 'MULTIPLIER'),
    );
  });
  it('khác khi mức tăng khác', () => {
    expect(scheduleKey(100, 200, 50, 'MULTIPLIER')).not.toBe(
      scheduleKey(100, 200, 80, 'MULTIPLIER'),
    );
  });
});

describe('computeSchedulesToDelete — APPEND-ONLY (chỉ xoá owned & !desired)', () => {
  it('XOÁ lịch do MÌNH tạo (owned) mà KHÔNG còn mong muốn', () => {
    const del = computeSchedulesToDelete(
      [live('mine', 100, 200, 50, 'MULTIPLIER')],
      [spec(300, 400, 99, 'MULTIPLIER')], // spec khác hẳn → mine không còn mong muốn
      ['mine'],
    );
    expect(del).toEqual(['mine']);
  });

  it('GIỮ lịch owned mà VẪN mong muốn (adopt, không xoá-tạo lại)', () => {
    const del = computeSchedulesToDelete(
      [live('mine', 100, 200, 50, 'MULTIPLIER')],
      [spec(100, 200, 50, 'MULTIPLIER')], // vẫn đúng khung/mức → giữ
      ['mine'],
    );
    expect(del).toEqual([]);
  });

  it('KHÔNG xoá lịch NGOẠI LAI giống hệt spec (adopt — an toàn cho lịch tay)', () => {
    const del = computeSchedulesToDelete(
      [live('foreign', 100, 200, 50, 'MULTIPLIER')],
      [spec(100, 200, 50, 'MULTIPLIER')],
      [], // không owned → KHÔNG đụng, dù trùng nội dung
    );
    expect(del).toEqual([]);
  });

  it('KHÔNG xoá lịch tay CÙNG KHUNG nhưng KHÁC MỨC', () => {
    const del = computeSchedulesToDelete(
      [live('manual', 100, 200, 80, 'MULTIPLIER')],
      [spec(100, 200, 50, 'MULTIPLIER')],
      [],
    );
    expect(del).toEqual([]);
  });

  it('KHÔNG xoá lịch KHÁC KHUNG giờ (dù owned? — owned nhưng !desired mới xoá)', () => {
    const del = computeSchedulesToDelete(
      [live('other', 500, 600, 50, 'MULTIPLIER')],
      [spec(100, 200, 50, 'MULTIPLIER')],
      [], // không owned → giữ
    );
    expect(del).toEqual([]);
  });

  it('KHÔNG xoá lịch cùng khung+mức nhưng KHÁC KIỂU (ABSOLUTE vs MULTIPLIER)', () => {
    const del = computeSchedulesToDelete(
      [live('abs', 100, 200, 50, 'ABSOLUTE')],
      [spec(100, 200, 50, 'MULTIPLIER')],
      ['abs'], // owned + khác kiểu ⇒ !desired ⇒ xoá (khung cũ của mình đã đổi kiểu)
    );
    expect(del).toEqual(['abs']);
  });

  it('kết hợp: xoá owned-đã-bỏ, giữ owned-còn-dùng + mọi lịch ngoại lai', () => {
    const del = computeSchedulesToDelete(
      [
        live('mine_old', 700, 800, 10, 'MULTIPLIER'), // owned, KHÔNG còn mong muốn → XOÁ
        live('mine_keep', 100, 200, 50, 'MULTIPLIER'), // owned, còn mong muốn → GIỮ
        live('foreign_exact', 300, 400, 60, 'MULTIPLIER'), // ngoại lai, trùng spec → GIỮ (adopt)
        live('foreign_other', 900, 1000, 5, 'ABSOLUTE'), // ngoại lai, không liên quan → GIỮ
      ],
      [spec(100, 200, 50, 'MULTIPLIER'), spec(300, 400, 60, 'MULTIPLIER')],
      ['mine_old', 'mine_keep'],
    );
    expect(del).toEqual(['mine_old']);
  });

  it('không có gì để xoá → rỗng', () => {
    expect(
      computeSchedulesToDelete([], [spec(100, 200, 50, 'MULTIPLIER')], []),
    ).toEqual([]);
  });
});

describe('computeSpecsToCreate — chỉ tạo khung CÒN THIẾU (adopt khung đã có)', () => {
  it('tạo spec CHƯA có trên Meta', () => {
    const toCreate = computeSpecsToCreate([], [spec(100, 200, 50, 'MULTIPLIER')]);
    expect(toCreate).toHaveLength(1);
  });

  it('KHÔNG tạo spec đã có giống hệt (adopt lịch ngoại lai)', () => {
    const toCreate = computeSpecsToCreate(
      [live('foreign', 100, 200, 50, 'MULTIPLIER')],
      [spec(100, 200, 50, 'MULTIPLIER')],
    );
    expect(toCreate).toEqual([]);
  });

  it('KHÔNG tạo spec đã có (adopt lịch của mình)', () => {
    const toCreate = computeSpecsToCreate(
      [live('mine', 100, 200, 50, 'MULTIPLIER')],
      [spec(100, 200, 50, 'MULTIPLIER')],
    );
    expect(toCreate).toEqual([]);
  });

  it('TẠO spec cùng khung nhưng KHÁC MỨC (khác key)', () => {
    const toCreate = computeSpecsToCreate(
      [live('manual', 100, 200, 80, 'MULTIPLIER')],
      [spec(100, 200, 50, 'MULTIPLIER')],
    );
    expect(toCreate).toHaveLength(1);
    expect(toCreate[0].budget_value).toBe(50);
  });

  it('mix: chỉ tạo spec chưa tồn tại, adopt spec đã có', () => {
    const toCreate = computeSpecsToCreate(
      [live('exact', 100, 200, 50, 'MULTIPLIER')],
      [spec(100, 200, 50, 'MULTIPLIER'), spec(300, 400, 60, 'MULTIPLIER')],
    );
    expect(toCreate).toHaveLength(1);
    expect(toCreate[0].time_start).toBe(300);
  });
});
