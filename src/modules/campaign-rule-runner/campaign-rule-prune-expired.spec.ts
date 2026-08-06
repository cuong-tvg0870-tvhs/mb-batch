import {
  deleteBudgetSchedules,
  selectExpiredOwnedSchedules,
  type LiveWindow,
} from './campaign-rule-executor';
import {
  MAX_EXPIRED_PRUNE_PER_RUN,
  PRUNE_MIN_HEADROOM_MS,
  pruneExpiredSchedulesEnabled,
  RULE_RUN_MAX_WALL_MS,
} from './campaign-rule-runner.constants';

jest.mock('facebook-nodejs-business-sdk', () => ({
  AdSet: class {},
  Campaign: class {},
  FacebookAdsApi: { init: jest.fn() },
  HighDemandPeriod: class {
    constructor(public id: string) {}
    async delete() {
      return (global as any).__hdpDelete(this.id);
    }
  },
}));

const NOW = 1_770_000_000; // unix giây, mốc "bây giờ" của mọi test dưới

const w = (id: string, endOffsetSec: number): LiveWindow => ({
  id,
  time_start: NOW + endOffsetSec - 3 * 3600,
  time_end: NOW + endOffsetSec,
});

/**
 * Meta chặn 50 "khoảng thời gian có nhu cầu cao" mỗi entity và KHÔNG tự dọn khung đã hết
 * hạn — xác minh trên prod 06/08/2026: 2 campaign kín đúng 50/50 mà cả 50 khung đều đã
 * kết thúc (cũ nhất từ 10/2025) ⇒ rule tắc vĩnh viễn, mọi lượt khớp đều FAILED.
 * Bộ test này khoá lại phạm vi được phép xoá — đây là thao tác XOÁ THẬT trên Meta.
 */
describe('chọn khung được phép xoá', () => {
  it('CHỈ khung đã kết thúc trong quá khứ', () => {
    const live = [
      w('qua-khu', -3600),
      w('vua-het', -1),
      w('dang-chay', +3600),
      w('tuong-lai', +86_400),
    ];
    const owned = new Set(['qua-khu', 'vua-het', 'dang-chay', 'tuong-lai']);

    const picked = selectExpiredOwnedSchedules(live, owned, NOW).map((x) => x.id);

    expect(picked).toEqual(['qua-khu', 'vua-het']);
  });

  it('KHÔNG đụng khung người dùng tự đặt trên Meta (không thuộc hệ thống)', () => {
    const live = [w('cua-he-thong', -7200), w('cua-nguoi-dung', -7200)];
    const owned = new Set(['cua-he-thong']);

    expect(selectExpiredOwnedSchedules(live, owned, NOW).map((x) => x.id)).toEqual([
      'cua-he-thong',
    ]);
  });

  it('xếp CŨ NHẤT trước (dọn rác lâu đời trước, đúng yêu cầu vận hành)', () => {
    const live = [w('b', -3600), w('c', -600), w('a', -864_000)];
    const owned = new Set(['a', 'b', 'c']);

    expect(selectExpiredOwnedSchedules(live, owned, NOW).map((x) => x.id)).toEqual([
      'a',
      'b',
      'c',
    ]);
  });

  it('bỏ qua mốc thời gian hỏng (không xoá nhầm vì parse lỗi)', () => {
    const live = [
      { id: 'nan', time_start: NOW, time_end: Number.NaN },
      { id: 'zero', time_start: NOW, time_end: 0 },
      { id: 'ok', time_start: NOW - 7200, time_end: NOW - 3600 },
    ];
    const owned = new Set(['nan', 'zero', 'ok']);

    expect(selectExpiredOwnedSchedules(live, owned, NOW).map((x) => x.id)).toEqual([
      'ok',
    ]);
  });

  it('tái hiện ca prod: 50/50 khung đều hết hạn → giải phóng được chỗ', () => {
    const live = Array.from({ length: 50 }, (_, i) => w(`s${i}`, -(i + 1) * 3600));
    const owned = new Set(live.map((x) => x.id));

    const picked = selectExpiredOwnedSchedules(live, owned, NOW);

    expect(picked).toHaveLength(50);
    // Trần mỗi lượt vẫn chừa đủ chỗ để tạo khung mới ngay lượt này.
    expect(MAX_EXPIRED_PRUNE_PER_RUN).toBeGreaterThan(0);
    expect(picked.slice(0, MAX_EXPIRED_PRUNE_PER_RUN).length).toBeLessThanOrEqual(50);
  });
});

describe('xoá khung trên Meta', () => {
  afterEach(() => {
    delete (global as any).__hdpDelete;
  });

  it('trả về đúng danh sách ĐÃ XOÁ để ghi vào nhật ký hệ thống', async () => {
    (global as any).__hdpDelete = async (id: string) => {
      if (id === 'x2') throw new Error('Meta từ chối');
      return {};
    };

    const res = await deleteBudgetSchedules(['x1', 'x2', 'x3']);

    expect(res.deletedIds).toEqual(['x1', 'x3']);
    expect(res.failedIds).toEqual(['x2']);
    expect(res.removed).toBe(2);
    expect(res.skippedIds).toEqual([]);
  });

  it('DỪNG SẠCH khi sắp hết hạn chót lượt chạy — phần còn lại vào skippedIds', async () => {
    // Vượt hạn chót = chạy không còn khóa `crr:<ruleId>` → nguy cơ bơm ngân sách hai lần.
    const seen: string[] = [];
    (global as any).__hdpDelete = async (id: string) => {
      seen.push(id);
      return {};
    };
    let calls = 0;
    const shouldStop = () => {
      calls += 1;
      return calls > 2; // cho xoá đúng 2 khung rồi chặn
    };

    const res = await deleteBudgetSchedules(['a', 'b', 'c', 'd', 'e'], shouldStop);

    expect(seen).toEqual(['a', 'b']);
    expect(res.deletedIds).toEqual(['a', 'b']);
    expect(res.skippedIds).toEqual(['c', 'd', 'e']);
  });

  it('biên an toàn phải nhỏ hơn hạn chót lượt chạy (nếu không thì không bao giờ xoá được)', () => {
    expect(PRUNE_MIN_HEADROOM_MS).toBeGreaterThan(0);
    expect(PRUNE_MIN_HEADROOM_MS).toBeLessThan(RULE_RUN_MAX_WALL_MS);
  });

  it('kill-switch: mặc định BẬT, tắt được bằng env mà không cần deploy', () => {
    const before = process.env.CAMPAIGN_RULE_PRUNE_EXPIRED;
    try {
      delete process.env.CAMPAIGN_RULE_PRUNE_EXPIRED;
      expect(pruneExpiredSchedulesEnabled()).toBe(true);
      process.env.CAMPAIGN_RULE_PRUNE_EXPIRED = 'false';
      expect(pruneExpiredSchedulesEnabled()).toBe(false);
      process.env.CAMPAIGN_RULE_PRUNE_EXPIRED = 'true';
      expect(pruneExpiredSchedulesEnabled()).toBe(true);
    } finally {
      if (before === undefined) delete process.env.CAMPAIGN_RULE_PRUNE_EXPIRED;
      else process.env.CAMPAIGN_RULE_PRUNE_EXPIRED = before;
    }
  });
});
