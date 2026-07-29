// Khẳng định engine điều kiện TÔN TRỌNG timeframe:
//   - VALUE đọc metric trên insight của params.timeframe.
//   - METRIC đọc left/right trên leftTimeframe/rightTimeframe (mở khoá so trend).
//   - Khung fetch lỗi (value=null trong map) → điều kiện KHÔNG khớp + explain ghi lý do.
//   - Back-compat: ctx chỉ có `insight` (không map) → đọc như cũ (today).
import {
  evaluateCondition,
  explainCondition,
  type EvalContext,
} from './campaign-rule-evaluator';

// Insight tối giản: chỉ cần field số resolveMetric đọc thẳng (spend).
const ins = (spend: number | null) => (spend == null ? {} : { spend });

const baseCtx = (over: Partial<EvalContext>): EvalContext => ({
  insight: {},
  entity: {},
  now: new Date('2026-07-29T05:00:00.000Z'),
  timezone: 'Asia/Ho_Chi_Minh',
  ...over,
});

describe('evaluateCondition — VALUE tôn trọng timeframe', () => {
  it('đọc metric trên insight của đúng khung (last_7d), KHÔNG dùng today', () => {
    const ctx = baseCtx({
      insight: ins(100), // today = 100
      insightByTimeframe: new Map<string, any>([
        ['today', ins(100)],
        ['last_7d', ins(900)],
      ]),
    });
    const cond = {
      compareType: 'VALUE',
      params: { metric: 'spend', operator: 'GREATER_THAN', amount: 500, timeframe: 'last_7d' },
    };
    // Trên last_7d=900 > 500 → khớp (nếu đọc nhầm today=100 sẽ KHÔNG khớp).
    expect(evaluateCondition(cond, ctx)).toBe(true);
  });

  it('back-compat: không có map → đọc trên ct.insight (today)', () => {
    const ctx = baseCtx({ insight: ins(100) });
    const cond = {
      compareType: 'VALUE',
      params: { metric: 'spend', operator: 'GREATER_THAN', amount: 50, timeframe: 'last_7d' },
    };
    expect(evaluateCondition(cond, ctx)).toBe(true); // today=100 > 50
  });

  it('timeframe thiếu → today (fallback allow-list)', () => {
    const ctx = baseCtx({
      insight: ins(100),
      insightByTimeframe: new Map<string, any>([['today', ins(100)]]),
    });
    const cond = {
      compareType: 'VALUE',
      params: { metric: 'spend', operator: 'LESS_THAN', amount: 200 },
    };
    expect(evaluateCondition(cond, ctx)).toBe(true);
  });
});

describe('evaluateCondition — METRIC so trend hai khung khác nhau', () => {
  it('spend[today] > 1.5× spend-TB? (today vs last_3d) — mỗi vế đọc đúng khung', () => {
    const ctx = baseCtx({
      insight: ins(300),
      insightByTimeframe: new Map<string, any>([
        ['today', ins(300)],
        ['last_3d', ins(150)],
      ]),
    });
    const cond = {
      compareType: 'METRIC',
      params: {
        leftMetric: 'spend',
        leftTimeframe: 'today',
        operator: 'GREATER_THAN',
        multiplier: 1.5,
        rightMetric: 'spend',
        rightTimeframe: 'last_3d',
      },
    };
    // 300 > 1.5 × 150 (=225) → true. Nếu cả 2 vế đọc cùng today (300>450) sẽ là false.
    expect(evaluateCondition(cond, ctx)).toBe(true);
  });
});

describe('evaluateCondition — cô lập lỗi khung', () => {
  it('khung lỗi (null trong map) → điều kiện KHÔNG khớp + explain ghi lý do', () => {
    const ctx = baseCtx({
      insight: ins(100),
      insightByTimeframe: new Map<string, any>([
        ['today', ins(100)],
        ['last_7d', null], // fetch lỗi
      ]),
      insightErrors: new Map<string, string>([['last_7d', 'Meta timeout']]),
    });
    const cond = {
      compareType: 'VALUE',
      params: { metric: 'spend', operator: 'GREATER_THAN', amount: 1, timeframe: 'last_7d' },
    };
    expect(evaluateCondition(cond, ctx)).toBe(false);
    const ex = explainCondition(cond, ctx);
    expect(ex.matched).toBe(false);
    expect(ex.actualText).toBe('không đo được');
    expect(ex.note).toContain('Lỗi lấy số liệu khung');
    expect(ex.note).toContain('Meta timeout');
    // Nhãn kèm khung ≠ today.
    expect(ex.label).toContain('7 ngày qua');
  });
});

describe('explainCondition — nhãn kèm timeframe', () => {
  it('today → giữ nhãn gốc; khác today → "(<khung>)"', () => {
    const ctx = baseCtx({
      insight: ins(100),
      insightByTimeframe: new Map<string, any>([
        ['today', ins(100)],
        ['last_7d', ins(700)],
      ]),
    });
    const today = explainCondition(
      { compareType: 'VALUE', params: { metric: 'spend', operator: 'GREATER_THAN', amount: 1, timeframe: 'today' } },
      ctx,
    );
    expect(today.label).toBe('Chi tiêu hôm nay');
    const week = explainCondition(
      { compareType: 'VALUE', params: { metric: 'spend', operator: 'GREATER_THAN', amount: 1, timeframe: 'last_7d' } },
      ctx,
    );
    expect(week.label).toBe('Chi tiêu (7 ngày qua)');
  });
});
