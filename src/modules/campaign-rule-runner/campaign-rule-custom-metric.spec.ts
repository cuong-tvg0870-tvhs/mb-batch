// Khẳng định các bảo đảm CHỐNG SAI SỐ của evaluator custom metric (budget-schedule):
//   - số học đúng (CPA/lợi nhuận) trên đúng snapshot base.
//   - null-propagation: base thiếu → null → điều kiện KHÔNG khớp (không âm thầm pass).
//   - chia 0 → null; Infinity/NaN → null (không lọt vào compare).
//   - format='percentage' → ×100 đúng một lần; numeric/currency KHÔNG scale.
//   - validate: base ∉ allow-list → chặn; cấm lồng custom_metric:; cú pháp/ngoặc.
import {
  evaluateCustomMetric,
  evaluateFormula,
  validateCustomFormula,
  type CustomFormula,
} from './campaign-rule-custom-metric';

const v = (value: string) => ({ type: 'variable' as const, value });
const n = (value: string) => ({ type: 'number' as const, value });
const o = (value: '+' | '-' | '*' | '/' | '(' | ')') => ({
  type: 'operator' as const,
  value,
});

// base resolver giả: trả số theo map, undefined → null (mô phỏng resolveMetric thiếu số)
const resolver = (map: Record<string, number | null>) => (k: string) =>
  k in map ? map[k] : null;

describe('evaluateFormula — số học & null/0', () => {
  it('CPA = spend / purchases (tiền MAJOR / count)', () => {
    const f: CustomFormula = [v('spend'), o('/'), v('purchases')];
    expect(evaluateFormula(f, resolver({ spend: 300000, purchases: 3 }))).toBe(
      100000,
    );
  });

  it('lợi nhuận = purchases_value - spend', () => {
    const f: CustomFormula = [v('purchases_value'), o('-'), v('spend')];
    expect(
      evaluateFormula(f, resolver({ purchases_value: 1000000, spend: 300000 })),
    ).toBe(700000);
  });

  it('tôn trọng độ ưu tiên + ngoặc', () => {
    // (spend + spend) / clicks = 200/10 = 20
    const f: CustomFormula = [
      o('('),
      v('spend'),
      o('+'),
      v('spend'),
      o(')'),
      o('/'),
      v('clicks'),
    ];
    expect(evaluateFormula(f, resolver({ spend: 100, clicks: 10 }))).toBe(20);
  });

  it('base thiếu số → NULL lan truyền (không coi là 0)', () => {
    const f: CustomFormula = [v('spend'), o('/'), v('purchases')];
    // purchases thiếu (đầu ngày chưa có đơn) → null, KHÔNG phải CPA=spend/0
    expect(evaluateFormula(f, resolver({ spend: 300000 }))).toBeNull();
  });

  it('chia 0 → null', () => {
    const f: CustomFormula = [v('spend'), o('/'), v('purchases')];
    expect(evaluateFormula(f, resolver({ spend: 300000, purchases: 0 }))).toBeNull();
  });

  it('hằng số + biến', () => {
    const f: CustomFormula = [v('spend'), o('*'), n('2')];
    expect(evaluateFormula(f, resolver({ spend: 50 }))).toBe(100);
  });
});

describe('evaluateCustomMetric — format scaling', () => {
  it('percentage → ×100 đúng một lần (CV = purchases/clicks)', () => {
    const f: CustomFormula = [v('purchases'), o('/'), v('clicks')];
    const val = evaluateCustomMetric(
      { id: 'x', formula: f, format: 'percentage' },
      resolver({ purchases: 2, clicks: 10 }),
    );
    // 0.2 → 20 (%)
    expect(val).toBe(20);
  });

  it('currency/numeric KHÔNG scale', () => {
    const f: CustomFormula = [v('spend'), o('/'), v('purchases')];
    expect(
      evaluateCustomMetric(
        { id: 'x', formula: f, format: 'currency' },
        resolver({ spend: 300000, purchases: 3 }),
      ),
    ).toBe(100000);
    expect(
      evaluateCustomMetric(
        { id: 'x', formula: f, format: 'numeric' },
        resolver({ spend: 300000, purchases: 3 }),
      ),
    ).toBe(100000);
  });

  it('percentage với base null → vẫn null (KHÔNG ×100 của null)', () => {
    const f: CustomFormula = [v('purchases'), o('/'), v('clicks')];
    expect(
      evaluateCustomMetric(
        { id: 'x', formula: f, format: 'percentage' },
        resolver({ clicks: 10 }),
      ),
    ).toBeNull();
  });
});

describe('validateCustomFormula', () => {
  const allow = new Set(['spend', 'purchases', 'purchases_value', 'clicks']);

  it('công thức hợp lệ', () => {
    const f: CustomFormula = [v('spend'), o('/'), v('purchases')];
    expect(validateCustomFormula(f, allow)).toEqual({ ok: true });
  });

  it('chặn base ngoài allow-list (vd daily_budget MINOR)', () => {
    const f: CustomFormula = [v('spend'), o('/'), v('daily_budget')];
    const r = validateCustomFormula(f, allow);
    expect(r.ok).toBe(false);
  });

  it('cấm lồng custom_metric:', () => {
    const f: CustomFormula = [v('custom_metric:abc'), o('+'), v('spend')];
    const r = validateCustomFormula(f, allow);
    expect(r.ok).toBe(false);
  });

  it('chặn 2 toán tử liền nhau', () => {
    const f: CustomFormula = [v('spend'), o('+'), o('/'), v('purchases')];
    expect(validateCustomFormula(f, allow).ok).toBe(false);
  });

  it('chặn ngoặc lệch', () => {
    const f: CustomFormula = [o('('), v('spend'), o('/'), v('purchases')];
    expect(validateCustomFormula(f, allow).ok).toBe(false);
  });

  it('chặn thiếu toán hạng', () => {
    const f: CustomFormula = [v('spend'), o('/')];
    expect(validateCustomFormula(f, allow).ok).toBe(false);
  });
});
