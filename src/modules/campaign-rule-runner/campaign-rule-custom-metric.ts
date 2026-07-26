/**
 * ============================================================================
 *  CUSTOM METRIC cho rule "tăng ngân sách theo lịch" (BUDGET_SCHEDULE).
 * ----------------------------------------------------------------------------
 *  PORT của evaluator care-ads (src/common/meta/custom-metrics/formula-evaluator.ts)
 *  — GIỮ NGUYÊN token shape {type,value,key}, shunting-yard→RPN, /0→null. Đây là
 *  file MỚI, self-contained, KHÔNG import mb-ads/care-ads (giữ tách repo).
 *
 *  Công thức = mảng token: variable (1 base metric snake của budget-schedule) /
 *  number (hằng) / operator (+ − × ÷ và ngoặc). Base metric resolve qua CHÍNH
 *  resolveMetric(baseKey, insight, entity) của runner (Meta LIVE today), nên số
 *  base KHỚP TUYỆT ĐỐI với metric native — không có nguồn sai số ở tầng base.
 *
 *  KHÁC care-ads MỘT ĐIỂM (cố ý, an toàn cho rule BƠM ngân sách):
 *    care-ads coerce base thiếu-số → 0 (Bir.ch parity: registry.resolve trả 0).
 *    Ở đây resolveMetric mb-batch trả NULL khi thiếu số; ta GIỮ null-propagation
 *    (bất kỳ base null → metric null → điều kiện coi như KHÔNG khớp → không bump).
 *    Lý do: ép 0 khiến CPA = spend/0-đơn → 0 → thoả "CPA < ngưỡng" → BƠM nhầm
 *    camp chưa có đơn. null-propagation khớp triết lý "không âm thầm pass" của
 *    resolveMetric hiện có. Muốn parity tuyệt đối care-ads: đổi 1 dòng ở
 *    evaluateCustomMetric (chú thích COERCE bên dưới).
 * ============================================================================
 */

export type CustomMetricFormatKind = 'numeric' | 'currency' | 'percentage';

export type CustomFormulaToken =
  | { type: 'variable'; value: string; key?: string }
  | { type: 'number'; value: string; key?: string }
  | { type: 'operator'; value: '+' | '-' | '*' | '/' | '(' | ')'; key?: string };

export type CustomFormula = CustomFormulaToken[];

/** Projection runtime của 1 row CustomMetric (không phụ thuộc Prisma). */
export interface CustomMetricEvalDef {
  id: string;
  formula: CustomFormula;
  format: CustomMetricFormatKind;
}

const ARITHMETIC = new Set(['+', '-', '*', '/']);
const PRECEDENCE: Record<string, number> = { '+': 1, '-': 1, '*': 2, '/': 2 };

export class FormulaError extends Error {}

function isArithmetic(token: CustomFormulaToken): boolean {
  return token.type === 'operator' && ARITHMETIC.has(token.value);
}

/** Shunting-yard: infix token → RPN. Ném FormulaError khi ngoặc lệch. */
function toRpn(tokens: CustomFormula): CustomFormulaToken[] {
  const output: CustomFormulaToken[] = [];
  const ops: CustomFormulaToken[] = [];

  for (const tok of tokens) {
    if (tok.type === 'variable' || tok.type === 'number') {
      output.push(tok);
    } else if (ARITHMETIC.has(tok.value)) {
      while (
        ops.length > 0 &&
        isArithmetic(ops[ops.length - 1]) &&
        PRECEDENCE[ops[ops.length - 1].value] >= PRECEDENCE[tok.value]
      ) {
        output.push(ops.pop()!);
      }
      ops.push(tok);
    } else if (tok.value === '(') {
      ops.push(tok);
    } else {
      // ')'
      let foundOpen = false;
      while (ops.length > 0) {
        const top = ops.pop()!;
        if (top.type === 'operator' && top.value === '(') {
          foundOpen = true;
          break;
        }
        output.push(top);
      }
      if (!foundOpen) throw new FormulaError('Dấu ngoặc không cân bằng');
    }
  }

  while (ops.length > 0) {
    const top = ops.pop()!;
    if (top.type === 'operator' && top.value === '(') {
      throw new FormulaError('Dấu ngoặc không cân bằng');
    }
    output.push(top);
  }

  return output;
}

/** Mô phỏng stack RPN để bắt toán tử thiếu toán hạng trước khi chạy. */
function validateRpnArity(rpn: CustomFormulaToken[]): void {
  let size = 0;
  for (const tok of rpn) {
    if (isArithmetic(tok)) {
      size -= 2;
      if (size < 0) throw new FormulaError('Thiếu toán hạng cho toán tử');
      size += 1;
    } else {
      size += 1;
    }
  }
  if (size !== 1) throw new FormulaError('Biểu thức không hợp lệ');
}

/**
 * Validate thuần (dùng chung create/update). KHÁC care-ads: variable phải nằm trong
 * `allowedBaseKeys` (allow-list base của budget-schedule) VÀ KHÔNG được là ref
 * `custom_metric:` (cấm lồng nhau → độ sâu tối đa = 1, chống đệ quy/vòng).
 *   1. ≥ 1 token.
 *   2. Mọi variable ∈ allowedBaseKeys, không mang prefix custom_metric:.
 *   3. Không 2 toán tử số học liền nhau.
 *   4. Parse shunting-yard thành công (ngoặc cân + arity đúng).
 */
export function validateCustomFormula(
  tokens: CustomFormula,
  allowedBaseKeys: Set<string>,
): { ok: true } | { ok: false; reason: string } {
  if (!Array.isArray(tokens) || tokens.length < 1) {
    return { ok: false, reason: 'Công thức phải có ít nhất 1 token' };
  }

  for (const tok of tokens) {
    if (tok.type === 'variable') {
      const v = String(tok.value || '').toLowerCase();
      if (v.startsWith('custom_metric:')) {
        return { ok: false, reason: 'Không được dùng custom metric bên trong công thức' };
      }
      if (!allowedBaseKeys.has(v)) {
        return { ok: false, reason: `Metric "${tok.value}" không dùng được ở ngữ cảnh này` };
      }
    }
  }

  for (let i = 1; i < tokens.length; i += 1) {
    if (isArithmetic(tokens[i - 1]) && isArithmetic(tokens[i])) {
      return { ok: false, reason: 'Không được đặt 2 toán tử liền nhau' };
    }
  }

  try {
    const rpn = toRpn(tokens);
    validateRpnArity(rpn);
  } catch (err) {
    return { ok: false, reason: (err as Error).message };
  }

  return { ok: true };
}

function applyOperator(op: string, a: number, b: number): number | null {
  switch (op) {
    case '+':
      return a + b;
    case '-':
      return a - b;
    case '*':
      return a * b;
    case '/':
      return b === 0 ? null : a / b;
    default:
      return null;
  }
}

/**
 * Đánh giá formula (đã validate) với resolver base đồng bộ.
 *   - variable → resolveVariable(key); null LAN TRUYỀN (cả formula → null).
 *   - number   → parseFloat.
 *   - chia 0   → null. Kết quả non-finite → null (chặn Infinity/NaN lọt vào compare).
 * Ném FormulaError chỉ khi token array sai cấu trúc (caller phải validate trước).
 */
export function evaluateFormula(
  tokens: CustomFormula,
  resolveVariable: (metric: string) => number | null,
): number | null {
  const rpn = toRpn(tokens);
  const stack: (number | null)[] = [];

  for (const tok of rpn) {
    if (tok.type === 'variable') {
      stack.push(resolveVariable(String(tok.value)));
    } else if (tok.type === 'number') {
      const n = Number.parseFloat(String(tok.value));
      stack.push(Number.isFinite(n) ? n : null);
    } else {
      const b = stack.pop();
      const a = stack.pop();
      if (a === undefined || b === undefined) {
        throw new FormulaError('Biểu thức không hợp lệ');
      }
      if (a === null || b === null) {
        stack.push(null);
        continue;
      }
      const r = applyOperator(tok.value, a, b);
      stack.push(r === null || !Number.isFinite(r) ? null : r);
    }
  }

  if (stack.length !== 1) throw new FormulaError('Biểu thức không hợp lệ');
  return stack[0];
}

/**
 * Giá trị cuối của 1 custom metric cho điều kiện rule. `resolveBase` = hàm resolve
 * base metric của runner (resolveMetric(key, insight, entity)). format='percentage'
 * → ×100 ĐÚNG MỘT LẦN (giống care-ads: user khai formula ra RATIO, hệ nhân 100 để
 * so ngưỡng "as-typed" 45 = 45%). numeric/currency KHÔNG scale.
 *
 * NULL-propagation: base thiếu → null → trả null (điều kiện skip, không bump nhầm).
 * Muốn parity care-ads (coerce 0): đổi `value` thành `value ?? 0` ở dòng COERCE.
 */
export function evaluateCustomMetric(
  def: CustomMetricEvalDef,
  resolveBase: (baseKey: string) => number | null,
): number | null {
  let value: number | null;
  try {
    value = evaluateFormula(def.formula, resolveBase);
  } catch {
    return null; // formula hỏng cấu trúc → coi như không đo được
  }
  // COERCE (đổi dòng này thành `value = value ?? 0;` nếu muốn parity care-ads tuyệt đối)
  if (value === null) return null;
  return def.format === 'percentage' ? value * 100 : value;
}
