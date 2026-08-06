import { DEFAULT_TIMEZONE } from './campaign-rule-runner.constants';
import type { CustomMetricEvalDef } from './campaign-rule-custom-metric';
import { resolveMetric } from './campaign-rule-metric.resolver';
import { zonedTimeParts } from './campaign-rule-schedule.util';
import { normalizeTimeframe, timeframeLabelVi } from './campaign-rule-timeframe';

/**
 * Bộ đánh giá cây điều kiện của một task.
 *
 * ctx.insight = insight LIVE khung HÔM NAY (giữ cho guard/snapshot/rolling); mỗi
 * điều kiện tự đọc số trên insight của ĐÚNG khung nó khai (ctx.insightByTimeframe).
 * ctx.entity = bản ghi DB (Campaign/AdSet) để lấy ngân sách. ctx.now/ctx.timezone
 * dùng cho điều kiện TIME.
 */

export interface EvalContext {
  // Insight khung HÔM NAY (back-compat: guard/snapshot/rolling luôn đọc trên khung này).
  insight: any;
  // Map khung-thời-gian(đã normalize) → insight row của khung đó. Điều kiện VALUE đọc
  // trên timeframe của nó; METRIC đọc left/right theo leftTimeframe/rightTimeframe.
  // Thiếu map/thiếu key → fallback ctx.insight (today) → back-compat khi caller cũ chỉ
  // truyền `insight`. Value = null nghĩa là fetch khung đó LỖI (điều kiện coi như không
  // khớp — resolveMetric trên null trả null nhờ optional-chaining).
  insightByTimeframe?: Map<string, any>;
  // Khung fetch LỖI → msg (chỉ để EXPLAIN ghi lý do; không ảnh hưởng logic matched).
  insightErrors?: Map<string, string>;
  entity: any;
  now: Date;
  timezone: string;
  // Registry custom metric đã prefetch & lọc context='BUDGET_SCHEDULE' (theo id ref
  // `custom_metric:<id>` lowercase). Optional: rule không dùng custom → undefined.
  customMetrics?: Map<string, CustomMetricEvalDef>;
}

/**
 * Chọn insight row theo timeframe của điều kiện. Có map + có key → dùng row đó (kể cả
 * null khi khung lỗi); còn lại fallback ctx.insight (today) để giữ hành vi cũ với caller
 * chỉ truyền `insight`. Metric của custom formula cũng resolve trên CHÍNH row này (vì
 * resolveMetric luồn cùng insight xuống base) → mọi vế của 1 điều kiện đồng khung.
 */
function insightForTimeframe(ctx: EvalContext, rawTf: unknown): any {
  const tf = normalizeTimeframe(rawTf);
  if (ctx.insightByTimeframe && ctx.insightByTimeframe.has(tf)) {
    return ctx.insightByTimeframe.get(tf);
  }
  return ctx.insight;
}

/** Lý do EXPLAIN khi khung nào đó fetch lỗi (dò theo thứ tự các timeframe truyền vào). */
function timeframeErrorNote(
  ctx: EvalContext,
  ...rawTfs: unknown[]
): string | undefined {
  if (!ctx.insightErrors) return undefined;
  for (const raw of rawTfs) {
    const tf = normalizeTimeframe(raw);
    const msg = ctx.insightErrors.get(tf);
    if (msg) return `Lỗi lấy số liệu khung ${timeframeLabelVi(tf)}: ${msg}`;
  }
  return undefined;
}

type Operator =
  | 'GREATER_THAN'
  | 'LESS_THAN'
  | 'GREATER_THAN_OR_EQUAL'
  | 'LESS_THAN_OR_EQUAL'
  | 'EQUAL'
  | 'NOT_EQUAL';

/** 6 toán tử so sánh số. */
export function compare(a: number, op: Operator | string, b: number): boolean {
  switch (op) {
    case 'GREATER_THAN':
      return a > b;
    case 'LESS_THAN':
      return a < b;
    case 'GREATER_THAN_OR_EQUAL':
      return a >= b;
    case 'LESS_THAN_OR_EQUAL':
      return a <= b;
    case 'EQUAL':
      return a === b;
    case 'NOT_EQUAL':
      return a !== b;
    default:
      return false;
  }
}

/**
 * Đánh giá một group đệ quy. Kết hợp con (conditions + childGroups) theo operator:
 * AND = mọi con true, OR = ít nhất 1 con true. Group RỖNG (không con) → true.
 */
export function evaluateGroup(group: any, ctx: EvalContext): boolean {
  if (!group) return true;

  const conditions: any[] = Array.isArray(group.conditions)
    ? group.conditions
    : [];
  const childGroups: any[] = Array.isArray(group.childGroups)
    ? group.childGroups
    : [];

  const results: boolean[] = [
    ...conditions.map((c) => evaluateCondition(c, ctx)),
    ...childGroups.map((g) => evaluateGroup(g, ctx)),
  ];

  if (results.length === 0) return true;
  return group.operator === 'OR'
    ? results.some(Boolean)
    : results.every(Boolean);
}

/**
 * Đánh giá một điều kiện lá theo compareType.
 * - VALUE: so metric (đọc trên khung params.timeframe) với hằng số amount.
 * - METRIC: so leftMetric[leftTimeframe] với (multiplier ?? 1) * rightMetric[rightTimeframe]
 *   — mỗi vế đọc trên insight của ĐÚNG khung nó khai (mở khoá so sánh trend, vd today vs last_3d).
 * - TIME: giờ hiện tại theo timezone của điều kiện (fallback ctx.timezone → default).
 * - RANKING: chưa hỗ trợ → false (không âm thầm pass).
 */
export function evaluateCondition(cond: any, ctx: EvalContext): boolean {
  if (!cond) return false;
  const p = cond.params || {};

  switch (cond.compareType) {
    case 'VALUE': {
      const insight = insightForTimeframe(ctx, p.timeframe);
      const v = resolveMetric(p.metric, insight, ctx.entity, ctx.customMetrics);
      if (v == null) return false;
      const amount = Number(p.amount);
      if (!Number.isFinite(amount)) return false;
      return compare(v, p.operator, amount);
    }

    case 'METRIC': {
      const leftInsight = insightForTimeframe(ctx, p.leftTimeframe);
      const rightInsight = insightForTimeframe(ctx, p.rightTimeframe);
      const left = resolveMetric(p.leftMetric, leftInsight, ctx.entity, ctx.customMetrics);
      const right = resolveMetric(p.rightMetric, rightInsight, ctx.entity, ctx.customMetrics);
      if (left == null || right == null) return false;
      const multiplier =
        p.multiplier == null || !Number.isFinite(Number(p.multiplier))
          ? 1
          : Number(p.multiplier);
      return compare(left, p.operator, multiplier * right);
    }

    case 'TIME': {
      // "account" (sentinel) hoặc rỗng → dùng tz account đã resolve ở ctx.timezone.
      const tz =
        !p.timezone || p.timezone === 'account'
          ? ctx.timezone || DEFAULT_TIMEZONE
          : p.timezone;
      const { weekday, hour } = zonedTimeParts(ctx.now, tz);
      const days = Array.isArray(p.daysOfWeek) ? p.daysOfWeek : [];
      const dayOk = days.includes(weekday);
      const targetHour = Number(p.hour);
      if (!Number.isFinite(targetHour)) return false;
      const hourOk =
        p.operator === 'GREATER_THAN' ? hour > targetHour : hour < targetHour;
      return dayOk && hourOk;
    }

    case 'RANKING':
    default:
      return false;
  }
}

// ============================================================================
//  GIẢI THÍCH (EXPLAIN): song song evaluate nhưng GHI LẠI vì sao đạt/không đạt để
//  hiển thị ở nhật ký. Không thay đổi logic đánh giá — matched giống evaluateGroup.
// ============================================================================

// Nhãn tiếng Việt cho metric (khớp condition-builder) — hiển thị nhật ký dễ đọc.
const METRIC_LABEL: Record<string, string> = {
  spend: 'Chi tiêu hôm nay',
  purchase_roas: 'ROAS hôm nay',
  website_purchase_roas: 'ROAS web hôm nay',
  purchases: 'Số đơn hôm nay',
  cpa: 'Chi phí/đơn (CPA)',
  cost_per_purchase: 'Chi phí/đơn (CPA)',
  cost_per_website_purchase: 'Chi phí/đơn web',
  cost_per_unique_website_purchase: 'Chi phí/đơn web (unique)',
  cost_per_result: 'Chi phí/kết quả',
  results: 'Số kết quả',
  impressions: 'Lượt hiển thị',
  reach: 'Tiếp cận',
  frequency: 'Tần suất',
  clicks: 'Lượt click',
  ctr: 'CTR (%)',
  cpc: 'CPC',
  cpm: 'CPM',
  cpp: 'Chi phí/1000 tiếp cận',
  inline_link_clicks: 'Lượt click link',
  inline_link_click_ctr: 'CTR link (%)',
  cost_per_inline_link_click: 'Chi phí/click link',
  outbound_clicks: 'Lượt click ra ngoài',
  website_purchases: 'Số đơn web hôm nay',
  purchases_value: 'Giá trị đơn hôm nay',
  website_purchase_value: 'Giá trị đơn web hôm nay',
  meta_purchases: 'Số đơn trên Meta hôm nay',
  meta_purchase_value: 'Giá trị đơn trên Meta hôm nay',
  cost_per_meta_purchase: 'Chi phí/đơn trên Meta',
  meta_purchase_roas: 'ROAS trên Meta hôm nay',
  average_meta_purchase_value: 'Giá trị đơn TB trên Meta',
  average_website_purchase_value: 'Giá trị đơn web TB',
  adds_to_cart: 'Thêm vào giỏ',
  website_adds_to_cart: 'Thêm vào giỏ (web)',
  adds_to_cart_value: 'Giá trị thêm vào giỏ',
  cost_per_add_to_cart: 'Chi phí/thêm giỏ',
  leads: 'Số lead',
  website_leads: 'Số lead (web)',
  website_leads_value: 'Giá trị lead (web)',
  checkouts_initiated: 'Bắt đầu thanh toán',
  checkouts_initiated_value: 'Giá trị bắt đầu thanh toán',
  cost_per_checkout_initiated: 'Chi phí/bắt đầu thanh toán',
  registrations_completed: 'Đăng ký hoàn tất',
  website_registrations_completed: 'Đăng ký hoàn tất (web)',
  cost_per_registration_completed: 'Chi phí/đăng ký',
  cost_per_website_registration_completed: 'Chi phí/đăng ký (web)',
  messaging_conversation_started: 'Tin nhắn bắt đầu',
  cost_per_messaging_conversation_started: 'Chi phí/tin nhắn',
  messaging_first_reply: 'Kết nối nhắn tin mới',
  cost_per_messaging_first_reply: 'Chi phí/kết nối nhắn tin',
  post_engagement: 'Tương tác bài viết',
  page_likes: 'Lượt thích Trang',
  post_comments: 'Bình luận',
  post_shares: 'Lượt chia sẻ',
  video_3sec_views: 'Xem video 3 giây',
  video_thruplay_watched_actions: 'ThruPlay',
  cost_per_thruplay: 'Chi phí/ThruPlay',
  video_15_sec_watched_actions: 'Xem video 15 giây',
  video_30_sec_watched_actions: 'Xem video 30 giây',
  video_p25_watched_actions: 'Xem video 25%',
  video_p50_watched_actions: 'Xem video 50%',
  video_p75_watched_actions: 'Xem video 75%',
  video_p95_watched_actions: 'Xem video 95%',
  video_p100_watched_actions: 'Xem video 100%',
  video_avg_time_watched_actions: 'Thời gian xem TB',
  daily_budget: 'Ngân sách ngày',
  lifetime_budget: 'Ngân sách trọn đời',
  hours_since_creation: 'Số giờ từ khi tạo',
};

const metricLabel = (key?: string | null): string =>
  (key && METRIC_LABEL[String(key).toLowerCase()]) || String(key ?? '—');

/**
 * Nhãn metric KÈM khung thời gian khi khung ≠ today. Ví dụ 'Chi tiêu hôm nay' + khung
 * last_7d → 'Chi tiêu (7 ngày qua)'. Bỏ hậu tố " hôm nay" của nhãn gốc để không lặp
 * ("hôm nay (7 ngày qua)"). Khung today → giữ NGUYÊN nhãn gốc (back-compat format log).
 */
const metricLabelWithTimeframe = (
  key: string | null | undefined,
  rawTf: unknown,
): string => {
  const base = metricLabel(key);
  const tf = normalizeTimeframe(rawTf);
  if (tf === 'today') return base;
  const stripped = base.replace(/\s*hôm nay$/i, '');
  return `${stripped} (${timeframeLabelVi(tf)})`;
};

const OP_SYMBOL: Record<string, string> = {
  GREATER_THAN: '>',
  LESS_THAN: '<',
  GREATER_THAN_OR_EQUAL: '≥',
  LESS_THAN_OR_EQUAL: '≤',
  EQUAL: '=',
  NOT_EQUAL: '≠',
};
const opSymbol = (op?: string): string => OP_SYMBOL[String(op ?? '')] || String(op ?? '?');

const WEEKDAY_VI = ['CN', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7'];

/** Số gọn: nguyên → không thập phân; lẻ → 2 số. */
const fmtNum = (n: number): string =>
  Number.isInteger(n) ? String(n) : n.toFixed(2);

export interface ConditionExplain {
  kind: 'condition';
  compareType: string;
  label: string; // "ROAS hôm nay"
  actual: number | null; // giá trị đọc được (null = không đo được)
  actualText: string; // hiển thị (đã format)
  operator: string; // ký hiệu so sánh
  threshold: string; // ngưỡng dạng chữ
  matched: boolean;
  note?: string; // ghi chú (vd không đọc được số liệu)
}

export interface GroupExplain {
  kind: 'group';
  operator: 'AND' | 'OR';
  matched: boolean;
  children: Array<ConditionExplain | GroupExplain>;
}

/** Giải thích 1 điều kiện lá: giá trị thực vs ngưỡng + đạt/không. */
export function explainCondition(cond: any, ctx: EvalContext): ConditionExplain {
  const p = cond?.params || {};
  const base = (over: Partial<ConditionExplain>): ConditionExplain => ({
    kind: 'condition',
    compareType: String(cond?.compareType ?? '—'),
    label: '—',
    actual: null,
    actualText: '—',
    operator: '',
    threshold: '',
    matched: false,
    ...over,
  });

  switch (cond?.compareType) {
    case 'VALUE': {
      const insight = insightForTimeframe(ctx, p.timeframe);
      const v = resolveMetric(p.metric, insight, ctx.entity, ctx.customMetrics);
      const amount = Number(p.amount);
      const okNums = v != null && Number.isFinite(amount);
      const errNote = timeframeErrorNote(ctx, p.timeframe);
      return base({
        label: metricLabelWithTimeframe(p.metric, p.timeframe),
        actual: v,
        actualText: v == null ? 'không đo được' : fmtNum(v),
        operator: opSymbol(p.operator),
        threshold: Number.isFinite(amount) ? fmtNum(amount) : '—',
        matched: okNums ? compare(v as number, p.operator, amount) : false,
        note:
          errNote ??
          (v == null ? 'Chưa có/không đọc được số liệu hôm nay' : undefined),
      });
    }
    case 'METRIC': {
      const leftInsight = insightForTimeframe(ctx, p.leftTimeframe);
      const rightInsight = insightForTimeframe(ctx, p.rightTimeframe);
      const left = resolveMetric(p.leftMetric, leftInsight, ctx.entity, ctx.customMetrics);
      const right = resolveMetric(p.rightMetric, rightInsight, ctx.entity, ctx.customMetrics);
      const mult =
        p.multiplier == null || !Number.isFinite(Number(p.multiplier))
          ? 1
          : Number(p.multiplier);
      const rhs = right == null ? null : mult * right;
      const ok = left != null && rhs != null;
      const rightLabel = metricLabelWithTimeframe(p.rightMetric, p.rightTimeframe);
      const errNote = timeframeErrorNote(ctx, p.leftTimeframe, p.rightTimeframe);
      return base({
        label: metricLabelWithTimeframe(p.leftMetric, p.leftTimeframe),
        actual: left,
        actualText: left == null ? 'không đo được' : fmtNum(left),
        operator: opSymbol(p.operator),
        threshold:
          rhs == null
            ? `${mult !== 1 ? `${fmtNum(mult)}× ` : ''}${rightLabel}`
            : `${fmtNum(rhs)} (${mult !== 1 ? `${fmtNum(mult)}× ` : ''}${rightLabel})`,
        matched: ok ? compare(left as number, p.operator, rhs as number) : false,
        note: errNote ?? (!ok ? 'Chưa có/không đọc được số liệu để so sánh' : undefined),
      });
    }
    case 'TIME': {
      const tz =
        !p.timezone || p.timezone === 'account'
          ? ctx.timezone || DEFAULT_TIMEZONE
          : p.timezone;
      const { weekday, hour } = zonedTimeParts(ctx.now, tz);
      const days: number[] = Array.isArray(p.daysOfWeek) ? p.daysOfWeek : [];
      const dayOk = days.includes(weekday);
      const targetHour = Number(p.hour);
      const hourOk = Number.isFinite(targetHour)
        ? p.operator === 'GREATER_THAN'
          ? hour > targetHour
          : hour < targetHour
        : false;
      const daysText = days.length
        ? days.map((d) => WEEKDAY_VI[d] ?? d).join(',')
        : 'mọi ngày';
      return base({
        label: 'Khung giờ',
        actual: hour,
        actualText: `${hour}h ${WEEKDAY_VI[weekday] ?? weekday}`,
        operator: opSymbol(p.operator),
        threshold: `${Number.isFinite(targetHour) ? `${targetHour}h` : '—'} · ${daysText}`,
        matched: dayOk && hourOk,
        note: !dayOk ? 'Ngoài các ngày đã chọn' : undefined,
      });
    }
    case 'RANKING':
    default:
      return base({
        label: 'Xếp hạng',
        note: 'Loại điều kiện chưa hỗ trợ đánh giá',
        matched: false,
      });
  }
}

/** Giải thích 1 group đệ quy (matched giống evaluateGroup). */
export function explainGroup(group: any, ctx: EvalContext): GroupExplain {
  const operator: 'AND' | 'OR' = group?.operator === 'OR' ? 'OR' : 'AND';
  const conditions: any[] = Array.isArray(group?.conditions)
    ? group.conditions
    : [];
  const childGroups: any[] = Array.isArray(group?.childGroups)
    ? group.childGroups
    : [];
  const children: Array<ConditionExplain | GroupExplain> = [
    ...conditions.map((c) => explainCondition(c, ctx)),
    ...childGroups.map((g) => explainGroup(g, ctx)),
  ];
  const matched =
    children.length === 0
      ? true
      : operator === 'OR'
        ? children.some((c) => c.matched)
        : children.every((c) => c.matched);
  return { kind: 'group', operator, matched, children };
}

/** Gom mọi điều kiện lá (phẳng) từ cây explain. */
function flattenConditions(node: ConditionExplain | GroupExplain): ConditionExplain[] {
  if (node.kind === 'condition') return [node];
  return node.children.flatMap(flattenConditions);
}

/**
 * Câu tóm tắt cho nhật ký: nếu KHÔNG đạt → liệt kê điều kiện chưa đạt (số thực vs
 * ngưỡng). Nếu đạt → "Đạt X/Y điều kiện". Dùng cho matchedConditionSummary.
 */
export function summarizeEvaluation(tree: GroupExplain): string {
  const leaves = flattenConditions(tree);
  if (leaves.length === 0) return 'Không có điều kiện → luôn đạt.';
  const failed = leaves.filter((c) => !c.matched);
  if (tree.matched) {
    return `Đạt điều kiện (${leaves.length - failed.length}/${leaves.length}).`;
  }
  const reasons = failed
    .slice(0, 4)
    .map(
      (c) =>
        `${c.label} ${c.actualText}${c.operator ? ` (cần ${c.operator} ${c.threshold})` : ''}`,
    );
  const more = failed.length > 4 ? ` +${failed.length - 4} điều kiện khác` : '';
  return `Chưa đạt: ${reasons.join('; ')}${more}.`;
}
