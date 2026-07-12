import { DEFAULT_TIMEZONE } from './campaign-rule-runner.constants';
import { resolveMetric } from './campaign-rule-metric.resolver';
import { zonedTimeParts } from './campaign-rule-schedule.util';

/**
 * Bộ đánh giá cây điều kiện của một task.
 *
 * ctx.insight = insight LIVE today của entity đang xét; ctx.entity = bản ghi DB
 * (Campaign/AdSet) để lấy ngân sách. ctx.now/ctx.timezone dùng cho điều kiện TIME.
 */

export interface EvalContext {
  insight: any;
  entity: any;
  now: Date;
  timezone: string;
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
 * - VALUE: so metric với hằng số amount.
 * - METRIC: so leftMetric với (multiplier ?? 1) * rightMetric. (BỎ QUA timeframe cho v1.)
 * - TIME: giờ hiện tại theo timezone của điều kiện (fallback ctx.timezone → default).
 * - RANKING: chưa hỗ trợ → false (không âm thầm pass).
 */
export function evaluateCondition(cond: any, ctx: EvalContext): boolean {
  if (!cond) return false;
  const p = cond.params || {};

  switch (cond.compareType) {
    case 'VALUE': {
      const v = resolveMetric(p.metric, ctx.insight, ctx.entity);
      if (v == null) return false;
      const amount = Number(p.amount);
      if (!Number.isFinite(amount)) return false;
      return compare(v, p.operator, amount);
    }

    case 'METRIC': {
      const left = resolveMetric(p.leftMetric, ctx.insight, ctx.entity);
      const right = resolveMetric(p.rightMetric, ctx.insight, ctx.entity);
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
