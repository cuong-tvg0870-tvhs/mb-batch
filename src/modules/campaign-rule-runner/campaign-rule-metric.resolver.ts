import { Logger } from '@nestjs/common';

/**
 * Phân giải một metricKey → số, từ insight LIVE (date_preset=today) + entity DB.
 *
 * Nguồn:
 * - Trực tiếp (số của insight): spend/impressions/reach/frequency/clicks/ctr/cpc/cpm.
 * - purchase_roas | website_purchase_roas: insight.purchase_roas là mảng
 *   {action_type,value} → lấy phần tử [0].value.
 * - purchases: dò trong insight.actions phần tử action_type mua hàng.
 * - results: insight.results nếu Meta trả (thường không có ở level today) → null nếu thiếu.
 * - daily_budget/lifetime_budget: đọc từ entity (đơn vị minor units).
 * - Metric KHÁC → null + log WARN (điều kiện sẽ coi như KHÔNG khớp, không âm thầm pass).
 */

const logger = new Logger('CampaignRuleMetricResolver');

const DIRECT_INSIGHT_KEYS = new Set([
  'spend',
  'impressions',
  'reach',
  'frequency',
  'clicks',
  'ctr',
  'cpc',
  'cpm',
]);

const PURCHASE_ACTION_TYPES = new Set([
  'purchase',
  'omni_purchase',
  'offsite_conversion.fb_pixel_purchase',
]);

/** Chuyển sang số hữu hạn; null nếu không phải số. */
function toNumber(value: any): number | null {
  if (value == null) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function resolveMetric(
  metricKey: string | null | undefined,
  insight: any,
  entity: any,
): number | null {
  if (!metricKey) return null;
  const key = String(metricKey).toLowerCase();

  if (DIRECT_INSIGHT_KEYS.has(key)) {
    return toNumber(insight?.[key]);
  }

  if (key === 'purchase_roas' || key === 'website_purchase_roas') {
    return toNumber(insight?.purchase_roas?.[0]?.value);
  }

  if (key === 'purchases') {
    const actions = insight?.actions;
    if (!Array.isArray(actions)) return null;
    const match = actions.find((a) => PURCHASE_ACTION_TYPES.has(a?.action_type));
    return match ? toNumber(match.value) : null;
  }

  if (key === 'results') {
    return toNumber(insight?.results);
  }

  if (key === 'daily_budget') {
    return toNumber(entity?.dailyBudget);
  }

  if (key === 'lifetime_budget') {
    return toNumber(entity?.lifetimeBudget);
  }

  logger.warn(
    `Metric "${metricKey}" chưa hỗ trợ → điều kiện coi như KHÔNG khớp (không âm thầm pass).`,
  );
  return null;
}
