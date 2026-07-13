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

/** Số lượt mua từ insight.actions (null nếu không có). */
function resolvePurchases(insight: any): number | null {
  const actions = insight?.actions;
  if (!Array.isArray(actions)) return null;
  const match = actions.find((a) => PURCHASE_ACTION_TYPES.has(a?.action_type));
  return match ? toNumber(match.value) : null;
}

/** Chi phí / kết quả (mua) — CPA. Ưu tiên cost_per_action_type của Meta (chính xác
 *  theo attribution), fallback spend/purchases. purchases=0 → null (KHÔNG coi CPA vô
 *  cực là hợp lệ để tránh điều kiện "CPA < X" âm thầm khớp khi chưa có đơn nào). */
function resolveCpa(insight: any): number | null {
  const cpaRows = insight?.cost_per_action_type;
  if (Array.isArray(cpaRows)) {
    const match = cpaRows.find((a) => PURCHASE_ACTION_TYPES.has(a?.action_type));
    const v = match ? toNumber(match.value) : null;
    if (v != null && v > 0) return v;
  }
  const spend = toNumber(insight?.spend);
  const purchases = resolvePurchases(insight);
  if (spend == null || purchases == null || purchases <= 0) return null;
  return spend / purchases;
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
    return resolvePurchases(insight);
  }

  // CPA / chi phí mỗi kết quả (mua). Khớp các metric CPA mà UI condition-builder
  // cho chọn: cost_per_purchase | cost_per_website_purchase | cost_per_unique_website_purchase.
  if (
    key === 'cpa' ||
    key === 'cost_per_purchase' ||
    key === 'cost_per_website_purchase' ||
    key === 'cost_per_unique_website_purchase' ||
    key === 'cost_per_result'
  ) {
    return resolveCpa(insight);
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
