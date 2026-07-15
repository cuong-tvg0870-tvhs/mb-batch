import { Logger } from '@nestjs/common';
import { extractCustomEventType, resolveResultSpec } from '../../common/metrics/insight-metrics';

/**
 * Phân giải một metricKey → số, từ insight LIVE (date_preset=today) + entity DB.
 *
 * Nguồn dữ liệu (tất cả đã có sẵn trong 1 lần getInsights, KHÔNG thêm request):
 * - Số trực tiếp (field số của insight): spend/impressions/reach/frequency/clicks/
 *   ctr/cpc/cpm/inline_link_clicks/inline_link_click_ctr/cost_per_inline_link_click.
 * - ROAS: insight.purchase_roas[0].value (purchase_roas & website_purchase_roas alias chung).
 * - Sự kiện pixel/tương tác (đếm): dò insight.actions[] theo danh sách action_type ưu
 *   tiên (LẤY PHẦN TỬ ĐẦU khớp → không cộng trùng nhiều biểu diễn cùng 1 sự kiện).
 * - Giá trị sự kiện: insight.action_values[] cùng cách.
 * - Chi phí / sự kiện (CPA): insight.cost_per_action_type[] (chính xác theo attribution
 *   của Meta), fallback spend/số-sự-kiện. count<=0 → null (KHÔNG coi CPA vô cực là hợp lệ).
 * - Metric mảng-thống-kê (outbound_clicks, video_*): insight[field][0].value.
 * - results: insight.results nếu Meta trả; thường KHÔNG có ở date_preset=today → suy theo
 *   optimization_goal của entity (resolveResultSpec) hoặc fallback purchases.
 * - cpp (chi phí/1000 người tiếp cận): suy từ spend/reach*1000.
 * - daily_budget/lifetime_budget: đọc từ entity (đơn vị minor units).
 * - hours_since_creation: từ rawPayload.created_time (fallback startTime) của entity.
 * - Metric KHÁC → null + log WARN (điều kiện coi như KHÔNG khớp, không âm thầm pass).
 *
 * Chuỗi action_type dưới đây được đối chiếu với dữ liệu THẬT trong production
 * (distinct action_type trên CampaignInsight.actions) để không map nhầm.
 */

const logger = new Logger('CampaignRuleMetricResolver');

/** Field số đọc thẳng từ insight (Meta trả number/string). */
const DIRECT_INSIGHT_KEYS = new Set([
  'spend',
  'impressions',
  'reach',
  'frequency',
  'clicks',
  'ctr',
  'cpc',
  'cpm',
  'inline_link_clicks',
  'inline_link_click_ctr',
  'cost_per_inline_link_click',
]);

/**
 * Danh sách action_type theo ƯU TIÊN cho mỗi loại sự kiện. Lấy phần tử ĐẦU TIÊN khớp
 * (không cộng dồn) để tránh đếm trùng khi Meta trả nhiều biểu diễn cùng 1 sự kiện
 * (vd purchase + omni_purchase + offsite_conversion.fb_pixel_purchase).
 */
const AT = {
  // Ưu tiên offsite pixel (đúng nghĩa "website purchase"); giữ tương thích resolver cũ.
  purchase: [
    'purchase',
    'omni_purchase',
    'offsite_conversion.fb_pixel_purchase',
    'onsite_web_purchase',
  ],
  add_to_cart: [
    'add_to_cart',
    'omni_add_to_cart',
    'offsite_conversion.fb_pixel_add_to_cart',
    'onsite_web_add_to_cart',
  ],
  lead: [
    'lead',
    'onsite_conversion.lead_grouped',
    'onsite_conversion.lead',
    'offsite_conversion.fb_pixel_lead',
  ],
  initiate_checkout: [
    'initiate_checkout',
    'omni_initiated_checkout',
    'offsite_conversion.fb_pixel_initiate_checkout',
    'onsite_web_initiate_checkout',
  ],
  complete_registration: [
    'complete_registration',
    'omni_complete_registration',
    'offsite_conversion.fb_pixel_complete_registration',
  ],
  messaging_started: ['onsite_conversion.messaging_conversation_started_7d'],
  messaging_first_reply: ['onsite_conversion.messaging_first_reply'],
  post_engagement: ['post_engagement'],
  like: ['like', 'onsite_conversion.post_net_like'],
  comment: ['comment', 'onsite_conversion.post_net_comment'],
  share: ['post'],
  video_view: ['video_view'],
};

type Spec =
  | { t: 'count'; at: string[] }
  | { t: 'value'; at: string[] }
  | { t: 'cost'; at: string[] }
  | { t: 'arr'; field: string }
  | { t: 'derived'; fn: (insight: any) => number | null };

/** metricKey (curated ở FE) → cách phân giải. */
const SPECS: Record<string, Spec> = {
  // --- Mua hàng: xử lý ở nhánh riêng (chuẩn cty purchaseValue/spend, KHÔNG dùng SPECS) ---
  // --- Thêm vào giỏ ---
  adds_to_cart: { t: 'count', at: AT.add_to_cart },
  website_adds_to_cart: { t: 'count', at: AT.add_to_cart },
  adds_to_cart_value: { t: 'value', at: AT.add_to_cart },
  cost_per_add_to_cart: { t: 'cost', at: AT.add_to_cart },
  // --- Lead ---
  leads: { t: 'count', at: AT.lead },
  website_leads: { t: 'count', at: AT.lead },
  website_leads_value: { t: 'value', at: AT.lead },
  // --- Bắt đầu thanh toán ---
  checkouts_initiated: { t: 'count', at: AT.initiate_checkout },
  checkouts_initiated_value: { t: 'value', at: AT.initiate_checkout },
  cost_per_checkout_initiated: { t: 'cost', at: AT.initiate_checkout },
  // --- Đăng ký ---
  registrations_completed: { t: 'count', at: AT.complete_registration },
  website_registrations_completed: { t: 'count', at: AT.complete_registration },
  cost_per_registration_completed: { t: 'cost', at: AT.complete_registration },
  cost_per_website_registration_completed: { t: 'cost', at: AT.complete_registration },
  // --- Tin nhắn ---
  messaging_conversation_started: { t: 'count', at: AT.messaging_started },
  cost_per_messaging_conversation_started: { t: 'cost', at: AT.messaging_started },
  messaging_first_reply: { t: 'count', at: AT.messaging_first_reply },
  cost_per_messaging_first_reply: { t: 'cost', at: AT.messaging_first_reply },
  // --- Tương tác ---
  post_engagement: { t: 'count', at: AT.post_engagement },
  page_likes: { t: 'count', at: AT.like },
  post_comments: { t: 'count', at: AT.comment },
  post_shares: { t: 'count', at: AT.share },
  video_3sec_views: { t: 'count', at: AT.video_view },
  outbound_clicks: { t: 'arr', field: 'outbound_clicks' },
  // --- Video (field mảng-thống-kê của Meta) ---
  video_thruplay_watched_actions: { t: 'arr', field: 'video_thruplay_watched_actions' },
  video_15_sec_watched_actions: { t: 'arr', field: 'video_15_sec_watched_actions' },
  video_30_sec_watched_actions: { t: 'arr', field: 'video_30_sec_watched_actions' },
  video_p25_watched_actions: { t: 'arr', field: 'video_p25_watched_actions' },
  video_p50_watched_actions: { t: 'arr', field: 'video_p50_watched_actions' },
  video_p75_watched_actions: { t: 'arr', field: 'video_p75_watched_actions' },
  video_p95_watched_actions: { t: 'arr', field: 'video_p95_watched_actions' },
  video_p100_watched_actions: { t: 'arr', field: 'video_p100_watched_actions' },
  video_avg_time_watched_actions: { t: 'arr', field: 'video_avg_time_watched_actions' },
  cost_per_thruplay: {
    t: 'derived',
    fn: (i) => costPerArr(i, 'video_thruplay_watched_actions'),
  },
  // cost_per_inline_link_click / inline_link_clicks / inline_link_click_ctr: đọc thẳng
  // field số của Meta ở DIRECT_INSIGHT_KEYS (không cần spec).
};

/** Chuyển sang số hữu hạn; null nếu không phải số. */
function toNumber(value: any): number | null {
  if (value == null) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

/** Lấy value của action_type ĐẦU TIÊN khớp trong 1 mảng action-stats (null nếu không có). */
function pickAction(arr: any, types: string[]): number | null {
  if (!Array.isArray(arr)) return null;
  for (const type of types) {
    const row = arr.find((a) => a?.action_type === type);
    if (row) {
      const v = toNumber(row.value);
      if (v != null) return v;
    }
  }
  return null;
}

/** CPA từ cost_per_action_type (ưu tiên), fallback spend/count. count<=0 → null. */
function pickCost(insight: any, types: string[]): number | null {
  const c = pickAction(insight?.cost_per_action_type, types);
  if (c != null && c > 0) return c;
  const spend = toNumber(insight?.spend);
  const count = pickAction(insight?.actions, types);
  if (spend == null || count == null || count <= 0) return null;
  return spend / count;
}

/** spend / (số của field mảng-thống-kê). null nếu thiếu. */
function costPerArr(insight: any, field: string): number | null {
  const spend = toNumber(insight?.spend);
  const count = toNumber(insight?.[field]?.[0]?.value);
  if (spend == null || count == null || count <= 0) return null;
  return spend / count;
}

/**
 * Số đơn / giá trị đơn theo CHUẨN CÔNG TY (khớp card & Engine 2): CỘNG đúng
 * `purchase` + `onsite_conversion.purchase` (2 sự kiện khác nhau: web offsite vs
 * onsite/messaging). Xem `getMetrics` trong common/utils/index.ts.
 */
const PURCHASE_SUM_TYPES = ['purchase', 'onsite_conversion.purchase'];

/** Cộng value của MỌI action_type khớp trong danh sách (null nếu mảng không hợp lệ). */
function sumActionTypes(arr: any, types: string[]): number | null {
  if (!Array.isArray(arr)) return null;
  let sum = 0;
  for (const row of arr) {
    if (types.includes(row?.action_type)) {
      const v = toNumber(row.value);
      if (v != null) sum += v;
    }
  }
  return sum;
}

/** Số đơn hôm nay (chuẩn cty). */
function resolvePurchaseCount(insight: any): number | null {
  return sumActionTypes(insight?.actions, PURCHASE_SUM_TYPES);
}

/** Giá trị đơn hôm nay (chuẩn cty) — dùng cho ROAS. */
function resolvePurchaseValue(insight: any): number | null {
  return sumActionTypes(insight?.action_values, PURCHASE_SUM_TYPES);
}

/**
 * ROAS theo CHUẨN CÔNG TY = purchaseValue / spend (KHÔNG dùng field purchase_roas
 * của Meta) — để rule budget-schedule khớp con số marketer thấy trên card/Engine 2.
 * Guard bơm-tiền (evalBumpGuard) cũng gọi resolveMetric('purchase_roas') nên tự đi theo.
 */
function resolveRoas(insight: any): number | null {
  const spend = toNumber(insight?.spend);
  if (spend == null || spend <= 0) return null;
  const value = resolvePurchaseValue(insight) ?? 0;
  return value / spend;
}

/** Chi phí / đơn (chuẩn cty) = spend / số đơn. đơn<=0 → null. */
function resolvePurchaseCpa(insight: any): number | null {
  const spend = toNumber(insight?.spend);
  const purchases = resolvePurchaseCount(insight);
  if (spend == null || purchases == null || purchases <= 0) return null;
  return spend / purchases;
}

/** cpp — chi phí / 1000 người tiếp cận. */
function resolveCpp(insight: any): number | null {
  const spend = toNumber(insight?.spend);
  const reach = toNumber(insight?.reach);
  if (spend == null || reach == null || reach <= 0) return null;
  return (spend / reach) * 1000;
}

/**
 * results — Meta thường KHÔNG trả ở date_preset=today. Suy theo optimization_goal của
 * entity (ad set) qua resolveResultSpec; fallback cuối = purchases.
 */
function resolveResults(insight: any, entity: any): number | null {
  const direct = toNumber(insight?.results);
  if (direct != null) return direct;

  const raw = entity?.rawPayload;
  const goal = raw?.optimization_goal ?? raw?.adset?.optimization_goal ?? null;
  const spec = resolveResultSpec(goal, extractCustomEventType(raw));
  if (spec.field === 'reach') return toNumber(insight?.reach);
  if (spec.field === 'impressions') return toNumber(insight?.impressions);
  if (spec.field === 'videoThruplay') {
    return toNumber(insight?.video_thruplay_watched_actions?.[0]?.value);
  }
  const v = pickAction(insight?.actions, spec.parts);
  if (v != null) return v;
  return resolvePurchaseCount(insight);
}

/** Số giờ kể từ khi entity được tạo (rawPayload.created_time, fallback startTime). */
function resolveHoursSinceCreation(entity: any): number | null {
  const raw = entity?.rawPayload;
  const src = raw?.created_time ?? entity?.startTime ?? null;
  if (!src) return null;
  const created = new Date(src).getTime();
  if (!Number.isFinite(created)) return null;
  const hours = (Date.now() - created) / 3_600_000;
  return hours >= 0 ? hours : null;
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

  // ROAS & số đơn theo CHUẨN CÔNG TY (khớp card/Engine 2) — KHÔNG dùng field
  // purchase_roas của Meta. Đổi ở đây kéo theo cả evalBumpGuard (cùng gọi resolveMetric).
  if (key === 'purchase_roas' || key === 'website_purchase_roas') {
    return resolveRoas(insight);
  }
  if (key === 'purchases' || key === 'website_purchases') {
    return resolvePurchaseCount(insight);
  }
  if (key === 'purchases_value' || key === 'website_purchase_value') {
    return resolvePurchaseValue(insight);
  }
  if (
    key === 'cost_per_purchase' ||
    key === 'cost_per_website_purchase' ||
    key === 'cost_per_unique_website_purchase'
  ) {
    return resolvePurchaseCpa(insight);
  }
  if (key === 'cost_per_result') {
    // spend / results (goal-aware) — kết quả có thể là đơn/lead/tin nhắn tuỳ mục tiêu.
    const spend = toNumber(insight?.spend);
    const results = resolveResults(insight, entity);
    return spend != null && results != null && results > 0 ? spend / results : null;
  }

  if (key === 'daily_budget') return toNumber(entity?.dailyBudget);
  if (key === 'lifetime_budget') return toNumber(entity?.lifetimeBudget);
  if (key === 'hours_since_creation') return resolveHoursSinceCreation(entity);
  if (key === 'cpp') return resolveCpp(insight);
  if (key === 'results') return resolveResults(insight, entity);

  const spec = SPECS[key];
  if (spec) {
    switch (spec.t) {
      case 'count':
        return pickAction(insight?.actions, spec.at);
      case 'value':
        return pickAction(insight?.action_values, spec.at);
      case 'cost':
        return pickCost(insight, spec.at);
      case 'arr':
        return toNumber(insight?.[spec.field]?.[0]?.value);
      case 'derived':
        return spec.fn(insight);
    }
  }

  logger.warn(
    `Metric "${metricKey}" chưa hỗ trợ → điều kiện coi như KHÔNG khớp (không âm thầm pass).`,
  );
  return null;
}

/**
 * Tập metricKey mà runner CHẤM ĐƯỢC ở màn "Lên lịch tăng ngân sách". FE dùng làm
 * allow-list (AllowedMetricsProvider) để KHÔNG xổ ra chỉ số không đo được. Giữ đồng bộ
 * với các nhánh của resolveMetric ở trên.
 */
export const BUDGET_SCHEDULE_SUPPORTED_METRICS: readonly string[] = [
  ...DIRECT_INSIGHT_KEYS,
  'purchase_roas',
  'website_purchase_roas',
  'daily_budget',
  'lifetime_budget',
  'hours_since_creation',
  'cpp',
  'results',
  ...Object.keys(SPECS),
];
