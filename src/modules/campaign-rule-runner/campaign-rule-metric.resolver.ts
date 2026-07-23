import { Logger } from '@nestjs/common';
import { extractCustomEventType, resolveResultSpec } from '../../common/metrics/insight-metrics';

/**
 * Phân giải một metricKey → số, từ insight LIVE (date_preset=today) + entity DB.
 *
 * Nguồn dữ liệu (tất cả đã có sẵn trong 1 lần getInsights, KHÔNG thêm request):
 * - Số trực tiếp (field số của insight): spend/impressions/reach/frequency/clicks/
 *   ctr/cpc/cpm/inline_link_clicks/inline_link_click_ctr/cost_per_inline_link_click.
 * - ROAS: đọc THẲNG field Meta — purchase_roas (omni) & website_purchase_roas (pixel web)
 *   là HAI field riêng, mỗi cái [{action_type,value}] → khớp số Ads Manager (KHÔNG tự chia).
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
  // --- Mua hàng (purchases/value/cost/ROAS): xử lý ở nhánh riêng khớp Meta, KHÔNG dùng SPECS ---
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
 * KHỚP META (bằng số trên Ads Manager). Mỗi "cột" mua hàng của Meta ứng với MỘT
 * action_type cụ thể — KHÔNG cộng dồn nhiều biểu diễn của cùng sự kiện:
 *   - Họ omni    (Purchases, Purchases value, Cost per purchase, Purchase ROAS)
 *     = `omni_purchase` — gộp mọi Facebook Business Tools (pixel + onsite + app + offline).
 *   - Họ website (Website purchases, Website purchase value, Cost per website purchase,
 *     Website purchase ROAS) = `offsite_conversion.fb_pixel_purchase` — chỉ purchase trên
 *     website qua Meta pixel.
 * Lịch sử: trước đây tính (purchase + onsite_conversion.purchase) / spend theo "chuẩn công
 * ty" (khớp card/Engine 2) nên LỆCH số Meta; đã đổi sang khớp Meta theo yêu cầu — số rule
 * giờ trùng Ads Manager (nhưng có thể khác con số trên card).
 */
const OMNI_PURCHASE_AT = ['omni_purchase', 'purchase'];
const WEB_PURCHASE_AT = ['offsite_conversion.fb_pixel_purchase'];

/**
 * ROAS đọc THẲNG field Meta (`purchase_roas` / `website_purchase_roas`) — mỗi field là
 * mảng [{action_type, value}]. Lấy đúng action_type kỳ vọng, fallback phần tử đầu. Đây là
 * con số Meta đã tự tính → khớp Ads Manager tuyệt đối (không tự chia value/spend nữa).
 */
function readRoasField(insight: any, field: string, preferAt: string): number | null {
  const arr = insight?.[field];
  if (!Array.isArray(arr) || arr.length === 0) return null;
  const hit = arr.find((r) => r?.action_type === preferAt);
  const v = toNumber(hit?.value);
  if (v != null) return v;
  return toNumber(arr[0]?.value);
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
  return pickAction(insight?.actions, OMNI_PURCHASE_AT);
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

  // ROAS — đọc THẲNG field Meta tương ứng (khớp Ads Manager). evalBumpGuard gọi
  // resolveMetric('purchase_roas') nên guard cũng tự đi theo số omni của Meta.
  if (key === 'purchase_roas') {
    return readRoasField(insight, 'purchase_roas', 'omni_purchase');
  }
  if (key === 'website_purchase_roas') {
    return readRoasField(insight, 'website_purchase_roas', 'offsite_conversion.fb_pixel_purchase');
  }
  // Số đơn / giá trị đơn / CPA — mỗi họ MỘT action_type (không cộng dồn), khớp Meta.
  if (key === 'purchases') return pickAction(insight?.actions, OMNI_PURCHASE_AT);
  if (key === 'website_purchases') return pickAction(insight?.actions, WEB_PURCHASE_AT);
  if (key === 'purchases_value') return pickAction(insight?.action_values, OMNI_PURCHASE_AT);
  if (key === 'website_purchase_value') {
    return pickAction(insight?.action_values, WEB_PURCHASE_AT);
  }
  if (key === 'cost_per_purchase') return pickCost(insight, OMNI_PURCHASE_AT);
  if (key === 'cost_per_website_purchase' || key === 'cost_per_unique_website_purchase') {
    return pickCost(insight, WEB_PURCHASE_AT);
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
  // Họ mua hàng khớp Meta (omni vs website pixel) — xử lý ở nhánh riêng, không nằm trong SPECS.
  'purchase_roas',
  'website_purchase_roas',
  'purchases',
  'website_purchases',
  'purchases_value',
  'website_purchase_value',
  'cost_per_purchase',
  'cost_per_website_purchase',
  'cost_per_unique_website_purchase',
  'daily_budget',
  'lifetime_budget',
  'hours_since_creation',
  'cpp',
  'results',
  ...Object.keys(SPECS),
];
