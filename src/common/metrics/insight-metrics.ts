/**
 * ============================================================================
 *  INSIGHT METRIC REGISTRY + AGGREGATOR (nguồn sự thật DUY NHẤT)
 * ----------------------------------------------------------------------------
 *  Định nghĩa MỖI metric đúng 1 lần: label, đơn vị, và CHIẾN LƯỢC TỔNG HỢP.
 *  Thay cho logic cộng/tính rải rác (đã trôi lệch) ở:
 *    - mb-ads  common/utils/insight-rollup.util.ts (sum/recalc)
 *    - mb-batch insight-sync.service.ts (sumMetrics/recalculateDerivedMetrics)
 *    - mb-frontend lib/utils.ts (calculateAggregatedMetrics/buildInsights)
 *  → File này giữ PARITY với bản mb-batch (giống convention meta-field.ts).
 *
 *  Chiến lược tổng hợp (agg) theo từng metric:
 *    additive     – cộng thẳng các ngày (spend, impressions, clicks, …)
 *    dedup        – reach/uniqueClicks: Meta khử trùng theo cửa sổ → cộng ngày
 *                   là XẤP XỈ (đội số). Vẫn cộng + gắn cờ `approxReach` khi
 *                   khoảng > 1 ngày để FE hiện badge "≈".
 *    weighted_avg – videoAvgWatchTime: trung bình có trọng số theo videoView.
 *    snapshot     – *Ranking (chuỗi phân loại): lấy ngày mới nhất, KHÔNG cộng.
 *    derived      – tính lại từ tổng additive (ctr, cpc, cpm, roas, cvr, …).
 *    result       – đếm theo optimization_goal của entity (bản đồ ODAX bên dưới).
 * ============================================================================
 */

export type MetricUnit =
  | 'currency'
  | 'percent'
  | 'ratio'
  | 'number'
  | 'duration'
  | 'text';

export type MetricAgg =
  | 'additive'
  | 'dedup'
  | 'weighted_avg'
  | 'snapshot'
  | 'derived'
  | 'result';

export type MetricGroup =
  | 'spend'
  | 'traffic'
  | 'reach'
  | 'conversion'
  | 'video'
  | 'quality';

export type EntityLevel = 'campaign' | 'adset' | 'ad' | 'creative';

export interface MetricDef {
  key: string;
  /** Nhãn tiếng Việt hiển thị cho marketer. */
  label: string;
  /** Nhãn ngắn cho header bảng. */
  short?: string;
  group: MetricGroup;
  unit: MetricUnit;
  agg: MetricAgg;
  /** Cấp áp dụng; bỏ trống = mọi cấp. */
  levels?: EntityLevel[];
  /** Trọng số cho weighted_avg. */
  weightField?: string;
  /** Có thể xấp xỉ khi khoảng > 1 ngày (reach/frequency/uniqueCtr). */
  approxOnMultiDay?: boolean;
  /** Ẩn khi goal không phải mua hàng (aov). */
  purchaseOnly?: boolean;
  hint?: string;
}

/* ==========================================================================
 *  BẢN ĐỒ ODAX RESULTS — optimization_goal → action_type tính là "kết quả"
 *  `unified` là action_type tổng đã khử trùng của Meta (ưu tiên nếu có);
 *  `parts` là các action_type thành phần (cộng khi KHÔNG có unified).
 * ========================================================================== */

export interface ResultActionSpec {
  /** action_type tổng đã dedup (ưu tiên). */
  unified?: string;
  /** thành phần để cộng khi thiếu unified. */
  parts: string[];
  /** kết quả lấy thẳng từ field số (reach/impressions/thruplay), không từ actions[]. */
  field?: string;
  /** result này là mua hàng (để bật aov/roas hợp lý). */
  isPurchase?: boolean;
}

/** custom_event_type (promoted_object) → action cho nhóm OFFSITE_CONVERSIONS/VALUE. */
export const RESULT_ACTION_BY_CUSTOM_EVENT: Record<string, ResultActionSpec> = {
  PURCHASE: {
    unified: 'purchase',
    parts: ['offsite_conversion.fb_pixel_purchase', 'onsite_conversion.purchase'],
    isPurchase: true,
  },
  LEAD: {
    unified: 'lead',
    parts: ['offsite_conversion.fb_pixel_lead', 'onsite_conversion.lead_grouped'],
  },
  COMPLETE_REGISTRATION: {
    unified: 'complete_registration',
    parts: [
      'offsite_conversion.fb_pixel_complete_registration',
      'offsite_conversion.complete_registration',
    ],
  },
  ADD_TO_CART: {
    unified: 'add_to_cart',
    parts: ['offsite_conversion.fb_pixel_add_to_cart'],
  },
  INITIATED_CHECKOUT: {
    unified: 'initiate_checkout',
    parts: ['offsite_conversion.fb_pixel_initiate_checkout'],
  },
  CONTENT_VIEW: {
    unified: 'view_content',
    parts: ['offsite_conversion.fb_pixel_view_content'],
  },
};

/** optimization_goal → cách lấy result. */
export const RESULT_ACTION_BY_GOAL: Record<string, ResultActionSpec> = {
  REACH: { parts: [], field: 'reach' },
  IMPRESSIONS: { parts: [], field: 'impressions' },
  LINK_CLICKS: { parts: ['link_click'] },
  LANDING_PAGE_VIEWS: { parts: ['landing_page_view'] },
  POST_ENGAGEMENT: { parts: ['post_engagement'] },
  PAGE_LIKES: { parts: ['like'] },
  CONVERSATIONS: {
    parts: ['onsite_conversion.messaging_conversation_started_7d'],
  },
  LEAD_GENERATION: {
    unified: 'lead',
    parts: ['onsite_conversion.lead_grouped', 'leadgen.other'],
  },
  QUALITY_LEAD: {
    unified: 'lead',
    parts: ['onsite_conversion.lead_grouped', 'leadgen.other'],
  },
  MESSAGING_PURCHASE_CONVERSION: {
    unified: 'onsite_conversion.purchase',
    parts: ['purchase'],
    isPurchase: true,
  },
  EVENT_RESPONSES: { parts: ['rsvp'] },
  VALUE: {
    unified: 'purchase',
    parts: ['offsite_conversion.fb_pixel_purchase', 'onsite_conversion.purchase'],
    isPurchase: true,
  },
  OFFSITE_CONVERSIONS: {
    // Placeholder — thực tế resolve theo custom_event_type (xem resolveResultSpec).
    unified: 'purchase',
    parts: ['offsite_conversion.fb_pixel_purchase', 'onsite_conversion.purchase'],
    isPurchase: true,
  },
  THRUPLAY: { parts: [], field: 'videoThruplay' },
  TWO_SECOND_CONTINUOUS_VIDEO_VIEWS: { parts: ['video_view'] },
  AD_RECALL_LIFT: { parts: ['estimated_ad_recallers'] },
  QUALITY_CALL: { parts: ['quality_call'] },
};

/** Fallback khi thiếu goal/custom_event_type: giữ định nghĩa cũ (purchase + registration). */
export const LEGACY_RESULT_SPEC: ResultActionSpec = {
  parts: [
    'purchase',
    'onsite_conversion.purchase',
    'complete_registration',
    'offsite_conversion.complete_registration',
  ],
  isPurchase: true,
};

/**
 * Chọn ResultActionSpec cho một entity dựa trên optimization_goal (+
 * custom_event_type nếu là OFFSITE_CONVERSIONS/VALUE). Thiếu dữ liệu →
 * fallback định nghĩa cũ.
 */
export function resolveResultSpec(
  optimizationGoal?: string | null,
  customEventType?: string | null,
): ResultActionSpec {
  const goal = optimizationGoal?.toUpperCase();
  if (!goal) return LEGACY_RESULT_SPEC;

  if (goal === 'OFFSITE_CONVERSIONS' || goal === 'VALUE') {
    const ev = customEventType?.toUpperCase();
    if (ev && RESULT_ACTION_BY_CUSTOM_EVENT[ev]) {
      return RESULT_ACTION_BY_CUSTOM_EVENT[ev];
    }
    // OFFSITE_CONVERSIONS thiếu event → mặc định PURCHASE; nếu goal lạ hẳn → legacy.
    return RESULT_ACTION_BY_GOAL[goal] ?? LEGACY_RESULT_SPEC;
  }

  return RESULT_ACTION_BY_GOAL[goal] ?? LEGACY_RESULT_SPEC;
}

/** Đọc promoted_object.custom_event_type từ rawPayload của AdSet. */
export function extractCustomEventType(
  rawPayload: any,
): string | null {
  const po =
    rawPayload?.promoted_object ??
    rawPayload?.promotedObject ??
    rawPayload?.adset?.promoted_object;
  return po?.custom_event_type ?? po?.customEventType ?? null;
}

/* ==========================================================================
 *  AGGREGATOR — tổng hợp nhiều dòng DAILY thành 1 bộ số theo đúng chiến lược.
 * ========================================================================== */

/** Các field số cộng thẳng được. */
export const ADDITIVE_FIELDS = [
  'impressions',
  'clicks',
  'spend',
  'purchases',
  'purchaseValue',
  'registrationComplete',
  'registrationCompleteValue',
  'messagingStarted',
  'messagingStartedValue',
  'outboundClicks',
  'outboundClicksValue',
  'videoPlay',
  'video3s',
  'video100',
  'videoThruplay',
  'videoView',
] as const;

/** Field dedup: cộng như XẤP XỈ, cần cờ approx khi > 1 ngày. */
export const DEDUP_FIELDS = ['reach', 'uniqueClicks'] as const;

const RANKING_FIELDS = [
  'qualityRanking',
  'engagementRateRanking',
  'conversionRateRanking',
] as const;

export interface DailyInsightRow {
  dateStart?: string | null;
  actions?: any;
  [k: string]: any;
}

/** Thông tin goal của 1 entity để tính result. */
export interface EntityGoalInfo {
  optimizationGoal?: string | null;
  customEventType?: string | null;
}

export interface AggregateOptions extends EntityGoalInfo {
  /** Bỏ tính result theo goal (dùng cho campaign gộp — result = ∑ con). */
  skipResult?: boolean;
}

export interface AggregatedInsight {
  [k: string]: any;
  /** reach/frequency/uniqueCtr là ước lượng (khoảng > 1 ngày). */
  approxReach: boolean;
}

/** Gom actions[] của 1 dòng thành map action_type → value (number). */
function actionsToMap(actions: any): Record<string, number> {
  const map: Record<string, number> = {};
  const arr = Array.isArray(actions)
    ? actions
    : Array.isArray(actions?.data)
      ? actions.data
      : null;
  if (!arr) return map;
  for (const a of arr) {
    const t = a?.action_type;
    if (!t) continue;
    const v = Number(a?.value);
    if (Number.isFinite(v)) map[t] = (map[t] || 0) + v;
  }
  return map;
}

/** map action_type→value (đã cộng) → mảng [{action_type, value}] chuẩn Meta. */
function mapToActionArray(
  map: Record<string, number>,
): { action_type: string; value: number }[] {
  return Object.entries(map).map(([action_type, value]) => ({ action_type, value }));
}

/** Đếm result từ map action đã cộng, theo spec (ưu tiên unified đã dedup). */
export function countResult(
  actionMap: Record<string, number>,
  additive: Record<string, number>,
  spec: ResultActionSpec,
): number {
  if (spec.field) return Math.round(additive[spec.field] || 0);
  if (spec.unified && actionMap[spec.unified] != null) {
    return Math.round(actionMap[spec.unified]);
  }
  let sum = 0;
  for (const p of spec.parts) sum += actionMap[p] || 0;
  return Math.round(sum);
}

/**
 * Tổng hợp danh sách dòng DAILY → 1 bộ metric đầy đủ, đúng chiến lược từng field.
 * Áp cho: rollup 4 bucket, custom-range list/detail, và footer tổng.
 */
export function aggregateDailyInsights(
  rows: DailyInsightRow[],
  opts: AggregateOptions = {},
): AggregatedInsight {
  const out: Record<string, any> = {};
  for (const f of ADDITIVE_FIELDS) out[f] = 0;
  for (const f of DEDUP_FIELDS) out[f] = 0;

  // weighted-avg video watch time
  let vawtNum = 0;
  let vawtDen = 0;
  // gộp actions toàn kỳ để đếm result theo goal
  const totalActions: Record<string, number> = {};
  // gộp action_values toàn kỳ (để tái sử dụng: tách omni vs website pixel, ROAS chuẩn…).
  // Cộng theo action_type là ĐÚNG chuẩn khi gộp nhiều ngày (giá trị đơn cộng thẳng).
  const totalActionValues: Record<string, number> = {};
  // snapshot ranking theo ngày mới nhất
  const dates = new Set<string>();
  let latestDate = '';
  const latestRanking: Record<string, string | null> = {};

  for (const row of rows) {
    for (const f of ADDITIVE_FIELDS) out[f] += Number(row[f] ?? 0) || 0;
    for (const f of DEDUP_FIELDS) out[f] += Number(row[f] ?? 0) || 0;

    // `||` (không phải `??`): videoView mặc định 0 (không bao giờ null) nên phải
    // coi 0 = thiếu để rơi xuống videoPlay (field thực sự có số làm trọng số).
    const w =
      Number(row.videoView) || Number(row.videoPlay) || Number(row.impressions) || 0;
    const avg = Number(row.videoAvgWatchTime ?? 0) || 0;
    if (w > 0 && avg > 0) {
      vawtNum += avg * w;
      vawtDen += w;
    }

    const am = actionsToMap(row.actions);
    for (const [t, v] of Object.entries(am)) {
      totalActions[t] = (totalActions[t] || 0) + v;
    }
    // camelCase (dòng DB) hoặc snake (raw Meta) — nhận cả hai cho chắc.
    const avm = actionsToMap(row.actionValues ?? row.action_values);
    for (const [t, v] of Object.entries(avm)) {
      totalActionValues[t] = (totalActionValues[t] || 0) + v;
    }

    const d = row.dateStart || '';
    if (d) {
      dates.add(d);
      if (d >= latestDate) {
        latestDate = d;
        for (const rf of RANKING_FIELDS) latestRanking[rf] = row[rf] ?? null;
      }
    }
  }

  // ===== dedup + approx cờ =====
  out.approxReach = dates.size > 1;

  // ===== weighted-avg =====
  out.videoAvgWatchTime = vawtDen > 0 ? +(vawtNum / vawtDen).toFixed(2) : 0;

  // ===== snapshot ranking =====
  for (const rf of RANKING_FIELDS) out[rf] = latestRanking[rf] ?? null;

  // ===== result theo goal (đếm ở cấp entity có goal) =====
  const spec = resolveResultSpec(opts.optimizationGoal, opts.customEventType);
  if (!opts.skipResult) {
    out.results = countResult(totalActions, out, spec);
  }

  // ===== derived (tính lại từ tổng) =====
  recomputeDerived(out, spec);

  // ===== actions/action_values đã gộp (mảng chuẩn Meta) — để rollup CreativeInsight có
  // dữ liệu thô đầy đủ, tái sử dụng sau (tách omni vs website pixel, ROAS theo nhu cầu).
  // Các model insight đều có cột actions/actionValues (Json) nên spread thẳng vào upsert.
  out.actions = mapToActionArray(totalActions);
  out.actionValues = mapToActionArray(totalActionValues);

  return out as AggregatedInsight;
}

/** Tính lại toàn bộ metric phái sinh từ các tổng additive/result đã có. */
export function recomputeDerived(
  m: Record<string, any>,
  spec: ResultActionSpec = LEGACY_RESULT_SPEC,
) {
  const impressions = m.impressions || 0;
  const reach = m.reach || 0;
  const clicks = m.clicks || 0;
  const uniqueClicks = m.uniqueClicks || 0;
  const spend = m.spend || 0;
  const purchaseValue = m.purchaseValue || 0;
  const results = m.results || 0;
  const videoPlay = m.videoPlay || 0;
  const video3s = m.video3s || 0;
  const video100 = m.video100 || 0;

  m.frequency = reach > 0 ? impressions / reach : 0;
  m.ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
  m.uniqueCtr = reach > 0 ? (uniqueClicks / reach) * 100 : 0;
  m.cpc = clicks > 0 ? spend / clicks : 0;
  m.cpm = impressions > 0 ? (spend / impressions) * 1000 : 0;
  m.roas = spend > 0 ? purchaseValue / spend : 0;
  m.cvr = clicks > 0 ? results / clicks : 0;
  m.costPerResult = results > 0 ? spend / results : 0;
  // aov chỉ có nghĩa với result mua hàng; goal khác → null (FE ẩn ô).
  m.aov = spec.isPurchase && results > 0 ? Math.round(purchaseValue / results) : null;
  m.adsCostRatio = m.roas > 0 ? 1 / m.roas : 0;
  m.hookRate = videoPlay > 0 ? +((video3s / videoPlay) * 100).toFixed(2) : 0;
  m.holdRate = video3s > 0 ? +((video100 / video3s) * 100).toFixed(2) : 0;
  return m;
}

/* ==========================================================================
 *  REGISTRY HIỂN THỊ — catalog metric cho danh sách cột + tile detail + picker.
 * ========================================================================== */

export const INSIGHT_METRIC_REGISTRY: MetricDef[] = [
  // ----- SPEND -----
  { key: 'spend', label: 'Chi tiêu', short: 'Chi tiêu', group: 'spend', unit: 'currency', agg: 'additive' },
  { key: 'results', label: 'Kết quả', short: 'Kết quả', group: 'conversion', unit: 'number', agg: 'result', hint: 'Đếm theo mục tiêu tối ưu của nhóm quảng cáo' },
  { key: 'costPerResult', label: 'Chi phí/kết quả', short: 'CP/KQ', group: 'conversion', unit: 'currency', agg: 'derived' },
  { key: 'adsCostRatio', label: '%Ads', short: '%Ads', group: 'spend', unit: 'percent', agg: 'derived' },
  { key: 'roas', label: 'ROAS', short: 'ROAS', group: 'conversion', unit: 'ratio', agg: 'derived' },
  { key: 'aov', label: 'Giá trị TB/đơn', short: 'AOV', group: 'conversion', unit: 'currency', agg: 'derived', purchaseOnly: true },

  // ----- REACH / TRAFFIC -----
  { key: 'impressions', label: 'Lượt hiển thị', short: 'Hiển thị', group: 'reach', unit: 'number', agg: 'additive' },
  { key: 'reach', label: 'Lượt tiếp cận', short: 'Tiếp cận', group: 'reach', unit: 'number', agg: 'dedup', approxOnMultiDay: true, hint: 'Meta khử trùng theo cửa sổ; số cho khoảng nhiều ngày là ước lượng' },
  { key: 'frequency', label: 'Tần suất', short: 'Tần suất', group: 'reach', unit: 'number', agg: 'derived', approxOnMultiDay: true },
  { key: 'clicks', label: 'Lượt nhấp', short: 'Nhấp', group: 'traffic', unit: 'number', agg: 'additive' },
  { key: 'uniqueClicks', label: 'Nhấp (người)', short: 'Nhấp UQ', group: 'traffic', unit: 'number', agg: 'dedup', approxOnMultiDay: true },
  { key: 'ctr', label: 'CTR', short: 'CTR', group: 'traffic', unit: 'percent', agg: 'derived' },
  { key: 'uniqueCtr', label: 'CTR (người)', short: 'CTR UQ', group: 'traffic', unit: 'percent', agg: 'derived', approxOnMultiDay: true },
  { key: 'cpc', label: 'CPC', short: 'CPC', group: 'traffic', unit: 'currency', agg: 'derived' },
  { key: 'cpm', label: 'CPM', short: 'CPM', group: 'reach', unit: 'currency', agg: 'derived' },
  { key: 'outboundClicks', label: 'Nhấp ra ngoài', short: 'Out', group: 'traffic', unit: 'number', agg: 'additive' },
  { key: 'cvr', label: 'Tỷ lệ chuyển đổi', short: 'CVR', group: 'conversion', unit: 'percent', agg: 'derived' },

  // ----- CONVERSION chi tiết -----
  { key: 'purchases', label: 'Lượt mua', short: 'Mua', group: 'conversion', unit: 'number', agg: 'additive' },
  { key: 'purchaseValue', label: 'Giá trị mua', short: 'GT mua', group: 'conversion', unit: 'currency', agg: 'additive' },
  { key: 'registrationComplete', label: 'Đăng ký', short: 'Đăng ký', group: 'conversion', unit: 'number', agg: 'additive' },
  { key: 'messagingStarted', label: 'Tin nhắn bắt đầu', short: 'Nhắn tin', group: 'conversion', unit: 'number', agg: 'additive' },

  // ----- VIDEO -----
  { key: 'videoAvgWatchTime', label: 'Thời lượng xem TB', short: 'Xem TB', group: 'video', unit: 'duration', agg: 'weighted_avg', weightField: 'videoView' },
  { key: 'hookRate', label: 'Hook Rate', short: 'Hook', group: 'video', unit: 'percent', agg: 'derived' },
  { key: 'holdRate', label: 'Hold Rate', short: 'Hold', group: 'video', unit: 'percent', agg: 'derived' },
  { key: 'videoThruplay', label: 'ThruPlay', short: 'ThruPlay', group: 'video', unit: 'number', agg: 'additive' },

  // ----- QUALITY (snapshot, không tổng hợp) -----
  { key: 'qualityRanking', label: 'Xếp hạng chất lượng', short: 'Chất lượng', group: 'quality', unit: 'text', agg: 'snapshot' },
  { key: 'engagementRateRanking', label: 'Xếp hạng tương tác', short: 'Tương tác', group: 'quality', unit: 'text', agg: 'snapshot' },
  { key: 'conversionRateRanking', label: 'Xếp hạng chuyển đổi', short: 'Chuyển đổi', group: 'quality', unit: 'text', agg: 'snapshot' },
];

export const METRIC_BY_KEY: Record<string, MetricDef> = Object.fromEntries(
  INSIGHT_METRIC_REGISTRY.map((m) => [m.key, m]),
);

/** Các key chỉ số DÙNG ĐƯỢC trong công thức custom (loại text/ranking). */
export const NUMERIC_METRIC_KEYS = new Set(
  INSIGHT_METRIC_REGISTRY.filter((m) => m.unit !== 'text').map((m) => m.key),
);
