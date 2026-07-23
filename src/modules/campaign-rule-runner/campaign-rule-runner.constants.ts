/**
 * Hằng số cho runner "campaign rule" (engine rule scheduling của MB).
 *
 * Runner này CHỈ chạy nhánh "Theo điều kiện": cron quét rule ACTIVE → đến lịch →
 * đọc insight LIVE từ Meta (date_preset=today) → đánh giá cây điều kiện → nếu khớp
 * thì autoExecute đẩy budget_schedule_specs lên Meta ngay, hoặc ghi PENDING chờ
 * user confirm. Action duy nhất hỗ trợ v1 = BUDGET_SCHEDULE_BUMP.
 */

/** Cron tick: mỗi 5 phút, theo giờ VN. */
export const CAMPAIGN_RULE_TICK_CRON = '*/5 * * * *';
export const CAMPAIGN_RULE_TICK_TIMEZONE = 'Asia/Ho_Chi_Minh';

/** Cửa sổ tick ~5 phút — dùng để bắt slot SPECIFIC vừa đi qua trong tick hiện tại. */
export const TICK_WINDOW_MS = 5 * 60 * 1000;

/** Dung sai cho lịch INTERVAL: coi là "đến hạn" nếu còn thiếu <= 60s. */
export const INTERVAL_TOLERANCE_MS = 60 * 1000;

/** Timezone fallback khi rule.timezone = "account" mà account không có tz. */
export const DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh';

/** TTL khóa phân tán (giây) — lớn hơn thời lượng chạy tối đa của một rule. */
export const RULE_LOCK_TTL_SECONDS = 300;

/** Độ sâu tối đa của cây group điều kiện được Prisma include. */
export const MAX_GROUP_DEPTH = 6;

/** Map interval string → mili giây. Khớp enum interval của CampaignRuleSchedule. */
export const INTERVAL_MS: Record<string, number> = {
  '15m': 15 * 60 * 1000,
  '30m': 30 * 60 * 1000,
  '60m': 60 * 60 * 1000,
  '3h': 3 * 60 * 60 * 1000,
  '6h': 6 * 60 * 60 * 1000,
  '12h': 12 * 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '36h': 36 * 60 * 60 * 1000,
  '72h': 72 * 60 * 60 * 1000,
};

/**
 * Fields insight request từ Meta cho MỖI entity (level campaign/adset).
 * Chỉ khung TODAY (date_preset=today) cho v1.
 */
export const INSIGHT_FIELDS = [
  'spend',
  'impressions',
  'reach',
  'frequency',
  'clicks',
  'ctr',
  'cpc',
  'cpm',
  'actions',
  'action_values',
  'purchase_roas',
  'website_purchase_roas',
  'cost_per_action_type',
  // Link/outbound clicks (đếm + cost/ctr) — cho các metric tương ứng ở dropdown.
  'inline_link_clicks',
  'inline_link_click_ctr',
  'cost_per_inline_link_click',
  'outbound_clicks',
  // Video — field mảng-thống-kê [{action_type,value}], đọc [0].value.
  'video_thruplay_watched_actions',
  'video_15_sec_watched_actions',
  'video_30_sec_watched_actions',
  'video_p25_watched_actions',
  'video_p50_watched_actions',
  'video_p75_watched_actions',
  'video_p95_watched_actions',
  'video_p100_watched_actions',
  'video_avg_time_watched_actions',
];
