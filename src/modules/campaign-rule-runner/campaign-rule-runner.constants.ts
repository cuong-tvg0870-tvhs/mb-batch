/**
 * Hằng số cho runner "campaign rule" (engine rule scheduling của MB).
 *
 * Runner này CHỈ chạy nhánh "Theo điều kiện": cron quét rule ACTIVE → đến lịch →
 * đọc insight LIVE từ Meta (date_preset=today) → đánh giá cây điều kiện → nếu khớp
 * thì autoExecute đẩy budget_schedule_specs lên Meta ngay, hoặc ghi PENDING chờ
 * user confirm. Action duy nhất hỗ trợ v1 = BUDGET_SCHEDULE_BUMP.
 */

/**
 * Cron tick: mỗi 5 phút, theo giờ VN.
 * Lệch khỏi phút :00 (2,7,12,...,57) để không dồn cục vào cron insight-sync/
 * media-sync (đều bắn ở :00) — tránh bão request Meta cùng lúc.
 * ĐÃ XÁC MINH an toàn với slot SPECIFIC giờ tròn (vd 05:00, 21:00): tick liền sau
 * mỗi slot luôn cách slot đó tối đa 4 phút (spacing tick vẫn đều 5 phút, chỉ dịch
 * pha), trong khi TICK_WINDOW_MS bắt slot "vừa đi qua" trong 5 phút — xem
 * campaign-rule-schedule.util.ts (isRuleDue, nhánh SPECIFIC: diff = nowMinutes -
 * slotMinutes, bắt khi 0 <= diff < windowMinutes). Slot giờ tròn không bao giờ bị bỏ lỡ.
 */
export const CAMPAIGN_RULE_TICK_CRON = '2-59/5 * * * *';
export const CAMPAIGN_RULE_TICK_TIMEZONE = 'Asia/Ho_Chi_Minh';

/** Cửa sổ tick ~5 phút — dùng để bắt slot SPECIFIC vừa đi qua trong tick hiện tại. */
export const TICK_WINDOW_MS = 5 * 60 * 1000;

/** Dung sai cho lịch INTERVAL: coi là "đến hạn" nếu còn thiếu <= 60s. */
export const INTERVAL_TOLERANCE_MS = 60 * 1000;

/** Timezone fallback khi rule.timezone = "account" mà account không có tz. */
export const DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh';

/** TTL khóa phân tán (giây) — lớn hơn thời lượng chạy tối đa của một rule. */
export const RULE_LOCK_TTL_SECONDS = 300;

/**
 * Biên an toàn trừ khỏi TTL khóa để tính hạn chót của một lượt chạy.
 *
 * VÌ SAO CẦN: khóa `crr:<ruleId>` chỉ là `SET NX EX 300` một lần, KHÔNG tự gia hạn.
 * Nếu một lượt chạy lâu hơn TTL thì khóa hết hạn GIỮA CHỪNG → replica khác chiếm được
 * và chạy CÙNG rule CÙNG entity đồng thời → cả hai gọi executeBudgetSchedule → tạo 2
 * khung chồng nhau → BƠM NGÂN SÁCH HAI LẦN. Vì vậy mọi thứ có thể ngủ/chờ trong một
 * lượt chạy đều phải nằm gọn trong RULE_RUN_MAX_WALL_MS.
 */
export const RULE_RUN_SAFETY_MARGIN_MS = 30 * 1000;

/** Hạn chót (mili giây) cho một lượt chạy rule, luôn nhỏ hơn TTL khóa. */
export const RULE_RUN_MAX_WALL_MS =
  RULE_LOCK_TTL_SECONDS * 1000 - RULE_RUN_SAFETY_MARGIN_MS;

/**
 * Trần thời gian (mili giây) của MỘT lần gọi Graph API — khớp `axios.defaults.timeout`
 * đặt ở `main.ts`. Dùng để ước lượng worst-case khi tính số lần retry còn cho phép.
 */
export const META_CALL_TIMEOUT_MS = 30 * 1000;

/**
 * Backoff cho lỗi RATE-LIMIT khi đọc insight (mặc định của executeMetaApiWithRetry là
 * 60s, quá dài: 60+120+180 = 360s > TTL khóa 300s → khóa chết giữa chừng).
 * 15s cho retry 1, 30s cho retry 2, 45s cho retry 3 (công thức sleep × retry).
 */
export const INSIGHT_RATELIMIT_SLEEP_MS = 15 * 1000;

/** Backoff cho lỗi MẠNG khi đọc insight: 5s/10s/15s. */
export const INSIGHT_NETWORK_SLEEP_MS = 5 * 1000;

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
