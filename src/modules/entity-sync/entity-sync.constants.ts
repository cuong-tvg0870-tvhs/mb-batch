export const ENTITY_SYNC_QUEUE = 'entity-sync-queue';

export const ENTITY_SYNC_JOBS = {
  /**
   * Đồng bộ metadata các thực thể Meta về DB: Ad Account (TKQC), Fanpage
   * (kèm Instagram + số WhatsApp), Pixel, Custom Audience, Product Catalog.
   * Chạy 1 ngày/lần — không đụng tới các job campaign/insight/media hiện có.
   */
  SYNC_META_ENTITIES: 'sync-meta-entities',
} as const;

export const ENTITY_SYNC_CONFIG = {
  /** Nghỉ giữa mỗi account để né rate-limit (ms). */
  accountSleepMs: Number(process.env.ENTITY_SYNC_ACCOUNT_SLEEP_MS || 2000),
  /** Meta paging size khi liệt kê account/fanpage. */
  pageLimit: 100,
  /** Meta paging size khi kéo custom audiences của một account. */
  audienceLimit: 50,
} as const;
