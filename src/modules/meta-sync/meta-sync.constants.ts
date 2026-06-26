export const META_SYNC_QUEUE = 'meta-sync-queue';

export const META_SYNC_JOBS = {
  SYNC_CAMPAIGN_CORE: 'sync-campaign-core',
} as const;

/**
 * Tuning knobs for the core campaign sync. Env vars override the defaults so the
 * job can be tuned per-environment without code changes.
 */
export const META_SYNC_CONFIG = {
  /** How far back to sync when an account has never been synced before. */
  lookbackDays: Number(process.env.META_CORE_SYNC_LOOKBACK_DAYS || 14),
  /** Overlap subtracted from the last successful sync to catch late updates. */
  overlapHours: Number(process.env.META_CORE_SYNC_OVERLAP_HOURS || 6),
  /** Max ad accounts synced concurrently. */
  accountConcurrency: Number(process.env.META_CORE_SYNC_ACCOUNT_CONCURRENCY || 4),
  /** Meta paging size per request. */
  pageLimit: 50,
  /** Max ids per IN-filter when hydrating parents/children. */
  idChunkSize: 50,
  /** A parent is considered fresh (skip re-hydration) within this window. */
  hydrationMaxAgeMs: 6 * 60 * 60 * 1000,
} as const;
