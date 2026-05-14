export enum InsightSyncLevel {
  ACCOUNT = 'account',
  CAMPAIGN = 'campaign',
  ADSET = 'adset',
  AD = 'ad',
}

export enum InsightSyncRange {
  TODAY = 'today',
  LAST_3D = 'last_3d',
  LAST_7D = 'last_7d',
  MAX = 'maximum',
}

export interface SyncAccountJobData {
  accountId: string;
  levels: InsightSyncLevel[];
  ranges: InsightSyncRange[];
}

export const INSIGHT_SYNC_QUEUE = 'insight-sync-queue';

export const INSIGHT_SYNC_JOBS = {
  SYNC_ACCOUNT: 'sync-account-insights',
  SYNC_AUDIENCE: 'sync-audience-insights',
};
