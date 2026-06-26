import { Injectable, Logger } from '@nestjs/common';
import {
  CreativeStatus,
  InsightRange,
  LevelInsight,
  Prisma,
} from '@prisma/client';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';
import { PrismaBatchHelper } from '../../common/helpers/prisma-batch.helper';
import {
  chunk,
  executeDbWithRetry,
  extractCampaignMetrics,
  isPermissionError,
  parseMetaError,
  sleep,
} from '../../common/utils';
import { MetaApiService } from '../meta-api/meta-api.service';
import { PrismaService } from '../prisma/prisma.service';
import { InsightSyncLevel, InsightSyncRange } from './insight-sync.constants';

dayjs.extend(utc);
dayjs.extend(timezone);

// Default ad-account timezone (matches the cron timeZone). Used when an account
// has no timezone stored, so date windows align with how Meta keys date_start.
const DEFAULT_ACCOUNT_TZ = 'Asia/Ho_Chi_Minh';
const MAX_SENTINEL_DATE = '1975-01-01';

@Injectable()
export class InsightSyncService {
  private readonly logger = new Logger(InsightSyncService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaApi: MetaApiService,
  ) {}

  /**
   * Main entry point for syncing insights for one account
   */
  async syncAccountInsights(
    accountId: string,
    levels: InsightSyncLevel[],
    ranges: InsightSyncRange[],
  ) {
    const targetLevels = levels.filter((level) =>
      [
        InsightSyncLevel.CAMPAIGN,
        InsightSyncLevel.ADSET,
        InsightSyncLevel.AD,
      ].includes(level),
    );

    if (targetLevels.length === 0) return;

    const tz = await this.getAccountTimezone(accountId);

    // Only the near-real-time TODAY job should call Meta. Other range jobs
    // rebuild materialized rollups locally from DAILY records.
    if (ranges.includes(InsightSyncRange.TODAY)) {
      for (const level of targetLevels) {
        // Isolate the Meta fetch per level: one level failing (or a token
        // problem) must NOT skip the local rollups below, which rebuild every
        // range from whatever DAILY data already exists in the DB.
        try {
          await this.syncRecentDailyInsights(accountId, level, tz);
        } catch (error) {
          if (isPermissionError(error)) {
            this.logger.warn(
              `[${accountId}] 🔑 Permission/token error fetching ${level} DAILY. Marking account for reauth.`,
            );
            await executeDbWithRetry(() =>
              this.prisma.account.update({
                where: { id: accountId },
                data: { needsReauth: true },
              }),
            );
          } else {
            this.logger.error(
              `[${accountId}] ❌ DAILY fetch failed for ${level}: ${parseMetaError(error).message}. Continuing with local rollups.`,
            );
          }
        }
      }
    }

    for (const level of targetLevels) {
      try {
        await this.rollupLevelInsights(accountId, level, ranges, tz);
      } catch (error) {
        this.logger.error(
          `[${accountId}] ❌ Rollup failed for ${level}: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }

    if (targetLevels.includes(InsightSyncLevel.AD)) {
      try {
        await this.aggregateCreativeInsights(accountId, ranges, tz);
      } catch (error) {
        this.logger.error(
          `[${accountId}] ❌ Creative aggregation failed: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
  }

  /**
   * Resolves the ad-account timezone so date windows (today/3d/7d/max) align
   * with how Meta keys date_start for that account. Falls back to the default
   * cron timezone when the account has none stored.
   */
  private async getAccountTimezone(accountId: string): Promise<string> {
    try {
      const account = await this.prisma.account.findUnique({
        where: { id: accountId },
        select: { timezone: true },
      });
      return this.normalizeTimezone(account?.timezone);
    } catch {
      return DEFAULT_ACCOUNT_TZ;
    }
  }

  private normalizeTimezone(tz?: string | null): string {
    if (!tz) return DEFAULT_ACCOUNT_TZ;
    // Validate against the IANA db; an invalid name throws in dayjs.tz().
    try {
      dayjs().tz(tz);
      return tz;
    } catch {
      return DEFAULT_ACCOUNT_TZ;
    }
  }

  private nowInTz(tz: string) {
    return dayjs().tz(tz);
  }

  private shouldSkipMaxRollup(
    existingMax: { dateStart?: string | null } | null | undefined,
    matchingDaily: Array<{ dateStart?: string | null }>,
  ): boolean {
    // No authoritative existing MAX -> always (re)compute.
    if (!existingMax?.dateStart) return false;
    // A sentinel/placeholder MAX (never had real DAILY) is NOT authoritative;
    // allow it to be recomputed so a previously-zeroed MAX can self-heal.
    if (existingMax.dateStart === MAX_SENTINEL_DATE) return false;

    const firstDailyDate = matchingDaily
      .map((insight) => insight.dateStart)
      .filter(Boolean)
      .sort()[0];

    // Shrink-protection: if the DAILY currently loaded doesn't reach as far back
    // as the existing MAX, keep the existing (wider) MAX instead of truncating.
    return !firstDailyDate || firstDailyDate > existingMax.dateStart;
  }

  private getWindowForRange(
    range: InsightSyncRange | InsightRange,
    tz: string = DEFAULT_ACCOUNT_TZ,
  ) {
    const now = this.nowInTz(tz);
    const todayStr = now.format('YYYY-MM-DD');
    const yesterdayStr = now.subtract(1, 'day').format('YYYY-MM-DD');

    if (range === InsightSyncRange.TODAY || range === InsightRange.TODAY) {
      return {
        range: InsightRange.TODAY,
        dateStart: todayStr,
        dateStop: todayStr,
      };
    }

    if (range === InsightSyncRange.LAST_3D || range === InsightRange.DAY_3) {
      return {
        range: InsightRange.DAY_3,
        dateStart: now.subtract(3, 'day').format('YYYY-MM-DD'),
        dateStop: yesterdayStr,
      };
    }

    if (range === InsightSyncRange.LAST_7D || range === InsightRange.DAY_7) {
      return {
        range: InsightRange.DAY_7,
        dateStart: now.subtract(7, 'day').format('YYYY-MM-DD'),
        dateStop: yesterdayStr,
      };
    }

    return {
      range: InsightRange.MAX,
      dateStart: null,
      dateStop: todayStr,
    };
  }

  private getRequestedWindows(
    ranges: InsightSyncRange[],
    tz: string = DEFAULT_ACCOUNT_TZ,
  ) {
    return ranges.map((range) => this.getWindowForRange(range, tz));
  }

  private getRangePointerField(range: InsightRange): string | null {
    const map = {
      [InsightRange.TODAY]: 'insightTodayId',
      [InsightRange.DAY_3]: 'insight3dId',
      [InsightRange.DAY_7]: 'insight7dId',
      [InsightRange.MAX]: 'insightMaxId',
      [InsightRange.DAILY]: null,
    };
    return map[range];
  }

  private getRollupKey(entityId: string, range: InsightRange) {
    return `${entityId}:${range}`;
  }

  private getRollupDateKey(
    entityId: string,
    range: InsightRange,
    dateStart: string | null,
  ) {
    return `${entityId}:${range}:${dateStart || ''}`;
  }

  private sumMetrics(target: Record<string, number>, source: any) {
    const additiveFields = [
      'impressions',
      'reach',
      'clicks',
      'uniqueClicks',
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
    ];

    for (const field of additiveFields) {
      const value = Number(source?.[field] ?? 0);
      if (Number.isFinite(value)) {
        target[field] = (target[field] || 0) + value;
      }
    }
  }

  private recalculateDerivedMetrics(target: Record<string, number>) {
    const additiveFields = [
      'impressions',
      'reach',
      'clicks',
      'uniqueClicks',
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
    ];

    for (const field of additiveFields) {
      target[field] = target[field] || 0;
    }

    const impressions = target.impressions || 0;
    const reach = target.reach || 0;
    const clicks = target.clicks || 0;
    const uniqueClicks = target.uniqueClicks || 0;
    const spend = target.spend || 0;
    const purchases = target.purchases || 0;
    const purchaseValue = target.purchaseValue || 0;
    const registrationComplete = target.registrationComplete || 0;
    const videoPlay = target.videoPlay || 0;
    const video3s = target.video3s || 0;
    const video100 = target.video100 || 0;
    const results = purchases + registrationComplete;

    target.frequency = reach > 0 ? impressions / reach : 0;
    target.ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
    target.uniqueCtr = reach > 0 ? (uniqueClicks / reach) * 100 : 0;
    target.cpc = clicks > 0 ? spend / clicks : 0;
    target.cpm = impressions > 0 ? (spend / impressions) * 1000 : 0;
    target.roas = spend > 0 ? purchaseValue / spend : 0;
    target.cvr = clicks > 0 ? registrationComplete / clicks : 0;
    target.results = Math.round(results);
    target.costPerResult = results > 0 ? spend / results : 0;
    target.aov = results > 0 ? Math.round(purchaseValue / results) : 0;
    target.adsCostRatio = target.roas > 0 ? 1 / target.roas : 0;
    target.hookRate =
      videoPlay > 0 ? +((video3s / videoPlay) * 100).toFixed(2) : 0;
    target.holdRate =
      video3s > 0 ? +((video100 / video3s) * 100).toFixed(2) : 0;
  }

  private isInsightInWindow(insight: any, window: any) {
    if (window.range === InsightRange.MAX) return true;
    return (
      insight.dateStart >= window.dateStart! &&
      insight.dateStart <= window.dateStop
    );
  }

  private shouldSplitInsightRequest(error: any) {
    const metaError = parseMetaError(error);
    const code = Number(metaError.code);
    const subcode = Number(metaError.subcode);
    const message = (metaError.message || '').toLowerCase();

    return (
      code === 1 ||
      code === 2 ||
      subcode === 2446079 ||
      message.includes('reduce the amount of data') ||
      message.includes('too much data') ||
      message.includes('unexpected error')
    );
  }

  private getDefaultInsightChunkSize(level: InsightSyncLevel) {
    return level === InsightSyncLevel.AD
      ? Number(process.env.INSIGHT_SYNC_AD_CHUNK_SIZE || 10)
      : Number(process.env.INSIGHT_SYNC_ENTITY_CHUNK_SIZE || 100);
  }

  private getMinAdaptiveInsightChunkSize(level: InsightSyncLevel) {
    return level === InsightSyncLevel.AD
      ? Number(process.env.INSIGHT_SYNC_AD_MIN_CHUNK_SIZE || 3)
      : Number(process.env.INSIGHT_SYNC_ENTITY_MIN_CHUNK_SIZE || 20);
  }

  private async fetchDailyInsightsAdaptive(
    accountId: string,
    level: InsightSyncLevel,
    ids: string[],
    timeRange: { since: string; until: string },
    depth = 0,
  ): Promise<any[]> {
    const minChunkSize = this.getMinAdaptiveInsightChunkSize(level);
    const canSplit = ids.length > minChunkSize;

    try {
      return await this.metaApi.getAccountInsights(accountId, {
        level: level as any,
        time_range: timeRange,
        time_increment: 1,
        ids,
        limit: 50,
        retryOptions: canSplit
          ? {
              maxRetries: Number(
                process.env.INSIGHT_SYNC_ADAPTIVE_RETRIES || 1,
              ),
              initialSleepMs: Number(
                process.env.INSIGHT_SYNC_ADAPTIVE_RETRY_SLEEP_MS || 15000,
              ),
              networkSleepMs: Number(
                process.env.INSIGHT_SYNC_NETWORK_RETRY_SLEEP_MS || 10000,
              ),
            }
          : undefined,
      });
    } catch (error) {
      if (!canSplit || !this.shouldSplitInsightRequest(error)) {
        throw error;
      }

      const middle = Math.ceil(ids.length / 2);
      const left = ids.slice(0, middle);
      const right = ids.slice(middle);
      const metaError = parseMetaError(error);

      this.logger.warn(
        `[${accountId}] Splitting DAILY ${level} insight request ${ids.length} ids -> ${left.length}+${right.length} after Meta error code=${metaError.code ?? '-'} subcode=${metaError.subcode ?? '-'} depth=${depth}.`,
      );

      await sleep(
        Number(process.env.INSIGHT_SYNC_ADAPTIVE_SPLIT_SLEEP_MS || 1000),
      );
      const leftResult = await this.fetchDailyInsightsAdaptive(
        accountId,
        level,
        left,
        timeRange,
        depth + 1,
      );
      await sleep(
        Number(process.env.INSIGHT_SYNC_ADAPTIVE_SPLIT_SLEEP_MS || 1000),
      );
      const rightResult = await this.fetchDailyInsightsAdaptive(
        accountId,
        level,
        right,
        timeRange,
        depth + 1,
      );

      return [...leftResult, ...rightResult];
    }
  }

  private async syncRecentDailyInsights(
    accountId: string,
    level: InsightSyncLevel,
    tz: string = DEFAULT_ACCOUNT_TZ,
  ) {
    this.logger.log(`[${accountId}] Syncing recent DAILY ${level} insights...`);

    const parentModel = this.getParentModel(level);
    const relationFieldId = this.getRelationField(level);
    const entityIdField = this.getEntityIdField(level);
    const prismaModel = this.getPrismaModel(level);
    const levelEnum = this.mapLevelToEnum(level);
    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const now = this.nowInTz(tz);
    const since = now.subtract(7, 'day').format('YYYY-MM-DD');
    const until = now.format('YYYY-MM-DD');

    const entities = (await executeDbWithRetry(() =>
      (this.prisma[parentModel] as any).findMany({
        where: {
          accountId,
          OR: [
            { status: { in: ['ACTIVE', 'IN_PROCESS'] } },
            {
              status: { in: ['PAUSED', 'ARCHIVED'] },
              updatedAt: { gte: dayjs().subtract(7, 'day').toDate() },
            },
          ],
        },
        select: { id: true },
      }),
    )) as Array<{ id: string }>;

    // Activity-based recency: also refresh entities that already have a DAILY
    // row in the window, regardless of status/updatedAt. `updatedAt` is not
    // bumped on Meta refresh for AdSet/Ad, so a recently-active entity that was
    // paused/archived >7 days ago would otherwise stop getting its last days
    // re-fetched while attribution is still settling.
    const recentlyActive = (await executeDbWithRetry(() =>
      (this.prisma[prismaModel] as any).findMany({
        where: {
          range: InsightRange.DAILY,
          dateStart: { gte: since },
          [parentModel]: { accountId },
        },
        select: { [relationFieldId]: true },
        distinct: [relationFieldId],
      }),
    )) as Array<Record<string, string>>;

    const entityIds = [
      ...new Set([
        ...entities.map((entity) => entity.id),
        ...recentlyActive.map((row) => row[relationFieldId]),
      ]),
    ];
    if (entityIds.length === 0) {
      this.logger.log(`[${accountId}] No ${level} entities need DAILY sync.`);
      return;
    }

    const chunkSize = this.getDefaultInsightChunkSize(level);
    const idChunks = chunk(entityIds, chunkSize) as string[][];

    let failedChunks = 0;
    for (let i = 0; i < idChunks.length; i++) {
      const idChunk = idChunks[i];
      this.logger.log(
        `[${accountId}] Fetching DAILY ${level}: chunk ${i + 1}/${idChunks.length}.`,
      );

      try {
        const insights = await this.fetchDailyInsightsAdaptive(
          accountId,
          level,
          idChunk,
          { since, until },
        );

        const dailyData = (insights || [])
          .filter((insight: any) => insight[entityIdField])
          .map((insight: any) => {
            const metrics = extractCampaignMetrics(insight);
            const { toPrismaJson } = require('../../common/utils');
            return {
              [relationFieldId]: insight[entityIdField],
              level: levelEnum,
              range: InsightRange.DAILY,
              dateStart: insight.date_start,
              dateStop: insight.date_stop,
              ...metrics,
              rawPayload: toPrismaJson(insight),
            };
          });

        if (dailyData.length > 0) {
          await executeDbWithRetry(async () => {
            await prismaHelper.upsertMany(
              dailyData,
              (item: any) => {
                const {
                  [relationFieldId]: rId,
                  dateStart,
                  range,
                  ...data
                } = item;
                return (this.prisma[prismaModel] as any).upsert({
                  where: {
                    [`${relationFieldId}_dateStart_range`]: {
                      [relationFieldId]: rId,
                      dateStart,
                      range,
                    },
                  },
                  update: data,
                  create: item,
                });
              },
              50,
            );
          });

          this.logger.log(
            `[${accountId}] Saved ${dailyData.length} DAILY ${level} records.`,
          );
        }
      } catch (error) {
        // Permission/token errors affect the whole account -> bubble up so the
        // caller flags needsReauth. Other (transient/too-much-data) chunk
        // failures are isolated: skip this chunk and keep the rest so the local
        // rollups still rebuild from the data we did get. The hourly job heals.
        if (isPermissionError(error)) throw error;
        failedChunks++;
        this.logger.error(
          `[${accountId}] ❌ DAILY ${level} chunk ${i + 1}/${idChunks.length} failed: ${parseMetaError(error).message}. Skipping chunk.`,
        );
      }

      if (i < idChunks.length - 1) {
        await sleep(Number(process.env.INSIGHT_SYNC_CHUNK_SLEEP_MS || 750));
      }
    }

    if (failedChunks > 0) {
      this.logger.warn(
        `[${accountId}] ⚠️ ${level}: ${failedChunks}/${idChunks.length} DAILY chunks failed (partial fetch). Rollups will use available DAILY rows.`,
      );
    }
  }

  private async rollupLevelInsights(
    accountId: string,
    level: InsightSyncLevel,
    ranges: InsightSyncRange[],
    tz: string = DEFAULT_ACCOUNT_TZ,
  ) {
    const windows = this.getRequestedWindows(ranges, tz);
    if (windows.length === 0) return;

    this.logger.log(
      `[${accountId}] Rolling up ${level} insights for ${windows.map((w) => w.range).join(', ')}...`,
    );

    const parentModel = this.getParentModel(level);
    const relationFieldId = this.getRelationField(level);
    const prismaModel = this.getPrismaModel(level);
    const levelEnum = this.mapLevelToEnum(level);
    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const parents = (await (this.prisma[parentModel] as any).findMany({
      where: { accountId, deletedAt: null },
      select: {
        id: true,
        insightTodayId: true,
        insight3dId: true,
        insight7dId: true,
        insightMaxId: true,
      },
    })) as Array<Record<string, any>>;

    if (parents.length === 0) return;

    const minWindowDate = windows
      .filter((window) => window.range !== InsightRange.MAX)
      .map((window) => window.dateStart!)
      .sort()[0];

    for (const parentChunk of chunk(parents, 200) as Array<typeof parents>) {
      const parentIds = parentChunk.map((parent) => parent.id);
      const hasMax = windows.some(
        (window) => window.range === InsightRange.MAX,
      );
      const rollupRanges = [...new Set(windows.map((window) => window.range))];
      const dailyWhere: any = {
        [relationFieldId]: { in: parentIds },
        range: InsightRange.DAILY,
      };

      if (!hasMax && minWindowDate) {
        dailyWhere.dateStart = { gte: minWindowDate };
      }

      const dailyInsights = await (this.prisma[prismaModel] as any).findMany({
        where: dailyWhere,
      });

      const dailyMap = new Map<string, any[]>();
      for (const insight of dailyInsights) {
        const parentId = insight[relationFieldId];
        if (!dailyMap.has(parentId)) dailyMap.set(parentId, []);
        dailyMap.get(parentId)!.push(insight);
      }

      const existingRollups = await (this.prisma[prismaModel] as any).findMany({
        where: {
          [relationFieldId]: { in: parentIds },
          range: { in: rollupRanges },
        },
        select: {
          id: true,
          [relationFieldId]: true,
          range: true,
          dateStart: true,
          updatedAt: true,
        },
        orderBy: [{ updatedAt: 'desc' }],
      });
      const existingRollupIds = new Set(
        existingRollups.map((insight: any) => insight.id),
      );
      const existingByRange = new Map<string, any>();
      const existingByRangeDate = new Map<string, any>();
      for (const insight of existingRollups) {
        const parentId = insight[relationFieldId];
        const rangeKey = this.getRollupKey(parentId, insight.range);
        const dateKey = this.getRollupDateKey(
          parentId,
          insight.range,
          insight.dateStart,
        );
        if (!existingByRange.has(rangeKey)) {
          existingByRange.set(rangeKey, insight);
        }
        if (!existingByRangeDate.has(dateKey)) {
          existingByRangeDate.set(dateKey, insight);
        }
      }

      const existingMaxIds = parentChunk
        .map((parent) => parent.insightMaxId)
        .filter(Boolean);
      const existingMaxInsights =
        existingMaxIds.length > 0
          ? await (this.prisma[prismaModel] as any).findMany({
              where: { id: { in: existingMaxIds } },
              select: { id: true, [relationFieldId]: true, dateStart: true },
            })
          : [];
      const existingMaxMap = new Map<string, any>();
      for (const insight of existingMaxInsights) {
        existingMaxMap.set(insight[relationFieldId], insight);
      }

      const rollupRecords: any[] = [];

      for (const parent of parentChunk) {
        const parentDaily = dailyMap.get(parent.id) || [];

        for (const window of windows) {
          const bucket: Record<string, number> = {};
          const matchingDaily = parentDaily.filter((insight) =>
            this.isInsightInWindow(insight, window),
          );

          if (window.range === InsightRange.MAX) {
            // Never write a zeroed sentinel MAX row when there is no DAILY to
            // sum: that placeholder ('1975-01-01') would permanently freeze MAX
            // at 0 via the shrink-protection guard. Leave the existing MAX as-is.
            if (matchingDaily.length === 0) continue;

            const existingMax = existingMaxMap.get(parent.id);
            if (this.shouldSkipMaxRollup(existingMax, matchingDaily)) {
              continue;
            }
          }

          for (const insight of matchingDaily) {
            this.sumMetrics(bucket, insight);
          }

          this.recalculateDerivedMetrics(bucket);

          const dateStart =
            window.range === InsightRange.MAX
              ? matchingDaily.map((insight) => insight.dateStart).sort()[0] ||
                MAX_SENTINEL_DATE
              : window.dateStart;

          const dateStop =
            window.range === InsightRange.MAX
              ? matchingDaily
                  .map((insight) => insight.dateStop || insight.dateStart)
                  .sort()
                  .slice(-1)[0] || window.dateStop
              : window.dateStop;
          const pointerField = this.getRangePointerField(window.range);
          const pointerId = pointerField ? parent[pointerField] : null;
          const exactExisting = existingByRangeDate.get(
            this.getRollupDateKey(parent.id, window.range, dateStart),
          );
          const fallbackExisting = existingByRange.get(
            this.getRollupKey(parent.id, window.range),
          );
          const targetId =
            exactExisting?.id ||
            (pointerId && existingRollupIds.has(pointerId)
              ? pointerId
              : fallbackExisting?.id);

          rollupRecords.push({
            _targetId: targetId,
            [relationFieldId]: parent.id,
            level: levelEnum,
            range: window.range,
            dateStart,
            dateStop,
            ...bucket,
          });
        }
      }

      await prismaHelper.upsertMany(
        rollupRecords,
        (item: any) => {
          const {
            _targetId,
            [relationFieldId]: rId,
            dateStart,
            range,
            ...data
          } = item;

          if (_targetId) {
            return (this.prisma[prismaModel] as any).update({
              where: { id: _targetId },
              data: {
                dateStart,
                range,
                ...data,
              },
            });
          }

          const createData = {
            [relationFieldId]: rId,
            dateStart,
            range,
            ...data,
          };

          return (this.prisma[prismaModel] as any).upsert({
            where: {
              [`${relationFieldId}_dateStart_range`]: {
                [relationFieldId]: rId,
                dateStart,
                range,
              },
            },
            update: data,
            create: createData,
          });
        },
        50,
      );

      if (rollupRecords.length === 0) continue;

      const pointerUpdates = await (this.prisma[prismaModel] as any).findMany({
        where: {
          OR: rollupRecords.map((record) => ({
            [relationFieldId]: record[relationFieldId],
            range: record.range,
            dateStart: record.dateStart,
          })),
        },
        select: { id: true, [relationFieldId]: true, range: true },
      });

      const updateMap = new Map<string, Record<string, string>>();
      for (const insight of pointerUpdates) {
        const field = this.getRangePointerField(insight.range);
        if (!field) continue;
        const parentId = insight[relationFieldId];
        if (!updateMap.has(parentId)) updateMap.set(parentId, {});
        updateMap.get(parentId)![field] = insight.id;
      }

      await prismaHelper.upsertMany(
        [...updateMap.entries()],
        ([id, data]) =>
          (this.prisma[parentModel] as any).update({
            where: { id },
            data,
          }),
        50,
      );

      const cleanupFilters = pointerUpdates
        .map((insight) => ({
          [relationFieldId]: insight[relationFieldId],
          range: insight.range,
          id: { not: insight.id },
        }))
        .filter((filter) => this.getRangePointerField(filter.range));

      for (const filterChunk of chunk(cleanupFilters, 100) as any[]) {
        await (this.prisma[prismaModel] as any).deleteMany({
          where: { OR: filterChunk },
        });
      }
    }

    this.logger.log(`[${accountId}] Finished ${level} local rollups.`);
  }

  private async syncLevelRange(
    accountId: string,
    level: InsightSyncLevel,
    range: InsightSyncRange,
  ) {
    this.logger.log(`[${accountId}] Syncing ${level} insights for ${range}...`);

    try {
      // 1. Fetch existing IDs and relations from Database first
      const parentModel = this.getParentModel(level);
      const { executeDbWithRetry } = require('../../common/utils');

      // Optimization: "Fill the Gaps" Strategy
      // 1. Always fetch for ACTIVE/IN_PROCESS entities to get latest numbers.
      // 2. Fetch for ANY entity (even PAUSED/ARCHIVED) if it's missing the insight record for this range.
      const parentInsightIdField = this.getInsightIdFieldOnParent(range);
      const where: any = {
        accountId,
        OR: [
          { status: { in: ['ACTIVE', 'IN_PROCESS'] } },
          {
            status: { in: ['PAUSED', 'ARCHIVED'] },
            updatedAt: { gte: dayjs().subtract(3, 'day').toDate() },
          },
        ],
      };

      // Nếu có field mapping tương ứng mới check NULL để "Fill the gaps"
      if (parentInsightIdField) {
        where.OR.push({ [parentInsightIdField]: null });
      }

      // Chỉ chọn id và trường ID liên kết cũ để check trạng thái tồn tại
      const selectFields: any = { id: true };
      if (parentInsightIdField) {
        selectFields[parentInsightIdField] = true;
      }

      const existingEntities = (await executeDbWithRetry(() =>
        (this.prisma[parentModel] as any).findMany({
          where,
          select: selectFields,
        }),
      )) as Array<{ id: string } & Record<string, string | null>>;

      const allIds = existingEntities.map((e) => e.id);
      if (allIds.length === 0) {
        this.logger.log(
          `[${accountId}] ⏭️ No ${level} found in DB for this range. Skipping.`,
        );
        return;
      }

      this.logger.log(
        `[${accountId}] 🔍 Found ${allIds.length} ${level} entities to sync for ${range}.`,
      );

      // Lưu trữ map id thực thể -> id insight cũ để tái sử dụng
      const entityRelationMap = new Map<string, string | null>();
      for (const ent of existingEntities) {
        entityRelationMap.set(
          ent.id,
          parentInsightIdField ? ent[parentInsightIdField] : null,
        );
      }

      // 2. Fetch from Meta in chunks to avoid filter limits
      const allInsights: any[] = [];
      const allDailyInsights: any[] = [];

      // Dynamic chunk size to avoid "Too many rows" error from Meta
      let chunkSize = 300;
      if (range === InsightSyncRange.MAX) {
        // MAXIMUM range is very heavy for Meta, use small chunks
        const maxMap = {
          [InsightSyncLevel.CAMPAIGN]: 50,
          [InsightSyncLevel.ADSET]: 50,
          [InsightSyncLevel.AD]: 50,
        };
        chunkSize = maxMap[level] || 100;
      } else {
        // Short-term ranges can handle larger chunks
        if (level === InsightSyncLevel.AD) chunkSize = 50;
        else if (level === InsightSyncLevel.ADSET) chunkSize = 300;
        else chunkSize = 500; // Campaign today/3d/7d is very light
      }

      const idChunks = chunk(allIds, chunkSize) as string[][];

      // Bounded concurrency helper to run requests in parallel safely
      const runWithLimit = async <T, R>(
        concurrency: number,
        items: T[],
        fn: (item: T, index: number) => Promise<R>,
      ): Promise<R[]> => {
        const results: Promise<R>[] = [];
        const executing: Promise<any>[] = [];
        for (let i = 0; i < items.length; i++) {
          const item = items[i];
          const p = Promise.resolve().then(() => fn(item, i));
          results.push(p);
          executing.push(p);
          const clean = () => {
            const idx = executing.indexOf(p);
            if (idx !== -1) executing.splice(idx, 1);
          };
          p.then(clean, clean);
          if (executing.length >= concurrency) {
            await Promise.race(executing);
          }
        }
        return Promise.all(results);
      };

      const chunkTasks = idChunks.map((idChunk, index) => {
        return async () => {
          this.logger.log(
            `[${accountId}] ⏳ Fetching ${level} insights: Chunk ${index + 1}/${idChunks.length}...`,
          );

          let insights: any[] = [];
          try {
            const res = await this.metaApi.getAccountInsights(accountId, {
              level: level as any,
              date_preset: range as string,
              ids: idChunk,
              limit: 50,
            });
            if (res) {
              this.logger.log(
                `[${accountId}] 📥 Received ${res.length} insights from Chunk ${index + 1}.`,
              );
              insights = res;
            }
          } catch (err: any) {
            this.logger.error(
              `[${accountId}] ❌ Error fetching insights in chunk ${index + 1}: ${err.message}`,
            );
            throw err;
          }

          let dailyInsights: any[] = [];
          if (range === InsightSyncRange.LAST_3D) {
            this.logger.log(
              `[${accountId}] ⏳ Fetching 3-day daily breakdowns for ${level}: Chunk ${index + 1}/${idChunks.length}...`,
            );
            try {
              const res = await this.metaApi.getAccountInsights(accountId, {
                level: level as any,
                date_preset: 'last_3d',
                time_increment: 1,
                ids: idChunk,
                limit: 50,
              });
              if (res && res.length > 0) {
                dailyInsights = res;
              }
            } catch (err: any) {
              this.logger.error(
                `[${accountId}] ❌ Error fetching 3-day daily breakdowns in chunk ${index + 1}: ${err.message}`,
              );
            }
          }

          return { insights, dailyInsights };
        };
      });

      // Chạy song song tối đa 3 chunks cùng lúc để tối ưu hóa tốc độ
      const chunkResults = await runWithLimit(3, chunkTasks, (task) => task());

      for (const res of chunkResults) {
        allInsights.push(...res.insights);
        allDailyInsights.push(...res.dailyInsights);
      }

      if (allInsights.length === 0) {
        this.logger.log(
          `[${accountId}] ℹ️ No ${level} insights returned from Meta for ${range}.`,
        );
        return;
      }

      const insights = allInsights;

      // 3. Transform and Map
      const prismaHelper = new PrismaBatchHelper(this.prisma);
      const levelEnum = this.mapLevelToEnum(level);
      const rangeEnum = this.mapRangeToEnum(range);
      const relationFieldId = this.getRelationField(level);
      const entityIdField = this.getEntityIdField(level);
      const prismaModel = this.getPrismaModel(level);

      this.logger.log(
        `[${accountId}] 🛠️ Mapping ${insights.length} insights for ${level} ${range}...`,
      );

      const toUpdate: any[] = [];
      const toCreate: any[] = [];

      const mappedData = insights
        .filter((i: any) => i[entityIdField])
        .map((i: any) => {
          const metrics = extractCampaignMetrics(i);
          const { toPrismaJson } = require('../../common/utils');
          const entityId = i[entityIdField];
          const existingInsightId = entityRelationMap.get(entityId);

          const record: any = {
            [relationFieldId]: entityId,
            level: levelEnum,
            range: rangeEnum,
            dateStart: i.date_start,
            dateStop: i.date_stop,
            ...metrics,
            rawPayload: toPrismaJson(i),
          };

          // Phân nhóm UPDATE hoặc CREATE để giảm tải cập nhật cha
          if (existingInsightId) {
            record.id = existingInsightId;
            toUpdate.push(record);
          } else {
            toCreate.push(record);
          }

          return record;
        });

      // Special handling for TODAY and LAST_3D
      let dailyData: any[] = [];
      if (range === InsightSyncRange.TODAY) {
        dailyData = mappedData.map((d) => {
          const { id, ...rest } = d;
          return {
            ...rest,
            range: 'DAILY',
          };
        });
      } else if (range === InsightSyncRange.LAST_3D) {
        if (allDailyInsights.length > 0) {
          dailyData = allDailyInsights
            .filter((i: any) => i[entityIdField])
            .map((i: any) => {
              const metrics = extractCampaignMetrics(i);
              const { toPrismaJson } = require('../../common/utils');
              return {
                [relationFieldId]: i[entityIdField],
                level: levelEnum,
                range: 'DAILY',
                dateStart: i.date_start,
                dateStop: i.date_stop,
                ...metrics,
                rawPayload: toPrismaJson(i),
              };
            });
        }
      }

      // 4. Batch Upsert to Database
      this.logger.log(
        `[${accountId}] 💾 Saving ${mappedData.length} summary records (Update: ${toUpdate.length}, Create: ${toCreate.length}) and ${dailyData.length} daily records to DB (${prismaModel})...`,
      );

      await executeDbWithRetry(async () => {
        // Cập nhật các bản ghi có ID liên kết cũ (Reuse ID)
        if (toUpdate.length > 0) {
          await prismaHelper.upsertMany(
            toUpdate,
            (item: any) => {
              const { id, ...data } = item;
              return (this.prisma[prismaModel] as any).update({
                where: { id },
                data,
              });
            },
            50,
          );
        }

        // Chèn mới các bản ghi chưa có ID liên kết
        if (toCreate.length > 0) {
          await prismaHelper.createManySafe(
            this.prisma[prismaModel] as any,
            toCreate,
            50,
          );
        }

        // Upsert trực tiếp dữ liệu DAILY để tránh việc delete-then-insert gây phình DB và mất atomicity
        if (dailyData.length > 0) {
          const uniqueKeyName = `${relationFieldId}_dateStart_range`;
          await prismaHelper.upsertMany(
            dailyData,
            (item: any) => {
              const {
                [relationFieldId]: rId,
                dateStart,
                range: rRange,
                ...data
              } = item;
              return (this.prisma[prismaModel] as any).upsert({
                where: {
                  [uniqueKeyName]: {
                    [relationFieldId]: rId,
                    dateStart,
                    range: rRange,
                  },
                },
                update: data,
                create: item,
              });
            },
            50,
          );
        }
      });

      // 5. Update parent record's insight relation ONLY for newly created insights (toCreate)
      if (toCreate.length > 0) {
        await this.updateParentRelations(level, range, toCreate);
      }

      this.logger.log(
        `[${accountId}] ✅ Finished ${level} ${range}: ${mappedData.length} records sync done.`,
      );
    } catch (error: unknown) {
      const {
        isPermissionError,
        parseMetaError,
      } = require('../../common/utils');
      const err = error as any;

      // Check for DB Error first (57P03 or message match)
      if (
        err?.code === '57P03' ||
        err?.message?.includes('not yet accepting connections')
      ) {
        this.logger.error(
          `[${accountId}] 🔥 Database Connection Failure: ${err.message}`,
        );
      } else if (isPermissionError(err)) {
        this.logger.warn(
          `[${accountId}] 🔑 Token expired or permission error. Marking account for reauth.`,
        );
        await this.prisma.account.update({
          where: { id: accountId },
          data: { needsReauth: true },
        });
      } else {
        // Handle Meta API or other errors
        const message = err?.response
          ? parseMetaError(err).message
          : (err?.message ?? 'Unknown error');
        this.logger.error(
          `[${accountId}] ❌ Error syncing ${level} ${range}: ${message}`,
        );
      }

      throw error; // Let BullMQ handle retry
    }
  }

  private mapLevelToEnum(level: InsightSyncLevel): LevelInsight {
    const map = {
      [InsightSyncLevel.CAMPAIGN]: LevelInsight.CAMPAIGN,
      [InsightSyncLevel.ADSET]: LevelInsight.ADSET,
      [InsightSyncLevel.AD]: LevelInsight.AD,
    };
    return map[level];
  }

  private mapRangeToEnum(range: InsightSyncRange): InsightRange {
    const map = {
      [InsightSyncRange.TODAY]: InsightRange.TODAY,
      [InsightSyncRange.LAST_3D]: InsightRange.DAY_3,
      [InsightSyncRange.LAST_7D]: InsightRange.DAY_7,
      [InsightSyncRange.MAX]: InsightRange.MAX,
    };
    return map[range];
  }

  private getRelationField(level: InsightSyncLevel): string {
    const map = {
      [InsightSyncLevel.CAMPAIGN]: 'campaignId',
      [InsightSyncLevel.ADSET]: 'adSetId',
      [InsightSyncLevel.AD]: 'adId',
    };
    return map[level];
  }

  private getEntityIdField(level: InsightSyncLevel): string {
    const map = {
      [InsightSyncLevel.CAMPAIGN]: 'campaign_id',
      [InsightSyncLevel.ADSET]: 'adset_id',
      [InsightSyncLevel.AD]: 'ad_id',
    };
    return map[level];
  }

  private getPrismaModel(level: InsightSyncLevel): string {
    const map = {
      [InsightSyncLevel.CAMPAIGN]: 'campaignInsight',
      [InsightSyncLevel.ADSET]: 'adSetInsight',
      [InsightSyncLevel.AD]: 'adInsight',
    };
    return map[level];
  }

  private getParentModel(level: InsightSyncLevel): string {
    const map = {
      [InsightSyncLevel.CAMPAIGN]: 'campaign',
      [InsightSyncLevel.ADSET]: 'adSet',
      [InsightSyncLevel.AD]: 'ad',
    };
    return map[level];
  }

  private getInsightIdFieldOnParent(range: InsightSyncRange): string {
    const map = {
      [InsightSyncRange.TODAY]: 'insightTodayId',
      [InsightSyncRange.LAST_3D]: 'insight3dId',
      [InsightSyncRange.LAST_7D]: 'insight7dId',
      [InsightSyncRange.MAX]: 'insightMaxId',
    };
    return map[range];
  }

  private async updateParentRelations(
    level: InsightSyncLevel,
    range: InsightSyncRange,
    mappedData: Array<Record<string, any>>,
  ) {
    const parentModel = this.getParentModel(level);
    const parentInsightIdField = this.getInsightIdFieldOnParent(range);

    // Nếu không có field tương ứng trên model cha (ví dụ range DAILY) thì bỏ qua việc update relation
    if (!parentInsightIdField) return;

    const relationFieldId = this.getRelationField(level);

    // After createManySafe, we need to fetch the IDs of the newly created insights
    // to link them back to the parent Campaigns/AdSets/Ads
    const entityIds = mappedData.map((d) => d[relationFieldId]);
    const prismaModel = this.getPrismaModel(level);

    const createdInsights = (await (this.prisma[prismaModel] as any).findMany({
      where: {
        [relationFieldId]: { in: entityIds },
        range: this.mapRangeToEnum(range),
      },
      select: { id: true, [relationFieldId]: true },
    })) as Array<Record<string, any> & { id: string }>;

    // Update in chunks
    for (const insightChunk of chunk(createdInsights, 100)) {
      await Promise.all(
        insightChunk.map(async (insight) => {
          try {
            await (this.prisma[parentModel] as any).update({
              where: { id: insight[relationFieldId] },
              data: { [parentInsightIdField]: insight.id },
            });
          } catch (err: any) {
            if (
              err instanceof Prisma.PrismaClientKnownRequestError &&
              err.code === 'P2003'
            ) {
              this.logger.warn(
                `[updateParentRelations] ⚠️ Foreign key constraint violation on ${parentModel} ${insight[relationFieldId]} update. Skipping...`,
              );
            } else {
              throw err;
            }
          }
        }),
      );
    }
  }

  async aggregateCreativeInsights(
    accountId: string,
    ranges: InsightSyncRange[] = [
      InsightSyncRange.TODAY,
      InsightSyncRange.LAST_3D,
      InsightSyncRange.LAST_7D,
      InsightSyncRange.MAX,
    ],
    tz: string = DEFAULT_ACCOUNT_TZ,
  ) {
    this.logger.log(`[${accountId}] Starting CreativeInsight local rollup...`);
    const start = Date.now();
    const prismaHelper = new PrismaBatchHelper(this.prisma);
    const windows = this.getRequestedWindows(ranges, tz);

    if (windows.length === 0) return;

    const creatives = await this.prisma.creative.findMany({
      where: { accountId },
      select: {
        id: true,
        insightTodayId: true,
        insight3dId: true,
        insight7dId: true,
        insightMaxId: true,
        ads: { select: { id: true } },
      },
    });

    if (creatives.length === 0) {
      this.logger.log(`[${accountId}] No creatives found to aggregate.`);
      return;
    }

    const minWindowDate = windows
      .filter((window) => window.range !== InsightRange.MAX)
      .map((window) => window.dateStart!)
      .sort()[0];

    for (const creativeChunk of chunk(creatives, 100) as Array<
      typeof creatives
    >) {
      const adIds = creativeChunk.flatMap((creative) =>
        creative.ads.map((ad) => ad.id),
      );

      const hasMax = windows.some(
        (window) => window.range === InsightRange.MAX,
      );
      const adInsightWhere: any = {
        adId: { in: adIds.length > 0 ? adIds : ['__NO_AD__'] },
        range: InsightRange.DAILY,
      };

      if (!hasMax && minWindowDate) {
        adInsightWhere.dateStart = { gte: minWindowDate };
      }

      const adInsights = await this.prisma.adInsight.findMany({
        where: adInsightWhere,
      });

      const adInsightMap = new Map<string, any[]>();
      for (const insight of adInsights) {
        if (!adInsightMap.has(insight.adId)) adInsightMap.set(insight.adId, []);
        adInsightMap.get(insight.adId)!.push(insight);
      }

      const creativeIds = creativeChunk.map((creative) => creative.id);
      const rollupRanges = [...new Set(windows.map((window) => window.range))];
      const existingRollups = await this.prisma.creativeInsight.findMany({
        where: {
          creativeId: { in: creativeIds },
          range: { in: rollupRanges },
        },
        select: {
          id: true,
          creativeId: true,
          range: true,
          dateStart: true,
          updatedAt: true,
        },
        orderBy: [{ updatedAt: 'desc' }],
      });
      const existingRollupIds = new Set(
        existingRollups.map((insight) => insight.id),
      );
      const existingByRange = new Map<string, any>();
      const existingByRangeDate = new Map<string, any>();
      for (const insight of existingRollups) {
        const rangeKey = this.getRollupKey(insight.creativeId, insight.range);
        const dateKey = this.getRollupDateKey(
          insight.creativeId,
          insight.range,
          insight.dateStart,
        );
        if (!existingByRange.has(rangeKey)) {
          existingByRange.set(rangeKey, insight);
        }
        if (!existingByRangeDate.has(dateKey)) {
          existingByRangeDate.set(dateKey, insight);
        }
      }

      const existingMaxIds = creativeChunk
        .map((creative) => creative.insightMaxId)
        .filter(Boolean);
      const existingMaxInsights =
        existingMaxIds.length > 0
          ? await this.prisma.creativeInsight.findMany({
              where: { id: { in: existingMaxIds } },
              select: { id: true, creativeId: true, dateStart: true },
            })
          : [];
      const existingMaxMap = new Map<string, any>();
      for (const insight of existingMaxInsights) {
        existingMaxMap.set(insight.creativeId, insight);
      }

      const rollupRecords: any[] = [];
      const creativeUpdates: Array<{ id: string; data: any }> = [];

      for (const creative of creativeChunk) {
        const creativeDaily = creative.ads.flatMap(
          (ad) => adInsightMap.get(ad.id) || [],
        );
        const rangeBuckets = new Map<InsightRange, Record<string, number>>();

        for (const window of windows) {
          const bucket: Record<string, number> = {};
          const matchingDaily = creativeDaily.filter((insight) =>
            this.isInsightInWindow(insight, window),
          );

          if (window.range === InsightRange.MAX) {
            // Don't write a zeroed sentinel MAX row when there is no DAILY to
            // sum (it would permanently freeze creative MAX at 0).
            if (matchingDaily.length === 0) continue;

            const existingMax = existingMaxMap.get(creative.id);
            if (this.shouldSkipMaxRollup(existingMax, matchingDaily)) {
              continue;
            }
          }

          for (const insight of matchingDaily) {
            this.sumMetrics(bucket, insight);
          }

          this.recalculateDerivedMetrics(bucket);
          rangeBuckets.set(window.range, bucket);

          const dateStart =
            window.range === InsightRange.MAX
              ? matchingDaily.map((insight) => insight.dateStart).sort()[0] ||
                MAX_SENTINEL_DATE
              : window.dateStart;

          const dateStop =
            window.range === InsightRange.MAX
              ? matchingDaily
                  .map((insight) => insight.dateStop || insight.dateStart)
                  .sort()
                  .slice(-1)[0] || window.dateStop
              : window.dateStop;
          const pointerField = this.getRangePointerField(window.range);
          const pointerId = pointerField
            ? (creative as any)[pointerField]
            : null;
          const exactExisting = existingByRangeDate.get(
            this.getRollupDateKey(creative.id, window.range, dateStart),
          );
          const fallbackExisting = existingByRange.get(
            this.getRollupKey(creative.id, window.range),
          );
          const targetId =
            exactExisting?.id ||
            (pointerId && existingRollupIds.has(pointerId)
              ? pointerId
              : fallbackExisting?.id);

          rollupRecords.push({
            _targetId: targetId,
            creativeId: creative.id,
            level: LevelInsight.AD,
            range: window.range,
            dateStart,
            dateStop,
            ...bucket,
          });
        }

        // Materialize per-day CreativeInsight rows (range=DAILY) by summing the
        // creative's ads' DAILY ad-insights per date. The batch rollup never
        // wrote these, so the creative detail DAILY chart was always empty.
        // Upserted by the unique (creativeId, dateStart, range) key; DAILY rows
        // carry no pointer field, so the cleanup step below never touches them.
        const creativeDailyBuckets = new Map<
          string,
          { dateStop: string; data: Record<string, number> }
        >();
        for (const insight of creativeDaily) {
          if (!insight.dateStart) continue;
          if (!creativeDailyBuckets.has(insight.dateStart)) {
            creativeDailyBuckets.set(insight.dateStart, {
              dateStop: insight.dateStop || insight.dateStart,
              data: {},
            });
          }
          const current = creativeDailyBuckets.get(insight.dateStart)!;
          const stop = insight.dateStop || insight.dateStart;
          if (stop > current.dateStop) current.dateStop = stop;
          this.sumMetrics(current.data, insight);
        }
        for (const [dateStart, dailyBucket] of creativeDailyBuckets.entries()) {
          this.recalculateDerivedMetrics(dailyBucket.data);
          rollupRecords.push({
            creativeId: creative.id,
            level: LevelInsight.AD,
            range: InsightRange.DAILY,
            dateStart,
            dateStop: dailyBucket.dateStop,
            ...dailyBucket.data,
          });
        }

        const maxBucket = rangeBuckets.get(InsightRange.MAX) || {};
        const day7Bucket = rangeBuckets.get(InsightRange.DAY_7) || {};
        const day3Bucket = rangeBuckets.get(InsightRange.DAY_3) || {};

        if (rangeBuckets.has(InsightRange.MAX)) {
          const maxSpend = maxBucket.spend ?? 0;
          const maxRevenue = maxBucket.purchaseValue ?? 0;
          const maxPurchases = maxBucket.purchases ?? 0;
          const maxClicks = maxBucket.clicks ?? 0;
          const maxImpressions = maxBucket.impressions ?? 0;
          const roasMax = maxSpend > 0 ? maxRevenue / maxSpend : 0;
          const ctrMax = maxImpressions > 0 ? maxClicks / maxImpressions : 0;
          const roas7d =
            (day7Bucket.spend ?? 0) > 0
              ? (day7Bucket.purchaseValue ?? 0) / day7Bucket.spend
              : 0;
          const roas3d =
            (day3Bucket.spend ?? 0) > 0
              ? (day3Bucket.purchaseValue ?? 0) / day3Bucket.spend
              : 0;

          let status: CreativeStatus = CreativeStatus.OTHER;
          if (maxSpend === 0) status = CreativeStatus.OTHER;
          else if (maxSpend <= 100000) status = CreativeStatus.NEED_SPEND;
          else if (
            ((maxSpend <= 500000 && roasMax >= 2) ||
              (maxSpend > 500000 && roasMax >= 2.2)) &&
            roas7d >= 2.5
          ) {
            status = CreativeStatus.SCALE_P1;
          } else if (
            ((maxSpend <= 500000 && roasMax >= 1.5) ||
              (maxSpend > 500000 && roasMax >= 1.8 && ctrMax > 0.03)) &&
            roas7d >= 2.2 &&
            roas3d >= 2.2
          ) {
            status = CreativeStatus.SCALE_P2;
          } else if (
            (maxSpend <= 500000 && maxPurchases < 1 && ctrMax > 0.03) ||
            (maxSpend > 500000 && roasMax < 1.8 && ctrMax > 0.03)
          ) {
            status = CreativeStatus.REVIEW;
          } else if (
            (maxSpend <= 500000 && maxPurchases < 1 && ctrMax < 0.03) ||
            (maxSpend > 500000 && roasMax < 1.8 && ctrMax < 0.03)
          ) {
            status = CreativeStatus.OFF;
          }

          creativeUpdates.push({
            id: creative.id,
            data: { performanceStatus: status, ...maxBucket },
          });
        }
      }

      await prismaHelper.upsertMany(
        rollupRecords,
        (item: any) => {
          const { _targetId, creativeId, dateStart, range, ...data } = item;

          if (_targetId) {
            return this.prisma.creativeInsight.update({
              where: { id: _targetId },
              data: {
                dateStart,
                range,
                ...data,
              },
            });
          }

          const createData = {
            creativeId,
            dateStart,
            range,
            ...data,
          };

          return this.prisma.creativeInsight.upsert({
            where: {
              creativeId_dateStart_range: {
                creativeId,
                dateStart,
                range,
              },
            },
            update: data,
            create: createData,
          });
        },
        50,
      );

      if (rollupRecords.length === 0) continue;

      // Only the pointer-backed ranges (today/3d/7d/max) need their ids resolved
      // to update the Creative pointer fields. Exclude the many DAILY rows added
      // above so this OR query stays small.
      const pointerRollupRecords = rollupRecords.filter(
        (record) => record.range !== InsightRange.DAILY,
      );

      const pointerRecords =
        pointerRollupRecords.length > 0
          ? await this.prisma.creativeInsight.findMany({
              where: {
                OR: pointerRollupRecords.map((record) => ({
                  creativeId: record.creativeId,
                  range: record.range,
                  dateStart: record.dateStart,
                })),
              },
              select: { id: true, creativeId: true, range: true },
            })
          : [];

      const pointerMap = new Map<string, Record<string, string>>();
      for (const insight of pointerRecords) {
        const field = this.getRangePointerField(insight.range);
        if (!field) continue;
        if (!pointerMap.has(insight.creativeId))
          pointerMap.set(insight.creativeId, {});
        pointerMap.get(insight.creativeId)![field] = insight.id;
      }

      await prismaHelper.upsertMany(
        [...pointerMap.entries()],
        ([id, data]) =>
          this.prisma.creative.update({
            where: { id },
            data,
          }),
        50,
      );

      const cleanupFilters = pointerRecords
        .map((insight) => ({
          creativeId: insight.creativeId,
          range: insight.range,
          id: { not: insight.id },
        }))
        .filter((filter) => this.getRangePointerField(filter.range));

      for (const filterChunk of chunk(cleanupFilters, 100) as any[]) {
        await this.prisma.creativeInsight.deleteMany({
          where: { OR: filterChunk },
        });
      }

      if (creativeUpdates.length > 0) {
        await prismaHelper.upsertMany(
          creativeUpdates,
          (item) =>
            this.prisma.creative.update({
              where: { id: item.id },
              data: item.data,
            }),
          50,
        );
      }
    }

    const duration = ((Date.now() - start) / 1000).toFixed(2);
    this.logger.log(
      `[${accountId}] CreativeInsight local rollup done in ${duration}s.`,
    );
  }

  /**
   * Syncs Audience Insights (Age/Gender breakdowns) for AdSets.
   */
  async syncAccountAudienceInsights(accountId: string) {
    this.logger.log(
      `[${accountId}] 👥 Starting Audience Insight sync (Age/Gender)...`,
    );
    const start = Date.now();

    try {
      const { executeDbWithRetry } = require('../../common/utils');

      // 1. Fetch AdSets that need sync
      // For Audience, we usually focus on active ones or ones missing audience data
      const adSets = (await executeDbWithRetry(() =>
        this.prisma.adSet.findMany({
          where: {
            accountId,
            status: { in: ['ACTIVE', 'IN_PROCESS'] },
          },
          select: { id: true },
        }),
      )) as Array<{ id: string }>;

      const adSetIds = adSets.map((a) => a.id);
      if (adSetIds.length === 0) {
        this.logger.log(
          `[${accountId}] No active AdSets found for Audience sync.`,
        );
        return;
      }

      this.logger.log(
        `[${accountId}] 🔍 Found ${adSetIds.length} AdSets for Audience sync.`,
      );

      const allInsights: any[] = [];
      const idChunks = chunk(adSetIds, 50) as string[][]; // Small chunks for breakdowns

      for (let i = 0; i < idChunks.length; i++) {
        const idChunk = idChunks[i];
        this.logger.log(
          `[${accountId}] ⏳ Fetching Audience: Chunk ${i + 1}/${idChunks.length}...`,
        );

        const insights = await this.metaApi.getAccountInsights(accountId, {
          level: 'adset' as any,
          date_preset: 'maximum',
          ids: idChunk,
          breakdowns: ['age', 'gender'],
          limit: 50,
        });

        if (insights) {
          this.logger.log(
            `[${accountId}] 📥 Received ${insights.length} breakdown records from Chunk ${i + 1}.`,
          );
          allInsights.push(...insights);
        }
      }

      if (allInsights.length === 0) {
        this.logger.log(
          `[${accountId}] ℹ️ No Audience insights returned from Meta.`,
        );
        return;
      }

      // 2. Transform and Batch Upsert
      const prismaHelper = new PrismaBatchHelper(this.prisma);
      const mappedData = allInsights
        .filter((i) => i.adset_id && i.age && i.gender)
        .map((i) => {
          const metrics = extractCampaignMetrics(i);
          return {
            adsetId: i.adset_id,
            age: i.age,
            gender: i.gender,
            level: LevelInsight.ADSET,
            range: InsightRange.MAX,
            dateStart: i.date_start,
            dateStop: i.date_stop,
            ...metrics,
          };
        });

      this.logger.log(
        `[${accountId}] 💾 Saving ${mappedData.length} Audience records to DB...`,
      );

      await executeDbWithRetry(async () => {
        // We delete all existing AdsetAudienceInsight for these adsets before inserting new ones
        // since audience data is usually an aggregate for MAX range.
        await this.prisma.adsetAudienceInsight.deleteMany({
          where: {
            adsetId: { in: adSetIds },
            range: InsightRange.MAX,
          },
        });

        await prismaHelper.createManySafe(
          this.prisma.adsetAudienceInsight,
          mappedData,
          100,
        );
      });

      const duration = ((Date.now() - start) / 1000).toFixed(2);
      this.logger.log(
        `[${accountId}] ✅ Finished Audience Insight sync in ${duration}s.`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(
        `[${accountId}] ❌ Error syncing Audience insights: ${message}`,
      );
      throw error;
    }
  }

  /**
   * Identifies and syncs missing daily insights for all entities in an account.
   * Useful for backfilling data or fixing gaps.
   * Daily cron is bounded to the recent attribution window to avoid expensive
   * lifetime "maximum" fetches. Historical full backfill should be a manual job.
   */
  async syncAccountMissingDailyInsights(accountId: string) {
    this.logger.log(
      `[${accountId}] 🔍 Starting bounded Missing Daily Insights sync...`,
    );
    const start = Date.now();

    const levels = [
      InsightSyncLevel.CAMPAIGN,
      InsightSyncLevel.ADSET,
      InsightSyncLevel.AD,
    ];

    const tz = await this.getAccountTimezone(accountId);
    let chunkFailures = 0;

    try {
      for (const level of levels) {
        const parentModel = this.getParentModel(level);
        const relationFieldId = this.getRelationField(level);
        const entityIdField = this.getEntityIdField(level);
        const prismaModel = this.getPrismaModel(level);

        const todayStr = this.nowInTz(tz).format('YYYY-MM-DD');
        const backfillSince = this.nowInTz(tz)
          .subtract(30, 'day')
          .format('YYYY-MM-DD');

        // 1. Fetch all relevant entities for recent bounded backfill.
        const selectFields: Record<string, boolean> = {
          id: true,
          name: true,
          createdAt: true,
        };
        if (level !== InsightSyncLevel.AD) {
          selectFields.startTime = true;
        }

        const entities = await (this.prisma[parentModel] as any).findMany({
          where: {
            accountId,
            status: { in: ['ACTIVE', 'IN_PROCESS', 'PAUSED', 'ARCHIVED'] },
          },
          select: selectFields,
        });

        if (entities.length === 0) continue;

        this.logger.log(
          `[${accountId}] Checking ${entities.length} ${level}s for missing days since ${backfillSince}.`,
        );

        // 2. Fetch existing recent DAILY insights in DB for these entities in bulk.
        const existingInsights = await (
          this.prisma[prismaModel] as any
        ).findMany({
          where: {
            [relationFieldId]: { in: entities.map((e) => e.id) },
            range: 'DAILY',
            dateStart: { gte: backfillSince },
          },
          select: { [relationFieldId]: true, dateStart: true },
        });

        // Group existing dates by entityId
        const existingDatesMap = new Map<string, Set<string>>();
        for (const ins of existingInsights) {
          const entId = ins[relationFieldId];
          if (!existingDatesMap.has(entId)) {
            existingDatesMap.set(entId, new Set());
          }
          existingDatesMap.get(entId)!.add(ins.dateStart);
        }

        // 3. Find entities with recent gaps and aggregate their missing dates.
        const entitiesToSync: Array<{
          id: string;
          minDate: string;
          maxDate: string;
        }> = [];
        for (const entity of entities) {
          const entityStart = dayjs(entity.startTime || entity.createdAt);
          const startDate = entityStart.isAfter(dayjs(backfillSince))
            ? entityStart.format('YYYY-MM-DD')
            : backfillSince;
          const endDate = todayStr;
          const existingDates = existingDatesMap.get(entity.id) || new Set();

          const missingDates: string[] = [];
          let current = dayjs(startDate);
          const end = dayjs(endDate);

          while (current.isBefore(end) || current.isSame(end)) {
            const dateStr = current.format('YYYY-MM-DD');
            if (!existingDates.has(dateStr)) {
              missingDates.push(dateStr);
            }
            current = current.add(1, 'day');
          }

          if (missingDates.length > 0) {
            entitiesToSync.push({
              id: entity.id,
              minDate: missingDates[0],
              maxDate: missingDates[missingDates.length - 1],
            });
          }
        }

        if (entitiesToSync.length === 0) {
          this.logger.log(
            `[${accountId}] No missing DAILY records found for ${level}.`,
          );
          continue;
        }

        this.logger.log(
          `[${accountId}] 🔍 Found ${entitiesToSync.length}/${entities.length} ${level}s with missing days. Syncing in chunks...`,
        );

        // 4. Fetch from Meta in chunks (bulk fetch)
        const entityChunks = chunk(
          entitiesToSync,
          this.getDefaultInsightChunkSize(level),
        ) as (typeof entitiesToSync)[];

        for (let i = 0; i < entityChunks.length; i++) {
          const entityChunk = entityChunks[i];
          const chunkIds = entityChunk.map((e) => e.id);

          this.logger.log(
            `[${accountId}] ⏳ Fetching missing daily for ${level}: Chunk ${i + 1}/${entityChunks.length}...`,
          );

          try {
            const chunkSince = entityChunk
              .map((entity) => entity.minDate)
              .sort()[0];
            const chunkUntil = entityChunk
              .map((entity) => entity.maxDate)
              .sort()
              .slice(-1)[0];

            const metaInsights = await this.fetchDailyInsightsAdaptive(
              accountId,
              level,
              chunkIds,
              { since: chunkSince, until: chunkUntil },
            );

            if (metaInsights && metaInsights.length > 0) {
              const mappedData = metaInsights
                .filter((mi: any) => mi[entityIdField])
                .map((mi: any) => {
                  const {
                    extractCampaignMetrics,
                    toPrismaJson,
                  } = require('../../common/utils');
                  const metrics = extractCampaignMetrics(mi);

                  return {
                    [relationFieldId]: mi[entityIdField],
                    level: this.mapLevelToEnum(level),
                    range: 'DAILY',
                    dateStart: mi.date_start,
                    dateStop: mi.date_stop,
                    ...metrics,
                    rawPayload: toPrismaJson(mi),
                  };
                });

              // Filter only records that are actually missing or recent (last 3 days to resolve attribution lag)
              const threeDaysAgoStr = this.nowInTz(tz)
                .subtract(2, 'day')
                .format('YYYY-MM-DD');

              const filteredData = mappedData.filter((item) => {
                const entId = item[relationFieldId];
                const existingDates = existingDatesMap.get(entId) || new Set();

                const isMissing = !existingDates.has(item.dateStart);
                const isRecent =
                  item.dateStart >= threeDaysAgoStr &&
                  item.dateStart <= todayStr;

                return isMissing || isRecent;
              });

              if (filteredData.length > 0) {
                const prismaHelper = new PrismaBatchHelper(this.prisma);

                await executeDbWithRetry(async () => {
                  await prismaHelper.upsertMany(filteredData, (item: any) => {
                    const {
                      [relationFieldId]: rId,
                      dateStart,
                      range,
                      ...data
                    } = item;
                    return (this.prisma[prismaModel] as any).upsert({
                      where: {
                        [`${relationFieldId}_dateStart_range`]: {
                          [relationFieldId]: rId,
                          dateStart,
                          range,
                        },
                      },
                      update: data,
                      create: item,
                    });
                  });
                });

                this.logger.log(
                  `[${accountId}] ✅ Chunk ${i + 1}/${entityChunks.length}: Saved ${filteredData.length} daily records for ${level}.`,
                );
              }
            }
          } catch (err: any) {
            // Permission/token problems affect everything -> bubble up so the
            // outer handler can flag needsReauth. Transient chunk failures are
            // isolated but counted so we can fail the job at the end (engaging
            // BullMQ retry) instead of reporting "finished" on partial data.
            if (isPermissionError(err)) throw err;
            chunkFailures++;
            this.logger.error(
              `[${accountId}] ❌ Error in missing daily chunk ${i + 1}: ${err.message}`,
            );
          }
        }
      }

      const rollupRanges = [
        InsightSyncRange.TODAY,
        InsightSyncRange.LAST_3D,
        InsightSyncRange.LAST_7D,
        InsightSyncRange.MAX,
      ];
      for (const level of levels) {
        await this.rollupLevelInsights(accountId, level, rollupRanges, tz);
      }
      await this.aggregateCreativeInsights(accountId, rollupRanges, tz);

      const duration = ((Date.now() - start) / 1000).toFixed(2);
      this.logger.log(
        `[${accountId}] ✨ Missing Daily Insights sync finished in ${duration}s${chunkFailures > 0 ? ` (with ${chunkFailures} failed chunks)` : ''}.`,
      );

      // Roll-ups ran on whatever we fetched, but surface the partial fetch so
      // BullMQ retries and fills the remaining gaps next attempt.
      if (chunkFailures > 0) {
        throw new Error(
          `Missing daily sync completed with ${chunkFailures} failed chunk(s); retrying to fill gaps.`,
        );
      }
    } catch (error) {
      if (isPermissionError(error)) {
        this.logger.warn(
          `[${accountId}] 🔑 Permission error during missing daily sync. Marking needsReauth.`,
        );
        await executeDbWithRetry(() =>
          this.prisma.account.update({
            where: { id: accountId },
            data: { needsReauth: true },
          }),
        );
        return;
      }
      this.logger.error(
        `[${accountId}] ❌ Error syncing missing daily insights: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }

  async slideInactiveInsights() {
    this.logger.log(
      '🚀 Starting local sliding window aggregation for inactive entities...',
    );
    const start = Date.now();

    const shortRanges = [
      InsightSyncRange.TODAY,
      InsightSyncRange.LAST_3D,
      InsightSyncRange.LAST_7D,
    ];
    const rollupAccounts = await this.prisma.account.findMany({
      where: { needsReauth: false, accountType: 'AD_ACCOUNT' as any },
      select: { id: true },
    });

    for (const account of rollupAccounts) {
      const tz = await this.getAccountTimezone(account.id);
      for (const level of [
        InsightSyncLevel.CAMPAIGN,
        InsightSyncLevel.ADSET,
        InsightSyncLevel.AD,
      ]) {
        await this.rollupLevelInsights(account.id, level, shortRanges, tz);
      }
      await this.aggregateCreativeInsights(account.id, shortRanges, tz);
    }

    const rollupDuration = ((Date.now() - start) / 1000).toFixed(2);
    this.logger.log(
      `✅ Finished local sliding window rollup job in ${rollupDuration}s.`,
    );
  }

  /**
   * 🟣 LIFETIME DAILY BACKFILL (gradual)
   *
   * Fills the gap between an entity's start (campaign/adset startTime, ad
   * createdAt) and its earliest DAILY row in the DB, so MAX reflects true
   * lifetime spend instead of only the recent 7/30-day window. Designed to run
   * as a low-frequency cron that processes a BOUNDED number of entities per run
   * (INSIGHT_LIFETIME_BACKFILL_ENTITIES_PER_RUN), so the historical fill spreads
   * over several runs without spiking the Meta API quota.
   *
   * Convergence without a schema watermark: after backfilling an entity we make
   * sure a DAILY row exists at its floor date (a single synthesized zero row if
   * Meta returned nothing for that day). Once a row exists at-or-before the
   * floor, the entity no longer shows a gap and is skipped on subsequent runs —
   * so it never re-fetches its whole lifetime.
   */
  async backfillLifetimeDailyInsights(accountId: string) {
    this.logger.log(
      `[${accountId}] 🟣 Starting gradual lifetime DAILY backfill...`,
    );
    const start = Date.now();

    const tz = await this.getAccountTimezone(accountId);
    const todayStr = this.nowInTz(tz).format('YYYY-MM-DD');
    const maxEntitiesPerRun = Number(
      process.env.INSIGHT_LIFETIME_BACKFILL_ENTITIES_PER_RUN || 50,
    );
    const subWindowDays = Number(
      process.env.INSIGHT_LIFETIME_BACKFILL_WINDOW_DAYS || 90,
    );

    const levels = [
      InsightSyncLevel.CAMPAIGN,
      InsightSyncLevel.ADSET,
      InsightSyncLevel.AD,
    ];
    const touchedLevels: InsightSyncLevel[] = [];

    try {
      for (const level of levels) {
        const parentModel = this.getParentModel(level);
        const relationFieldId = this.getRelationField(level);
        const entityIdField = this.getEntityIdField(level);
        const prismaModel = this.getPrismaModel(level);
        const levelEnum = this.mapLevelToEnum(level);
        const prismaHelper = new PrismaBatchHelper(this.prisma);

        const selectFields: Record<string, boolean> = {
          id: true,
          createdAt: true,
        };
        if (level !== InsightSyncLevel.AD) selectFields.startTime = true;

        const entities = (await executeDbWithRetry(() =>
          (this.prisma[parentModel] as any).findMany({
            where: {
              accountId,
              status: { in: ['ACTIVE', 'IN_PROCESS', 'PAUSED', 'ARCHIVED'] },
            },
            select: selectFields,
          }),
        )) as Array<{ id: string; createdAt: Date; startTime?: Date | null }>;

        if (entities.length === 0) continue;

        // Earliest existing DAILY date per entity (single grouped query).
        const grouped = (await (this.prisma[prismaModel] as any).groupBy({
          by: [relationFieldId],
          where: {
            [relationFieldId]: { in: entities.map((e) => e.id) },
            range: InsightRange.DAILY,
          },
          _min: { dateStart: true },
        })) as Array<Record<string, any>>;

        const earliestMap = new Map<string, string>();
        for (const g of grouped) {
          if (g._min?.dateStart) earliestMap.set(g[relationFieldId], g._min.dateStart);
        }

        // Entities whose lifetime is not yet covered: no DAILY at all, or the
        // earliest DAILY starts after the entity's floor date.
        const gapEntities: Array<{ id: string; floor: string; until: string }> =
          [];
        for (const entity of entities) {
          const floorDate = dayjs(entity.startTime || entity.createdAt).format(
            'YYYY-MM-DD',
          );
          if (floorDate > todayStr) continue;

          const earliest = earliestMap.get(entity.id);
          if (!earliest) {
            gapEntities.push({ id: entity.id, floor: floorDate, until: todayStr });
          } else if (earliest > floorDate) {
            const until = dayjs(earliest)
              .subtract(1, 'day')
              .format('YYYY-MM-DD');
            gapEntities.push({ id: entity.id, floor: floorDate, until });
          }

          if (gapEntities.length >= maxEntitiesPerRun) break;
        }

        if (gapEntities.length === 0) {
          this.logger.log(
            `[${accountId}] ${level}: no lifetime gaps to backfill.`,
          );
          continue;
        }

        touchedLevels.push(level);
        this.logger.log(
          `[${accountId}] ${level}: backfilling ${gapEntities.length} entities (bounded ${maxEntitiesPerRun}/run).`,
        );

        for (const ent of gapEntities) {
          // Build sequential sub-windows [floor .. until] of subWindowDays each.
          const segments: Array<{ since: string; until: string }> = [];
          let cursor = ent.floor;
          while (cursor <= ent.until) {
            let segUntil = dayjs(cursor)
              .add(subWindowDays - 1, 'day')
              .format('YYYY-MM-DD');
            if (segUntil > ent.until) segUntil = ent.until;
            segments.push({ since: cursor, until: segUntil });
            cursor = dayjs(segUntil).add(1, 'day').format('YYYY-MM-DD');
          }

          let hasRowAtFloor = false;
          for (const seg of segments) {
            try {
              const insights = await this.fetchDailyInsightsAdaptive(
                accountId,
                level,
                [ent.id],
                { since: seg.since, until: seg.until },
              );

              const dailyData = (insights || [])
                .filter((insight: any) => insight[entityIdField])
                .map((insight: any) => {
                  const metrics = extractCampaignMetrics(insight);
                  const { toPrismaJson } = require('../../common/utils');
                  return {
                    [relationFieldId]: insight[entityIdField],
                    level: levelEnum,
                    range: InsightRange.DAILY,
                    dateStart: insight.date_start,
                    dateStop: insight.date_stop,
                    ...metrics,
                    rawPayload: toPrismaJson(insight),
                  };
                });

              if (dailyData.some((d) => d.dateStart <= ent.floor)) {
                hasRowAtFloor = true;
              }

              if (dailyData.length > 0) {
                await executeDbWithRetry(async () => {
                  await prismaHelper.upsertMany(
                    dailyData,
                    (item: any) => {
                      const {
                        [relationFieldId]: rId,
                        dateStart,
                        range,
                        ...data
                      } = item;
                      return (this.prisma[prismaModel] as any).upsert({
                        where: {
                          [`${relationFieldId}_dateStart_range`]: {
                            [relationFieldId]: rId,
                            dateStart,
                            range,
                          },
                        },
                        update: data,
                        create: item,
                      });
                    },
                    50,
                  );
                });
              }
            } catch (error) {
              if (isPermissionError(error)) throw error;
              this.logger.error(
                `[${accountId}] ❌ Lifetime backfill ${level} ${ent.id} [${seg.since}..${seg.until}] failed: ${parseMetaError(error).message}. Skipping segment.`,
              );
            }

            await sleep(
              Number(process.env.INSIGHT_LIFETIME_BACKFILL_SLEEP_MS || 1000),
            );
          }

          // Watermark: ensure a DAILY row exists at the floor so this entity is
          // not re-detected as a gap next run. A zero row is truthful (no
          // activity that day) and contributes 0 to MAX.
          if (!hasRowAtFloor) {
            await executeDbWithRetry(() =>
              (this.prisma[prismaModel] as any).upsert({
                where: {
                  [`${relationFieldId}_dateStart_range`]: {
                    [relationFieldId]: ent.id,
                    dateStart: ent.floor,
                    range: InsightRange.DAILY,
                  },
                },
                update: {},
                create: {
                  [relationFieldId]: ent.id,
                  level: levelEnum,
                  range: InsightRange.DAILY,
                  dateStart: ent.floor,
                  dateStop: ent.floor,
                },
              }),
            );
          }
        }
      }

      // Rebuild rollups (esp. MAX) for the levels we touched so the newly
      // backfilled history is reflected immediately.
      if (touchedLevels.length > 0) {
        const ranges = [
          InsightSyncRange.TODAY,
          InsightSyncRange.LAST_3D,
          InsightSyncRange.LAST_7D,
          InsightSyncRange.MAX,
        ];
        for (const level of touchedLevels) {
          await this.rollupLevelInsights(accountId, level, ranges, tz);
        }
        await this.aggregateCreativeInsights(accountId, ranges, tz);
      }

      const duration = ((Date.now() - start) / 1000).toFixed(2);
      this.logger.log(
        `[${accountId}] ✨ Lifetime DAILY backfill finished in ${duration}s (levels: ${touchedLevels.join(',') || 'none'}).`,
      );
    } catch (error) {
      if (isPermissionError(error)) {
        this.logger.warn(
          `[${accountId}] 🔑 Permission error during lifetime backfill. Marking needsReauth.`,
        );
        await executeDbWithRetry(() =>
          this.prisma.account.update({
            where: { id: accountId },
            data: { needsReauth: true },
          }),
        );
        return;
      }
      this.logger.error(
        `[${accountId}] ❌ Lifetime backfill error: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }
}
