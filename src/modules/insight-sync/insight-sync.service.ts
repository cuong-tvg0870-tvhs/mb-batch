import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { CreativeStatus, InsightRange, LevelInsight, Prisma } from '@prisma/client';
import dayjs from 'dayjs';
import { PrismaBatchHelper } from '../../common/helpers/prisma-batch.helper';
import { chunk, extractCampaignMetrics } from '../../common/utils';
import { MetaApiService } from '../meta-api/meta-api.service';
import { PrismaService } from '../prisma/prisma.service';
import { InsightSyncLevel, InsightSyncRange } from './insight-sync.constants';

@Injectable()
export class InsightSyncService implements OnModuleInit {
  private readonly logger = new Logger(InsightSyncService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaApi: MetaApiService,
  ) {}

  async onModuleInit() {
    if (process.env.INSIGHT_AGGREGATE_ON_BOOT !== 'true') return;

    setTimeout(async () => {
      try {
        const accounts = await this.prisma.account.findMany({
          where: { needsReauth: false, accountType: 'AD_ACCOUNT' as any },
          select: { id: true },
        });
        for (const account of accounts) {
          await this.aggregateCreativeInsights(account.id);
        }
        this.logger.log('✅ Triggered boot aggregation successfully.');
      } catch (err: any) {
        this.logger.error(`Failed to trigger boot aggregation: ${err.message}`);
      }
    }, 5000);
  }

  /**
   * Main entry point for syncing insights for one account
   */
  async syncAccountInsights(
    accountId: string,
    levels: InsightSyncLevel[],
    ranges: InsightSyncRange[],
  ) {
    for (const level of levels) {
      for (const range of ranges) {
        await this.syncLevelRange(accountId, level, range);
      }

      if (level === InsightSyncLevel.AD) {
        await this.aggregateCreativeInsights(accountId);
      }
    }
  }

  private async syncLevelRange(
    accountId: string,
    level: InsightSyncLevel,
    range: InsightSyncRange,
  ) {
    this.logger.log(`[${accountId}] Syncing ${level} insights for ${range}...`);

    try {
      // 1. Fetch existing IDs from Database first
      const parentModel = this.getParentModel(level);
      const { executeDbWithRetry } = require('../../common/utils');

      // Optimization: "Fill the Gaps" Strategy
      // 1. Always fetch for ACTIVE/IN_PROCESS entities to get latest numbers.
      // 2. Fetch for ANY entity (even PAUSED/ARCHIVED) if it's missing the insight record for this range.
      const parentInsightIdField = this.getInsightIdFieldOnParent(range);
      const where: any = {
        accountId,
        OR: [{ status: { in: ['ACTIVE', 'IN_PROCESS'] } }],
      };

      // Nếu có field mapping tương ứng mới check NULL để "Fill the gaps"
      if (parentInsightIdField) {
        where.OR.push({ [parentInsightIdField]: null });
      }

      const existingEntities = (await executeDbWithRetry(() =>
        (this.prisma[parentModel] as any).findMany({
          where,
          select: { id: true },
        }),
      )) as Array<{ id: string }>;

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

      // 2. Fetch from Meta in chunks to avoid filter limits
      const allInsights: any[] = [];

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

      for (let i = 0; i < idChunks.length; i++) {
        const idChunk = idChunks[i];
        this.logger.log(
          `[${accountId}] ⏳ Fetching ${level} insights: Chunk ${i + 1}/${idChunks.length}...`,
        );

        const insights = await this.metaApi.getAccountInsights(accountId, {
          level: level as any,
          date_preset: range as string,
          ids: idChunk,
          limit: 50,
        });
        if (insights) {
          this.logger.log(
            `[${accountId}] 📥 Received ${insights.length} insights from Chunk ${i + 1}.`,
          );
          allInsights.push(...insights);
        }
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

      const mappedData = insights
        .filter((i: any) => i[entityIdField])
        .map((i: any) => {
          const metrics = extractCampaignMetrics(i);
          const { toPrismaJson } = require('../../common/utils');
          return {
            [relationFieldId]: i[entityIdField],
            level: levelEnum,
            range: rangeEnum,
            dateStart: i.date_start,
            dateStop: i.date_stop,
            ...metrics,
            rawPayload: toPrismaJson(i),
          };
        });

      // Special handling for TODAY: also insert as DAILY
      let dailyData: any[] = [];
      if (range === InsightSyncRange.TODAY) {
        dailyData = mappedData.map((d) => ({
          ...d,
          range: 'DAILY',
        }));
      }

      // 4. Batch Upsert to Database
      const entityIds = mappedData.map((d) => d[relationFieldId]);
      const dateStarts = [...new Set(mappedData.map((d) => d.dateStart))];

      this.logger.log(
        `[${accountId}] 💾 Saving ${mappedData.length} records to DB (${prismaModel})...`,
      );

      await executeDbWithRetry(async () => {
        // Delete existing records for this range
        await (this.prisma[prismaModel] as any).deleteMany({
          where: {
            [relationFieldId]: { in: entityIds },
            range: rangeEnum,
          },
        });

        // If TODAY, also delete existing DAILY records for the same dates
        if (range === InsightSyncRange.TODAY) {
          await (this.prisma[prismaModel] as any).deleteMany({
            where: {
              [relationFieldId]: { in: entityIds },
              range: 'DAILY',
              dateStart: { in: dateStarts },
            },
          });
        }

        // Insert primary data
        await prismaHelper.createManySafe(
          this.prisma[prismaModel] as any,
          mappedData,
          50,
        );

        // Insert daily data if TODAY
        if (dailyData.length > 0) {
          await prismaHelper.createManySafe(
            this.prisma[prismaModel] as any,
            dailyData,
            50,
          );
        }
      });

      // 5. Update parent record's insight relation
      await this.updateParentRelations(level, range, mappedData);
      if (range === InsightSyncRange.TODAY) {
        // Also update DAILY relation if needed, though usually TODAY is what matters for quick view
        await this.updateParentRelations(level, 'daily' as any, dailyData);
      }

      this.logger.log(
        `[${accountId}] ✅ Finished ${level} ${range}: ${mappedData.length} records updated.`,
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

  /**
   * Aggregates AdInsights into CreativeInsights for a specific account.
   * Ported from meta.cron.ts and optimized for account-level processing.
   */
  async aggregateCreativeInsights(accountId: string) {
    this.logger.log(
      `[${accountId}] 🚀 Starting CreativeInsight aggregation...`,
    );
    const start = Date.now();

    const prismaHelper = new PrismaBatchHelper(this.prisma);
    const today = dayjs().format('YYYY-MM-DD');
    const sevenDaysAgo = dayjs().subtract(6, 'day').format('YYYY-MM-DD');
    const threeDaysAgo = dayjs().subtract(2, 'day').format('YYYY-MM-DD');

    // 1. Load all creatives for this account
    const creatives = await this.prisma.creative.findMany({
      where: { accountId },
      select: {
        id: true,
        ads: { select: { id: true } },
      },
    });

    if (creatives.length === 0) {
      this.logger.log(`[${accountId}] No creatives found to aggregate.`);
      return;
    }

    const batchSize = 100;
    for (let i = 0; i < creatives.length; i += batchSize) {
      const batch = creatives.slice(i, i + batchSize);
      const adIds = batch.flatMap((c) => c.ads.map((a) => a.id));
      if (!adIds.length) continue;

      // 2. Load AdInsights for these ads
      const insights = await this.prisma.adInsight.findMany({
        where: {
          adId: { in: adIds },
          range: {
            in: [
              InsightRange.MAX,
              InsightRange.DAY_7,
              InsightRange.DAY_3,
              InsightRange.TODAY,
              InsightRange.DAILY,
            ],
          },
        },
      });

      // 3. Group insights by AdId
      const insightMap = new Map<string, any[]>();
      for (const ins of insights) {
        if (!insightMap.has(ins.adId)) insightMap.set(ins.adId, []);
        insightMap.get(ins.adId)!.push(ins);
      }

      const creativeInsightUpserts: any[] = [];
      const creativeUpdates: any[] = [];

      const sumMetrics = (target: Record<string, number>, source: any) => {
        for (const key in source) {
          if (typeof source[key] === 'number') {
            target[key] = (target[key] || 0) + source[key];
          }
        }
      };

      const recalculateDerivedMetrics = (target: Record<string, number>) => {
        const impressions = target.impressions || 0;
        const clicks = target.clicks || 0;
        const spend = target.spend || 0;
        const purchases = target.purchases || 0;
        const purchaseValue = target.purchaseValue || 0;
        const registrationComplete = target.registrationComplete || 0;
        const results = purchases + registrationComplete;
        const videoPlay = target.videoPlay || 0;
        const video3s = target.video3s || 0;
        const video100 = target.video100 || 0;
        const uniqueClicks = target.uniqueClicks || 0;
        const reach = target.reach || 0;

        target.ctr = impressions > 0 ? (clicks / impressions) * 100 : 0;
        target.cpc = clicks > 0 ? spend / clicks : 0;
        target.cpm = impressions > 0 ? (spend / impressions) * 1000 : 0;
        target.roas = spend > 0 ? purchaseValue / spend : 0;
        target.cvr = clicks > 0 ? purchases / clicks : 0;
        target.costPerResult = results > 0 ? spend / results : 0;
        target.aov = results > 0 ? purchaseValue / results : 0;
        target.adsCostRatio = target.roas > 0 ? 1 / target.roas : 0;
        target.hookRate =
          videoPlay > 0 ? +((video3s / videoPlay) * 100).toFixed(2) : 0;
        target.holdRate =
          video3s > 0 ? +((video100 / video3s) * 100).toFixed(2) : 0;
        target.uniqueCtr = reach > 0 ? (uniqueClicks / reach) * 100 : 0;
        target.results = results;
      };

      // 4. Calculate for each Creative in batch
      for (const creative of batch) {
        const ads = creative.ads.map((a) => a.id);
        const bucket = {
          max: {} as Record<string, number>,
          last7d: {} as Record<string, number>,
          last3d: {} as Record<string, number>,
          today: {} as Record<string, number>,
          daily: {} as Record<string, Record<string, number>>,
        };

        for (const adId of ads) {
          const adInsights = insightMap.get(adId) || [];
          for (const ins of adInsights) {
            if (ins.range === InsightRange.MAX) sumMetrics(bucket.max, ins);
            if (ins.range === InsightRange.DAY_7)
              sumMetrics(bucket.last7d, ins);
            if (ins.range === InsightRange.DAY_3)
              sumMetrics(bucket.last3d, ins);
            if (ins.range === InsightRange.TODAY) sumMetrics(bucket.today, ins);
            if (ins.range === InsightRange.DAILY) {
              if (!bucket.daily[ins.dateStart])
                bucket.daily[ins.dateStart] = {};
              sumMetrics(bucket.daily[ins.dateStart], ins);
            }
          }
        }

        recalculateDerivedMetrics(bucket.max);
        recalculateDerivedMetrics(bucket.last7d);
        recalculateDerivedMetrics(bucket.last3d);
        recalculateDerivedMetrics(bucket.today);
        for (const dateStart of Object.keys(bucket.daily)) {
          recalculateDerivedMetrics(bucket.daily[dateStart]);
        }

        // Add to upsert list
        const ranges: any[] = [
          {
            range: InsightRange.MAX,
            dateStart: '1975-01-01',
            data: bucket.max,
          },
          {
            range: InsightRange.DAY_7,
            dateStart: sevenDaysAgo,
            data: bucket.last7d,
          },
          {
            range: InsightRange.DAY_3,
            dateStart: threeDaysAgo,
            data: bucket.last3d,
          },
          { range: InsightRange.TODAY, dateStart: today, data: bucket.today },
        ];

        for (const dateStart of Object.keys(bucket.daily)) {
          ranges.push({
            range: InsightRange.DAILY,
            dateStart: dateStart,
            data: bucket.daily[dateStart],
          });
        }

        for (const r of ranges) {
          creativeInsightUpserts.push({
            creativeId: creative.id,
            dateStart: r.dateStart,
            range: r.range,
            data: { dateStop: today, ...r.data },
          });
        }

        // Calculate performance status
        const maxSpend = bucket.max.spend ?? 0;
        const maxRevenue = bucket.max.purchaseValue ?? 0;
        const maxPurchases = bucket.max.purchases ?? 0;
        const maxClicks = bucket.max.clicks ?? 0;
        const maxImpressions = bucket.max.impressions ?? 0;

        const roasMax = maxSpend > 0 ? maxRevenue / maxSpend : 0;
        const ctrMax = maxImpressions > 0 ? maxClicks / maxImpressions : 0;
        const roas7d =
          (bucket.last7d.spend ?? 0) > 0
            ? (bucket.last7d.purchaseValue ?? 0) / bucket.last7d.spend
            : 0;
        const roas3d =
          (bucket.last3d.spend ?? 0) > 0
            ? (bucket.last3d.purchaseValue ?? 0) / bucket.last3d.spend
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
          data: { performanceStatus: status, ...bucket.max },
        });
      }

      // 5. Delete existing insights for these ranges to avoid duplicates when dateStart changes
      await this.prisma.creativeInsight.deleteMany({
        where: {
          creativeId: { in: batch.map((c) => c.id) },
          range: {
            in: [
              InsightRange.MAX,
              InsightRange.DAY_7,
              InsightRange.DAY_3,
              InsightRange.TODAY,
              InsightRange.DAILY,
            ],
          },
        },
      });

      // 5.1 Batch Create CreativeInsights
      const createData = creativeInsightUpserts.map((item) => ({
        creativeId: item.creativeId,
        dateStart: item.dateStart,
        range: item.range,
        ...item.data,
      }));

      await prismaHelper.createManySafe(
        this.prisma.creativeInsight as any,
        createData,
        100,
      );

      // 6. Fetch Insight IDs to update relations on Creative
      const insightRecords = await this.prisma.creativeInsight.findMany({
        where: {
          creativeId: { in: batch.map((c) => c.id) },
          range: {
            in: [
              InsightRange.MAX,
              InsightRange.DAY_7,
              InsightRange.DAY_3,
              InsightRange.TODAY,
            ],
          },
        },
        select: { id: true, creativeId: true, range: true },
      });

      const insightMapByCreative = new Map<string, any>();
      for (const r of insightRecords) {
        if (!insightMapByCreative.has(r.creativeId))
          insightMapByCreative.set(r.creativeId, {});
        const obj = insightMapByCreative.get(r.creativeId);
        if (r.range === InsightRange.MAX) obj.max = r.id;
        if (r.range === InsightRange.DAY_7) obj.d7 = r.id;
        if (r.range === InsightRange.DAY_3) obj.d3 = r.id;
        if (r.range === InsightRange.TODAY) obj.today = r.id;
      }

      // 7. Update Creative performance status and insight IDs
      await prismaHelper.upsertMany(creativeUpdates, async (item) => {
        const ref = insightMapByCreative.get(item.id);
        try {
          await this.prisma.creative.update({
            where: { id: item.id },
            data: {
              ...item.data,
              insightMaxId: ref?.max,
              insight7dId: ref?.d7,
              insight3dId: ref?.d3,
              insightTodayId: ref?.today,
            },
          });
        } catch (err: any) {
          if (
            err instanceof Prisma.PrismaClientKnownRequestError &&
            err.code === 'P2003'
          ) {
            this.logger.warn(
              `[${accountId}] ⚠️ Foreign key constraint violation on creative ${item.id} update. Skipping...`,
            );
          } else {
            throw err;
          }
        }
      });
    }

    const duration = ((Date.now() - start) / 1000).toFixed(2);
    this.logger.log(
      `[${accountId}] ✨ CreativeInsight aggregation done in ${duration}s.`,
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
   * Uses the range defined by the MAX insight of each entity.
   */
  async syncAccountMissingDailyInsights(accountId: string) {
    this.logger.log(
      `[${accountId}] 🔍 Starting Missing Daily Insights sync based on MAX range...`,
    );
    const start = Date.now();

    const levels = [
      InsightSyncLevel.CAMPAIGN,
      InsightSyncLevel.ADSET,
      InsightSyncLevel.AD,
    ];

    try {
      for (const level of levels) {
        const parentModel = this.getParentModel(level);
        const relationFieldId = this.getRelationField(level);
        const entityIdField = this.getEntityIdField(level);
        const prismaModel = this.getPrismaModel(level);

        // 1. Fetch all relevant entities that have a MAX insight
        const entities = await (this.prisma[parentModel] as any).findMany({
          where: {
            accountId,
            status: { in: ['ACTIVE', 'PAUSED'] },
            insightMaxId: { not: null },
          },
          select: { id: true, name: true, insightMaxId: true },
        });

        if (entities.length === 0) continue;

        // 2. Fetch the MAX insights to get their date ranges
        const maxInsightIds = entities
          .map((e) => e.insightMaxId)
          .filter(Boolean);
        const maxInsights = await (this.prisma[prismaModel] as any).findMany({
          where: { id: { in: maxInsightIds } },
          select: { id: true, dateStart: true, dateStop: true },
        });
        const maxInsightMap = new Map<string, any>(
          maxInsights.map((i: any) => [i.id, i]),
        );

        this.logger.log(
          `[${accountId}] Checking ${entities.length} ${level}s for missing days based on their MAX records...`,
        );

        // 3. For each entity, find gaps in DAILY insights
        for (const entity of entities) {
          const maxInsight = maxInsightMap.get(entity.insightMaxId);
          if (!maxInsight) continue;

          const startDate = maxInsight.dateStart;
          const endDate = maxInsight.dateStop;

          const existingInsights = await (
            this.prisma[prismaModel] as any
          ).findMany({
            where: {
              [relationFieldId]: entity.id,
              range: 'DAILY',
              dateStart: { gte: startDate, lte: endDate },
            },
            select: { dateStart: true },
          });

          const existingDates = new Set(
            existingInsights.map((i: any) => i.dateStart),
          );
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
            this.logger.log(
              `[${accountId}] ${level} ${entity.id} is missing ${missingDates.length} days. Syncing...`,
            );

            // Fetch from Meta for this specific entity and date range
            // Optimization: instead of fetching day-by-day, fetch the whole range from min missing to max missing with time_increment=1
            // then filter for only missing ones in code.
            const minDate = missingDates[0];
            const maxDate = missingDates[missingDates.length - 1];

            const metaInsights = await this.metaApi.getAccountInsights(
              accountId,
              {
                level: level as any,
                date_preset: 'maximum',
                time_increment: 1,
                ids: [entity.id],
              },
            );
            if (metaInsights && metaInsights.length > 0) {
              const mappedData = metaInsights.map((i: any) => {
                const {
                  extractCampaignMetrics,
                  toPrismaJson,
                } = require('../../common/utils');
                const metrics = extractCampaignMetrics(i);

                return {
                  [relationFieldId]: i[entityIdField],
                  level: this.mapLevelToEnum(level),
                  range: 'DAILY',
                  dateStart: i.date_start,
                  dateStop: i.date_stop,
                  ...metrics,
                  rawPayload: toPrismaJson(i),
                };
              });

              if (mappedData.length > 0) {
                const prismaHelper = new PrismaBatchHelper(this.prisma);
                await prismaHelper.upsertMany(mappedData, (item: any) => {
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

                this.logger.log(
                  `[${accountId}] ✅ Saved ${mappedData.length} missing daily records for ${level} ${entity.id}.`,
                );
              }
            }
          }
        }
      }

      const duration = ((Date.now() - start) / 1000).toFixed(2);
      this.logger.log(
        `[${accountId}] ✨ Missing Daily Insights sync finished in ${duration}s.`,
      );
    } catch (error) {
      this.logger.error(
        `[${accountId}] ❌ Error syncing missing daily insights: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }
}
