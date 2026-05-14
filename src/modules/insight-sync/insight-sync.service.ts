import { Injectable, Logger } from '@nestjs/common';
import { CreativeStatus, InsightRange, LevelInsight } from '@prisma/client';
import dayjs from 'dayjs';
import { PrismaBatchHelper } from '../../common/helpers/prisma-batch.helper';
import { chunk, extractCampaignMetrics } from '../../common/utils';
import { PrismaService } from '../prisma/prisma.service';
import { InsightSyncLevel, InsightSyncRange } from './insight-sync.constants';
import { MetaApiService } from './meta-api.service';

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
    // Process levels in parallel to speed up account sync
    await Promise.all(
      levels.map(async (level) => {
        // Keep ranges sequential within a level to be safe with Meta rate limits
        for (const range of ranges) {
          await this.syncLevelRange(accountId, level, range);
        }

        // After all AD ranges are synced, aggregate Creative insights
        if (level === InsightSyncLevel.AD) {
          await this.aggregateCreativeInsights(accountId);
        }
      }),
    );
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
        OR: [
          { status: { in: ['ACTIVE', 'IN_PROCESS'] } },
          { [parentInsightIdField]: null },
        ],
      };

      const existingEntities = (await executeDbWithRetry(() =>
        (this.prisma[parentModel] as any).findMany({
          where,
          select: { id: true },
        }),
      )) as Array<{ id: string }>;

      const allIds = existingEntities.map((e) => e.id);
      if (allIds.length === 0) {
        this.logger.log(`[${accountId}] ⏭️ No ${level} found in DB for this range. Skipping.`);
        return;
      }

      this.logger.log(`[${accountId}] 🔍 Found ${allIds.length} ${level} entities to sync for ${range}.`);

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
        if (level === InsightSyncLevel.AD) chunkSize = 100;
        else if (level === InsightSyncLevel.ADSET) chunkSize = 300;
        else chunkSize = 500; // Campaign today/3d/7d is very light
      }

      const idChunks = chunk(allIds, chunkSize) as string[][];

      for (let i = 0; i < idChunks.length; i++) {
        const idChunk = idChunks[i];
        this.logger.log(`[${accountId}] ⏳ Fetching ${level} insights: Chunk ${i + 1}/${idChunks.length}...`);
        
        const insights = await this.metaApi.getAccountInsights(accountId, {
          level: level as any,
          date_preset: range as string,
          ids: idChunk,
          limit: 50,
        });
        if (insights) {
          this.logger.log(`[${accountId}] 📥 Received ${insights.length} insights from Chunk ${i + 1}.`);
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

      this.logger.log(`[${accountId}] 🛠️ Mapping ${insights.length} insights for ${level} ${range}...`);

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

      this.logger.log(`[${accountId}] 💾 Saving ${mappedData.length} records to DB (${prismaModel})...`);

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
        insightChunk.map((insight) =>
          (this.prisma[parentModel] as any).update({
            where: { id: insight[relationFieldId] },
            data: { [parentInsightIdField]: insight.id },
          }),
        ),
      );
    }
  }

  /**
   * Aggregates AdInsights into CreativeInsights for a specific account.
   * Ported from meta.cron.ts and optimized for account-level processing.
   */
  async aggregateCreativeInsights(accountId: string) {
    this.logger.log(`[${accountId}] 🚀 Starting CreativeInsight aggregation...`);
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
          range: { in: [InsightRange.MAX, InsightRange.DAY_7, InsightRange.DAY_3, InsightRange.TODAY] },
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

      // 4. Calculate for each Creative in batch
      for (const creative of batch) {
        const ads = creative.ads.map((a) => a.id);
        const bucket = {
          max: {} as Record<string, number>,
          last7d: {} as Record<string, number>,
          last3d: {} as Record<string, number>,
          today: {} as Record<string, number>,
        };

        for (const adId of ads) {
          const adInsights = insightMap.get(adId) || [];
          for (const ins of adInsights) {
            if (ins.range === InsightRange.MAX) sumMetrics(bucket.max, ins);
            if (ins.range === InsightRange.DAY_7) sumMetrics(bucket.last7d, ins);
            if (ins.range === InsightRange.DAY_3) sumMetrics(bucket.last3d, ins);
            if (ins.range === InsightRange.TODAY) sumMetrics(bucket.today, ins);
          }
        }

        // Add to upsert list
        const ranges = [
          { range: InsightRange.MAX, dateStart: '1975-01-01', data: bucket.max },
          { range: InsightRange.DAY_7, dateStart: sevenDaysAgo, data: bucket.last7d },
          { range: InsightRange.DAY_3, dateStart: threeDaysAgo, data: bucket.last3d },
          { range: InsightRange.TODAY, dateStart: today, data: bucket.today },
        ];

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
        const roas7d = (bucket.last7d.spend ?? 0) > 0 ? (bucket.last7d.purchaseValue ?? 0) / bucket.last7d.spend : 0;
        const roas3d = (bucket.last3d.spend ?? 0) > 0 ? (bucket.last3d.purchaseValue ?? 0) / bucket.last3d.spend : 0;

        let status: CreativeStatus = CreativeStatus.OTHER;
        if (maxSpend === 0) status = CreativeStatus.OTHER;
        else if (maxSpend <= 100000) status = CreativeStatus.NEED_SPEND;
        else if (((maxSpend <= 500000 && roasMax >= 2) || (maxSpend > 500000 && roasMax >= 2.2)) && roas7d >= 2.5) {
          status = CreativeStatus.SCALE_P1;
        } else if (((maxSpend <= 500000 && roasMax >= 1.5) || (maxSpend > 500000 && roasMax >= 1.8 && ctrMax > 0.03)) && roas7d >= 2.2 && roas3d >= 2.2) {
          status = CreativeStatus.SCALE_P2;
        } else if ((maxSpend <= 500000 && maxPurchases < 1 && ctrMax > 0.03) || (maxSpend > 500000 && roasMax < 1.8 && ctrMax > 0.03)) {
          status = CreativeStatus.REVIEW;
        } else if ((maxSpend <= 500000 && maxPurchases < 1 && ctrMax < 0.03) || (maxSpend > 500000 && roasMax < 1.8 && ctrMax < 0.03)) {
          status = CreativeStatus.OFF;
        }

        creativeUpdates.push({
          id: creative.id,
          data: { performanceStatus: status, ...bucket.max },
        });
      }

      // 5. Batch Upsert CreativeInsights
      await prismaHelper.upsertMany(creativeInsightUpserts, (item) =>
        this.prisma.creativeInsight.upsert({
          where: {
            creativeId_dateStart_range: {
              creativeId: item.creativeId,
              dateStart: item.dateStart,
              range: item.range,
            },
          },
          update: item.data,
          create: {
            creativeId: item.creativeId,
            dateStart: item.dateStart,
            range: item.range,
            ...item.data,
          },
        }),
      );

      // 6. Fetch Insight IDs to update relations on Creative
      const insightRecords = await this.prisma.creativeInsight.findMany({
        where: {
          creativeId: { in: batch.map((c) => c.id) },
          range: { in: [InsightRange.MAX, InsightRange.DAY_7, InsightRange.DAY_3, InsightRange.TODAY] },
        },
        select: { id: true, creativeId: true, range: true },
      });

      const insightMapByCreative = new Map<string, any>();
      for (const r of insightRecords) {
        if (!insightMapByCreative.has(r.creativeId)) insightMapByCreative.set(r.creativeId, {});
        const obj = insightMapByCreative.get(r.creativeId);
        if (r.range === InsightRange.MAX) obj.max = r.id;
        if (r.range === InsightRange.DAY_7) obj.d7 = r.id;
        if (r.range === InsightRange.DAY_3) obj.d3 = r.id;
        if (r.range === InsightRange.TODAY) obj.today = r.id;
      }

      // 7. Update Creative performance status and insight IDs
      await prismaHelper.upsertMany(creativeUpdates, (item) => {
        const ref = insightMapByCreative.get(item.id);
        return this.prisma.creative.update({
          where: { id: item.id },
          data: {
            ...item.data,
            insightMaxId: ref?.max,
            insight7dId: ref?.d7,
            insight3dId: ref?.d3,
            insightTodayId: ref?.today,
          },
        });
      });
    }

    const duration = ((Date.now() - start) / 1000).toFixed(2);
    this.logger.log(`[${accountId}] ✨ CreativeInsight aggregation done in ${duration}s.`);
  }

  /**
   * Syncs Audience Insights (Age/Gender breakdowns) for AdSets.
   */
  async syncAccountAudienceInsights(accountId: string) {
    this.logger.log(`[${accountId}] 👥 Starting Audience Insight sync (Age/Gender)...`);
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
        this.logger.log(`[${accountId}] No active AdSets found for Audience sync.`);
        return;
      }

      this.logger.log(`[${accountId}] 🔍 Found ${adSetIds.length} AdSets for Audience sync.`);

      const allInsights: any[] = [];
      const idChunks = chunk(adSetIds, 50) as string[][]; // Small chunks for breakdowns

      for (let i = 0; i < idChunks.length; i++) {
        const idChunk = idChunks[i];
        this.logger.log(`[${accountId}] ⏳ Fetching Audience: Chunk ${i + 1}/${idChunks.length}...`);

        const insights = await this.metaApi.getAccountInsights(accountId, {
          level: 'adset' as any,
          date_preset: 'maximum',
          ids: idChunk,
          breakdowns: ['age', 'gender'],
          limit: 50,
        });

        if (insights) {
          this.logger.log(`[${accountId}] 📥 Received ${insights.length} breakdown records from Chunk ${i + 1}.`);
          allInsights.push(...insights);
        }
      }

      if (allInsights.length === 0) {
        this.logger.log(`[${accountId}] ℹ️ No Audience insights returned from Meta.`);
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

      this.logger.log(`[${accountId}] 💾 Saving ${mappedData.length} Audience records to DB...`);

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
      this.logger.log(`[${accountId}] ✅ Finished Audience Insight sync in ${duration}s.`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`[${accountId}] ❌ Error syncing Audience insights: ${message}`);
      throw error;
    }
  }
}
