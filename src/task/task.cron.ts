import {
  Injectable,
  InternalServerErrorException,
  Logger,
  OnModuleInit,
} from '@nestjs/common';
import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';

import { PrismaService } from 'src/modules/prisma/prisma.service';

import {
  chunk,
  extractCampaignMetrics,
  fetchAll,
  parseMetaError,
  sleep,
  toPrismaJson,
} from 'src/common/utils';

import {
  AD_FIELDS,
  AD_IMAGE_FIELDS,
  AD_INSIGHT_FIELDS,
  AD_VIDEO_FIELDS,
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
  CREATIVE_FIELDS,
} from 'src/common/utils/meta-field';

import * as dayjs from 'dayjs';

import { CreativeStatus, InsightRange, LevelInsight } from '@prisma/client';
import { MetaTransformHelper } from 'src/common/helpers/meta-transform.helper';
import { PrismaBatchHelper } from 'src/common/helpers/prisma-batch.helper';

/* =====================================================
   CRON SERVICE
===================================================== */

@Injectable()
export class TaskCron implements OnModuleInit {
  private readonly logger = new Logger(TaskCron.name);
  private initialized = false;

  constructor(private readonly prisma: PrismaService) {}

  /* =====================================================
     INIT SDK
  ===================================================== */
  private init() {
    if (!this.initialized) {
      FacebookAdsApi.init(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
      this.initialized = true;
    }
  }

  async onModuleInit() {
    this.logger.log('🚀 TaskCron initialized');
    // await this.syncMaxCampaignInsightsJob();
  }

  // @Cron('0 5 0 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncCampaignCore() {
    this.logger.log('🔄 Sync Campaign Core');
    await this.syncCampaignData();
    this.logger.log('✅ Sync Campaign Core DONE');
  }

  // @Cron('0 5 1 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxCampaignInsightsJob() {
    this.logger.log('🔄 Sync MAX Campaign Insights');
    await this.syncMaxCampaignInsights();
    this.logger.log('✅ MAX Campaign DONE');
  }

  // @Cron('0 10 2 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdsetInsightsJob() {
    this.logger.log('🔄 Sync MAX Adset Insights');
    await this.syncMaxAdSetInsights();
    this.logger.log('✅ MAX Adset DONE');
  }

  // @Cron('0 15 3 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdsetAudienceInsightsJob() {
    this.logger.log('🔄 Sync MAX Adset Audience Insights');
    await this.syncMaxAdSetAudienceInsights();
    this.logger.log('✅ MAX Adset Audience DONE');
  }

  // @Cron('0 20 4 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdInsightsJob() {
    this.logger.log('🔄 Sync MAX Ad Insights');
    await this.syncMaxAdInsights();
    this.logger.log('✅ MAX Ad DONE');
  }

  // @Cron('0 25 5 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdAudienceInsightsJob() {
    this.logger.log('🔄 Sync MAX Ad Audience Insights');
    await this.syncMaxAdSetAudienceInsights();
    this.logger.log('✅ MAX Ad Audience DONE');
  }

  // @Cron('0 30 6,12,17 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyCampaignInsightsJob() {
    this.logger.log('🔄 Sync DAILY Campaign Insights');
    await this.syncDailyCampaignInsights();
    this.logger.log('✅ DAILY Campaign DONE');
  }

  // @Cron('0 35 7,13,18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyAdsetInsightsJob() {
    this.logger.log('🔄 Sync DAILY Adset Insights');
    await this.syncDailyAdSetInsights();
    this.logger.log('✅ DAILY Adset DONE');
  }

  // @Cron('0 40 8,13,18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyAdInsightsJob() {
    this.logger.log('🔄 Sync DAILY Ad Insights');
    await this.syncDailyAdInsights();
    this.logger.log('✅ DAILY Ad DONE');

    this.logger.log('🔄 Analytic Creative Insight');
    await this.calculateCreativeInsightFromAdInsightParallel();
    this.logger.log('✅ Analytic Creative Insight');
  }

  /* =====================================================
     UPSERT LOGIC (BATCH)
  ===================================================== */
  async upsertFullStructure(campaigns: any[], accountId: string) {
    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const campaignData = [];
    const adsetData = [];
    const adData = [];
    const creativeData = [];

    for (const c of campaigns) {
      campaignData.push(MetaTransformHelper.campaign(c, accountId));

      for (const as of c.adsets?.data || []) {
        adsetData.push(MetaTransformHelper.adset(as, accountId, c.id));

        for (const ad of as.ads?.data || []) {
          adData.push(MetaTransformHelper.ad(ad, accountId, c.id, as.id));

          const creative = MetaTransformHelper.creative(ad, accountId);
          if (creative) creativeData.push(creative);
        }
      }
    }

    this.logger.log(
      `📦 Data: campaign=${campaignData.length}, adset=${adsetData.length}, ad=${adData.length}`,
    );

    const pageIds = [
      ...new Set(creativeData.map((c) => c.pageId).filter(Boolean)),
    ];

    const videoIds = [
      ...new Set(creativeData.map((c) => c.videoId).filter(Boolean)),
    ];

    const imageHashes = [
      ...new Set(creativeData.map((c) => c.imageHash).filter(Boolean)),
    ];

    const fanpages = await this.prisma.fanpage.findMany({
      where: { id: { in: pageIds } },
    });

    const existingVideos = await this.prisma.adVideo.findMany({
      where: { id: { in: videoIds } },
    });

    const existingImages = await this.prisma.adImage.findMany({
      where: { hash: { in: imageHashes } },
    });

    const fanpageMap = new Map(fanpages.map((f) => [f.id, f]));
    const videoSet = new Set(existingVideos.map((v) => v.id));
    const imageSet = new Set(existingImages.map((i) => i.hash));

    const newVideos = [];
    const newImages = [];

    for (const item of creativeData) {
      // ✅ map systemPageId
      if (item.pageId && fanpageMap.has(item.pageId)) {
        item.systemPageId = item.pageId;
      }

      // ✅ video
      if (item.videoId && !videoSet.has(item.videoId)) {
        newVideos.push({
          id: item.videoId,
          accountId: item.accountId,
          thumbnailUrl: item.thumbnailUrl,
        });
        videoSet.add(item.videoId); // tránh duplicate trong loop
      }

      // ✅ image
      if (item.imageHash && !imageSet.has(item.imageHash)) {
        newImages.push({
          id: `${accountId.replaceAll('act_', '')}:${item.imageHash}`,
          hash: item.imageHash,
          accountId: item.accountId,
          url: item.thumbnailUrl,
        });
        imageSet.add(item.imageHash);
      }
    }

    await prismaHelper.createManySafe(this.prisma.adVideo, newVideos);

    await prismaHelper.createManySafe(this.prisma.adImage, newImages);

    // 🔥 batch insert
    await prismaHelper.upsertMany(
      campaignData,
      (item) =>
        this.prisma.campaign.upsert({
          where: { id: item.id },
          update: item,
          create: item,
        }),
      20,
    );

    await prismaHelper.upsertMany(
      adsetData,
      (item) =>
        this.prisma.adSet.upsert({
          where: { id: item.id },
          update: item,
          create: item,
        }),
      20,
    );

    await prismaHelper.upsertMany(
      creativeData,
      (item) =>
        this.prisma.creative.upsert({
          where: { id: item.id },
          update: item,
          create: item,
        }),
      20,
    );

    await prismaHelper.upsertMany(
      adData,
      (item) =>
        this.prisma.ad.upsert({
          where: { id: item.id },
          update: item,
          create: item,
        }),
      20,
    );
  }

  /* =====================================================
     MAIN SYNC
  ===================================================== */
  async syncCampaignData() {
    this.logger.log('⏰ Sync Campaign Data...');
    this.init();

    try {
      const accounts = await this.prisma.account.findMany({
        where: { needsReauth: false },
      });

      for (const account of accounts) {
        try {
          const adAccount = new AdAccount(account.id);

          const lastCampaign = await this.prisma.campaign.findFirst({
            where: { accountId: account.id },
            orderBy: { updatedAt: 'desc' },
            select: { updatedAt: true },
          });

          const lastSyncUnix = lastCampaign
            ? Math.floor(
                (new Date(lastCampaign.updatedAt).getTime() - 5 * 60 * 1000) /
                  1000,
              )
            : Math.floor((Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000);

          const fields = [
            ...CAMPAIGN_FIELDS,
            `adsets.limit(100){${ADSET_FIELDS.join(',')},
              ads.limit(100){${AD_FIELDS.filter((f) => f !== 'creative').join(
                ',',
              )},
              creative{${CREATIVE_FIELDS.join(',')}}}}`,
          ];

          const cursor = await adAccount.getCampaigns(
            fields,
            {
              limit: 50,
              filtering: [
                {
                  field: 'updated_time',
                  operator: 'GREATER_THAN',
                  value: lastSyncUnix,
                },
              ],
            },
            true,
          );

          const campaigns = await fetchAll(cursor);

          this.logger.log(
            `📊 Account ${account.id} → ${campaigns.length} campaigns`,
          );

          await this.upsertFullStructure(campaigns, account.id);

          await sleep(1000); // tránh spam API
        } catch (error) {
          const metaError = parseMetaError(error);
          this.logger.error(`❌ Account ${account.id}: ${metaError.message}`);
        }
      }
    } catch (err) {
      throw new InternalServerErrorException(parseMetaError(err));
    }
  }

  async syncMaxCampaignInsights() {
    this.logger.log('🔄 Sync MAX Campaign Insight');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const campaigns = await this.prisma.campaign.findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(campaigns);

    let totalProcessed = 0;

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      this.logger.log(`➡️ Account ${accountId} - ${ids.length} campaigns`);

      for (const idsChunk of chunk(ids, 50)) {
        try {
          // ================= FETCH =================
          const cursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              limit: 100,
              level: 'campaign',
              date_preset: 'maximum',
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              filtering: [
                { field: 'campaign.id', operator: 'IN', value: idsChunk },
              ],
            },
            true,
          );

          const insights = await fetchAll(cursor);

          if (!insights.length) {
            this.logger.log(`⚠️ Empty chunk`);
            continue;
          }

          const validInsights = insights.filter((i) => i.campaign_id);

          const campaignIds = [
            ...new Set(validInsights.map((i) => i.campaign_id)),
          ];

          this.logger.log(
            `📦 ${validInsights.length} insights | ${campaignIds.length} campaigns`,
          );

          // ================= DELETE (NHẸ) =================
          await this.prisma.campaignInsight.deleteMany({
            where: {
              campaignId: { in: campaignIds },
              range: InsightRange.MAX,
            },
          });

          // ================= TRANSFORM =================
          const insightData = validInsights.map((i) => {
            const metrics = extractCampaignMetrics(i);

            return {
              campaignId: i.campaign_id,
              level: LevelInsight.CAMPAIGN,
              range: InsightRange.MAX,
              dateStart: i.date_start,
              dateStop: i.date_stop,
              ...metrics,
              rawPayload: i,
            };
          });

          // ================= INSERT =================
          await prismaHelper.createManySafe(
            this.prisma.campaignInsight,
            insightData,
          );

          // ================= UPDATE CAMPAIGN (BATCH) =================
          const campaignUpdateData = validInsights.map((i) => {
            const metrics = extractCampaignMetrics(i);

            return {
              id: i.campaign_id,
              ...metrics,
            };
          });

          await prismaHelper.upsertMany(campaignUpdateData, (item) =>
            this.prisma.campaign.update({
              where: { id: item.id },
              data: item,
            }),
          );

          totalProcessed += validInsights.length;

          this.logger.log(`✅ Chunk done (${validInsights.length} insights)`);

          await sleep(800);
        } catch (error) {
          this.logger.error(
            `❌ Account ${accountId}: ${parseMetaError(error).message}`,
          );
        }
      }
    }

    this.logger.log(`🎯 DONE MAX Campaign Insight - Total: ${totalProcessed}`);
  }

  async syncDailyCampaignInsights() {
    this.logger.log('🔄 Sync DAILY Campaign Insights');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);
    const today = dayjs().startOf('day');

    // ================= 1. MAX =================
    const maxInsights = await this.prisma.campaignInsight.findMany({
      where: {
        range: InsightRange.MAX,
        level: LevelInsight.CAMPAIGN,
        spend: { gt: 0 },
        campaign: { account: { needsReauth: false } },
      },
      select: {
        campaignId: true,
        dateStart: true,
        dateStop: true,
        campaign: { select: { accountId: true } },
      },
    });

    if (!maxInsights.length) return;

    // ================= 2. LAST DAILY =================
    const lastDailies = await this.prisma.campaignInsight.findMany({
      where: { range: InsightRange.DAILY },
      select: { campaignId: true, dateStart: true },
      orderBy: { dateStart: 'desc' },
    });

    const lastDailyMap = new Map<string, string>();

    for (const d of lastDailies) {
      if (!lastDailyMap.has(d.campaignId)) {
        lastDailyMap.set(d.campaignId, d.dateStart);
      }
    }

    let totalFetched = 0;
    let totalUpserted = 0;

    const BATCH_SIZE = 20;

    for (const batch of chunk(maxInsights, BATCH_SIZE)) {
      await Promise.all(
        batch.map(async (max) => {
          const accountId = max.campaign?.accountId;
          if (!accountId) return;

          const maxStart = dayjs(max.dateStart);
          const maxStopRaw = dayjs(max.dateStop);

          if (maxStopRaw.add(3, 'day').isBefore(today)) return;

          const maxStop = maxStopRaw.isAfter(today) ? today : maxStopRaw;

          const last = lastDailyMap.get(max.campaignId);

          if (last && dayjs(last).isSame(today, 'day')) return;

          let since = last ? dayjs(last).subtract(2, 'day') : maxStart;

          if (since.isBefore(maxStart)) since = maxStart;
          if (since.isAfter(maxStop)) return;

          const adAccount = new AdAccount(accountId);

          try {
            this.logger.log(
              `📅 ${max.campaignId}: ${since.format(
                'YYYY-MM-DD',
              )} → ${maxStop.format('YYYY-MM-DD')}`,
            );

            const cursor = await adAccount.getInsights(
              AD_INSIGHT_FIELDS,
              {
                limit: 100,
                level: 'campaign',
                time_increment: 1,
                date_preset: 'maximum',
                action_attribution_windows: '7d_click',
                action_breakdowns: 'action_type',
                time_range: {
                  since: since.format('YYYY-MM-DD'),
                  until: maxStop.format('YYYY-MM-DD'),
                },
                filtering: [
                  {
                    field: 'campaign.id',
                    operator: 'EQUAL',
                    value: max.campaignId,
                  },
                ],
              },
              true,
            );

            const insights = await fetchAll(cursor);

            if (!insights.length) return;

            totalFetched += insights.length;

            const valid = insights.filter((i) => i.campaign_id);

            const insightData = valid.map((i) => {
              const metrics = extractCampaignMetrics(i);

              return {
                campaignId: i.campaign_id,
                dateStart: i.date_start,
                range: InsightRange.DAILY,
                data: {
                  dateStop: i.date_start,
                  level: LevelInsight.CAMPAIGN,
                  ...metrics,
                  rawPayload: i,
                },
              };
            });

            await prismaHelper.upsertMany(insightData, (item) =>
              this.prisma.campaignInsight.upsert({
                where: {
                  campaignId_dateStart_range: {
                    campaignId: item.campaignId,
                    dateStart: item.dateStart,
                    range: item.range,
                  },
                },
                update: item.data,
                create: {
                  campaignId: item.campaignId,
                  dateStart: item.dateStart,
                  range: item.range,
                  ...item.data,
                },
              }),
            );

            totalUpserted += insightData.length;
          } catch (error: any) {
            const metaError = parseMetaError(error);
            this.logger.error(
              `❌ DAILY failed ${max.campaignId}: ${metaError.message}`,
            );
          }
        }),
      );

      // nghỉ giữa batch để tránh rate limit
      await sleep(800);
    }

    this.logger.log(
      `✅ DAILY DONE | fetched: ${totalFetched} | upserted: ${totalUpserted}`,
    );
  }

  async syncMaxAdSetInsights() {
    this.logger.log('🔄 Sync MAX Adset Insight');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const adSets = await this.prisma.adSet.findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(adSets);

    let totalProcessed = 0;

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      this.logger.log(`➡️ Account ${accountId} - ${ids.length} adSets`);

      for (const idsChunk of chunk(ids, 50)) {
        try {
          // ================= FETCH =================
          const cursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              limit: 100,
              level: 'adset',
              date_preset: 'maximum',
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              filtering: [
                { field: 'adset.id', operator: 'IN', value: idsChunk },
              ],
            },
            true,
          );

          const insights = await fetchAll(cursor);

          if (!insights.length) {
            this.logger.log(`⚠️ Empty chunk`);
            continue;
          }

          const validInsights = insights.filter((i) => i.campaign_id);

          const adSetIds = [
            ...new Set(validInsights.map((i) => i.campaign_id)),
          ];

          this.logger.log(
            `📦 ${validInsights.length} insights | ${adSetIds.length} adsets`,
          );

          // ================= DELETE (NHẸ) =================
          await this.prisma.adSetInsight.deleteMany({
            where: {
              adSetId: { in: adSetIds },
              range: InsightRange.MAX,
            },
          });

          // ================= TRANSFORM =================
          const insightData = validInsights.map((i) => {
            const metrics = extractCampaignMetrics(i);

            return {
              adSetId: i.adset_id,
              level: LevelInsight.ADSET,
              range: InsightRange.MAX,
              dateStart: i.date_start,
              dateStop: i.date_stop,
              ...metrics,
              rawPayload: i,
            };
          });

          // ================= INSERT =================
          await prismaHelper.createManySafe(
            this.prisma.adSetInsight,
            insightData,
          );

          // ================= UPDATE CAMPAIGN (BATCH) =================
          const adSetUpdateData = validInsights.map((i) => {
            const metrics = extractCampaignMetrics(i);

            return { id: i.adset_id, ...metrics };
          });

          await prismaHelper.upsertMany(adSetUpdateData, (item) =>
            this.prisma.adSet.update({
              where: { id: item.id },
              data: item,
            }),
          );

          totalProcessed += validInsights.length;

          this.logger.log(`✅ Chunk done (${validInsights.length} insights)`);

          await sleep(800);
        } catch (error) {
          this.logger.error(
            `❌ Account ${accountId}: ${parseMetaError(error).message}`,
          );
        }
      }
    }

    this.logger.log(`🎯 DONE MAX adSet Insight - Total: ${totalProcessed}`);
  }

  async syncDailyAdSetInsights() {
    this.logger.log('🔄 Sync DAILY adSet Insights');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);
    const today = dayjs().startOf('day');

    // ================= 1. MAX =================
    const maxInsights = await this.prisma.adSetInsight.findMany({
      where: {
        range: InsightRange.MAX,
        level: LevelInsight.ADSET,
        spend: { gt: 0 },
        adSet: { account: { needsReauth: false } },
      },
      select: {
        dateStart: true,
        dateStop: true,
        adSetId: true,
        adSet: { select: { accountId: true } },
      },
    });

    if (!maxInsights.length) return;

    // ================= 2. LAST DAILY =================
    const lastDailies = await this.prisma.adSetInsight.findMany({
      where: { range: InsightRange.DAILY },
      select: { adSetId: true, dateStart: true },
      orderBy: { dateStart: 'desc' },
    });

    const lastDailyMap = new Map<string, string>();

    for (const d of lastDailies) {
      if (!lastDailyMap.has(d.adSetId)) {
        lastDailyMap.set(d.adSetId, d.dateStart);
      }
    }

    let totalFetched = 0;
    let totalUpserted = 0;

    const BATCH_SIZE = 20;

    for (const batch of chunk(maxInsights, BATCH_SIZE)) {
      await Promise.all(
        batch.map(async (max) => {
          const accountId = max.adSet?.accountId;
          if (!accountId) return;

          const maxStart = dayjs(max.dateStart);
          const maxStopRaw = dayjs(max.dateStop);

          if (maxStopRaw.add(3, 'day').isBefore(today)) return;

          const maxStop = maxStopRaw.isAfter(today) ? today : maxStopRaw;

          const last = lastDailyMap.get(max.adSetId);

          if (last && dayjs(last).isSame(today, 'day')) return;

          let since = last ? dayjs(last).subtract(2, 'day') : maxStart;

          if (since.isBefore(maxStart)) since = maxStart;
          if (since.isAfter(maxStop)) return;

          const adAccount = new AdAccount(accountId);

          try {
            this.logger.log(
              `📅 ${max.adSetId}: ${since.format(
                'YYYY-MM-DD',
              )} → ${maxStop.format('YYYY-MM-DD')}`,
            );

            const cursor = await adAccount.getInsights(
              AD_INSIGHT_FIELDS,
              {
                limit: 100,
                level: 'adset',
                time_increment: 1,
                date_preset: 'maximum',
                action_attribution_windows: '7d_click',
                action_breakdowns: 'action_type',
                time_range: {
                  since: since.format('YYYY-MM-DD'),
                  until: maxStop.format('YYYY-MM-DD'),
                },
                filtering: [
                  {
                    field: 'adset.id',
                    operator: 'EQUAL',
                    value: max.adSetId,
                  },
                ],
              },
              true,
            );

            const insights = await fetchAll(cursor);

            if (!insights.length) return;

            totalFetched += insights.length;

            const valid = insights.filter((i) => i.campaign_id);

            const insightData = valid.map((i) => {
              const metrics = extractCampaignMetrics(i);

              return {
                adSetId: i.adset_id,
                dateStart: i.date_start,
                range: InsightRange.DAILY,
                data: {
                  dateStop: i.date_start,
                  level: LevelInsight.ADSET,
                  ...metrics,
                  rawPayload: i,
                },
              };
            });

            await prismaHelper.upsertMany(insightData, (item) =>
              this.prisma.adSetInsight.upsert({
                where: {
                  adSetId_dateStart_range: {
                    adSetId: item.adSetId,
                    dateStart: item.dateStart,
                    range: item.range,
                  },
                },
                update: item.data,
                create: {
                  adSetId: item.adSetId,
                  dateStart: item.dateStart,
                  range: item.range,
                  ...item.data,
                },
              }),
            );

            totalUpserted += insightData.length;
          } catch (error: any) {
            const metaError = parseMetaError(error);
            this.logger.error(
              `❌ DAILY failed ${max.adSetId}: ${metaError.message}`,
            );
          }
        }),
      );

      // nghỉ giữa batch để tránh rate limit
      await sleep(800);
    }

    this.logger.log(
      `✅ DAILY DONE | fetched: ${totalFetched} | upserted: ${totalUpserted}`,
    );
  }

  // AD
  async syncMaxAdInsights() {
    this.logger.log('🔄 Sync MAX Ad Insight');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const ads = await this.prisma.ad.findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(ads);

    let totalProcessed = 0;

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      this.logger.log(`➡️ Account ${accountId} - ${ids.length} ads`);

      for (const idsChunk of chunk(ids, 50)) {
        try {
          // ================= FETCH =================
          const cursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              limit: 100,
              level: 'ad',
              date_preset: 'maximum',
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              filtering: [{ field: 'ad.id', operator: 'IN', value: idsChunk }],
            },
            true,
          );

          const insights = await fetchAll(cursor);

          if (!insights.length) {
            this.logger.log(`⚠️ Empty chunk`);
            continue;
          }

          const validInsights = insights.filter((i) => i.ad_id);

          const adIds = [...new Set(validInsights.map((i) => i.ad_id))];

          this.logger.log(
            `📦 ${validInsights.length} insights | ${adIds.length} ad`,
          );

          // ================= DELETE (NHẸ) =================
          await this.prisma.adInsight.deleteMany({
            where: {
              adId: { in: adIds },
              range: InsightRange.MAX,
            },
          });

          // ================= TRANSFORM =================
          const insightData = validInsights.map((i) => {
            const metrics = extractCampaignMetrics(i);

            return {
              adId: i.ad_id,
              level: LevelInsight.AD,
              range: InsightRange.MAX,
              dateStart: i.date_start,
              dateStop: i.date_stop,
              ...metrics,
              rawPayload: i,
            };
          });

          // ================= INSERT =================
          await prismaHelper.createManySafe(this.prisma.adInsight, insightData);

          // ================= UPDATE CAMPAIGN (BATCH) =================
          const adUpdateData = validInsights.map((i) => {
            const metrics = extractCampaignMetrics(i);

            return { id: i.ad_id, ...metrics };
          });

          await prismaHelper.upsertMany(adUpdateData, (item) =>
            this.prisma.ad.update({
              where: { id: item.id },
              data: item,
            }),
          );

          totalProcessed += validInsights.length;

          this.logger.log(`✅ Chunk done (${validInsights.length} insights)`);

          await sleep(800);
        } catch (error) {
          this.logger.error(
            `❌ Account ${accountId}: ${parseMetaError(error).message}`,
          );
        }
      }
    }

    this.logger.log(`🎯 DONE MAX adSet Insight - Total: ${totalProcessed}`);
  }

  async syncDailyAdInsights() {
    this.logger.log('🔄 Sync DAILY ad Insights');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);
    const today = dayjs().startOf('day');

    // ================= 1. MAX =================
    const maxInsights = await this.prisma.adInsight.findMany({
      where: {
        range: InsightRange.MAX,
        level: LevelInsight.AD,
        spend: { gt: 0 },
        ad: { account: { needsReauth: false } },
      },
      select: {
        dateStart: true,
        dateStop: true,
        adId: true,
        ad: { select: { accountId: true } },
      },
    });

    if (!maxInsights.length) return;

    // ================= 2. LAST DAILY =================
    const lastDailies = await this.prisma.adInsight.findMany({
      where: { range: InsightRange.DAILY },
      select: { adId: true, dateStart: true },
      orderBy: { dateStart: 'desc' },
    });

    const lastDailyMap = new Map<string, string>();

    for (const d of lastDailies) {
      if (!lastDailyMap.has(d.adId)) {
        lastDailyMap.set(d.adId, d.dateStart);
      }
    }

    let totalFetched = 0;
    let totalUpserted = 0;

    const BATCH_SIZE = 20;

    for (const batch of chunk(maxInsights, BATCH_SIZE)) {
      await Promise.all(
        batch.map(async (max) => {
          const accountId = max.ad?.accountId;
          if (!accountId) return;

          const maxStart = dayjs(max.dateStart);
          const maxStopRaw = dayjs(max.dateStop);

          if (maxStopRaw.add(3, 'day').isBefore(today)) return;

          const maxStop = maxStopRaw.isAfter(today) ? today : maxStopRaw;

          const last = lastDailyMap.get(max.adId);

          if (last && dayjs(last).isSame(today, 'day')) return;

          let since = last ? dayjs(last).subtract(2, 'day') : maxStart;

          if (since.isBefore(maxStart)) since = maxStart;
          if (since.isAfter(maxStop)) return;

          const adAccount = new AdAccount(accountId);

          try {
            this.logger.log(
              `📅 ${max.adId}: ${since.format('YYYY-MM-DD')} → ${maxStop.format(
                'YYYY-MM-DD',
              )}`,
            );

            const cursor = await adAccount.getInsights(
              AD_INSIGHT_FIELDS,
              {
                limit: 100,
                level: 'ad',
                time_increment: 1,
                date_preset: 'maximum',
                action_attribution_windows: '7d_click',
                action_breakdowns: 'action_type',
                time_range: {
                  since: since.format('YYYY-MM-DD'),
                  until: maxStop.format('YYYY-MM-DD'),
                },
                filtering: [
                  {
                    field: 'ad.id',
                    operator: 'EQUAL',
                    value: max.adId,
                  },
                ],
              },
              true,
            );

            const insights = await fetchAll(cursor);

            if (!insights.length) return;

            totalFetched += insights.length;

            const valid = insights.filter((i) => i.ad_id);

            const insightData = valid.map((i) => {
              const metrics = extractCampaignMetrics(i);

              return {
                adId: i.ad_id,
                dateStart: i.date_start,
                range: InsightRange.DAILY,
                data: {
                  dateStop: i.date_start,
                  level: LevelInsight.AD,
                  ...metrics,
                  rawPayload: i,
                },
              };
            });

            await prismaHelper.upsertMany(insightData, (item) =>
              this.prisma.adInsight.upsert({
                where: {
                  adId_dateStart_range: {
                    adId: item.adId,
                    dateStart: item.dateStart,
                    range: item.range,
                  },
                },
                update: item.data,
                create: {
                  adId: item.adId,
                  dateStart: item.dateStart,
                  range: item.range,
                  ...item.data,
                },
              }),
            );

            totalUpserted += insightData.length;
          } catch (error: any) {
            const metaError = parseMetaError(error);
            this.logger.error(
              `❌ DAILY failed ${max.adId}: ${metaError.message}`,
            );
          }
        }),
      );

      // nghỉ giữa batch để tránh rate limit
      await sleep(800);
    }

    this.logger.log(
      `✅ DAILY DONE | fetched: ${totalFetched} | upserted: ${totalUpserted}`,
    );
  }

  async syncMaxAdSetAudienceInsights() {
    this.logger.log('🔄 Sync MAX adset audience Insight');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const adSets = await this.prisma.adSet.findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(adSets);

    let totalProcessed = 0;

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      this.logger.log(
        `➡️ Account ${accountId} - ${ids.length} adSets Audience`,
      );

      for (const idsChunk of chunk(ids, 50)) {
        try {
          // ================= FETCH =================
          const cursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              limit: 100,
              level: 'adset',
              date_preset: 'maximum',
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              breakdowns: ['age', 'gender'],
              filtering: [
                { field: 'adset.id', operator: 'IN', value: idsChunk },
              ],
            },
            true,
          );

          const insights = await fetchAll(cursor);

          if (!insights.length) {
            this.logger.log(`⚠️ Empty chunk`);
            continue;
          }

          const validInsights = insights.filter((i) => i.campaign_id);

          const adSetIds = [
            ...new Set(validInsights.map((i) => i.campaign_id)),
          ];

          this.logger.log(
            `📦 ${validInsights.length} insights | ${adSetIds.length} adsets audience`,
          );

          // ================= DELETE (NHẸ) =================
          await this.prisma.adsetAudienceInsight.deleteMany({
            where: {
              adsetId: { in: adSetIds },
              range: InsightRange.MAX,
            },
          });

          // ================= TRANSFORM =================
          const insightData = validInsights.map((i) => {
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
              rawPayload: i,
            };
          });

          // ================= INSERT =================
          await prismaHelper.createManySafe(
            this.prisma.adsetAudienceInsight,
            insightData,
          );

          // ================= UPDATE CAMPAIGN (BATCH) =================
          totalProcessed += validInsights.length;

          this.logger.log(`✅ Chunk done (${validInsights.length} insights)`);

          await sleep(800);
        } catch (error) {
          this.logger.error(
            `❌ Account ${accountId}: ${parseMetaError(error).message}`,
          );
        }
      }
    }

    this.logger.log(`🎯 DONE MAX adSet Insight - Total: ${totalProcessed}`);
  }

  async calculateCreativeInsightFromAdInsightParallel(batchSize = 20) {
    console.log('🚀 Start calculate CreativeInsight (optimized)...');

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const today = dayjs().format('YYYY-MM-DD');
    const sevenDaysAgo = dayjs().subtract(6, 'day').format('YYYY-MM-DD');
    const threeDaysAgo = dayjs().subtract(2, 'day').format('YYYY-MM-DD');

    const creatives = await this.prisma.creative.findMany({
      select: {
        id: true,
        ads: { select: { id: true } },
      },
    });

    function sumMetrics(target: Record<string, number>, source: any) {
      for (const key in source) {
        if (typeof source[key] === 'number') {
          target[key] = (target[key] || 0) + source[key];
        }
      }
    }

    for (let i = 0; i < creatives.length; i += batchSize) {
      const batch = creatives.slice(i, i + batchSize);

      // ================= 1. GOM AD IDS =================
      const adIds = batch.flatMap((c) => c.ads.map((a) => a.id));

      if (!adIds.length) continue;

      // ================= 2. LOAD 1 LẦN =================
      const insights = await this.prisma.adInsight.findMany({
        where: {
          adId: { in: adIds },
          range: { in: ['DAILY', 'MAX'] },
        },
      });

      // ================= 3. GROUP THEO CREATIVE =================
      const insightMap = new Map<string, any[]>();

      for (const ins of insights) {
        if (!insightMap.has(ins.adId)) {
          insightMap.set(ins.adId, []);
        }
        insightMap.get(ins.adId)!.push(ins);
      }

      // ================= 4. BUILD DATA =================
      const creativeInsightUpserts: any[] = [];
      const creativeUpdates: any[] = [];

      for (const creative of batch) {
        const adIds = creative.ads.map((a) => a.id);
        if (!adIds.length) continue;

        const bucket = {
          daily: {} as Record<string, Record<string, number>>,
          max: {} as Record<string, number>,
          last7d: {} as Record<string, number>,
          last3d: {} as Record<string, number>,
        };

        // 👉 merge tất cả adInsight của creative
        for (const adId of adIds) {
          const adInsights = insightMap.get(adId) || [];

          for (const ins of adInsights) {
            if (ins.range === 'DAILY') {
              if (!bucket.daily[ins.dateStart]) {
                bucket.daily[ins.dateStart] = {};
              }

              sumMetrics(bucket.daily[ins.dateStart], ins);

              if (ins.dateStart >= sevenDaysAgo && ins.dateStart <= today) {
                sumMetrics(bucket.last7d, ins);
              }
              if (ins.dateStart >= threeDaysAgo && ins.dateStart <= today) {
                sumMetrics(bucket.last3d, ins);
              }
            }

            if (ins.range === 'MAX') {
              sumMetrics(bucket.max, ins);
            }
          }
        }

        // ================= PREPARE UPSERT =================
        for (const date in bucket.daily) {
          creativeInsightUpserts.push({
            creativeId: creative.id,
            dateStart: date,
            range: 'DAILY',
            data: {
              dateStop: date,
              ...bucket.daily[date],
            },
          });
        }

        creativeInsightUpserts.push({
          creativeId: creative.id,
          dateStart: '1975-01-01',
          range: 'MAX',
          data: {
            dateStop: today,
            ...bucket.max,
          },
        });

        creativeInsightUpserts.push({
          creativeId: creative.id,
          dateStart: sevenDaysAgo,
          range: 'DAY_7',
          data: {
            dateStop: today,
            ...bucket.last7d,
          },
        });

        creativeInsightUpserts.push({
          creativeId: creative.id,
          dateStart: threeDaysAgo,
          range: 'DAY_3',
          data: {
            dateStop: today,
            ...bucket.last3d,
          },
        });

        // ================= STATUS =================
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

        let status: CreativeStatus;

        if (maxSpend === 0) status = CreativeStatus.TEST;
        else if (maxSpend <= 100_000) status = CreativeStatus.NEED_SPEND;
        else if (
          (maxSpend <= 500_000 && roasMax >= 2) ||
          (maxSpend > 500_000 && roasMax >= 2.2) ||
          roas7d >= 2.5
        )
          status = CreativeStatus.SCALE_P1;
        else if (
          (maxSpend <= 500_000 && roasMax >= 1.5) ||
          (maxSpend > 500_000 && roasMax >= 1.8 && ctrMax > 0.03) ||
          (roas7d >= 2.2 && roas3d >= 2.2)
        )
          status = CreativeStatus.SCALE_P2;
        else if (
          (maxSpend <= 500_000 && maxPurchases < 1 && ctrMax > 0.03) ||
          (maxSpend > 500_000 && roasMax < 1.8 && ctrMax > 0.03)
        )
          status = CreativeStatus.REVIEW;
        else status = CreativeStatus.OFF;

        creativeUpdates.push({
          id: creative.id,
          data: {
            performanceStatus: status,
            ...bucket.max,
          },
        });
      }

      // ================= 5. UPSERT BATCH =================
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

      // ================= 6. UPDATE CREATIVE =================
      await prismaHelper.upsertMany(creativeUpdates, (item) =>
        this.prisma.creative.update({
          where: { id: item.id },
          data: item.data,
        }),
      );

      console.log(
        `✅ Batch ${i / batchSize + 1} done (${batch.length} creatives)`,
      );
    }

    console.log('🎯 DONE CreativeInsight optimized');
  }

  private groupByAccount(records: any[]) {
    return records.reduce<Record<string, string[]>>((acc, r) => {
      (acc[r.accountId] ||= []).push(r.id);
      return acc;
    }, {});
  }

  // XỬ Lý VIDEO

  async syncImage() {
    this.logger.log('🔄 Sync AdImage (optimized)');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    try {
      const existingImages = await this.prisma.adImage.findMany({
        where: {
          account: { needsReauth: false },
          OR: [{ url: null }],
        },
        select: { hash: true, accountId: true },
      });

      if (!existingImages.length) return;

      const byAccount = this.groupByAccount(
        existingImages.map((img) => ({
          id: img.hash,
          accountId: img.accountId,
        })),
      );

      for (const [accountId, hashes] of Object.entries(byAccount)) {
        const adAccount = new AdAccount(accountId);

        for (const hashChunk of chunk(hashes, 50)) {
          try {
            const cursor = await adAccount.getAdImages(AD_IMAGE_FIELDS, {
              limit: 100,
              hashes: hashChunk,
            });

            const images = await fetchAll(cursor);

            if (!images.length) continue;

            const updateData = images.map((img) => ({
              hash: img.hash,
              accountId,
              data: {
                name: img?.name,
                url: img?.permalink_url || img?.url,
                permalink_url: img?.permalink_url,
                height: img?.height,
                width: img?.width,
                rawPayload: toPrismaJson(img),
                status: img?.status,
                createdTime: img?.created_time
                  ? new Date(img.created_time)
                  : undefined,
                createdAt: img?.created_time
                  ? new Date(img.created_time)
                  : undefined,
                updatedAt: new Date(),
              },
            }));

            await prismaHelper.upsertMany(updateData, (item) =>
              this.prisma.adImage.updateMany({
                where: {
                  hash: item.hash,
                  accountId: item.accountId,
                },
                data: item.data,
              }),
            );

            await sleep(800);
          } catch (error) {
            this.logger.error(
              `❌ syncImage ${accountId}: ${parseMetaError(error).message}`,
            );
          }
        }
      }

      this.logger.log(`✅ Updated ${existingImages.length} images`);
    } catch (err) {
      this.logger.error(`❌ syncImage fatal: ${err.message}`);
    }
  }

  async syncVideo() {
    this.logger.log('🔄 Sync AdVideo (optimized)');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);
    const api = FacebookAdsApi.getDefaultApi();

    try {
      const existingVideos = await this.prisma.adVideo.findMany({
        where: {
          account: { needsReauth: false },
          status: null,
          // creatives: {
          //   // some: {
          //   //   NOT: {
          //   //     rawPayload: {
          //   //       path: ['actor_id'],
          //   //       not: null,
          //   //     },
          //   //   },
          //   // },
          // },
        },
        select: { id: true },
      });

      if (!existingVideos.length) return;

      const videoIds = existingVideos.map((v) => v.id);

      for (const idsChunk of chunk(videoIds, 1)) {
        try {
          const response = (await api.call('GET', [], {
            ids: idsChunk.join(','),
            fields: AD_VIDEO_FIELDS.join(','),
          })) as any;

          const videos = Object.values(response || {});

          if (!videos.length) continue;

          const updateData = videos.map((vid: any) => ({
            id: vid.id,
            data: {
              title: vid?.title || vid?.name,
              accountId: vid?.account_id,
              source: vid?.source,
              status: vid?.status?.video_status || vid?.status,
              thumbnailUrl: vid?.thumbnails?.data?.find(
                (tn) => tn?.is_preferred,
              )?.url,
              length: vid?.length,
              rawPayload: toPrismaJson(vid),
              updatedAt: new Date(),
            },
          }));

          await prismaHelper.upsertMany(updateData, (item) =>
            this.prisma.adVideo.update({
              where: { id: item.id },
              data: item.data,
            }),
          );

          this.logger.log(`✅ Updated ${videos.length} videos`);

          await sleep(800);
        } catch (error) {
          this.logger.error(
            `❌ syncVideo chunk: ${parseMetaError(error).message}`,
          );
        }
      }
    } catch (err) {
      this.logger.error(`❌ syncVideo fatal: ${err.message}`);
    }
  }
}
