import {
  Injectable,
  InternalServerErrorException,
  Logger,
  OnModuleInit,
} from '@nestjs/common';
import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';

import { PrismaService } from 'src/modules/prisma/prisma.service';

import {
  buildSubfolderFields,
  chunk,
  extractCampaignMetrics,
  fetchAll,
  fetchAllWithAPIEndpoint,
  flattenFolders,
  FolderNode,
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

import { Cron } from '@nestjs/schedule';
import {
  AssetType,
  CreativeStatus,
  InsightRange,
  LevelInsight,
} from '@prisma/client';
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
    // this.logger.log('🚀 TaskCron initialized');
    await this.syncVideoBM();
    // await this.syncDailyCampaignInsightsJob();
    // await this.syncDailyAdsetInsightsJob();
    // await this.syncDailyAdInsightsJob();
  }

  /**
   * ================================
   * 🔹 CORE DATA (1 lần / ngày)
   * ================================
   */

  @Cron('5 0 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncCampaignCore() {
    this.logger.log('🔄 Sync Campaign Core');
    await this.syncCampaignData();
    this.logger.log('✅ Sync Campaign Core DONE');
  }

  @Cron('5 21 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncImageData() {
    this.logger.log('🔄 Sync Image Core');
    await this.syncImage();
    this.logger.log('✅ Sync Image DONE');
  }

  @Cron('5 22 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncVideoData() {
    this.logger.log('🔄 Sync Video Core');
    await this.syncVideo();
    this.logger.log('✅ Sync Video DONE');
  }

  @Cron('5 20 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncFolderData() {
    this.logger.log('🔄 Sync Asset Core');
    await this.syncFolderCreative();
    this.logger.log('✅ Sync asset DONE');
  }

  /**
   * ================================
   * 🔹 MAX INSIGHTS (1 lần / ngày)
   * Pipeline: Campaign → AdSet → Audience → Ad
   * ================================
   */

  @Cron('5 1 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxCampaignInsightsJob() {
    this.logger.log('🔄 Sync MAX Campaign Insights');
    await this.syncAllCampaignInsights();
    this.logger.log('✅ MAX Campaign DONE');
  }

  @Cron('15 2 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdsetInsightsJob() {
    this.logger.log('🔄 Sync MAX Adset Insights');
    await this.syncAllAdSetInsights();
    this.logger.log('✅ MAX Adset DONE');
  }

  @Cron('25 3 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdInsightsJob() {
    this.logger.log('🔄 Sync MAX Ad Insights');
    await this.syncAllAdInsights();
    this.logger.log('✅ MAX Ad DONE');
  }

  @Cron('35 4 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdsetAudienceInsightsJob() {
    this.logger.log('🔄 Sync MAX Adset Audience Insights');
    await this.syncMaxAdSetAudienceInsights();
    this.logger.log('✅ MAX Adset Audience DONE');
  }

  /**
   * ================================
   * 🔁 DAILY INSIGHTS (3 lần / ngày)
   * Pipeline chuẩn:
   * Campaign → AdSet → Ad → Creative
   * ================================
   */

  /**
   * 🟢 Campaign DAILY
   */
  @Cron('0 6,12,18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyCampaignInsightsJob() {
    this.logger.log('🔄 Sync DAILY Campaign Insights');
    await this.syncDailyCampaignInsights();
    this.logger.log('✅ DAILY Campaign DONE');
  }

  /**
   * 🟡 AdSet DAILY (delay sau Campaign)
   */
  @Cron('15 6,12,18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyAdsetInsightsJob() {
    this.logger.log('🔄 Sync DAILY Adset Insights');
    await this.syncDailyAdSetInsights();
    this.logger.log('✅ DAILY Adset DONE');
  }

  /**
   * 🔵 Ad DAILY + Creative Analytics (delay sau AdSet)
   */
  @Cron('30 6 * * *', { timeZone: 'Asia/Ho_Chi_Minh' }) // 06:30
  @Cron('0 13 * * *', { timeZone: 'Asia/Ho_Chi_Minh' }) // 13:00
  @Cron('40 18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' }) // 18:40
  async syncDailyAdInsightsJob() {
    this.logger.log('🔄 Sync DAILY Ad Insights');
    await this.syncDailyAdInsights();
    this.logger.log('✅ DAILY Ad DONE');

    this.logger.log('🔄 Analytic Creative Insight');
    await this.calculateCreativeInsightFromAdInsight();
    this.logger.log('✅ Analytic Creative Insight DONE');
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

    const videoMap = new Map<string, any>();
    const imageMap = new Map<string, any>();

    const fanpages = await this.prisma.fanpage.findMany({
      where: { id: { in: pageIds } },
    });

    const fanpageMap = new Map(fanpages.map((f) => [f.id, f]));

    for (const item of creativeData) {
      // ✅ map systemPageId
      if (item.pageId && fanpageMap.has(item.pageId)) {
        item.systemPageId = item.pageId;
      }

      // ✅ VIDEO (dedup theo videoId)
      if (item.videoId && !videoMap.has(item.videoId)) {
        videoMap.set(item.videoId, {
          id: item.videoId,
          accountId: item.accountId,
          thumbnailUrl: item.thumbnailUrl,
        });
      }

      // ✅ IMAGE (dedup theo accountId + hash)
      if (item.imageHash) {
        const key = `${(item.accountId as string).replaceAll('act_', '')}:${
          item.imageHash
        }`;

        if (!imageMap.has(key)) {
          imageMap.set(key, {
            id: key,
            hash: item.imageHash,
            accountId: item.accountId,
            url: item.thumbnailUrl,
          });
          item.imageId = key;
        }
      }
    }

    const newVideos = Array.from(videoMap.values());
    const newImages = Array.from(imageMap.values());

    await prismaHelper.createManySafe(this.prisma.adImage, newImages, 20);

    await prismaHelper.createManySafe(this.prisma.adVideo, newVideos, 20);

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
              limit: 10,
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

  async syncAllAdSetInsights() {
    this.logger.log('🔄 Sync MAX + 3D + 7D AdSet Insight');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const adSets = await this.prisma.adSet.findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(adSets);

    let totalProcessed = 0;

    // 🔥 3 JOBS
    const JOBS = [
      {
        range: InsightRange.MAX,
        datePreset: 'maximum',
        field: 'insightMaxId',
      },
      {
        range: InsightRange.DAY_3,
        datePreset: 'last_3d',
        field: 'insight3dId',
      },
      {
        range: InsightRange.DAY_7,
        datePreset: 'last_7d',
        field: 'insight7dId',
      },
      {
        range: InsightRange.TODAY,
        datePreset: 'today',
        field: 'insightTodayId',
      },
    ];

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      this.logger.log(`➡️ Account ${accountId} - ${ids.length} adSets`);

      for (const idsChunk of chunk(ids, 50)) {
        try {
          // 🔥 CALL API SONG SONG
          const results = await Promise.all(
            JOBS.map(async (job) => {
              const cursor = await adAccount.getInsights(
                AD_INSIGHT_FIELDS,
                {
                  limit: 100,
                  level: 'adset',
                  date_preset: job.datePreset,
                  action_attribution_windows: '7d_click',
                  action_breakdowns: 'action_type',
                  filtering: [
                    { field: 'adset.id', operator: 'IN', value: idsChunk },
                  ],
                },
                true,
              );

              const insights = await fetchAll(cursor);

              return {
                job,
                insights: insights.filter((i) => i.adset_id),
              };
            }),
          );

          // ================= PROCESS =================
          for (const { job, insights } of results) {
            if (!insights.length) continue;

            const adSetIds = [...new Set(insights.map((i) => i.adset_id))];

            this.logger.log(`📦 ${job.range}: ${insights.length} insights`);

            // ❗ giữ logic MAX (delete)

            await this.prisma.adSetInsight.deleteMany({
              where: {
                adSetId: { in: adSetIds },
                range: job.range,
              },
            });

            // ================= TRANSFORM =================
            const insightData = insights.map((i) => {
              const metrics = extractCampaignMetrics(i);

              return {
                adSetId: i.adset_id,
                level: LevelInsight.ADSET,
                range: job.range,
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

            // ================= GET INSERTED IDS =================
            const inserted = await this.prisma.adSetInsight.findMany({
              where: {
                adSetId: { in: adSetIds },
                range: job.range,
              },
              select: { id: true, adSetId: true },
            });

            const map = new Map(inserted.map((i) => [i.adSetId, i.id]));

            // ================= UPDATE ADSET =================
            await prismaHelper.upsertMany(adSetIds, (id) => {
              const insightId = map.get(id) || null;

              const data: any = {
                [job.field]: insightId,
              };

              // 🔥 giữ logic MAX (update metrics)
              if (job.range === InsightRange.MAX) {
                const insight = insights.find((i) => i.adset_id === id);
                if (insight) {
                  Object.assign(data, extractCampaignMetrics(insight));
                }
              }

              return this.prisma.adSet.update({
                where: { id },
                data,
              });
            });

            totalProcessed += insights.length;

            this.logger.log(
              `✅ ${job.range} done (${insights.length} insights)`,
            );
          }

          await sleep(800);
        } catch (error) {
          this.logger.error(
            `❌ Account ${accountId}: ${parseMetaError(error).message}`,
          );
        }
      }
    }

    this.logger.log(
      `🎯 DONE MAX + 3D + 7D AdSet Insight - Total: ${totalProcessed}`,
    );
  }

  async syncAllCampaignInsights() {
    this.logger.log('🔄 Sync MAX + 3D + 7D Campaign Insight');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const campaigns = await this.prisma.campaign.findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(campaigns);

    let totalProcessed = 0;

    // 🔥 3 JOBS
    const JOBS = [
      {
        range: InsightRange.MAX,
        datePreset: 'maximum',
        campaignField: 'insightMaxId',
      },
      {
        range: InsightRange.DAY_3,
        datePreset: 'last_3d',
        campaignField: 'insight3dId',
      },
      {
        range: InsightRange.DAY_7,
        datePreset: 'last_7d',
        campaignField: 'insight7dId',
      },

      {
        range: InsightRange.TODAY,
        datePreset: 'today',
        campaignField: 'insightTodayId',
      },
    ];

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      this.logger.log(`➡️ Account ${accountId} - ${ids.length} campaigns`);

      for (const idsChunk of chunk(ids, 50)) {
        try {
          // 🔥 chạy song song 3 API (nhanh hơn 3x)
          const results = await Promise.all(
            JOBS.map(async (job) => {
              const cursor = await adAccount.getInsights(
                AD_INSIGHT_FIELDS,
                {
                  limit: 100,
                  level: 'campaign',
                  date_preset: job.datePreset,
                  action_attribution_windows: '7d_click',
                  action_breakdowns: 'action_type',
                  filtering: [
                    { field: 'campaign.id', operator: 'IN', value: idsChunk },
                  ],
                },
                true,
              );

              const insights = await fetchAll(cursor);

              return {
                job,
                insights: insights.filter((i) => i.campaign_id),
              };
            }),
          );

          // ================= PROCESS =================
          for (const { job, insights } of results) {
            if (!insights.length) continue;

            const campaignIds = [
              ...new Set(insights.map((i) => i.campaign_id)),
            ];

            this.logger.log(`📦 ${job.range}: ${insights.length} insights`);

            await this.prisma.campaignInsight.deleteMany({
              where: {
                campaignId: { in: campaignIds },
                range: job.range,
              },
            });

            // ================= TRANSFORM =================
            const insightData = insights.map((i) => {
              const metrics = extractCampaignMetrics(i);

              return {
                campaignId: i.campaign_id,
                level: LevelInsight.CAMPAIGN,
                range: job.range,
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

            // ================= GET INSERTED IDS =================
            const inserted = await this.prisma.campaignInsight.findMany({
              where: {
                campaignId: { in: campaignIds },
                range: job.range,
              },
              select: { id: true, campaignId: true },
            });

            const map = new Map(inserted.map((i) => [i.campaignId, i.id]));

            // ================= UPDATE CAMPAIGN =================
            await prismaHelper.upsertMany(campaignIds, (id) => {
              const insightId = map.get(id) || null;

              const data: any = {
                [job.campaignField]: insightId,
              };

              // 🔥 giữ logic MAX (update metrics vào campaign)
              if (job.range === InsightRange.MAX) {
                const insight = insights.find((i) => i.campaign_id === id);
                if (insight) {
                  Object.assign(data, extractCampaignMetrics(insight));
                }
              }

              return this.prisma.campaign.update({
                where: { id },
                data,
              });
            });

            totalProcessed += insights.length;

            this.logger.log(
              `✅ ${job.range} done (${insights.length} insights)`,
            );
          }

          await sleep(800);
        } catch (error) {
          this.logger.error(
            `❌ Account ${accountId}: ${parseMetaError(error).message}`,
          );
        }
      }
    }

    this.logger.log(
      `🎯 DONE MAX + 3D + 7D Campaign Insight - Total: ${totalProcessed}`,
    );
  }

  // AD
  async syncAllAdInsights() {
    this.logger.log('🔄 Sync MAX + 3D + 7D Ad Insight');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const ads = await this.prisma.ad.findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(ads);

    let totalProcessed = 0;

    const JOBS = [
      {
        range: InsightRange.MAX,
        datePreset: 'maximum',
        field: 'insightMaxId',
      },
      {
        range: InsightRange.DAY_3,
        datePreset: 'last_3d',
        field: 'insight3dId',
      },
      {
        range: InsightRange.DAY_7,
        datePreset: 'last_7d',
        field: 'insight7dId',
      },
      {
        range: InsightRange.TODAY,
        datePreset: 'today',
        field: 'insightTodayId',
      },
    ];

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      this.logger.log(`➡️ Account ${accountId} - ${ids.length} ads`);

      for (const idsChunk of chunk(ids, 50)) {
        try {
          // 🔥 CALL API SONG SONG
          const results = await Promise.all(
            JOBS.map(async (job) => {
              const cursor = await adAccount.getInsights(
                AD_INSIGHT_FIELDS,
                {
                  limit: 100,
                  level: 'ad',
                  date_preset: job.datePreset,
                  action_attribution_windows: '7d_click',
                  action_breakdowns: 'action_type',
                  filtering: [
                    { field: 'ad.id', operator: 'IN', value: idsChunk },
                  ],
                },
                true,
              );

              const insights = await fetchAll(cursor);

              return {
                job,
                insights: insights.filter((i) => i.ad_id),
              };
            }),
          );

          // ================= PROCESS =================
          for (const { job, insights } of results) {
            if (!insights.length) continue;

            const adIds = [...new Set(insights.map((i) => i.ad_id))];

            this.logger.log(`📦 ${job.range}: ${insights.length} insights`);

            // 🔥 giữ logic MAX

            await this.prisma.adInsight.deleteMany({
              where: {
                adId: { in: adIds },
                range: job.range,
              },
            });

            // ================= TRANSFORM =================
            const insightData = insights.map((i) => {
              const metrics = extractCampaignMetrics(i);

              return {
                adId: i.ad_id,
                level: LevelInsight.AD,
                range: job.range,
                dateStart: i.date_start,
                dateStop: i.date_stop,
                ...metrics,
                rawPayload: i,
              };
            });

            // ================= INSERT =================
            await prismaHelper.createManySafe(
              this.prisma.adInsight,
              insightData,
            );

            // ================= GET INSERTED IDS =================
            const inserted = await this.prisma.adInsight.findMany({
              where: {
                adId: { in: adIds },
                range: job.range,
              },
              select: { id: true, adId: true },
            });

            const map = new Map(inserted.map((i) => [i.adId, i.id]));

            // ================= UPDATE AD =================
            await prismaHelper.upsertMany(adIds, (id) => {
              const insightId = map.get(id) || null;

              const data: any = {
                [job.field]: insightId,
              };

              // 🔥 giữ logic MAX (update metrics)
              if (job.range === InsightRange.MAX) {
                const insight = insights.find((i) => i.ad_id === id);
                if (insight) {
                  Object.assign(data, extractCampaignMetrics(insight));
                }
              }

              return this.prisma.ad.update({
                where: { id },
                data,
              });
            });

            totalProcessed += insights.length;

            this.logger.log(
              `✅ ${job.range} done (${insights.length} insights)`,
            );
          }

          await sleep(800);
        } catch (error) {
          this.logger.error(
            `❌ Account ${accountId}: ${parseMetaError(error).message}`,
          );
        }
      }
    }

    this.logger.log(
      `🎯 DONE MAX + 3D + 7D Ad Insight - Total: ${totalProcessed}`,
    );
  }

  // SYNC DAILY
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

            const todayInsight = insightData.find(
              (item) => item.dateStart == today.format('YYYY-MM-DD'),
            );

            if (todayInsight) {
              this.prisma.campaignInsight.upsert({
                where: {
                  campaignId_dateStart_range: {
                    dateStart: todayInsight.dateStart,
                    campaignId: todayInsight.campaignId,
                    range: InsightRange.TODAY,
                  },
                  dateStop: todayInsight.dateStart,
                },
                update: {
                  ...todayInsight.data,
                  range: InsightRange.TODAY,
                },
                create: {
                  ...todayInsight.data,
                  range: InsightRange.TODAY,
                  campaignId: todayInsight.campaignId,
                  dateStart: todayInsight.dateStart,
                },
              });
            }

            await prismaHelper.upsertMany(insightData, (item) => {
              if (item.dateStart == today.format('YYYY-MM-DD'))
                return this.prisma.campaignInsight.upsert({
                  where: {
                    campaignId_dateStart_range: {
                      campaignId: item.campaignId,
                      dateStart: item.dateStart,
                      range: InsightRange.TODAY,
                    },
                    dateStop: item.dateStart,
                  },
                  update: { ...item.data, range: InsightRange.TODAY },
                  create: {
                    ...item.data,
                    campaignId: item.campaignId,
                    range: InsightRange.TODAY,
                    dateStart: item.dateStart,
                    dateStop: item.dateStart,
                  },
                });
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

            const todayInsight = insightData.find(
              (item) => item.dateStart == today.format('YYYY-MM-DD'),
            );

            if (todayInsight) {
              this.prisma.adSetInsight.upsert({
                where: {
                  adSetId_dateStart_range: {
                    dateStart: todayInsight.dateStart,
                    adSetId: todayInsight.adSetId,
                    range: InsightRange.TODAY,
                  },
                  dateStop: todayInsight.dateStart,
                },
                update: { ...todayInsight.data, range: InsightRange.TODAY },
                create: {
                  ...todayInsight.data,
                  range: InsightRange.TODAY,
                  adSetId: todayInsight.adSetId,
                  dateStart: todayInsight.dateStart,
                },
              });
            }

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
                  { field: 'ad.id', operator: 'EQUAL', value: max.adId },
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

            const todayInsight = insightData.find(
              (item) => item.dateStart == today.format('YYYY-MM-DD'),
            );

            if (todayInsight) {
              this.prisma.adInsight.upsert({
                where: {
                  adId_dateStart_range: {
                    dateStart: todayInsight.dateStart,
                    adId: todayInsight.adId,
                    range: InsightRange.TODAY,
                  },
                  dateStop: todayInsight.dateStart,
                },
                update: {
                  ...todayInsight.data,
                  range: InsightRange.TODAY,
                },
                create: {
                  ...todayInsight.data,
                  range: InsightRange.TODAY,
                  adId: todayInsight.adId,
                  dateStart: todayInsight.dateStart,
                },
              });
            }

            await prismaHelper.upsertMany(insightData, (item) => {
              if (item.dateStart === today.format('YYYY-MM-DD'))
                return this.prisma.adInsight.upsert({
                  where: {
                    adId_dateStart_range: {
                      adId: item.adId,
                      dateStart: item.dateStart,
                      range: InsightRange.TODAY,
                    },
                  },
                  update: { ...item.data, range: InsightRange.TODAY },
                  create: {
                    ...item.data,
                    range: InsightRange.TODAY,
                    adId: item.adId,
                    dateStart: item.dateStart,
                    dateStop: item.dateStart,
                  },
                });
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

  // SYNC AUDIENT
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

  // SYNC CREATIVE

  async calculateCreativeInsightFromAdInsight(batchSize = 50) {
    console.log('🚀 Start calculate CreativeInsight FINAL...');

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const today = dayjs().format('YYYY-MM-DD');
    const sevenDaysAgo = dayjs().subtract(6, 'day').format('YYYY-MM-DD');
    const threeDaysAgo = dayjs().subtract(2, 'day').format('YYYY-MM-DD');

    // ================= LOAD CREATIVE =================
    const creatives = await this.prisma.creative.findMany({
      select: {
        id: true,
        ads: { select: { id: true } },
      },
    });

    // ================= HELPER =================
    function sumMetrics(target: Record<string, number>, source: any) {
      for (const key in source) {
        if (typeof source[key] === 'number') {
          target[key] = (target[key] || 0) + source[key];
        }
      }
    }

    // ================= LOOP BATCH =================
    for (let i = 0; i < creatives.length; i += batchSize) {
      const batch = creatives.slice(i, i + batchSize);

      const adIds = batch.flatMap((c) => c.ads.map((a) => a.id));
      if (!adIds.length) continue;

      // ================= LOAD INSIGHT =================
      const insights = await this.prisma.adInsight.findMany({
        where: {
          adId: { in: adIds },
          range: { in: ['MAX', 'DAY_7', 'DAY_3', 'TODAY'] }, // 🔥 bỏ DAILY
        },
      });

      // ================= GROUP BY AD =================
      const insightMap = new Map<string, any[]>();
      for (const ins of insights) {
        if (!insightMap.has(ins.adId)) {
          insightMap.set(ins.adId, []);
        }
        insightMap.get(ins.adId)!.push(ins);
      }

      const creativeInsightUpserts: any[] = [];
      const creativeUpdates: any[] = [];

      // ================= LOOP CREATIVE =================
      for (const creative of batch) {
        const adIds = creative.ads.map((a) => a.id);
        if (!adIds.length) continue;

        const bucket = {
          max: {} as Record<string, number>,
          last7d: {} as Record<string, number>,
          last3d: {} as Record<string, number>,
          today: {} as Record<string, number>,
        };

        // ================= MERGE =================
        for (const adId of adIds) {
          const adInsights = insightMap.get(adId) || [];

          for (const ins of adInsights) {
            if (ins.range === 'MAX') sumMetrics(bucket.max, ins);
            if (ins.range === 'DAY_7') sumMetrics(bucket.last7d, ins);
            if (ins.range === 'DAY_3') sumMetrics(bucket.last3d, ins);
            if (ins.range === 'TODAY') sumMetrics(bucket.today, ins);
          }
        }

        // ================= UPSERT PAYLOAD =================
        creativeInsightUpserts.push(
          {
            creativeId: creative.id,
            dateStart: '1975-01-01',
            range: 'MAX',
            data: { dateStop: today, ...bucket.max },
          },
          {
            creativeId: creative.id,
            dateStart: sevenDaysAgo,
            range: 'DAY_7',
            data: { dateStop: today, ...bucket.last7d },
          },
          {
            creativeId: creative.id,
            dateStart: threeDaysAgo,
            range: 'DAY_3',
            data: { dateStop: today, ...bucket.last3d },
          },

          {
            creativeId: creative.id,
            dateStart: today,
            range: 'TODAY',
            data: { dateStop: today, ...bucket.today },
          },
        );

        // ================= CALCULATE STATUS =================
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

        if (maxSpend === 0) status = CreativeStatus.OTHER;
        else if (maxSpend <= 100_000) status = CreativeStatus.NEED_SPEND;
        else if (
          ((maxSpend <= 500_000 && roasMax >= 2) ||
            (maxSpend > 500_000 && roasMax >= 2.2)) &&
          roas7d >= 2.5
        ) {
          status = CreativeStatus.SCALE_P1;
        } else if (
          ((maxSpend <= 500_000 && roasMax >= 1.5) ||
            (maxSpend > 500_000 && roasMax >= 1.8 && ctrMax > 0.03)) &&
          roas7d >= 2.2 &&
          roas3d >= 2.2
        ) {
          status = CreativeStatus.SCALE_P2;
        } else if (
          (maxSpend <= 500_000 && maxPurchases < 1 && ctrMax > 0.03) ||
          (maxSpend > 500_000 && roasMax < 1.8 && ctrMax > 0.03)
        ) {
          status = CreativeStatus.REVIEW;
        } else if (
          (maxSpend <= 500_000 && maxPurchases < 1 && ctrMax < 0.03) ||
          (maxSpend > 500_000 && roasMax < 1.8 && ctrMax < 0.03)
        ) {
          status = CreativeStatus.OFF;
        } else {
          status = CreativeStatus.OTHER;
        }

        creativeUpdates.push({
          id: creative.id,
          data: {
            performanceStatus: status,
            ...bucket.max,
          },
        });
      }

      // ================= UPSERT INSIGHT =================
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

      // ================= FETCH INSIGHT ID =================
      const insightRecords = await this.prisma.creativeInsight.findMany({
        where: {
          creativeId: { in: batch.map((c) => c.id) },
          range: { in: ['MAX', 'DAY_7', 'DAY_3', 'TODAY'] },
        },
        select: {
          id: true,
          creativeId: true,
          range: true,
        },
      });

      const insightMapByCreative = new Map<
        string,
        { max?: string; d7?: string; d3?: string; today?: string }
      >();

      for (const r of insightRecords) {
        if (!insightMapByCreative.has(r.creativeId)) {
          insightMapByCreative.set(r.creativeId, {});
        }

        const obj = insightMapByCreative.get(r.creativeId)!;

        if (r.range === 'MAX') obj.max = r.id;
        if (r.range === 'DAY_7') obj.d7 = r.id;
        if (r.range === 'DAY_3') obj.d3 = r.id;
        if (r.range === 'TODAY') obj.today = r.id;
      }

      // ================= UPDATE CREATIVE =================
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

      console.log(
        `✅ Batch ${i / batchSize + 1} done (${batch.length} creatives)`,
      );
    }

    console.log('🎯 DONE CreativeInsight FINAL');
  }

  // HELPER
  private groupByAccount(records: any[]) {
    return records.reduce<Record<string, string[]>>((acc, r) => {
      (acc[r.accountId] ||= []).push(r.id);
      return acc;
    }, {});
  }

  async syncVideo() {
    this.logger.log('🔄 Sync Ad Video (optimized)');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    try {
      const existingVideos = await this.prisma.adVideo.findMany({
        where: { account: { needsReauth: false } },
        select: { id: true, accountId: true, thumbnailUrl: true },
      });

      if (!existingVideos.length) {
        this.logger.log('⚠️ No videos to sync');
        return;
      }

      this.logger.log(`📦 Total videos: ${existingVideos.length}`);

      const byAccount = this.groupByAccount(
        existingVideos.map((vid) => ({
          id: vid.id,
          accountId: vid.accountId,
        })),
      );

      let totalProcessed = 0;

      for (const [accountId, ids] of Object.entries(byAccount)) {
        this.logger.log(
          `\n==============================\n📊 ACCOUNT: ${accountId}\n📦 VIDEOS: ${ids.length}\n==============================`,
        );

        const adAccount = new AdAccount(accountId);

        let chunkIndex = 0;

        for (const hashChunk of chunk(ids, 50)) {
          chunkIndex++;

          this.logger.log(
            `\n➡️ [${accountId}] Chunk ${chunkIndex} | size: ${hashChunk.length}`,
          );

          this.logger.log(`🆔 IDs: ${hashChunk.map((i) => i).join(', ')}`);

          try {
            const cursor = await adAccount.getAdVideos(AD_VIDEO_FIELDS, {
              limit: 100,
              filtering: [{ field: 'id', operator: 'IN', value: hashChunk }],
            });

            const videos = await fetchAll(cursor);

            this.logger.log(
              `📥 API returned ${videos.length} videos for chunk ${chunkIndex}`,
            );

            if (!videos.length) continue;

            const updateData = videos.map((vid) => {
              const currentVideo = existingVideos.find((v) => v.id == vid.id);

              const hasThumb =
                currentVideo?.thumbnailUrl?.includes('https://scontent');

              return {
                id: vid.id,
                accountId,
                data: {
                  title: vid?.title || vid?.name,
                  accountId: vid?.account_id,
                  source: vid?.source,
                  status: vid?.status?.video_status || vid?.status,
                  thumbnailUrl: hasThumb
                    ? vid?.thumbnails?.data?.find((tn) => tn?.is_preferred)?.url
                    : undefined,
                  length: vid?.length,
                  rawPayload: toPrismaJson(vid),
                  updatedAt: new Date(),
                },
              };
            });

            this.logger.log(
              `🛠 Upserting ${updateData.length} videos (chunk ${chunkIndex})`,
            );

            await prismaHelper.upsertMany(updateData, (item) =>
              this.prisma.adVideo.updateMany({
                where: {
                  id: item.id,
                  accountId: item.accountId,
                },
                data: item.data,
              }),
            );

            totalProcessed += updateData.length;

            this.logger.log(
              `✅ Chunk ${chunkIndex} done | total processed: ${totalProcessed}`,
            );

            await sleep(800);
          } catch (error) {
            this.logger.error(
              `❌ syncVideo ${accountId} chunk ${chunkIndex}: ${parseMetaError(error).message}`,
            );
          }
        }

        this.logger.log(
          `🎯 ACCOUNT DONE: ${accountId} | processed: ${totalProcessed}`,
        );
      }

      this.logger.log(`🏁 DONE syncVideo | TOTAL: ${totalProcessed}`);
    } catch (err) {
      this.logger.error(`❌ syncVideo fatal: ${parseMetaError(err).message}`);
    }
  }

  async syncVideoBM() {
    this.logger.log('🔄 Sync Ad Video (optimized)');
    this.init();
    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    try {
      const existingVideos = await this.prisma.adVideo.findMany({
        where: {
          account: { needsReauth: false },
          status: null,
        },
        select: { id: true, accountId: true, thumbnailUrl: true },
      });

      if (!existingVideos.length) {
        this.logger.log('⚠️ No videos to sync');
        return;
      }

      this.logger.log(`📦 Total videos: ${existingVideos.length}`);

      const byAccount = this.groupByAccount(
        existingVideos.map((vid) => ({
          id: vid.id,
          accountId: vid.accountId,
        })),
      );

      let totalProcessed = 0;

      for (const [accountId, ids] of Object.entries(byAccount)) {
        this.logger.log(
          `\n==============================\n📊 ACCOUNT: ${accountId}\n📦 VIDEOS: ${ids.length}\n==============================`,
        );

        let chunkIndex = 0;

        for (const hashChunk of chunk(ids, 10)) {
          (await Promise.all(
            hashChunk.map(async (id) => {
              try {
                const cursor = await api.call('GET', [''], {
                  ids: id,
                  fields: AD_VIDEO_FIELDS,
                });

                const videos = Object.values(cursor);

                this.logger.log(
                  `📥 API returned ${videos.length} videos for chunk ${chunkIndex}`,
                );

                const updateData = videos.map((vid) => {
                  const currentVideo = existingVideos.find(
                    (v) => v.id == vid.id,
                  );

                  const hasThumb =
                    currentVideo?.thumbnailUrl?.includes('https://scontent');

                  return {
                    id: vid.id,
                    accountId,
                    data: {
                      title: vid?.title || vid?.name,
                      accountId: vid?.account_id,
                      source: vid?.source,
                      status: vid?.status?.video_status || vid?.status,
                      thumbnailUrl: hasThumb
                        ? vid?.thumbnails?.data?.find((tn) => tn?.is_preferred)
                            ?.url
                        : undefined,
                      length: vid?.length,
                      rawPayload: toPrismaJson(vid),
                      updatedAt: new Date(),
                    },
                  };
                });

                this.logger.log(
                  `🛠 Upserting ${updateData.length} videos (chunk ${chunkIndex})`,
                );

                await prismaHelper.upsertMany(updateData, (item) =>
                  this.prisma.adVideo.updateMany({
                    where: {
                      id: item.id,
                      accountId: item.accountId,
                    },
                    data: item.data,
                  }),
                );

                totalProcessed += updateData.length;

                this.logger.log(
                  `✅ Chunk ${chunkIndex} done | total processed: ${totalProcessed}`,
                );

                await sleep(800);
              } catch (error) {
                this.logger.error(
                  `❌ syncVideo ${accountId} chunk ${chunkIndex}: ${parseMetaError(error).message}`,
                );
              }
            }),
          ),
            chunkIndex++);

          this.logger.log(
            `\n➡️ [${accountId}] Chunk ${chunkIndex} | size: ${hashChunk.length}`,
          );

          this.logger.log(`🆔 IDs: ${hashChunk.map((i) => i).join(', ')}`);
        }

        this.logger.log(
          `🎯 ACCOUNT DONE: ${accountId} | processed: ${totalProcessed}`,
        );
      }

      this.logger.log(`🏁 DONE syncVideo | TOTAL: ${totalProcessed}`);
    } catch (err) {
      this.logger.error(`❌ syncVideo fatal: ${parseMetaError(err).message}`);
    }
  }

  async syncImage() {
    this.logger.log('🔄 Sync AdImage (optimized)');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    try {
      const existingImages = await this.prisma.adImage.findMany({
        where: {
          account: { needsReauth: false },
          status: null,
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
      this.logger.error(`❌ syncImage fatal: ${parseMetaError(err).message}`);
    }
  }

  async syncFolderCreative() {
    this.logger.log('🔄 Sync Creative Folder (optimized)');
    this.init();

    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const ACCOUNT_ID = '1916878948527753';

    try {
      // 1️⃣ Fetch folders
      this.logger.log('📁 [1/5] Fetching folders from Meta API...');

      const foldersRes = (await api.call(
        'GET',
        [ACCOUNT_ID, 'creative_folders'],
        {
          limit: 100,
          fields: [
            'id',
            'name',
            'creation_time',
            'description',
            'parent_folder{id,name}',
            buildSubfolderFields(3),
          ],
        },
      )) as { data: FolderNode[] };

      const folderData = flattenFolders(foldersRes?.data ?? [], undefined, []);

      this.logger.log(`📁 Total folders fetched: ${folderData.length}`);

      await this.prisma.creativeFolder.createMany({
        data: folderData,
        skipDuplicates: true,
      });

      this.logger.log('✅ Folders saved to DB');

      const folders = await this.prisma.creativeFolder.findMany({
        select: { id: true },
      });

      this.logger.log(`📁 Total folders in DB: ${folders.length}`);

      // 2️⃣ Load existing assets
      this.logger.log('🧠 [2/5] Loading existing assets from DB...');

      const existingAssets = await this.prisma.creativeAsset.findMany({
        select: { id: true, folderId: true },
      });

      this.logger.log(`🧠 Existing assets: ${existingAssets.length}`);

      const existingSet = new Set(
        existingAssets.map((a) => `${a.id}_${a.folderId}`),
      );

      // 3️⃣ Fetch creatives per folder
      this.logger.log('🎨 [3/5] Fetching creatives per folder...');

      const CONCURRENCY = 5;
      let processedFolders = 0;

      for (const folderChunk of chunk(folders, CONCURRENCY)) {
        this.logger.log(
          `📦 Processing folder chunk (${processedFolders}/${folders.length})`,
        );

        await Promise.all(
          folderChunk.map(async (folder) => {
            try {
              this.logger.log(`➡️ Fetch creatives for folder ${folder.id}`);

              const cursor = await api.call('GET', [ACCOUNT_ID, 'creatives'], {
                limit: 50,
                fields: [
                  'id',
                  'name',
                  'type',
                  'url',
                  'hash',
                  'width',
                  'height',
                  'duration',
                  'thumbnail',
                  'video_id',
                  'creation_time',
                ],
                creative_folder_id: folder.id,
              });

              const assets = await fetchAllWithAPIEndpoint(cursor);

              this.logger.log(
                `📂 Folder ${folder.id} → fetched ${assets.length} assets`,
              );

              if (!assets.length) return;

              const updateData = assets.map((a) => {
                const key = `${a.id}_${folder.id}`;
                return {
                  id: a.id,
                  folderId: folder.id,
                  data: {
                    name: a.name,
                    width: a.width,
                    height: a.height,
                    duration: a.duration,
                    thumbnail: a.thumbnail,
                    imageUrl: a.url,
                    imageHash: a.hash,
                    video_id: a.video_id,
                    type: a.video_id ? AssetType.VIDEO : AssetType.IMAGE,
                    creation_time: a?.creation_time,
                    createdAtLocal: new Date(),
                    updatedAt: new Date(),
                  },
                  isExist: existingSet.has(key),
                };
              });

              const createCount = updateData.filter((i) => !i.isExist).length;
              const updateCount = updateData.length - createCount;

              this.logger.log(
                `📝 Folder ${folder.id} → create: ${createCount}, update: ${updateCount}`,
              );

              await prismaHelper.upsertMany(updateData, (item) => {
                if (item.isExist) {
                  return this.prisma.creativeAsset.updateMany({
                    where: {
                      id: item.id,
                      folderId: item.folderId,
                    },
                    data: item.data,
                  });
                }

                return this.prisma.creativeAsset.create({
                  data: {
                    id: item.id,
                    folderId: item.folderId,
                    ...item.data,
                  },
                });
              });

              this.logger.log(`✅ Folder ${folder.id} synced`);

              await sleep(500);
            } catch (err) {
              this.logger.error(
                `❌ Folder ${folder.id}: ${parseMetaError(err).message}`,
              );
            }
          }),
        );

        processedFolders += folderChunk.length;
      }

      // 4️⃣ Sync video details
      this.logger.log('🎬 [4/5] Sync video details...');

      const videoAssets = await this.prisma.creativeAsset.findMany({
        where: { video_id: { not: null } },
        select: { video_id: true },
      });

      const videoIds = videoAssets.map((v) => v.video_id);

      this.logger.log(`🎬 Total video assets: ${videoIds.length}`);

      let processedVideos = 0;

      for (const idsChunk of chunk(videoIds, 50)) {
        try {
          this.logger.log(
            `🎬 Fetch video batch (${processedVideos}/${videoIds.length})`,
          );

          const res = await api.call('GET', ['/'], {
            ids: idsChunk.join(','),
            fields: 'source,thumbnails',
          });

          const videosMap = res || {};

          const updatePayload = Object.entries(videosMap).map(
            ([videoId, vid]: any) => ({
              video_id: videoId,
              data: {
                video_source: vid?.source,
                video_thumbnails: vid?.thumbnails,
                updatedAt: new Date(),
              },
            }),
          );

          this.logger.log(`🎬 Updating ${updatePayload.length} videos`);

          await prismaHelper.upsertMany(updatePayload, (item) =>
            this.prisma.creativeAsset.updateMany({
              where: { video_id: item.video_id },
              data: item.data,
            }),
          );

          processedVideos += idsChunk.length;

          await sleep(800);
        } catch (error) {
          this.logger.error(
            `❌ sync video batch: ${parseMetaError(error).message}`,
          );
        }
      }

      this.logger.log('🎉 [5/5] DONE syncFolderCreative');
    } catch (err) {
      this.logger.error(
        `❌ syncFolderCreative fatal: ${parseMetaError(err).message}`,
      );
    }
  }
}
