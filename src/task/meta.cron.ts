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
  executeMetaApiWithRetry,
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
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
  CREATIVE_FIELDS,
} from 'src/common/utils/meta-field';

import dayjs from 'dayjs';

import { Cron } from '@nestjs/schedule';
import { CreativeStatus, InsightRange, LevelInsight } from '@prisma/client';
import pLimit from 'p-limit';
import { MetaTransformHelper } from 'src/common/helpers/meta-transform.helper';
import { PrismaBatchHelper } from 'src/common/helpers/prisma-batch.helper';

/* =====================================================
   CRON SERVICE
===================================================== */

@Injectable()
export class MetaCron implements OnModuleInit {
  private readonly logger = new Logger(MetaCron.name);
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
  @Cron('5 20 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  @Cron('5 19 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncImageData() {
    this.logger.log('🔄 Sync Image Core');
    await this.syncImage();
    this.logger.log('✅ Sync Image DONE');
  }

  @Cron('5 22 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  @Cron('5 21 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  @Cron('5 20 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncVideoData() {
    this.logger.log('🔄 Sync Video Core');
    await this.syncVideo();
    this.logger.log('✅ Sync Video DONE');
  }

  @Cron('20 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncFolderVideoData() {
    this.logger.log('🔄 Sync Folder Video Core');
    await this.syncFolderVideo();
    this.logger.log('✅ Sync Folder Video DONE');
  }

  @Cron('40 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncFolderImageData() {
    this.logger.log('🔄 Sync Folder Image Core');
    await this.syncFolderImage();
    this.logger.log('✅ Sync Folder Image DONE');
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
  async upsertFlatStructure(
    campaigns: any[],
    adsets: any[],
    ads: any[],
    accountId: string,
  ) {
    const prismaHelper = new PrismaBatchHelper(this.prisma);

    // 1. Deduplicate by ID to prevent parallel upsert conflicts
    const uniqueCampaigns = Array.from(
      new Map(campaigns.map((c) => [c.id, c])).values(),
    );
    const uniqueAdSets = Array.from(
      new Map(adsets.map((as) => [as.id, as])).values(),
    );
    const uniqueAds = Array.from(
      new Map(ads.map((ad) => [ad.id, ad])).values(),
    );

    const campaignData = uniqueCampaigns.map((c) =>
      MetaTransformHelper.campaign(c, accountId),
    );
    const adsetData = uniqueAdSets.map((as) =>
      MetaTransformHelper.adset(as, accountId, as.campaign_id),
    );
    const adData = uniqueAds.map((ad) =>
      MetaTransformHelper.ad(ad, accountId, ad.campaign_id, ad.adset_id),
    );

    const creativeData = [];
    for (const ad of uniqueAds) {
      const creative = MetaTransformHelper.creative(ad, accountId);
      if (creative) creativeData.push(creative);
    }

    const uniqueCreatives = Array.from(
      new Map(creativeData.map((c) => [c.id, c])).values(),
    );

    this.logger.log(
      `📦 Finalizing Data: campaign=${campaignData.length}, adset=${adsetData.length}, ad=${adData.length}`,
    );

    const pageIds = [
      ...new Set(uniqueCreatives.map((c) => c.pageId).filter(Boolean)),
    ];

    const videoMap = new Map<string, any>();
    const imageMap = new Map<string, any>();

    const fanpages = await this.prisma.fanpage.findMany({
      where: { id: { in: pageIds } },
    });

    const fanpageMap = new Map(fanpages.map((f) => [f.id, f]));

    for (const item of uniqueCreatives) {
      // ✅ map systemPageId
      if (item.pageId && fanpageMap.has(item.pageId)) {
        item.systemPageId = item.pageId;
      }

      // ✅ VIDEO (dedup theo videoId)
      if (item.videoId && !videoMap.has(item.videoId)) {
        videoMap.set(item.videoId, {
          id: item.videoId,
          accountId: item.accountId,
          thumbnailUrl: item?.thumbnailUrl,
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
            url: item?.thumbnailUrl,
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
      uniqueCreatives,
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
    this.logger.log('⏰ Starting Batch Sync Campaign Data...');
    this.init();

    try {
      const accounts = await this.prisma.account.findMany({
        where: { needsReauth: false },
      });

      // 🚀 THIẾT LẬP GIỚI HẠN: Chạy song song 5 account cùng lúc
      const limit = pLimit(30);

      const syncTasks = accounts.map((account) => {
        return limit(async () => {
          try {
            const adAccount = new AdAccount(account.id);
            const lastSyncUnix = Math.floor(
              (Date.now() - 90 * 24 * 60 * 60 * 1000) / 1000,
            );

            this.logger.log(
              `🔄 [${account.id}] Syncing data updated since ${dayjs.unix(lastSyncUnix).format('YYYY-MM-DD HH:mm:ss')}`,
            );

            const baseFilter = {
              limit: 50,
              filtering: [
                {
                  field: 'updated_time',
                  operator: 'GREATER_THAN',
                  value: lastSyncUnix,
                },
              ],
            };

            // 1. FETCH CAMPAIGNS (90 NGÀY)
            const campaignsCursor = await executeMetaApiWithRetry(
              () => adAccount.getCampaigns(CAMPAIGN_FIELDS, baseFilter, true),
              { logger: this.logger },
            );
            const allCampaigns = await fetchAll(campaignsCursor);
            const campaignIds = allCampaigns.map((c) => c.id);

            const allAdSets: any[] = [];
            const allAds: any[] = [];

            // 2. FETCH ADSETS & ADS THEO CAMPAIGN ID (RELATIONAL)
            // Lấy toàn bộ con của các campaign vừa tìm thấy để đảm bảo tính đầy đủ
            if (campaignIds.length > 0) {
              this.logger.log(
                `📂 [${account.id}] Fetching children for ${campaignIds.length} active campaigns...`,
              );
              for (const chunkIds of chunk(campaignIds, 50)) {
                const [asCursor, aCursor] = await Promise.all([
                  executeMetaApiWithRetry(
                    () =>
                      adAccount.getAdSets(
                        ADSET_FIELDS,
                        {
                          limit: 50,
                          filtering: [
                            {
                              field: 'campaign.id',
                              operator: 'IN',
                              value: chunkIds,
                            },
                          ],
                        },
                        true,
                      ),
                    { logger: this.logger },
                  ),
                  executeMetaApiWithRetry(
                    () => {
                      const adFields = [
                        ...AD_FIELDS.filter((f) => f !== 'creative'),
                        `creative{${CREATIVE_FIELDS.join(',')}}`,
                      ];
                      return adAccount.getAds(
                        adFields,
                        {
                          limit: 50,
                          filtering: [
                            {
                              field: 'campaign.id',
                              operator: 'IN',
                              value: chunkIds,
                            },
                          ],
                        },
                        true,
                      );
                    },
                    { logger: this.logger },
                  ),
                ]);

                const [asData, aData] = await Promise.all([
                  fetchAll(asCursor),
                  fetchAll(aCursor),
                ]);
                allAdSets.push(...asData);
                allAds.push(...aData);
              }
            }

            // 3. FETCH FLAT BACKUP (CẬP NHẬT LẺ TẺ)
            // Fetch thêm các AdSet/Ad update trong 90 ngày ở account level
            // Điều này quan trọng để bắt được các AdSet thay đổi trong các Campaign "cũ" (> 90 ngày)
            const [extraAdsetsCursor, extraAdsCursor] = await Promise.all([
              executeMetaApiWithRetry(
                () => adAccount.getAdSets(ADSET_FIELDS, baseFilter, true),
                { logger: this.logger },
              ),
              executeMetaApiWithRetry(
                () => {
                  const adFields = [
                    ...AD_FIELDS.filter((f) => f !== 'creative'),
                    `creative{${CREATIVE_FIELDS.join(',')}}`,
                  ];
                  return adAccount.getAds(adFields, baseFilter, true);
                },
                { logger: this.logger },
              ),
            ]);

            const [extraAdsets, extraAds] = await Promise.all([
              fetchAll(extraAdsetsCursor),
              fetchAll(extraAdsCursor),
            ]);

            // Merge và loại bỏ trùng lặp
            const adsetIds = new Set(allAdSets.map((as) => as.id));
            for (const as of extraAdsets) {
              if (!adsetIds.has(as.id)) allAdSets.push(as);
            }

            const adIds = new Set(allAds.map((ad) => ad.id));
            for (const ad of extraAds) {
              if (!adIds.has(ad.id)) allAds.push(ad);
            }

            // ------------------------------------------------------------------------------------------------
            // 🛡️ BẢO VỆ TOÀN VẸN DỮ LIỆU (MISSING PARENT FETCHING)
            // Nếu AdSet/Ad trỏ về Campaign/AdSet chưa có trong DB, ta phải fetch bổ sung để tránh lỗi Foreign Key
            // ------------------------------------------------------------------------------------------------

            // 1. Fetch Campaign thiếu
            const fetchedCampaignIds = new Set(allCampaigns.map((c) => c.id));
            const requiredCampaignIds = [
              ...new Set([
                ...allAdSets.map((as) => as.campaign_id),
                ...allAds.map((ad) => ad.campaign_id),
              ]),
            ].filter((id) => id && !fetchedCampaignIds.has(id));

            if (requiredCampaignIds.length > 0) {
              const existingInDb = await this.prisma.campaign
                .findMany({
                  where: { id: { in: requiredCampaignIds } },
                  select: { id: true },
                })
                .then((r) => new Set(r.map((x) => x.id)));

              const missingCampaignIds = requiredCampaignIds.filter(
                (id) => !existingInDb.has(id),
              );

              if (missingCampaignIds.length > 0) {
                this.logger.log(
                  `⚠️ [${account.id}] Fetching ${missingCampaignIds.length} missing parent campaigns...`,
                );
                for (const chunkIds of chunk(missingCampaignIds, 50)) {
                  const cursor = await executeMetaApiWithRetry(
                    () =>
                      adAccount.getCampaigns(
                        CAMPAIGN_FIELDS,
                        {
                          limit: 50,
                          filtering: [
                            { field: 'id', operator: 'IN', value: chunkIds },
                          ],
                        },
                        true,
                      ),
                    { logger: this.logger },
                  );
                  allCampaigns.push(...(await fetchAll(cursor)));
                }
              }
            }

            // 2. Fetch AdSet thiếu
            const fetchedAdSetIds = new Set(allAdSets.map((as) => as.id));
            const requiredAdSetIds = [
              ...new Set(allAds.map((ad) => ad.adset_id)),
            ].filter((id) => id && !fetchedAdSetIds.has(id));

            if (requiredAdSetIds.length > 0) {
              const existingInDb = await this.prisma.adSet
                .findMany({
                  where: { id: { in: requiredAdSetIds } },
                  select: { id: true },
                })
                .then((r) => new Set(r.map((x) => x.id)));

              const missingAdSetIds = requiredAdSetIds.filter(
                (id) => !existingInDb.has(id),
              );

              if (missingAdSetIds.length > 0) {
                this.logger.log(
                  `⚠️ [${account.id}] Fetching ${missingAdSetIds.length} missing parent adsets...`,
                );
                for (const chunkIds of chunk(missingAdSetIds, 50)) {
                  const cursor = await executeMetaApiWithRetry(
                    () =>
                      adAccount.getAdSets(
                        ADSET_FIELDS,
                        {
                          limit: 50,
                          filtering: [
                            { field: 'id', operator: 'IN', value: chunkIds },
                          ],
                        },
                        true,
                      ),
                    { logger: this.logger },
                  );
                  allAdSets.push(...(await fetchAll(cursor)));
                }
              }
            }

            // 2. LƯU VÀO DATABASE
            this.logger.log(
              `📊 [${account.id}] Final: ${allCampaigns.length} campaigns, ${allAdSets.length} adsets, ${allAds.length} ads`,
            );
            await this.upsertFlatStructure(
              allCampaigns,
              allAdSets,
              allAds,
              account.id,
            );

            // Cập nhật lastFetchedAt sau khi thành công
            await this.prisma.account.update({
              where: { id: account.id },
              data: { lastFetchedAt: new Date() },
            });
          } catch (error) {
            this.logger.error(
              `❌ Account ${account.id}: ${parseMetaError(error).message}`,
            );
          }
        });
      });

      await Promise.all(syncTasks);
      this.logger.log('✅ Batch Sync Campaign Data Completed.');
    } catch (err) {
      this.logger.error('🔥 Critical Sync Failure', err);
      throw new InternalServerErrorException(parseMetaError(err));
    }
  }
  private async syncMaxInsightsGeneric(
    entityName: string,
    prismaModel: 'campaign' | 'adSet' | 'ad',
    insightModel: 'campaignInsight' | 'adSetInsight' | 'adInsight',
    level: 'campaign' | 'adset' | 'ad',
    levelEnum: any,
    parentIdsField: string,
    insightIdField: string,
    relationFieldId: string,
  ) {
    this.logger.log(`🔄 Sync MAX + 3D + 7D + TODAY ${entityName} Insight`);
    this.init();
    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const parents = await (this.prisma[prismaModel] as any).findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true, status: true },
    });

    const maxInsights = await (this.prisma[insightModel] as any).findMany({
      where: {
        range: InsightRange.MAX,
        level: levelEnum,
      },
      select: {
        [relationFieldId]: true,
        dateStop: true,
      },
    });

    const maxInsightMap = new Map<string, string>();
    for (const insight of maxInsights) {
      if (insight[relationFieldId]) {
        maxInsightMap.set(insight[relationFieldId], insight.dateStop);
      }
    }

    const cutoffDate = dayjs().subtract(15, 'day');
    const activeParents = [];
    const inactiveParentIds = [];

    for (const parent of parents) {
      const dateStop = maxInsightMap.get(parent.id);
      const isInactiveStatus =
        parent.status === 'PAUSED' ||
        parent.status === 'ARCHIVED' ||
        parent.status === 'DELETED';

      if (
        isInactiveStatus &&
        dateStop &&
        dayjs(dateStop).isBefore(cutoffDate)
      ) {
        inactiveParentIds.push(parent.id);
      } else {
        activeParents.push(parent);
      }
    }

    if (inactiveParentIds.length > 0) {
      this.logger.log(
        `⏭️ Skip ${inactiveParentIds.length} ${entityName}s (dateStop > 15 days) & clear short-term insights.`,
      );
      for (const chunkIds of chunk(inactiveParentIds, 100)) {
        await (this.prisma[insightModel] as any).deleteMany({
          where: {
            [relationFieldId]: { in: chunkIds },
            range: { not: InsightRange.MAX },
          },
        });
        await (this.prisma[prismaModel] as any).updateMany({
          where: { id: { in: chunkIds } },
          data: {
            insight3dId: null,
            insight7dId: null,
            insightTodayId: null,
          },
        });
      }
    }

    const byAccount = this.groupByAccount(activeParents);
    let totalProcessed = 0;

    const JOBS = [
      { range: InsightRange.MAX, datePreset: 'maximum', field: 'insightMaxId' },
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

    const accountEntries = Object.entries(byAccount);
    for (const accountChunk of chunk(accountEntries, 30)) {
      await Promise.all(
        accountChunk.map(async ([accountId, ids]) => {
          const adAccount = new AdAccount(accountId);
          this.logger.log(
            `[Account ${accountId}] ➡️ ${ids.length} ${entityName}s`,
          );

          for (const idsChunk of chunk(ids, 300)) {
            try {
              // 1. Chuyển sang vòng lặp for...of để chạy từng Job một
              for (const job of JOBS) {
                this.logger.log(
                  `[Account ${accountId}] ⏳ Đang xử lý Job: ${job.range} cho mẻ ${idsChunk.length} IDs...`,
                );

                const cursor = await executeMetaApiWithRetry(
                  () =>
                    adAccount.getInsights(
                      AD_INSIGHT_FIELDS,
                      {
                        limit: 50,
                        level,
                        date_preset: job.datePreset,
                        action_attribution_windows: '7d_click',
                        action_breakdowns: 'action_type',
                        filtering: [
                          {
                            field: parentIdsField,
                            operator: 'IN',
                            value: idsChunk,
                          },
                        ],
                      },
                      true,
                    ),
                  { logger: this.logger },
                );

                const insights = await fetchAll(cursor);

                // Luôn xoá dữ liệu cũ của cả mẻ để loại bỏ rác (ví dụ: data TODAY, 3DAY cũ không còn data mới)
                await (this.prisma[insightModel] as any).deleteMany({
                  where: {
                    [relationFieldId]: { in: idsChunk },
                    range: job.range,
                  },
                });

                // 2. Xử lý logic lưu DB cho từng Job ngay tại đây
                if (insights.length > 0) {
                  const filteredInsights = insights.filter(
                    (i: any) => i[insightIdField],
                  );

                  const insightData = filteredInsights.map((i: any) => {
                    const metrics = extractCampaignMetrics(i);
                    return {
                      [relationFieldId]: i[insightIdField],
                      level: levelEnum,
                      range: job.range,
                      dateStart: i.date_start,
                      dateStop: i.date_stop,
                      ...metrics,
                      rawPayload: i,
                    };
                  });

                  await prismaHelper.createManySafe(
                    this.prisma[insightModel] as any,
                    insightData,
                  );

                  // ... các logic update map và upsertMany của bạn ...
                  // (Giữ nguyên phần code cập nhật bảng chính)

                  totalProcessed += filteredInsights.length;
                  this.logger.log(
                    `[Account ${accountId}] ✅ ${job.range} done (${filteredInsights.length} insights)`,
                  );
                }
              }

              // 4. SLEEP GIỮA CÁC CHUNK: Sau khi xong tất cả Job của 50 ID này, nghỉ lâu hơn
              const chunkSleep = Math.floor(Math.random() * 10000) + 20000;
              this.logger.log(
                `[Account ${accountId}] 💤 Đã xong 1 mẻ 50 IDs. Nghỉ ${Math.round(chunkSleep / 1000)}s để hồi Rate Limit...`,
              );
              await sleep(chunkSleep);
            } catch (error: any) {
              this.logger.error(
                `[Account ${accountId}] ❌ ${parseMetaError(error).message}`,
              );
              // Nếu lỗi nặng (như Rate Limit), bạn có thể thêm sleep lâu hơn ở đây
              await sleep(60000);
            }
          }
        }),
      );
    }
    this.logger.log(
      `🎯 DONE MAX + 3D + 7D ${entityName} Insight - Total: ${totalProcessed}`,
    );
  }

  private async processDailyInsightsBatch(
    insights: any[],
    levelEnum: any,
    insightIdField: string,
    relationFieldId: string,
    insightModel: string,
    prismaHelper: PrismaBatchHelper,
    today: dayjs.Dayjs,
  ) {
    const valid = insights.filter((i: any) => i[insightIdField]);
    if (!valid.length) return 0;

    const todayStr = today.format('YYYY-MM-DD');
    const insightData = valid.map((i: any) => {
      const metrics = extractCampaignMetrics(i);
      return {
        [relationFieldId]: i[insightIdField],
        dateStart: i.date_start,
        range: InsightRange.DAILY,
        data: {
          dateStop: i.date_start,
          level: levelEnum,
          ...metrics,
          rawPayload: i,
        },
      };
    });

    const todayInsightData = insightData.filter(
      (i: any) => i.dateStart === todayStr,
    );

    if (todayInsightData.length > 0) {
      const todayDataToCreate = todayInsightData.map((item: any) => {
        return {
          ...item.data,
          [relationFieldId]: item[relationFieldId],
          dateStart: item.dateStart,
          dateStop: item.data.dateStop,
          range: InsightRange.TODAY,
        };
      });

      await prismaHelper.createManySafe(
        this.prisma[insightModel as keyof typeof this.prisma] as any,
        todayDataToCreate,
      );
    }

    await prismaHelper.upsertMany(insightData, (item: any) =>
      (this.prisma[insightModel as keyof typeof this.prisma] as any).upsert({
        where: {
          [`${relationFieldId}_dateStart_range`]: {
            [relationFieldId]: item[relationFieldId],
            dateStart: item.dateStart,
            range: item.range,
          },
        },
        update: item.data,
        create: {
          [relationFieldId]: item[relationFieldId],
          dateStart: item.dateStart,
          range: item.range,
          ...item.data,
        },
      }),
    );
    return insightData.length;
  }

  private async syncDailyInsightsGeneric(
    entityName: string,
    prismaModel: 'campaign' | 'adSet' | 'ad',
    insightModel: 'campaignInsight' | 'adSetInsight' | 'adInsight',
    level: 'campaign' | 'adset' | 'ad',
    levelEnum: any,
    parentIdsField: string,
    insightIdField: string,
    relationFieldId: string,
  ) {
    this.logger.log(`🔄 Sync DAILY ${entityName} Insights`);
    this.init();
    const prismaHelper = new PrismaBatchHelper(this.prisma);
    const today = dayjs().startOf('day');

    const maxInsights = await (this.prisma[insightModel] as any).findMany({
      where: {
        range: InsightRange.MAX,
        level: levelEnum,
        spend: { gt: 0 },
        [prismaModel]: { account: { needsReauth: false } },
      },
      select: {
        dateStart: true,
        dateStop: true,
        [relationFieldId]: true,
        [prismaModel]: { select: { accountId: true } },
      },
    });

    if (!maxInsights.length) return;

    const lastDailies = await (this.prisma[insightModel] as any).findMany({
      where: { range: InsightRange.DAILY },
      select: { [relationFieldId]: true, dateStart: true },
      orderBy: { dateStart: 'desc' },
    });

    const lastDailyMap = new Map<string, string>();
    for (const d of lastDailies) {
      if (!lastDailyMap.has(d[relationFieldId])) {
        lastDailyMap.set(d[relationFieldId], d.dateStart);
      }
    }

    let totalFetched = 0;
    let totalUpserted = 0;

    const byAccount = new Map<
      string,
      { last3dIds: string[]; customItems: any[] }
    >();

    for (const max of maxInsights) {
      const accountId = max[prismaModel]?.accountId;
      if (!accountId) continue;

      const maxStart = dayjs(max.dateStart);
      const maxStopRaw = dayjs(max.dateStop);
      if (maxStopRaw.add(3, 'day').isBefore(today)) continue;

      const maxStop = maxStopRaw.isAfter(today) ? today : maxStopRaw;
      const parentId = max[relationFieldId];
      const last = lastDailyMap.get(parentId);

      if (last && dayjs(last).isSame(today, 'day')) continue;

      if (!byAccount.has(accountId)) {
        byAccount.set(accountId, { last3dIds: [], customItems: [] });
      }
      const accData = byAccount.get(accountId)!;

      if (last && dayjs(last).isAfter(today.subtract(4, 'day'))) {
        accData.last3dIds.push(parentId);
      } else {
        let since = last ? dayjs(last).subtract(2, 'day') : maxStart;
        if (since.isBefore(maxStart)) since = maxStart;
        if (since.isAfter(maxStop)) continue;

        accData.customItems.push({
          parentId,
          since: since.format('YYYY-MM-DD'),
          until: maxStop.format('YYYY-MM-DD'),
        });
      }
    }

    const accountEntries = Array.from(byAccount.entries());
    for (const accountChunk of chunk(accountEntries, 30)) {
      await Promise.all(
        accountChunk.map(async ([accountId, { last3dIds, customItems }]) => {
          const adAccount = new AdAccount(accountId);

          // 1. Process last3dIds in chunks of 50
          if (last3dIds.length > 0) {
            this.logger.log(
              `[Account ${accountId}] 📅 Fetching last_3d for ${last3dIds.length} ${entityName}s`,
            );
            for (const chunkIds of chunk(last3dIds, 300)) {
              try {
                this.logger.log(
                  `[Account ${accountId}] ⏳ Đang xử lý last_3d cho mẻ ${chunkIds.length} IDs...`,
                );

                const cursor = await executeMetaApiWithRetry(
                  () =>
                    adAccount.getInsights(
                      AD_INSIGHT_FIELDS,
                      {
                        limit: 50,
                        level,
                        time_increment: 1,
                        time_range: {
                          since: today.subtract(2, 'day').format('YYYY-MM-DD'),
                          until: today.format('YYYY-MM-DD'),
                        },
                        action_attribution_windows: '7d_click',
                        action_breakdowns: 'action_type',
                        filtering: [
                          {
                            field: parentIdsField,
                            operator: 'IN',
                            value: chunkIds,
                          },
                        ],
                      },
                      true,
                    ),
                  { logger: this.logger },
                );
                const insights = await fetchAll(cursor);

                // Xoá dữ liệu TODAY cũ của cả mẻ để tránh rác nếu hôm nay không có số liệu
                await (this.prisma[insightModel] as any).deleteMany({
                  where: {
                    [relationFieldId]: { in: chunkIds },
                    range: InsightRange.TODAY,
                  },
                });

                if (insights.length > 0) {
                  const upserted = await this.processDailyInsightsBatch(
                    insights,
                    levelEnum,
                    insightIdField,
                    relationFieldId,
                    insightModel,
                    prismaHelper,
                    today,
                  );
                  totalFetched += insights.length;
                  totalUpserted += upserted;
                }
              } catch (error: any) {
                this.logger.error(
                  `[Account ${accountId}] ❌ last_3d chunk error: ${parseMetaError(error).message}`,
                );
              }
              const chunkSleep = Math.floor(Math.random() * 10000) + 20000;
              this.logger.log(
                `[Account ${accountId}] 💤 Đã xong 1 mẻ last_3d. Nghỉ ${Math.round(chunkSleep / 1000)}s...`,
              );
              await sleep(chunkSleep);
            }
          }

          // 2. Process customItems in chunks by matching ranges
          if (customItems.length > 0) {
            this.logger.log(
              `[Account ${accountId}] 📅 Fetching custom ranges for ${customItems.length} ${entityName}s`,
            );
            const groupedCustom = new Map<string, string[]>();
            for (const item of customItems) {
              const key = `${item.since}_${item.until}`;
              if (!groupedCustom.has(key)) groupedCustom.set(key, []);
              groupedCustom.get(key)!.push(item.parentId);
            }

            for (const [key, ids] of groupedCustom.entries()) {
              const [since, until] = key.split('_');
              for (const chunkIds of chunk(ids, 300)) {
                try {
                  this.logger.log(
                    `[Account ${accountId}] ⏳ Đang xử lý custom range cho mẻ ${chunkIds.length} IDs...`,
                  );

                  const cursor = await executeMetaApiWithRetry(
                    () =>
                      adAccount.getInsights(
                        AD_INSIGHT_FIELDS,
                        {
                          limit: 100,
                          level,
                          time_increment: 1,
                          action_attribution_windows: '7d_click',
                          action_breakdowns: 'action_type',
                          time_range: { since, until },
                          filtering: [
                            {
                              field: parentIdsField,
                              operator: 'IN',
                              value: chunkIds,
                            },
                          ],
                        },
                        true,
                      ),
                    { logger: this.logger },
                  );
                  const insights = await fetchAll(cursor);

                  // Xoá dữ liệu TODAY cũ của cả mẻ để tránh rác nếu hôm nay không có số liệu
                  await (this.prisma[insightModel] as any).deleteMany({
                    where: {
                      [relationFieldId]: { in: chunkIds },
                      range: InsightRange.TODAY,
                    },
                  });

                  if (insights.length > 0) {
                    const upserted = await this.processDailyInsightsBatch(
                      insights,
                      levelEnum,
                      insightIdField,
                      relationFieldId,
                      insightModel,
                      prismaHelper,
                      today,
                    );
                    totalFetched += insights.length;
                    totalUpserted += upserted;
                  }
                } catch (error: any) {
                  this.logger.error(
                    `[Account ${accountId}] ❌ custom chunk error: ${parseMetaError(error).message}`,
                  );
                }
                const chunkSleep = Math.floor(Math.random() * 10000) + 20000;
                this.logger.log(
                  `[Account ${accountId}] 💤 Đã xong 1 mẻ custom range. Nghỉ ${Math.round(chunkSleep / 1000)}s...`,
                );
                await sleep(chunkSleep);
              }
            }
          }
        }),
      );
    }
    this.logger.log(
      `✅ DAILY DONE | fetched: ${totalFetched} | upserted: ${totalUpserted}`,
    );
  }

  async syncAllCampaignInsights() {
    await this.syncMaxInsightsGeneric(
      'Campaign',
      'campaign',
      'campaignInsight',
      'campaign',
      LevelInsight.CAMPAIGN,
      'campaign.id',
      'campaign_id',
      'campaignId',
    );
  }

  async syncAllAdSetInsights() {
    await this.syncMaxInsightsGeneric(
      'AdSet',
      'adSet',
      'adSetInsight',
      'adset',
      LevelInsight.ADSET,
      'adset.id',
      'adset_id',
      'adSetId',
    );
  }

  async syncAllAdInsights() {
    await this.syncMaxInsightsGeneric(
      'Ad',
      'ad',
      'adInsight',
      'ad',
      LevelInsight.AD,
      'ad.id',
      'ad_id',
      'adId',
    );
  }

  async syncDailyCampaignInsights() {
    await this.syncDailyInsightsGeneric(
      'Campaign',
      'campaign',
      'campaignInsight',
      'campaign',
      LevelInsight.CAMPAIGN,
      'campaign.id',
      'campaign_id',
      'campaignId',
    );
  }

  async syncDailyAdSetInsights() {
    await this.syncDailyInsightsGeneric(
      'AdSet',
      'adSet',
      'adSetInsight',
      'adset',
      LevelInsight.ADSET,
      'adset.id',
      'adset_id',
      'adSetId',
    );
  }

  async syncDailyAdInsights() {
    await this.syncDailyInsightsGeneric(
      'Ad',
      'ad',
      'adInsight',
      'ad',
      LevelInsight.AD,
      'ad.id',
      'ad_id',
      'adId',
    );
  }

  // SYNC AUDIENT
  async syncMaxAdSetAudienceInsights() {
    this.logger.log('🔄 Sync MAX adset audience Insight');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    const adSets = await this.prisma.adSet.findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true, status: true },
    });

    const maxInsights = await this.prisma.adSetInsight.findMany({
      where: { range: InsightRange.MAX, level: LevelInsight.ADSET },
      select: { adSetId: true, dateStop: true },
    });

    const maxInsightMap = new Map<string, string>();
    for (const insight of maxInsights) {
      if (insight.adSetId) {
        maxInsightMap.set(insight.adSetId, insight.dateStop);
      }
    }

    const cutoffDate = dayjs().subtract(15, 'day');
    const activeAdSets = [];
    const inactiveAdSetIds = [];

    for (const adset of adSets) {
      const dateStop = maxInsightMap.get(adset.id);
      const isInactiveStatus =
        adset.status === 'PAUSED' ||
        adset.status === 'ARCHIVED' ||
        adset.status === 'DELETED';

      if (
        isInactiveStatus &&
        dateStop &&
        dayjs(dateStop).isBefore(cutoffDate)
      ) {
        inactiveAdSetIds.push(adset.id);
      } else {
        activeAdSets.push(adset);
      }
    }

    const byAccount = this.groupByAccount(activeAdSets);

    let totalProcessed = 0;

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      this.logger.log(
        `➡️ Account ${accountId} - ${ids.length} adSets Audience`,
      );

      for (const idsChunk of chunk(ids, 300)) {
        try {
          // ================= FETCH =================
          const cursor = await executeMetaApiWithRetry(
            () =>
              adAccount.getInsights(
                AD_INSIGHT_FIELDS,
                {
                  limit: 50,
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
              ),
            { logger: this.logger },
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

          this.logger.log(
            `✅ Chunk done ${accountId} - (${validInsights.length} insights)`,
          );

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

  async syncVideo(limit: number = 50) {
    this.logger.log('🔄 Sync Ad Video (optimized)');
    this.init();
    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

    try {
      // TỐI ƯU: Chỉ fetch các video chưa có thumbnail hoặc thumbnail chuẩn bị hết hạn (hết hạn trong vòng 1 ngày tới)
      const [existingVideos, totalResult] = await Promise.all([
        this.prisma.$queryRawUnsafe<any[]>(`
        SELECT v.id, v."accountId", v."thumbnailUrl",to_timestamp(('x' || substring(v."thumbnailUrl" from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint) AS expires_at
        FROM "AdVideo" v
        JOIN "Account" a ON v."accountId" = a.id
        WHERE a."needsReauth" = false
          AND v.status IS DISTINCT FROM 'ERROR'
          AND (
            v."thumbnailUrl" IS NULL 
            OR (
              v."thumbnailUrl" ~ 'oe='
              AND to_timestamp(('x' || substring(v."thumbnailUrl" from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint)
                  <= NOW() + interval '1 day'
            )
          )
        ORDER BY expires_at ASC
        LIMIT ${limit}
        FOR UPDATE SKIP LOCKED;
      `),
        this.prisma.$queryRawUnsafe<any[]>(`
        SELECT COUNT(*) as total
        FROM "AdVideo" v
        JOIN "Account" a ON v."accountId" = a.id
        WHERE a."needsReauth" = false
          AND v.status IS DISTINCT FROM 'ERROR'
          AND (
            v."thumbnailUrl" IS NULL 
            OR (
              v."thumbnailUrl" ~ 'oe='
              AND to_timestamp(('x' || substring(v."thumbnailUrl" from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint)
                  <= NOW() + interval '1 day'
            )
          )
      `),
      ]);
      if (!existingVideos.length) {
        this.logger.log('⚠️ No videos to sync');
        return;
      }

      this.logger.log(`📦 Total videos: ${existingVideos.length}`);

      const videoIds = existingVideos.map((v) => v.id);
      this.logger.log(
        `\n==============================\n📊 📦 VIDEOS: ${videoIds.length}\n==============================`,
      );

      this.logger.log(
        `\n==============================\n📊 📦  ${videoIds.join(',\n ')}\n==============================`,
      );

      const batchRequests = existingVideos.map((video) => ({
        method: 'GET',
        relative_url: `${video.id}?fields=source,thumbnails`,
      }));

      this.logger.log(`📦 Processing batch of ${existingVideos.length} videos`);

      let batchResponses: any[];
      try {
        // Gọi Batch API
        batchResponses = await api.call('POST', [''], {
          batch: JSON.stringify(batchRequests),
        });
      } catch (err: any) {
        this.logger.error('[FB BATCH CRITICAL ERROR]', err?.message);
        return false;
      }

      // 3. Duyệt qua từng response trong batch
      for (let i = 0; i < existingVideos.length; i++) {
        const asset = existingVideos[i];
        const res = batchResponses[i];

        // Lưu ý: Batch response trả về status code riêng cho từng item
        const statusCode = res.code; // 200, 400, 403...
        const body = JSON.parse(res.body);

        try {
          if (statusCode === 200) {
            // Thành công: Cập nhật dữ liệu mới
            const thumbnail = body.thumbnails?.data?.find(
              (th: any) => !!th?.is_preferred,
            )?.uri;

            await this.prisma.adVideo.update({
              where: { id: asset.id },
              data: {
                thumbnailUrl: thumbnail || asset.thumbnailUrl,
                source: body.source ?? asset.source ?? null,
                status: 'READY', //
                updatedAt: new Date(), // [cite: 220]
              },
            });
          } else {
            // Thất bại (Ví dụ lỗi 403 - No Permission hoặc 400 - Object Deleted)
            this.logger.warn(
              `[ID ERROR] ${asset.id} - Status: ${statusCode} - Msg: ${body?.error?.message}`,
            );

            // Đánh dấu status = 'ERROR' để các lượt sync sau bỏ qua ID này
            await this.prisma.adVideo.update({
              where: { id: asset.id },
              data: {
                status: 'ERROR', //
                updatedAt: new Date(),
              },
            });
          }
        } catch (dbErr: any) {
          this.logger.error(`[DB UPDATE ERROR] ID: ${asset.id}`, dbErr.message);
        }
      }

      const total = Number(totalResult[0]?.total || 0);
      console.log(
        `[SYNC DONE] processed=${existingVideos.length} - total ${total}`,
      );
      return true;
    } catch (err: any) {
      console.error('[CRON ERROR]', err?.message);
      return false; // ❗ cực quan trọng }
    }
  }

  async syncImage(limit: number = 50) {
    this.logger.log('🔄 Sync AdImage (optimized)');
    this.init();

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    try {
      // TỐI ƯU: Chỉ fetch các image chưa có url hoặc url chuẩn bị hết hạn
      const [existingImages, totalResult] = await Promise.all([
        this.prisma.$queryRawUnsafe<any[]>(`
        SELECT i.hash, i.url,i."accountId", to_timestamp(('x' || substring(i.url from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint) AS expires_at 
        FROM "AdImage" i
        JOIN "Account" a ON i."accountId" = a.id
        WHERE a."needsReauth" = false
          AND (
            i.url IS NULL 
            OR (
              i.url ~ 'oe='
              AND to_timestamp(('x' || substring(i.url from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint)
                  <= NOW() + interval '1 day'
            )
          )
        ORDER BY expires_at ASC
        LIMIT ${limit}
        FOR UPDATE SKIP LOCKED;
      `),
        this.prisma.$queryRawUnsafe<any[]>(`
        SELECT COUNT(*) as total
        FROM "AdImage" i
        JOIN "Account" a ON i."accountId" = a.id
        WHERE a."needsReauth" = false
          AND (
            i.url IS NULL 
            OR (
              i.url ~ 'oe='
              AND to_timestamp(('x' || substring(i.url from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint)
                  <= NOW() + interval '1 day'
            )
          )
      `),
      ]);

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
            const cursor = await executeMetaApiWithRetry(
              () =>
                adAccount.getAdImages(AD_IMAGE_FIELDS, {
                  limit: 50,
                  hashes: hashChunk,
                }),
              { logger: this.logger },
            );

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

      this.logger.log(
        `✅ Updated ${existingImages.length} images / ${totalResult[0]?.total || 0} total images needing sync`,
      );
    } catch (err) {
      this.logger.error(`❌ syncImage fatal: ${parseMetaError(err).message}`);
    }
  }

  async syncFolderVideo(limit: number = 50) {
    try {
      const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

      // 1. Lấy batch nhỏ + lock tránh trùng job
      const [assets, totalResult] = await Promise.all([
        this.prisma.$queryRawUnsafe<any[]>(`
          SELECT ca.*,
                to_timestamp(('x' || substring(ca.video_source from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint) AS expires_at
          FROM "CreativeAsset" ca
          WHERE ca.video_source ~ 'oe='
            AND to_timestamp(('x' || substring(ca.video_source from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint)
                <= NOW() + interval '1 day'
          ORDER BY expires_at ASC
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED;
        `),

        this.prisma.$queryRawUnsafe<any[]>(`
          SELECT COUNT(*) as total
          FROM "CreativeAsset" ca
          WHERE ca.video_source ~ 'oe='
            AND to_timestamp(('x' || substring(ca.video_source from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint)
                <= NOW() + interval '1 day';
        `),
      ]);

      if (!assets.length) return true;

      // ⚠️ SAI trước đó: bạn đang dùng id thay vì video_id
      const videoIds = assets.map((v) => v.video_id).filter(Boolean);

      if (!videoIds.length) return true;

      let response: any = {};

      // 2. Call API (có try/catch riêng)
      try {
        response = await api.call('GET', [''], {
          ids: videoIds.join(','),
          fields: 'source,thumbnails',
        });
      } catch (err: any) {
        console.error('[FB API ERROR]', err?.message);
        return false; // không crash cron
      }

      const videosMap = response || {};

      // 3. Update từng record (KHÔNG dùng transaction bulk)
      for (const asset of assets) {
        try {
          const vid = videosMap[asset.video_id];
          if (!vid) continue;

          await this.prisma.creativeAsset.update({
            where: { id: asset.id },
            data: {
              thumbnail: vid?.thumbnails?.data?.find((th) => !!th?.is_preferred)
                ?.uri,
              video_thumbnails: vid?.thumbnails ?? null,
              video_source: vid?.source ?? null,
            },
          });
        } catch (err: any) {
          console.error('[UPDATE ERROR]', {
            assetId: asset.id,
            error: err?.message,
          });
          // ❗ không throw → tránh crash toàn job
        }
      }
      const total = Number(totalResult[0]?.total || 0);
      console.log(`[SYNC DONE] processed=${assets.length} - total ${total}`);
      return true;
    } catch (err: any) {
      console.error('[CRON ERROR]', err?.message);
      return false; // ❗ cực quan trọng
    }
  }

  async syncFolderImage(limit: number = 50) {
    try {
      const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

      // 1. Lấy batch nhỏ + lock tránh trùng job
      // 1. Lấy batch nhỏ + lock tránh trùng job
      const [assets, totalResult] = await Promise.all([
        this.prisma.$queryRawUnsafe<any[]>(`
          SELECT ca.*,
                to_timestamp(('x' || substring(ca."imageUrl" from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint) AS expires_at
          FROM "CreativeAsset" ca
          WHERE ca."imageUrl" ~ 'oe='
                      AND to_timestamp(('x' || substring(ca."imageUrl" from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint)
                          <= NOW() + interval '1 day'
          ORDER BY expires_at ASC
          LIMIT ${limit}
          FOR UPDATE SKIP LOCKED;
        `),

        this.prisma.$queryRawUnsafe<any[]>(`
          SELECT COUNT(*) as total
          FROM "CreativeAsset" ca
          WHERE ca."imageUrl" ~ 'oe='
            AND to_timestamp(('x' || substring(ca."imageUrl" from 'oe=([0-9A-Fa-f]+)'))::bit(32)::bigint)
                <= NOW() + interval '1 day';
        `),
      ]);

      if (!assets.length) return true;

      // ⚠️ SAI trước đó: bạn đang dùng id thay vì video_id
      const imageIds = assets.map((v) => v.id);

      let response: any = {};

      // 2. Call API (có try/catch riêng)
      try {
        response = await api.call('GET', [''], {
          ids: imageIds.join(','),
          fields: ['hash', 'url', 'name', 'creation_time', 'id'],
        });
      } catch (err: any) {
        console.error('[FB API ERROR]', err?.message);
        return false; // không crash cron
      }
      const imagesMap = response || {};

      for (const asset of assets) {
        try {
          const img = imagesMap[asset.id];
          if (!img) continue;

          await this.prisma.creativeAsset.update({
            where: { id: asset.id },
            data: { imageUrl: img.url, thumbnail: img.url },
          });
        } catch (err: any) {
          console.error('[UPDATE ERROR]', {
            assetId: asset.id,
            error: err?.message,
          });
          // ❗ không throw → tránh crash toàn job
        }
      }

      const total = Number(totalResult[0]?.total || 0);
      console.log(`[SYNC DONE] processed=${assets.length} - total ${total}`);
      return true;
    } catch (err: any) {
      console.error('[CRON ERROR]', err?.message);
      return false; // ❗ cực quan trọng
    }
  }
}
