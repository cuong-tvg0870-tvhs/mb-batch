import {
  Injectable,
  InternalServerErrorException,
  Logger,
} from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { CreativeStatus, InsightRange, LevelInsight } from '@prisma/client';
import * as dayjs from 'dayjs';

import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
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
import { UpsertService } from 'src/modules/campaign-sync-service/upsert.service';
import { MetaService } from 'src/modules/meta/meta.service';
import { PrismaService } from 'src/modules/prisma/prisma.service';

@Injectable()
export class TaskCron {
  private readonly logger = new Logger(TaskCron.name);
  private initialized = false;

  constructor(
    private readonly prisma: PrismaService,
    private upsertDataService: UpsertService,
    private metaService: MetaService,
  ) {}

  private init() {
    if (!this.initialized) {
      FacebookAdsApi.init(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
      this.initialized = true;
    }
  }

  /* =====================================================
     MAIN CRON
  ===================================================== */

  async onModuleInit() {
    this.logger.log('🚀 App started → scan video immediately');
    // await this.syncCampaignData();

    await this.syncMaxCampaignInsights();
    // await this.syncDailyCampaignInsights();

    // await this.syncMaxAdsetInsights();
    // await this.syncMaxAdsetAudienceInsights();
    // await this.syncDailyAdsetInsights();

    // await this.syncMaxAdInsights();
    // await this.syncMaxAdAudienceInsights();
    // await this.syncDailyAdInsights();

    // await this.syncCampaignCore();

    // await this.syncImage();
    // await this.syncVideo();

    // await this.calculateCreativeInsightFromAdInsight();
  }

  @Cron('0 5 0 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncCampaignCore() {
    this.logger.log('🔄 Sync Campaign Core');
    await this.syncCampaignData();
    this.logger.log('✅ Sync Campaign Core DONE');
  }

  @Cron('0 5 1 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxCampaignInsightsJob() {
    this.logger.log('🔄 Sync MAX Campaign Insights');
    await this.syncMaxCampaignInsights();
    this.logger.log('✅ MAX Campaign DONE');
  }

  @Cron('0 10 2 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdsetInsightsJob() {
    this.logger.log('🔄 Sync MAX Adset Insights');
    await this.syncMaxAdsetInsights();
    this.logger.log('✅ MAX Adset DONE');
  }

  @Cron('0 15 3 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdsetAudienceInsightsJob() {
    this.logger.log('🔄 Sync MAX Adset Audience Insights');
    await this.syncMaxAdsetAudienceInsights();
    this.logger.log('✅ MAX Adset Audience DONE');
  }

  @Cron('0 20 4 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdInsightsJob() {
    this.logger.log('🔄 Sync MAX Ad Insights');
    await this.syncMaxAdInsights();
    this.logger.log('✅ MAX Ad DONE');
  }

  @Cron('0 25 5 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxAdAudienceInsightsJob() {
    this.logger.log('🔄 Sync MAX Ad Audience Insights');
    await this.syncMaxAdAudienceInsights();
    this.logger.log('✅ MAX Ad Audience DONE');
  }

  @Cron('0 30 6,12,17 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyCampaignInsightsJob() {
    this.logger.log('🔄 Sync DAILY Campaign Insights');
    await this.syncDailyCampaignInsights();
    this.logger.log('✅ DAILY Campaign DONE');
  }

  @Cron('0 35 7,13,18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyAdsetInsightsJob() {
    this.logger.log('🔄 Sync DAILY Adset Insights');
    await this.syncDailyAdsetInsights();
    this.logger.log('✅ DAILY Adset DONE');
  }

  @Cron('0 40 8,13,48 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyAdInsightsJob() {
    this.logger.log('🔄 Sync DAILY Ad Insights');
    await this.syncDailyAdInsights();
    this.logger.log('✅ DAILY Ad DONE');

    this.logger.log('🔄 Analytic Creative Insight');
    await this.calculateCreativeInsightFromAdInsight();
    this.logger.log('✅ Analytic Creative Insight');
  }

  async upsertFullStructure(campaigns: any[], accountId: string) {
    for (const c of campaigns) {
      try {
        // Sử dụng Transaction cho mỗi Campaign và các con của nó
        await this.prisma.$transaction(
          async (tx) => {
            // 1. Upsert Campaign [1]
            await tx.campaign.upsert({
              where: { id: c.id },
              update: {
                name: c.name,
                status: c.status,
                objective: c.objective,
                buyingType: c.buying_type,
                effectiveBudget: Number(
                  c.daily_budget ?? c.lifetime_budget ?? 0,
                ),
                dailyBudget: Number(c.daily_budget),
                lifetimeBudget: Number(c.lifetime_budget),
                rawPayload: toPrismaJson(c),
                lastFetchedAt: new Date(),
                createdAt: c.created_time
                  ? new Date(c.created_time)
                  : undefined,
                updatedAt: c.updated_time
                  ? new Date(c.updated_time)
                  : undefined,
                systemCampaignId: c.systemCampaignId || undefined,

                ...(c?.insights?.data && Number(c?.insights?.data?.length) > 0
                  ? extractCampaignMetrics(c.insights.data[0])
                  : {}),
              },

              create: {
                id: c.id,
                accountId,
                name: c.name,
                status: c.status,
                objective: c.objective,
                buyingType: c.buying_type,
                dailyBudget: Number(c.daily_budget),
                effectiveBudget: Number(
                  c.daily_budget ?? c.lifetime_budget ?? 0,
                ),
                lifetimeBudget: Number(c.lifetime_budget),
                rawPayload: toPrismaJson(c),
                lastFetchedAt: new Date(),
                createdAt: c.created_time
                  ? new Date(c.created_time)
                  : undefined,
                updatedAt: c.updated_time
                  ? new Date(c.updated_time)
                  : undefined,

                systemCampaignId: c.systemCampaignId || undefined,

                ...(c?.insights?.data && Number(c?.insights?.data?.length) > 0
                  ? extractCampaignMetrics(c.insights.data[0])
                  : {}),
              },
            });

            if (c.adsets?.data) {
              for (const as of c.adsets.data) {
                // 2. Upsert AdSet [2]
                await tx.adSet.upsert({
                  where: { id: as.id },
                  update: {
                    name: as.name,
                    status: as.status,
                    optimizationGoal: as.optimization_goal,
                    billingEvent: as.billing_event,
                    bidStrategy: as.bid_strategy,
                    dailyBudget: Number(as.daily_budget),
                    lifetimeBudget: Number(as.lifetime_budget),
                    targeting: as.targeting,
                    rawPayload: toPrismaJson(as),
                    lastFetchedAt: new Date(),
                    createdAt: as.created_time
                      ? new Date(as.created_time)
                      : undefined,
                    updatedAt: as.updated_time
                      ? new Date(as.updated_time)
                      : undefined,
                    effectiveBudget: Number(
                      as?.daily_budget || as?.lifetime_budget || 0,
                    ),
                    ...(as?.insights?.data &&
                    Number(as?.insights?.data?.length) > 0
                      ? extractCampaignMetrics(as.insights.data[0])
                      : {}),
                  },
                  create: {
                    id: as.id,
                    accountId,
                    campaignId: c.id,
                    name: as.name,
                    status: as.status,
                    optimizationGoal: as.optimization_goal,
                    billingEvent: as.billing_event,
                    bidStrategy: as.bid_strategy,
                    dailyBudget: Number(as.daily_budget),
                    lifetimeBudget: Number(as.lifetime_budget),
                    effectiveBudget: Number(
                      as?.daily_budget || as?.lifetime_budget || 0,
                    ),
                    targeting: as.targeting,
                    rawPayload: toPrismaJson(as),
                    lastFetchedAt: new Date(),
                    createdAt: as.created_time
                      ? new Date(as.created_time)
                      : undefined,
                    updatedAt: as.updated_time
                      ? new Date(as.updated_time)
                      : undefined,

                    ...(as?.insights?.data &&
                    Number(as?.insights?.data?.length) > 0
                      ? extractCampaignMetrics(as.insights.data[0])
                      : {}),
                  },
                });

                if (as.ads?.data) {
                  for (const ad of as.ads.data) {
                    // 3. Upsert Ad [2]
                    const creative = ad.creative;

                    const pageId =
                      creative?.effective_object_story_id?.split('_')[0];
                    const postId =
                      creative?.effective_object_story_id?.split('_')[1];

                    const fanpage = pageId
                      ? await tx.fanpage.findUnique({ where: { id: pageId } })
                      : null;

                    if (creative.video_id) {
                      await tx.adVideo.upsert({
                        where: { id: creative.video_id },
                        update: {},
                        create: { id: creative.video_id, accountId },
                      });
                    }

                    await tx.creative.upsert({
                      where: { id: creative.id },
                      update: {
                        name: creative.name,
                        creativeType: creative.object_type,
                        imageHash: creative.image_hash,
                        videoId: creative.video_id,
                        thumbnailUrl: creative.thumbnail_url,
                        objectStoryId: creative.object_story_id,
                        effectObjectStoryId: creative.effective_object_story_id,
                        pageId,
                        postId,
                        systemPageId: fanpage?.id,
                        rawPayload: toPrismaJson(creative),
                        lastFetchedAt: new Date(),
                        createdAt: ad.created_time
                          ? new Date(ad.created_time)
                          : undefined,
                        updatedAt: ad.updated_time
                          ? new Date(ad.updated_time)
                          : undefined,
                      },
                      create: {
                        id: creative.id,
                        accountId,
                        name: creative.name,
                        creativeType: creative.object_type,
                        imageHash: creative.image_hash,
                        videoId: creative.video_id,
                        thumbnailUrl: creative.thumbnail_url,
                        objectStoryId: creative.object_story_id,
                        effectObjectStoryId: creative.effective_object_story_id,
                        pageId,
                        postId,
                        systemPageId: fanpage?.id,
                        rawPayload: toPrismaJson(creative),
                        lastFetchedAt: new Date(),
                        createdAt: ad.created_time
                          ? new Date(ad.created_time)
                          : undefined,
                        updatedAt: ad.updated_time
                          ? new Date(ad.updated_time)
                          : undefined,
                      },
                    });
                    await tx.ad.upsert({
                      where: { id: ad.id },
                      update: {
                        name: ad.name,
                        status: ad.status,
                        effectiveStatus: ad.effective_status,
                        configuredStatus: ad.configured_status,
                        creativeId: ad.creative?.id,
                        rawPayload: toPrismaJson(ad),
                        lastFetchedAt: new Date(),

                        createdAt: ad.created_time
                          ? new Date(ad.created_time)
                          : undefined,
                        updatedAt: ad.updated_time
                          ? new Date(ad.updated_time)
                          : undefined,
                        ...(ad?.insights?.data &&
                        Number(ad?.insights?.data?.length) > 0
                          ? extractCampaignMetrics(ad.insights.data[0])
                          : {}),
                      },
                      create: {
                        id: ad.id,
                        accountId,
                        campaignId: c.id,
                        adsetId: as.id,
                        name: ad.name,
                        status: ad.status,
                        effectiveStatus: ad.effective_status,
                        configuredStatus: ad.configured_status,
                        creativeId: ad.creative?.id,
                        rawPayload: toPrismaJson(ad),
                        lastFetchedAt: new Date(),
                        createdAt: ad.created_time
                          ? new Date(ad.created_time)
                          : undefined,
                        updatedAt: ad.updated_time
                          ? new Date(ad.updated_time)
                          : undefined,
                        ...(ad?.insights?.data &&
                        Number(ad?.insights?.data?.length) > 0
                          ? extractCampaignMetrics(ad.insights.data[0])
                          : {}),
                      },
                    });
                  }
                }
              }
            }
          },
          {
            timeout: 10000, // Set timeout 10s cho mỗi cụm Campaign
          },
        );
      } catch (err) {
        // Nếu một Campaign lỗi, các Campaign khác vẫn tiếp tục sync
        console.error(`Transaction failed for Campaign ${c.id}:`, err);
      }
    }
  }

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

          // Sử dụng Fields Expansion để lấy dữ liệu phân cấp [4]
          // Lưu ý: special_ad_categories là bắt buộc từ API v7.0 [5]
          const fields = [
            ...CAMPAIGN_FIELDS,
            `adsets.limit(100){${ADSET_FIELDS.join(',')},
            ads.limit(100){${AD_FIELDS.filter((f) => f !== 'creative').join(',')},
            creative{${CREATIVE_FIELDS.join(',')}}}}`,
          ];

          const campaignsCursor = await adAccount.getCampaigns(
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

          const campaigns = await fetchAll(campaignsCursor);

          // Xử lý Upsert dữ liệu vào DB thông qua service
          await this.upsertFullStructure(campaigns, account.id);
          console.log('DONE', account.id, campaigns.length);
          await sleep(1000); // Tránh spam API liên tục
        } catch (error) {
          const metaError = parseMetaError(error);
          this.logger.error(`Lỗi Account ${account.id}: ${metaError.message}`);
          // Nếu gặp lỗi 80004 hoặc 613, nên dừng hoặc chờ lâu hơn [1]
        }
      }
    } catch (err) {
      throw new InternalServerErrorException(parseMetaError(err));
    }
  }

  // /* =====================================================
  //    CAMPAIGN MAX
  // ===================================================== */

  async syncMaxCampaignInsights() {
    this.logger.log('🔄 Sync MAX Campaign Insight (Delete then Create)');
    this.init();

    const campaigns = await this.prisma.campaign.findMany({
      where: { account: { needsReauth: false } },
      select: { id: true, accountId: true },
    });
    const byAccount = this.groupByAccount(campaigns);

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      for (const idsChunk of chunk(ids, 50)) {
        try {
          const cursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              level: 'campaign',
              date_preset: 'maximum', // Theo nguồn [1], 'lifetime' đã bị thay thế bởi 'maximum'
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              filtering: [
                { field: 'campaign.id', operator: 'IN', value: idsChunk },
              ],
            },
            true,
          );

          const insights = await fetchAll(cursor);
          if (insights.length === 0) continue;

          // Bắt đầu Transaction để Xóa và Tạo lại [Conversation History]
          await this.prisma.$transaction(
            async (tx) => {
              const campaignIdsInInsight = insights
                .map((i) => i.campaign_id)
                .filter((id): id is string => !!id);

              // 1. XÓA CÁC DATA CŨ CỦA INSIGHT [Yêu cầu của bạn]
              await tx.campaignInsight.deleteMany({
                where: {
                  campaignId: { in: campaignIdsInInsight },
                  range: InsightRange.MAX,
                },
              });

              // 2. CREATE LẠI DATA MỚI
              for (const i of insights) {
                if (!i.campaign_id) continue;
                const metrics = extractCampaignMetrics(i);

                await tx.campaignInsight.create({
                  data: {
                    campaignId: i.campaign_id,
                    level: LevelInsight.CAMPAIGN,
                    range: InsightRange.MAX,
                    dateStart: i.date_start,
                    dateStop: i.date_stop,
                    ...metrics,
                    rawPayload: i,
                  },
                });

                // Cập nhật metrics trực tiếp vào bảng Campaign để xem nhanh
                await tx.campaign.update({
                  where: { id: i.campaign_id },
                  data: { ...metrics },
                });
              }
            },
            {
              timeout: 20000, // Tăng timeout cho các transaction xóa/tạo lớn
            },
          );

          await sleep(800); // Tránh lỗi Rate Limit 80004 [2]
        } catch (error) {
          this.logger.error(
            `Lỗi Account ${accountId}: ${parseMetaError(error).message}`,
          );
        }
      }
    }
    this.logger.log('✅ MAX Campaign DONE');
  }

  // /* =====================================================
  //    CAMPAIGN DAILY (dựa theo MAX)
  // ===================================================== */

  async syncDailyCampaignInsights() {
    this.logger.log(
      '🔄 Bắt đầu đồng bộ DAILY Campaign Insights dựa trên dữ liệu MAX',
    );
    this.init();

    const today = dayjs().startOf('day');

    // 1. Lấy danh sách các Campaign có dữ liệu MAX và có chi tiêu (spend > 0)
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

    for (const max of maxInsights) {
      const accountId = max.campaign?.accountId;
      if (!accountId) continue;

      const maxStart = dayjs(max.dateStart);
      const maxStopRaw = dayjs(max.dateStop);

      // ⛔ TỐI ƯU: Nếu chiến dịch đã kết thúc quá 3 ngày, không cần quét hàng ngày nữa [Best Practice]
      if (maxStopRaw.add(3, 'day').isBefore(today)) {
        continue;
      }

      // Xác định điểm kết thúc của dải dữ liệu quét (không vượt quá ngày hôm nay)
      const maxStop = maxStopRaw.isAfter(today) ? today : maxStopRaw;

      // 2. Tìm ngày cuối cùng đã đồng bộ DAILY trong DB để xác định điểm bắt đầu (since)
      const lastDaily = await this.prisma.campaignInsight.findFirst({
        where: {
          campaignId: max.campaignId,
          range: InsightRange.DAILY,
        },
        orderBy: { dateStart: 'desc' },
      });

      let since: dayjs.Dayjs;
      if (lastDaily) {
        // 🔥 Rolling back 2-3 ngày để cập nhật các chỉ số chuyển đổi muộn (delayed attribution)
        since = dayjs(lastDaily.dateStart).subtract(2, 'day');
      } else {
        since = maxStart;
      }

      // Đảm bảo 'since' không trước ngày bắt đầu của Campaign và không sau ngày kết thúc
      if (since.isBefore(maxStart)) since = maxStart;
      if (since.isAfter(maxStop)) continue;

      const adAccount = new AdAccount(accountId);

      try {
        this.logger.log(
          `📅 Campaign ${max.campaignId}: Quét từ ${since.format('YYYY-MM-DD')} đến ${maxStop.format('YYYY-MM-DD')}`,
        );

        const cursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'campaign',
            time_increment: 1, // Bắt buộc để lấy dữ liệu breakdown theo từng ngày
            date_preset: 'maximum', // Thay thế 'lifetime' theo quy định từ v10.0 [1]
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

        // 3. SỬ DỤNG TRANSACTION ĐỂ TỐI ƯU HÓA DB [CONVERSATION HISTORY]
        await this.prisma.$transaction(async (tx) => {
          for (const i of insights) {
            if (!i.campaign_id) continue;
            const date = i.date_start;

            await tx.campaignInsight.upsert({
              where: {
                campaignId_dateStart_range: {
                  campaignId: i.campaign_id,
                  dateStart: date,
                  range: InsightRange.DAILY,
                },
              },
              update: {
                dateStop: date,
                ...extractCampaignMetrics(i),
                rawPayload: i,
              },
              create: {
                campaignId: i.campaign_id,
                level: LevelInsight.CAMPAIGN,
                range: InsightRange.DAILY,
                dateStart: date,
                dateStop: date,
                ...extractCampaignMetrics(i),
                rawPayload: i,
              },
            });
          }
        });

        // Tránh lỗi Rate Limit 80004 (Too many calls) [2]
        await sleep(1500);
      } catch (error: any) {
        const metaError = parseMetaError(error);
        this.logger.error(
          `❌ DAILY Campaign failed: ${max.campaignId} - ${metaError.message}`,
        );
      }
    }

    this.logger.log('✅ DAILY Campaign DONE');
  }

  // /* =====================================================
  //    ADSET MAX
  // ===================================================== */
  async syncMaxAdsetInsights() {
    this.logger.log('🔄 Sync MAX Adset Insight (Delete then Create)');
    this.init();

    // 1. Lấy danh sách AdSet thuộc các Account còn hiệu lực
    const adsets = await this.prisma.adSet.findMany({
      where: { campaign: { account: { needsReauth: false } } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(adsets);

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      // Chia lô 50 ID để tối ưu hóa truy vấn theo lô (Batch Fetching) [5]
      for (const idsChunk of chunk(ids, 50)) {
        try {
          const cursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              level: 'adset',
              // 'maximum' thay thế cho 'lifetime' từ v10.0, trả về tối đa 37 tháng [1]
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
          if (insights.length === 0) continue;

          // 2. SỬ DỤNG TRANSACTION ĐỂ XÓA VÀ TẠO MỚI DỮ LIỆU [CONVERSATION HISTORY]
          await this.prisma.$transaction(
            async (tx) => {
              const adsetIdsInInsight = insights
                .map((i) => i.adset_id)
                .filter((id): id is string => !!id);

              // Xóa các bản ghi MAX cũ của các AdSet này để tránh rác dữ liệu
              await tx.adSetInsight.deleteMany({
                where: {
                  adSetId: { in: adsetIdsInInsight },
                  range: InsightRange.MAX,
                },
              });

              for (const i of insights) {
                if (!i.adset_id) continue;
                const metrics = extractCampaignMetrics(i);

                // Tạo mới bản ghi Insight
                await tx.adSetInsight.create({
                  data: {
                    adSetId: i.adset_id,
                    level: LevelInsight.ADSET,
                    range: InsightRange.MAX,
                    dateStart: i.date_start,
                    dateStop: i.date_stop,
                    ...metrics,
                    rawPayload: i,
                  },
                });

                // Đồng bộ metrics vào bảng AdSet để truy vấn nhanh [6]
                await tx.adSet.update({
                  where: { id: i.adset_id },
                  data: { ...metrics },
                });
              }
            },
            {
              timeout: 20000, // Tăng timeout cho các tác vụ xóa/tạo khối lượng lớn
            },
          );

          // 3. Nghỉ để tránh lỗi Rate Limit 80004/613 từ Meta [3], [4]
          await sleep(800);
        } catch (error) {
          const metaError = parseMetaError(error);
          this.logger.error(
            `Lỗi Sync Adset Insight Account ${accountId}: ${metaError.message}`,
          );
        }
      }
    }

    this.logger.log('✅ MAX ADSET DONE');
  }

  async syncMaxAdsetAudienceInsights() {
    this.logger.log('🔄 Sync MAX ADSET Audience Insight (Delete then Create)');
    this.init();

    // 1. Lấy danh sách AdSet từ các Account không cần reauth [2, 3]
    const adsets = await this.prisma.adSet.findMany({
      where: { campaign: { account: { needsReauth: false } } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(adsets);

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      // Chia lô 50 ID để tối ưu hóa API và tránh Rate Limit 80004 [4, 5]
      for (const idsChunk of chunk(ids, 50)) {
        try {
          const audientCursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              level: 'adset',
              // 'maximum' thay thế cho 'lifetime' từ v10.0, trả về tối đa 37 tháng [1]
              date_preset: 'maximum',
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              filtering: [
                { field: 'adset.id', operator: 'IN', value: idsChunk },
              ],
              breakdowns: ['age', 'gender'], // Phân tích theo độ tuổi và giới tính
            },
            true,
          );

          const audients = await fetchAll(audientCursor);
          if (audients.length === 0) continue;

          // 2. SỬ DỤNG TRANSACTION ĐỂ XÓA VÀ TẠO MỚI DỮ LIỆU [CONVERSATION HISTORY]
          await this.prisma.$transaction(
            async (tx) => {
              const adsetIdsInResult = [
                ...new Set(audients.map((a) => a.adset_id).filter(Boolean)),
              ];

              // Xóa các bản ghi audience cũ của các AdSet này ở dải MAX [6]
              await tx.adsetAudienceInsight.deleteMany({
                where: {
                  adsetId: { in: adsetIdsInResult as string[] },
                  range: InsightRange.MAX,
                },
              });

              // Tạo lại dữ liệu mới từ kết quả API
              for (const audient of audients) {
                if (!audient.adset_id) continue;

                const metrics = extractCampaignMetrics(audient);

                await tx.adsetAudienceInsight.create({
                  data: {
                    adsetId: audient.adset_id,
                    age: audient.age,
                    gender: audient.gender,
                    level: LevelInsight.ADSET,
                    range: InsightRange.MAX,
                    dateStart: audient.date_start,
                    dateStop: audient.date_stop,
                    ...metrics,
                    rawPayload: audient,
                  },
                });
              }
            },
            {
              timeout: 30000, // Tăng timeout cho các transaction có breakdowns (thường nhiều record hơn)
            },
          );

          // Nghỉ 800ms để tránh lỗi giới hạn tốc độ API 613 [4]
          await sleep(800);
        } catch (error) {
          const metaError = parseMetaError(error);
          this.logger.error(
            `Lỗi Sync Audience Insight Account ${accountId}: ${metaError.message}`,
          );
        }
      }
    }

    this.logger.log('✅ MAX ADSET AUDIENCE DONE');
  }

  // /* =====================================================
  //    ADSET DAILY (dựa theo MAX)
  // ===================================================== */

  async syncDailyAdsetInsights() {
    this.logger.log('🔄 Sync DAILY Adset Insight (Optimized Batch)');
    this.init();

    const today = dayjs().startOf('day');

    // 1. Lấy danh sách AdSet cần đồng bộ dựa trên dữ liệu MAX có chi tiêu
    const maxInsights = await this.prisma.adSetInsight.findMany({
      where: {
        range: InsightRange.MAX,
        spend: { gt: 0 },
        adSet: { campaign: { account: { needsReauth: false } } },
      },
      select: {
        adSetId: true,
        adSet: { select: { accountId: true } },
      },
    });

    if (maxInsights.length === 0) return;

    // Nhóm các AdSet ID theo từng AccountId
    const byAccount = this.groupByAccount(
      maxInsights.map((m) => ({ id: m.adSetId, accountId: m.adSet.accountId })),
    );

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      // Chia lô 50 ID để tối ưu hóa request và tránh lỗi URL quá dài
      for (const idsChunk of chunk(ids, 50)) {
        try {
          // 🔥 Backfill 3 ngày gần nhất để cập nhật dữ liệu chuyển đổi muộn
          const sinceDate = today.subtract(3, 'day').format('YYYY-MM-DD');
          const untilDate = today.format('YYYY-MM-DD');

          this.logger.log(
            `📅 Account ${accountId}: Syncing ${idsChunk.length} adsets from ${sinceDate}`,
          );

          const cursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              level: 'adset',
              time_increment: 1, // Lấy dữ liệu chia theo từng ngày
              date_preset: 'maximum', // Thay thế 'lifetime' theo v10.0+ [2]
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              time_range: { since: sinceDate, until: untilDate },
              filtering: [
                { field: 'adset.id', operator: 'IN', value: idsChunk }, // Quét hàng loạt
              ],
            },
            true,
          );

          const insights = await fetchAll(cursor);
          if (insights.length === 0) continue;

          // 2. SỬ DỤNG TRANSACTION ĐỂ UPSERT DỮ LIỆU HÀNG LOẠT
          await this.prisma.$transaction(
            async (tx) => {
              for (const i of insights) {
                if (!i.adset_id) continue;

                const metrics = extractCampaignMetrics(i);
                const date = i.date_start;

                await tx.adSetInsight.upsert({
                  where: {
                    adSetId_dateStart_range: {
                      adSetId: i.adset_id,
                      dateStart: date,
                      range: InsightRange.DAILY,
                    },
                  },
                  update: {
                    dateStop: date,
                    ...metrics,
                    rawPayload: i,
                  },
                  create: {
                    adSetId: i.adset_id,
                    level: LevelInsight.ADSET,
                    range: InsightRange.DAILY,
                    dateStart: date,
                    dateStop: date,
                    ...metrics,
                    rawPayload: i,
                  },
                });
              }
            },
            {
              timeout: 15000, // Tăng timeout cho các transaction lớn
            },
          );

          // Nghỉ 1 giây giữa các chunk để bảo vệ hạn mức API [1]
          await sleep(1000);
        } catch (error) {
          const metaError = parseMetaError(error);
          this.logger.error(
            `Lỗi Daily Adset Insight Account ${accountId}: ${metaError.message}`,
          );
        }
      }
    }

    this.logger.log('✅ DAILY ADSET DONE');
  }
  // //
  // /* =====================================================
  //    AD MAX
  // ===================================================== */

  async syncMaxAdInsights() {
    this.logger.log('🔄 Sync MAX Ad Insight (Delete then Create)');
    this.init();

    // 1. Lấy danh sách Ad thuộc các Account không cần reauth
    // Truy vấn thông qua quan hệ để đảm bảo tính chính xác của accountId
    const ads = await this.prisma.ad.findMany({
      where: { adset: { campaign: { account: { needsReauth: false } } } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(ads);

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      // Chia lô 50 ID để tối ưu hóa truy vấn theo lô (Batch Fetching)
      for (const idsChunk of chunk(ids, 50)) {
        try {
          const cursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              level: 'ad',
              date_preset: 'maximum',
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              filtering: [{ field: 'ad.id', operator: 'IN', value: idsChunk }],
            },
            true,
          );

          const insights = await fetchAll(cursor);
          if (insights.length === 0) continue;

          // 2. SỬ DỤNG TRANSACTION ĐỂ XÓA VÀ TẠO MỚI DỮ LIỆU
          await this.prisma.$transaction(
            async (tx) => {
              const adIdsInInsight = insights
                .map((i) => i.ad_id)
                .filter((id): id is string => !!id);

              // Xóa các bản ghi MAX cũ của các Ad này để đảm bảo dữ liệu sạch
              await tx.adInsight.deleteMany({
                where: {
                  adId: { in: adIdsInInsight },
                  range: InsightRange.MAX,
                },
              });

              for (const i of insights) {
                if (!i.ad_id) continue;
                const metrics = extractCampaignMetrics(i);

                // Tạo mới bản ghi Insight
                await tx.adInsight.create({
                  data: {
                    adId: i.ad_id,
                    level: LevelInsight.AD,
                    range: InsightRange.MAX,
                    dateStart: i.date_start,
                    dateStop: i.date_stop,
                    ...metrics,
                    rawPayload: i,
                  },
                });

                // Đồng bộ trực tiếp metrics vào bảng Ad để truy vấn nhanh [Conversation History]
                await tx.ad.update({
                  where: { id: i.ad_id },
                  data: { ...metrics },
                });
              }
            },
            {
              timeout: 20000, // Tăng timeout cho các tác vụ xử lý hàng loạt
            },
          );

          // 3. Nghỉ để tránh lỗi Rate Limit 80004 (Too many calls) hoặc 613 [2, 3]
          await sleep(800);
        } catch (error) {
          const metaError = parseMetaError(error);
          this.logger.error(
            `Lỗi Sync Ad Insight Account ${accountId}: ${metaError.message}`,
          );
        }
      }
    }

    this.logger.log('✅ MAX AD DONE');
  }

  async syncMaxAdAudienceInsights() {
    this.logger.log('🔄 Sync MAX AD Audience Insight (Delete then Create)');
    this.init();

    // 1. Lấy danh sách Ad từ các Account không cần reauth [3, 4]
    const ads = await this.prisma.ad.findMany({
      where: { adset: { campaign: { account: { needsReauth: false } } } },
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(ads);

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      // Sử dụng chunk nhỏ (ví dụ 20) vì dữ liệu breakdown age/gender trả về rất nhiều dòng [2]
      for (const idsChunk of chunk(ids, 20)) {
        try {
          const audientCursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              level: 'ad',
              // Theo tài liệu, date_preset = lifetime đã bị thay thế bởi maximum (tối đa 37 tháng) [1]
              date_preset: 'maximum',
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              filtering: [{ field: 'ad.id', operator: 'IN', value: idsChunk }],
              breakdowns: ['age', 'gender'], // Phân tích chi tiết theo độ tuổi và giới tính
            },
            true,
          );

          const audients = await fetchAll(audientCursor);
          if (audients.length === 0) continue;

          // 2. SỬ DỤNG TRANSACTION ĐỂ XÓA VÀ TẠO MỚI DỮ LIỆU [Conversation History]
          await this.prisma.$transaction(
            async (tx) => {
              const adIdsInResult = [
                ...new Set(audients.map((a) => a.ad_id).filter(Boolean)),
              ];

              // Xóa các bản ghi audience cũ của các Ad này ở dải MAX để đảm bảo dữ liệu sạch
              await tx.adAudienceInsight.deleteMany({
                where: {
                  adId: { in: adIdsInResult as string[] },
                  range: InsightRange.MAX,
                },
              });

              // Tạo lại dữ liệu mới từ kết quả API
              for (const audient of audients) {
                if (!audient.ad_id) continue;

                const metrics = extractCampaignMetrics(audient);

                await tx.adAudienceInsight.create({
                  data: {
                    adId: audient.ad_id,
                    age: audient.age,
                    gender: audient.gender,
                    level: LevelInsight.AD,
                    range: InsightRange.MAX,
                    dateStart: audient.date_start,
                    dateStop: audient.date_stop,
                    ...metrics,
                    rawPayload: audient,
                  },
                });
              }
            },
            {
              timeout: 40000, // Tăng timeout vì dữ liệu breakdowns thường rất lớn
            },
          );

          // 3. Nghỉ để tránh lỗi Rate Limit 80004 hoặc 613 [2, 5]
          await sleep(800);
        } catch (error) {
          const metaError = parseMetaError(error);
          this.logger.error(
            `Lỗi Sync Ad Audience Insight Account ${accountId}: ${metaError.message}`,
          );
        }
      }
    }

    this.logger.log('✅ MAX AD AUDIENCE DONE');
  }

  // /* =====================================================
  //    AD DAILY (dựa theo MAX)
  // ===================================================== */

  async syncDailyAdInsights() {
    this.logger.log('🔄 Sync DAILY Ad Insight (Optimized Batch)');
    this.init();

    const today = dayjs().startOf('day');

    // 1. Lấy danh sách Ad cần đồng bộ dựa trên dữ liệu MAX có chi tiêu [Conversation History]
    const maxInsights = await this.prisma.adInsight.findMany({
      where: {
        range: InsightRange.MAX,
        spend: { gt: 0 },
        ad: { adset: { campaign: { account: { needsReauth: false } } } },
      },
      select: {
        adId: true,
        ad: { select: { accountId: true } },
      },
    });

    if (maxInsights.length === 0) return;

    // Nhóm các Ad ID theo từng AccountId để thực hiện Batch Fetching
    const byAccount = this.groupByAccount(
      maxInsights.map((m) => ({ id: m.adId, accountId: m.ad.accountId })),
    );

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      // Chia lô 50 ID để tối ưu hóa request và tránh lỗi giới hạn tham số API [1]
      for (const idsChunk of chunk(ids, 50)) {
        try {
          // 🔥 Backfill 3 ngày gần nhất để cập nhật dữ liệu chuyển đổi muộn (attribution delay)
          const sinceDate = today.subtract(3, 'day').format('YYYY-MM-DD');
          const untilDate = today.format('YYYY-MM-DD');

          this.logger.log(
            `📅 Account ${accountId}: Syncing ${idsChunk.length} ads from ${sinceDate}`,
          );

          const cursor = await adAccount.getInsights(
            AD_INSIGHT_FIELDS,
            {
              level: 'ad',
              time_increment: 1, // Bắt buộc để nhận dữ liệu breakdown theo từng ngày [3]
              // Sử dụng 'maximum' thay cho 'lifetime' đã bị vô hiệu hóa từ v10.0 [4]
              date_preset: 'maximum',
              action_attribution_windows: '7d_click',
              action_breakdowns: 'action_type',
              time_range: { since: sinceDate, until: untilDate },
              filtering: [
                { field: 'ad.id', operator: 'IN', value: idsChunk }, // Quét hàng loạt theo lô [1]
              ],
            },
            true,
          );

          const insights = await fetchAll(cursor);
          if (insights.length === 0) continue;

          // 2. SỬ DỤNG TRANSACTION ĐỂ UPSERT DỮ LIỆU HÀNG LOẠT [Conversation History]
          await this.prisma.$transaction(
            async (tx) => {
              for (const i of insights) {
                if (!i.ad_id) continue;

                const metrics = extractCampaignMetrics(i);
                const date = i.date_start;

                await tx.adInsight.upsert({
                  where: {
                    adId_dateStart_range: {
                      adId: i.ad_id,
                      dateStart: date,
                      range: InsightRange.DAILY,
                    },
                  },
                  update: {
                    dateStop: date,
                    ...metrics,
                    rawPayload: i,
                  },
                  create: {
                    adId: i.ad_id,
                    level: LevelInsight.AD,
                    range: InsightRange.DAILY,
                    dateStart: date,
                    dateStop: date,
                    ...metrics,
                    rawPayload: i,
                  },
                });
              }
            },
            {
              timeout: 20000, // Tăng timeout cho các transaction quy mô lớn
            },
          );

          // 3. Nghỉ để bảo vệ hạn mức API (Rate Limit) [1, 2]
          await sleep(1200);
        } catch (error) {
          const metaError = parseMetaError(error);
          this.logger.error(
            `Lỗi Daily Ad Insight Account ${accountId}: ${metaError.message}`,
          );
        }
      }
    }

    this.logger.log('✅ DAILY AD DONE');
  }

  async calculateCreativeInsightFromAdInsight() {
    console.log('Start calculate CreativeInsight...');

    const today = dayjs().format('YYYY-MM-DD');
    const sevenDaysAgo = dayjs().subtract(6, 'day').format('YYYY-MM-DD');
    const threeDaysAgo = dayjs().subtract(2, 'day').format('YYYY-MM-DD');

    // 1️⃣ load data 1 lần
    const [creatives, insights] = await Promise.all([
      this.prisma.creative.findMany({
        select: {
          id: true,
          ads: { select: { id: true } },
        },
      }),
      this.prisma.adInsight.findMany({
        where: {
          range: { in: ['DAILY', 'MAX'] },
        },
      }),
    ]);

    // 2️⃣ map ad → creative
    const adToCreative = new Map<string, string>();
    for (const c of creatives) {
      for (const ad of c.ads) {
        adToCreative.set(ad.id, c.id);
      }
    }

    // 3️⃣ group theo creative
    const creativeMap = new Map<string, any>();

    for (const ins of insights) {
      const creativeId = adToCreative.get(ins.adId);
      if (!creativeId) continue;

      if (!creativeMap.has(creativeId)) {
        creativeMap.set(creativeId, {
          daily: {},
          max: [],
          last7d: [],
          last3d: [],
        });
      }

      const bucket = creativeMap.get(creativeId);

      if (ins.range === 'DAILY') {
        // DAILY
        if (!bucket.daily[ins.dateStart]) {
          bucket.daily[ins.dateStart] = {};
        }

        sumMetric(bucket.daily[ins.dateStart], ins);

        // 7d
        if (ins.dateStart >= sevenDaysAgo && ins.dateStart <= today) {
          bucket.last7d.push(ins);
        }

        // 3d
        if (ins.dateStart >= threeDaysAgo && ins.dateStart <= today) {
          bucket.last3d.push(ins);
        }
      }

      if (ins.range === 'MAX') {
        bucket.max.push(ins);
      }
    }

    // helper
    function sumMetric(target, source) {
      for (const key in source) {
        if (typeof source[key] === 'number') {
          target[key] = (target[key] || 0) + source[key];
        }
      }
    }

    // 4️⃣ transaction write
    await this.prisma.$transaction(
      async (tx) => {
        for (const [creativeId, data] of creativeMap) {
          // DAILY
          for (const date in data.daily) {
            await tx.creativeInsight.upsert({
              where: {
                creativeId_dateStart_range: {
                  creativeId,
                  dateStart: date,
                  range: 'DAILY',
                },
              },
              update: data.daily[date],
              create: {
                creativeId,
                dateStart: date,
                dateStop: date,
                range: 'DAILY',
                ...data.daily[date],
              },
            });
          }

          // MAX
          const maxSum = {} as any;
          data.max.forEach((m) => sumMetric(maxSum, m));

          await tx.creativeInsight.upsert({
            where: {
              creativeId_dateStart_range: {
                creativeId,
                dateStart: '1975-01-01',
                range: 'MAX',
              },
            },
            update: maxSum,
            create: {
              creativeId,
              range: 'MAX',
              dateStart: '1975-01-01',
              dateStop: today,
              ...maxSum,
            },
          });

          // 7d
          const sum7d = {} as any;
          data.last7d.forEach((m) => sumMetric(sum7d, m));

          await tx.creativeInsight.upsert({
            where: {
              creativeId_dateStart_range: {
                creativeId,
                dateStart: sevenDaysAgo,
                range: 'DAY_7',
              },
            },
            update: sum7d,
            create: {
              creativeId,
              range: 'DAY_7',
              dateStart: sevenDaysAgo,
              dateStop: today,
              ...sum7d,
            },
          });

          // 3d
          const sum3d = {} as any;
          data.last3d.forEach((m) => sumMetric(sum3d, m));

          await tx.creativeInsight.upsert({
            where: {
              creativeId_dateStart_range: {
                creativeId,
                dateStart: threeDaysAgo,
                range: 'DAY_3',
              },
            },
            update: sum3d,
            create: {
              creativeId,
              range: 'DAY_3',
              dateStart: threeDaysAgo,
              dateStop: today,
              ...sum3d,
            },
          });

          // 👉 giữ nguyên rule engine (copy logic cũ của bạn vào đây)
          const maxSpend = maxSum.spend ?? 0;
          const maxRevenue = maxSum.purchaseValue ?? 0;
          const maxPurchases = maxSum.purchases ?? 0;
          const maxClicks = maxSum.clicks ?? 0;
          const maxImpressions = maxSum.impressions ?? 0;

          const roasMax = maxSpend > 0 ? maxRevenue / maxSpend : 0;
          const ctrMax = maxImpressions > 0 ? maxClicks / maxImpressions : 0;

          const spend7d = sum7d.spend ?? 0;
          const revenue7d = sum7d.purchaseValue ?? 0;
          const roas7d = spend7d > 0 ? revenue7d / spend7d : 0;

          const spend3d = sum3d.spend ?? 0;
          const revenue3d = sum3d.purchaseValue ?? 0;
          const roas3d = spend3d > 0 ? revenue3d / spend3d : 0;

          let status: CreativeStatus;

          // TEST
          if (maxSpend === 0) {
            status = CreativeStatus.TEST;
          }

          // NEED_SPEND
          else if (maxSpend > 0 && maxSpend <= 100000) {
            status = CreativeStatus.NEED_SPEND;
          }

          // SCALE_P1
          else if (
            (maxSpend > 100000 && maxSpend <= 500000 && roasMax >= 2) ||
            (maxSpend > 500000 && roasMax >= 2.2) ||
            roas7d >= 2.5
          ) {
            status = CreativeStatus.SCALE_P1;
          }

          // SCALE_P2
          else if (
            (maxSpend > 100000 && maxSpend <= 500000 && roasMax >= 1.5) ||
            (maxSpend > 500000 && roasMax >= 1.8 && ctrMax > 0.03) ||
            (roas7d >= 2.2 && roas3d >= 2.2)
          ) {
            status = CreativeStatus.SCALE_P2;
          }

          // REVIEW
          else if (
            (maxSpend > 100000 &&
              maxSpend <= 500000 &&
              maxPurchases < 1 &&
              ctrMax > 0.03) ||
            (maxSpend > 500000 && roasMax < 1.8 && ctrMax > 0.03)
          ) {
            status = CreativeStatus.REVIEW;
          }

          // OFF
          else {
            status = CreativeStatus.OFF;
          }
          await tx.creative.update({
            where: { id: creativeId },
            data: {
              performanceStatus: status,
              ...maxSum,
            },
          });
        }
      },
      { timeout: 20000 },
    );

    console.log('CreativeInsight updated');
  }

  async syncImage() {
    this.logger.log('🔄 Chỉ cập nhật AdImage đã tồn tại trong DB...');
    this.init();
    const expiredDate = new Date(Date.now() - 12 * 60 * 60 * 1000);

    try {
      // 1. Lấy danh sách các hình ảnh hiện có, nhóm theo accountId
      const existingImages = await this.prisma.adImage.findMany({
        where: {
          account: { needsReauth: false },
          OR: [{ url: null }, { updatedAt: { lt: expiredDate } }],
        },
        select: {
          hash: true,
          accountId: true,
          updatedAt: true,
          url: true,
          createdTime: true,
        },
      });

      if (existingImages.length === 0) return;

      const needRefresh = existingImages.filter((img) => {
        if (!img.url) return true;

        return this.isMetaUrlExpired(img.url);
      });

      const byAccount = this.groupByAccount(
        existingImages.map((img) => ({
          id: img.hash,
          accountId: img.accountId,
        })),
      );

      for (const [accountId, hashes] of Object.entries(byAccount)) {
        const adAccount = new AdAccount(accountId);

        // 2. Chia nhỏ mảng hashes (ví dụ 50 cái) để gửi 1 request duy nhất
        for (const hashChunk of chunk(hashes, 50)) {
          try {
            const cursor = await adAccount.getAdImages(
              AD_IMAGE_FIELDS,
              { hashes: hashChunk }, // Meta hỗ trợ filter trực tiếp theo mảng hashes
            );

            const images = await fetchAll(cursor);

            if (images.length > 0) {
              await this.prisma.$transaction(
                images.map((img) =>
                  this.prisma.adImage.updateMany({
                    where: { hash: img.hash, accountId },
                    data: {
                      name: img?.name,
                      url: img?.permalink_url || img?.url,
                      permalink_url: img?.permalink_url,
                      height: img?.height,
                      width: img?.width,
                      rawPayload: toPrismaJson(img),
                      status: img?.status,
                      createdTime: img?.created_time
                        ? new Date(img?.created_time)
                        : undefined,
                      createdAt: img?.created_time
                        ? new Date(img?.created_time)
                        : undefined,
                      updatedAt: new Date(),
                    },
                  }),
                ),
              );
            }
            // Tránh lỗi 80004 (too many calls) [1]
            await sleep(1000);
          } catch (error) {
            this.logger.error(
              `Lỗi syncImage Account ${accountId}: ${parseMetaError(error).message}`,
            );
          }
        }
      }
      console.log(`cập nhật ${existingImages.length} image`);
    } catch (err) {
      this.logger.error(`Lỗi nghiêm trọng syncImage: ${err.message}`);
    }
  }

  async syncVideo() {
    this.logger.log(
      '🔄 Cập nhật AdVideo đã tồn tại trong DB (Sử dụng Multiple Object IDs)',
    );
    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
    const expiredDate = new Date(Date.now() - 12 * 60 * 60 * 1000);
    try {
      // 1. Lấy toàn bộ danh sách Video ID hiện có trong DB [2]
      // Chúng ta không cần groupBy theo accountId nữa vì sẽ gọi trực tiếp qua ID video
      const existingVideos = await this.prisma.adVideo.findMany({
        where: {
          account: { needsReauth: false },
          OR: [{ thumbnailUrl: null }, { updatedAt: { lt: expiredDate } }],
        },
        select: { id: true },
      });

      if (existingVideos.length === 0) {
        this.logger.warn('⚠️ Không có video nào trong DB để cập nhật.');
        return;
      }

      const videoIds = existingVideos.map((v) => v.id);

      // 2. Batch Processing: Lấy 50 video mỗi request bằng Multiple Object IDs (?ids=...)
      // Thay vì quét edge của Account, chúng ta truy vấn trực tiếp các Node Video
      for (const idsChunk of chunk(videoIds, 50)) {
        try {
          // Sử dụng FacebookAdsApi.instance.call để thực hiện request GET /?ids=ID1,ID2...
          // Thêm 'account_id' vào fields để đảm bảo accountId trong DB luôn chính xác
          const response = (await api.call(
            'GET',
            ['/'], // Endpoint gốc
            {
              ids: idsChunk.join(','),
              fields: [...AD_VIDEO_FIELDS].join(','),
            },
          )) as any;
          // Kết quả trả về là một Object với keys là ID của từng video
          const videosMap = response || {};
          const videos = Object.values(videosMap);

          if (videos.length > 0) {
            // Sử dụng Prisma Transaction để cập nhật hàng loạt nhằm đảm bảo tính toàn vẹn [3]
            await this.prisma.$transaction(
              videos.map((vid: any) => {
                return this.prisma.adVideo.update({
                  where: { id: vid.id },
                  data: {
                    title: vid?.title || vid?.name,
                    accountId: vid?.account_id,
                    source: vid?.source || undefined,
                    status: vid?.status?.video_status || vid?.status,
                    thumbnailUrl:
                      vid?.thumbnails?.data?.find((tn) => tn?.is_preferred)
                        ?.url || vid?.picture,
                    length: vid?.length,
                    rawPayload: toPrismaJson(vid),
                  },
                });
              }),
            );
          }

          this.logger.log(
            `✅ Đã cập nhật thông tin cho ${videos.length} video`,
          );

          // Nghỉ giữa các lô để bảo vệ hạn mức API chung [1]
          await sleep(1000);
        } catch (error) {
          this.logger.error(
            `Lỗi syncVideo Multiple IDs: ${parseMetaError(error).message}`,
          );
        }
      }
    } catch (err) {
      this.logger.error(`Lỗi nghiêm trọng syncVideo: ${err.message}`);
    }
  }

  private getMetaUrlExpireDate(url?: string): Date | null {
    if (!url) return null;

    try {
      const u = new URL(url);
      const oe = u.searchParams.get('oe');
      if (!oe) return null;

      const timestamp = parseInt(oe, 16);
      return new Date(timestamp * 1000);
    } catch {
      return null;
    }
  }
  private isMetaUrlExpired(url?: string): boolean {
    const expireDate = this.getMetaUrlExpireDate(url);
    if (!expireDate) return true;

    return expireDate.getTime() < Date.now();
  }

  // // HELPER
  private groupByAccount(records: any[]) {
    return records.reduce<Record<string, string[]>>((acc, r) => {
      (acc[r.accountId] ||= []).push(r.id);
      return acc;
    }, {});
  }
}
