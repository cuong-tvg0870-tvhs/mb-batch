import {
  Injectable,
  InternalServerErrorException,
  Logger,
} from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { InsightRange, LevelInsight } from '@prisma/client';
import * as dayjs from 'dayjs';
import {
  Ad,
  AdAccount,
  AdSet,
  FacebookAdsApi,
} from 'facebook-nodejs-business-sdk';
import { groupBy } from 'lodash';
import {
  chunk,
  extractCampaignMetrics,
  fetchAll,
  LIMIT_DATA,
  parseMetaError,
  sleep,
} from 'src/common/utils';
import {
  AD_INSIGHT_FIELDS,
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
    // await this.syncCampaignCore();
    await this.syncMaxInsights();
    // await this.syncDailyInsights();
  }

  @Cron('0 0 0,12 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncCampaignCore() {
    await this.syncCampaignService();
  }

  @Cron('0 10 0,6,12,18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMaxInsights() {
    // await this.syncMaxCampaignInsights();
    await this.syncMaxAdsetInsights();
    // await this.syncMaxAdInsights();
  }

  @Cron('0 20 3,9,15,21 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyInsights() {
    await this.syncDailyCampaignInsights();
    await this.syncDailyAdsetInsights();
    await this.syncDailyAdInsights();
  }

  async syncCampaignService() {
    this.logger.log('⏰ Sync Campaign Data...');
    this.init();

    try {
      const accounts = await this.prisma.account.findMany({});

      for (const acc of accounts) {
        this.logger.log(`🔹 Account: ${acc.name} (${acc.id})`);

        const adAccount = new AdAccount(acc.id);

        /**
         * 1️⃣ Lấy mốc updated_time mới nhất trong DB
         */
        const lastCampaign = await this.prisma.campaign.findFirst({
          where: { accountId: acc.id },
          orderBy: { updatedAt: 'desc' },
          select: { updatedAt: true },
        });

        const lastSyncUnix = lastCampaign
          ? Math.floor(
              (new Date(lastCampaign.updatedAt).getTime() - 5 * 60 * 1000) /
                1000,
            )
          : Math.floor((Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000);

        /**
         * 2️⃣ Fetch Campaign thay đổi
         */
        const campaignCursor = await adAccount.getCampaigns(
          [
            ...CAMPAIGN_FIELDS,
            `insights.date_preset(maximum).limit(1).level(campaign){${AD_INSIGHT_FIELDS.join(
              ',',
            )}}`,
          ],
          {
            limit: LIMIT_DATA,
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

        const campaigns = await fetchAll(campaignCursor);

        if (!campaigns.length) continue;

        const campaignIds = campaigns.map((c) => c.id);

        /**
         * 3️⃣ Fetch AdSet theo campaign.id IN (...)
         */
        const adSets: any[] = [];

        for (const ids of chunk(campaignIds, 50)) {
          const adsetCursor = await adAccount.getAdSets(
            [
              ...ADSET_FIELDS,
              `insights.date_preset(maximum).limit(1).level(adset){${AD_INSIGHT_FIELDS.join(
                ',',
              )}}`,
            ],
            {
              limit: LIMIT_DATA,
              filtering: [{ field: 'campaign.id', operator: 'IN', value: ids }],
            },
            true,
          );

          const result = await fetchAll(adsetCursor);
          adSets.push(...result);

          await sleep(500);
        }

        const adsetIds = adSets.map((a) => a.id);

        /**
         * 4️⃣ Fetch Ads theo adset.id IN (...)
         */
        const ads: any[] = [];

        for (const ids of chunk(adsetIds, 50)) {
          const adCursor = await adAccount.getAds(
            [
              Ad.Fields.id,
              Ad.Fields.account_id,
              Ad.Fields.campaign_id,
              Ad.Fields.adset_id,
              Ad.Fields.name,
              Ad.Fields.status,
              Ad.Fields.effective_status,
              Ad.Fields.created_time,
              Ad.Fields.updated_time,
              `creative{${CREATIVE_FIELDS.join(',')}}`,
              `insights.date_preset(maximum).limit(1).level(ad){${AD_INSIGHT_FIELDS.join(
                ',',
              )}}`,
            ],
            {
              limit: LIMIT_DATA,
              filtering: [{ field: 'adset.id', operator: 'IN', value: ids }],
            },
            true,
          );

          const result = await fetchAll(adCursor);
          ads.push(...result);

          await sleep(500);
        }

        /**
         * 5️⃣ Build mapping
         */
        const adSetsByCampaign = groupBy(adSets, (as) => as.campaign_id);
        const adsByAdSet = groupBy(ads, (ad) => ad.adset_id);

        for (const ad of ads) {
          await this.metaService.syncAdAssetsLegacy(
            adAccount,
            adAccount.id,
            ad,
          );

          await this.prisma.$transaction(async (tx) => {
            await this.upsertDataService.upsertCreativeLegacy(tx, acc.id, ad);
          });
        }
        /**
         * 6️⃣ Upsert tree
         */
        for (const campaign of campaigns) {
          await this.prisma.$transaction(async (tx) => {
            await this.upsertDataService.upsertCampaign(tx, acc.id, campaign);

            const campaignAdSets = adSetsByCampaign[campaign.id] ?? [];

            for (const adset of campaignAdSets) {
              await this.upsertDataService.upsertAdSet(
                tx,
                acc.id,
                campaign.id,
                adset,
              );

              const adsetAds = adsByAdSet[adset.id] ?? [];

              for (const ad of adsetAds) {
                await this.upsertDataService.upsertAdLegacy(
                  tx,
                  acc.id,
                  campaign.id,
                  adset.id,
                  ad,
                );
              }
            }
          });
        }

        this.logger.log(
          `✅ Account ${acc.id} synced: ${campaigns.length} campaigns`,
        );
      }

      this.logger.log('--- END Campaign Data ---');
      return { status: 'DONE' };
    } catch (err) {
      throw new InternalServerErrorException(parseMetaError(err));
    }
  }

  /* =====================================================
     CAMPAIGN MAX
  ===================================================== */

  async syncMaxCampaignInsights() {
    this.logger.log('🔄 Sync MAX Campaign Insight');
    this.init();

    const campaigns = await this.prisma.campaign.findMany({
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(campaigns);

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      for (const idsChunk of chunk(ids, 50)) {
        const cursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'campaign',
            date_preset: 'maximum',
            filtering: [
              { field: 'campaign.id', operator: 'IN', value: idsChunk },
            ],
          },
          true,
        );

        const insights = await fetchAll(cursor);

        for (const i of insights) {
          if (!i.campaign_id) continue;

          await this.prisma.campaignInsight.upsert({
            where: {
              campaignId_dateStart_range: {
                dateStart: i.date_start,
                campaignId: i.campaign_id,
                range: InsightRange.MAX,
              },
            },
            update: {
              dateStart: i.date_start,
              dateStop: i.date_stop,
              ...extractCampaignMetrics(i),
              rawPayload: i,
            },
            create: {
              campaignId: i.campaign_id,
              level: LevelInsight.CAMPAIGN,
              range: InsightRange.MAX,
              dateStart: i.date_start,
              dateStop: i.date_stop,
              ...extractCampaignMetrics(i),
              rawPayload: i,
            },
          });

          await this.prisma.campaign.update({
            where: { id: i.campaign_id },
            data: { ...extractCampaignMetrics(i) },
          });
        }

        await sleep(800);
      }
    }

    this.logger.log('✅ MAX Campaign DONE');
  }

  /* =====================================================
     CAMPAIGN DAILY (dựa theo MAX)
  ===================================================== */

  async syncDailyCampaignInsights() {
    this.logger.log('🔄 Sync DAILY Campaign Insight');
    this.init();

    const today = dayjs().startOf('day');

    const maxInsights = await this.prisma.campaignInsight.findMany({
      where: {
        range: InsightRange.MAX,
        level: LevelInsight.CAMPAIGN,
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

      // ⛔ nếu MAX đã kết thúc quá lâu thì skip
      if (maxStopRaw.add(3, 'day').isBefore(today)) {
        continue;
      }

      const maxStop = maxStopRaw.isAfter(today) ? today : maxStopRaw;

      const lastDaily = await this.prisma.campaignInsight.findFirst({
        where: { campaignId: max.campaignId, range: InsightRange.DAILY },
        orderBy: { dateStart: 'desc' },
      });

      let since: dayjs.Dayjs;

      if (lastDaily) {
        // 🔥 rolling back 3 ngày để tránh thiếu data
        since = dayjs(lastDaily.dateStart).subtract(2, 'day');
      } else {
        since = maxStart;
      }

      if (since.isBefore(maxStart)) since = maxStart;
      if (since.isAfter(maxStop)) continue;

      const adAccount = new AdAccount(accountId);

      try {
        this.logger.log(
          `📅 Campaign ${max.campaignId} → ${since.format(
            'DD/MM',
          )} → ${maxStop.format('DD/MM')}`,
        );
        const cursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'campaign',
            time_increment: 1,
            date_preset: 'maximum',
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

        for (const i of insights) {
          if (!i.campaign_id) continue;
          const date = i.date_start;
          await this.prisma.campaignInsight.upsert({
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

        await sleep(2000);
      } catch (error: any) {
        this.logger.error(`❌ DAILY Campaign failed: ${max.campaignId}`);
        this.logger.error(error?.response?.body || error?.message);
        this.logger.error(error);
      }
    }

    this.logger.log('✅ DAILY Campaign DONE');
  }

  /* =====================================================
     ADSET MAX
  ===================================================== */

  async syncMaxAdsetInsights() {
    this.logger.log('🔄 Sync MAX Adset Insight');
    this.init();

    const adsets = await this.prisma.adSet.findMany({
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(adsets);

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      for (const idsChunk of chunk(ids, 50)) {
        const cursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'adset',
            date_preset: 'maximum',
            filtering: [{ field: 'adset.id', operator: 'IN', value: idsChunk }],
          },
          true,
        );

        const insights = await fetchAll(cursor);

        for (const i of insights) {
          if (!i.adset_id) continue;

          await this.prisma.adSetInsight.upsert({
            where: {
              adSetId_dateStart_range: {
                dateStart: i.date_start,
                adSetId: i.adset_id,
                range: InsightRange.MAX,
              },
            },
            update: {
              dateStart: i.date_start,
              dateStop: i.date_stop,
              ...extractCampaignMetrics(i),
              rawPayload: i,
            },
            create: {
              adSetId: i.adset_id,
              level: LevelInsight.ADSET,
              range: InsightRange.MAX,
              dateStart: i.date_start,
              dateStop: i.date_stop,
              ...extractCampaignMetrics(i),
              rawPayload: i,
            },
          });

          await this.prisma.adSet.update({
            where: { id: i.adset_id },
            data: { ...extractCampaignMetrics(i) },
          });
        }

        const audientCursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'adset',
            date_preset: 'maximum',
            filtering: [{ field: 'adset.id', operator: 'IN', value: idsChunk }],
            breakdowns: ['age', 'gender'], // 👈 thêm dòng này
          },
          true,
        );

        const audients = await fetchAll(audientCursor);
        for (const audient of audients) {
          if (!audient.adset_id) continue;

          await this.prisma.adsetAudienceInsight.upsert({
            where: {
              adsetId_age_gender_level_range_dateStart: {
                adsetId: audient.adset_id,
                age: audient.age,
                gender: audient.gender,
                level: LevelInsight.ADSET,
                dateStart: audient.date_start,
                range: InsightRange.MAX,
              },
            },
            update: {
              age: audient.age,
              gender: audient.gender,
              dateStart: audient.date_start,
              dateStop: audient.date_stop,
              ...extractCampaignMetrics(audient),
              rawPayload: audient,
            },
            create: {
              adsetId: audient.adset_id,
              age: audient.age,
              gender: audient.gender,
              level: LevelInsight.ADSET,
              range: InsightRange.MAX,

              dateStart: audient.date_start,
              dateStop: audient.date_stop,
              ...extractCampaignMetrics(audient),
              rawPayload: audient,
            },
          });

          await this.prisma.ad.update({
            where: { id: audient.ad_id },
            data: { ...extractCampaignMetrics(audient) },
          });
        }

        await sleep(800);
      }
    }

    this.logger.log('✅ MAX ADSET DONE');
  }

  /* =====================================================
     ADSET DAILY (dựa theo MAX)
  ===================================================== */

  async syncDailyAdsetInsights() {
    this.logger.log('🔄 Sync DAILY Adset Insight');
    this.init();

    const today = dayjs().startOf('day');

    const maxInsights = await this.prisma.adSetInsight.findMany({
      where: { range: InsightRange.MAX, level: LevelInsight.ADSET },
      select: {
        adSetId: true,
        dateStart: true,
        dateStop: true,
        adSet: { select: { accountId: true } },
      },
    });

    for (const max of maxInsights) {
      const accountId = max.adSet?.accountId;
      if (!accountId) continue;

      const maxStart = dayjs(max.dateStart);
      const maxStopRaw = dayjs(max.dateStop);

      // ⛔ nếu MAX đã kết thúc quá lâu thì skip
      if (maxStopRaw.add(3, 'day').isBefore(today)) {
        continue;
      }

      const maxStop = maxStopRaw.isAfter(today) ? today : maxStopRaw;

      const lastDaily = await this.prisma.adSetInsight.findFirst({
        where: { adSetId: max.adSetId, range: InsightRange.DAILY },
        orderBy: { dateStart: 'desc' },
      });

      let since: dayjs.Dayjs;

      if (lastDaily) {
        // 🔥 rolling back 3 ngày để tránh thiếu data
        since = dayjs(lastDaily.dateStart).subtract(2, 'day');
      } else {
        since = maxStart;
      }

      if (since.isBefore(maxStart)) since = maxStart;
      if (since.isAfter(maxStop)) continue;

      const adAccount = new AdAccount(accountId);

      try {
        this.logger.log(
          `📅 Adset ${max.adSetId} → ${since.format(
            'DD/MM/YYYY',
          )} → ${maxStop.format('DD/MM/YYYY')}`,
        );

        const adset = new AdSet(max.adSetId);
        const cursor = await adset.getInsights(AD_INSIGHT_FIELDS, {
          level: 'adset',
          time_increment: 1,
          date_preset: 'maximum',
          time_range: {
            since: since.format('YYYY-MM-DD'),
            until: maxStop.format('YYYY-MM-DD'),
          },
        });

        const insights = await fetchAll(cursor);
        for (const i of insights) {
          if (!i.adset_id) continue;
          const date = i.date_start;
          await this.prisma.adSetInsight.upsert({
            where: {
              adSetId_dateStart_range: {
                adSetId: i.adset_id,
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
              adSetId: i.adset_id,
              level: LevelInsight.ADSET,
              range: InsightRange.DAILY,
              dateStart: date,
              dateStop: date,
              ...extractCampaignMetrics(i),
              rawPayload: i,
            },
          });
        }

        await sleep(2000);
      } catch (error: any) {
        this.logger.error(`❌ DAILY Adset failed: ${max.adSetId}`);
        this.logger.error(error?.response?.body || error?.message);
        this.logger.error(error);
      }
    }

    this.logger.log('✅ DAILY ADSET DONE');
  }
  //
  /* =====================================================
     AD MAX
  ===================================================== */

  async syncMaxAdInsights() {
    this.logger.log('🔄 Sync MAX Ad Insight');
    this.init();

    const ads = await this.prisma.ad.findMany({
      select: { id: true, accountId: true },
    });

    const byAccount = this.groupByAccount(ads);

    for (const [accountId, ids] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      for (const idsChunk of chunk(ids, 50)) {
        const cursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'ad',
            date_preset: 'maximum',
            filtering: [{ field: 'ad.id', operator: 'IN', value: idsChunk }],
          },
          true,
        );

        const insights = await fetchAll(cursor);

        for (const i of insights) {
          if (!i.ad_id) continue;

          await this.prisma.adInsight.upsert({
            where: {
              adId_dateStart_range: {
                dateStart: i.date_start,
                adId: i.ad_id,
                range: InsightRange.MAX,
              },
            },
            update: {
              dateStart: i.date_start,
              dateStop: i.date_stop,
              ...extractCampaignMetrics(i),
              rawPayload: i,
            },
            create: {
              adId: i.ad_id,
              level: LevelInsight.AD,
              range: InsightRange.MAX,
              dateStart: i.date_start,
              dateStop: i.date_stop,
              ...extractCampaignMetrics(i),
              rawPayload: i,
            },
          });

          await this.prisma.ad.update({
            where: { id: i.ad_id },
            data: { ...extractCampaignMetrics(i) },
          });
        }

        await sleep(800);
      }
    }

    this.logger.log('✅ MAX AD DONE');
  }

  /* =====================================================
     AD DAILY (dựa theo MAX)
  ===================================================== */

  async syncDailyAdInsights() {
    this.logger.log('🔄 Sync DAILY AD Insight');
    this.init();

    const today = dayjs().startOf('day');

    const maxInsights = await this.prisma.adInsight.findMany({
      where: { range: InsightRange.MAX, level: LevelInsight.AD },
      select: {
        adId: true,
        dateStart: true,
        dateStop: true,
        ad: { select: { accountId: true } },
      },
    });

    for (const max of maxInsights) {
      const accountId = max.ad?.accountId;
      if (!accountId) continue;

      const maxStart = dayjs(max.dateStart);
      const maxStopRaw = dayjs(max.dateStop);

      // ⛔ nếu MAX đã kết thúc quá lâu thì skip
      if (maxStopRaw.add(3, 'day').isBefore(today)) {
        continue;
      }

      const maxStop = maxStopRaw.isAfter(today) ? today : maxStopRaw;

      const lastDaily = await this.prisma.adInsight.findFirst({
        where: { adId: max.adId, range: InsightRange.DAILY },
        orderBy: { dateStart: 'desc' },
      });

      let since: dayjs.Dayjs;

      if (lastDaily) {
        // 🔥 rolling back 3 ngày để tránh thiếu data
        since = dayjs(lastDaily.dateStart).subtract(2, 'day');
      } else {
        since = maxStart;
      }

      if (since.isBefore(maxStart)) since = maxStart;
      if (since.isAfter(maxStop)) continue;

      const adAccount = new AdAccount(accountId);

      try {
        this.logger.log(
          `📅 AD ${max.adId} → ${since.format(
            'DD/MM/YYYY',
          )} → ${maxStop.format('DD/MM/YYYY')}`,
        );

        const ad = new Ad(max.adId);
        const cursor = await ad.getInsights(AD_INSIGHT_FIELDS, {
          level: 'ad',
          time_increment: 1,
          date_preset: 'maximum',
          time_range: {
            since: since.format('YYYY-MM-DD'),
            until: maxStop.format('YYYY-MM-DD'),
          },
        });

        const insights = await fetchAll(cursor);
        for (const i of insights) {
          if (!i.ad_id) continue;
          const date = i.date_start;
          await this.prisma.adInsight.upsert({
            where: {
              adId_dateStart_range: {
                adId: i.ad_id,
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
              adId: i.ad_id,
              level: LevelInsight.AD,
              range: InsightRange.DAILY,
              dateStart: date,
              dateStop: date,
              ...extractCampaignMetrics(i),
              rawPayload: i,
            },
          });
        }

        await sleep(2000);
      } catch (error: any) {
        this.logger.error(`❌ DAILY AD failed: ${max.adId}`);
        this.logger.error(error?.response?.body || error?.message);
        this.logger.error(error);
      }
    }

    this.logger.log('✅ DAILY AD DONE');
  }
  //

  // HELPER
  private groupByAccount(records: any[]) {
    return records.reduce<Record<string, string[]>>((acc, r) => {
      (acc[r.accountId] ||= []).push(r.id);
      return acc;
    }, {});
  }
}
