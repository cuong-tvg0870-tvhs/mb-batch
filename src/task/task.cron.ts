import { InjectQueue } from '@nestjs/bull';
import {
  Injectable,
  InternalServerErrorException,
  Logger,
} from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InsightRange, LevelInsight } from '@prisma/client';
import { Queue } from 'bull';
import * as dayjs from 'dayjs';
import { Ad, AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
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
import { PrismaService } from 'src/modules/prisma/prisma.service';
@Injectable()
export class TaskCron {
  private readonly logger = new Logger(TaskCron.name);

  constructor(
    private readonly prisma: PrismaService,
    private upsertDataService: UpsertService,
    @InjectQueue('meta-sync')
    private readonly queue: Queue,
  ) {}

  private initialized = false;

  private init() {
    if (!this.initialized) {
      FacebookAdsApi.init(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
      this.initialized = true;
    }
  }

  async onModuleInit() {
    this.logger.log('🚀 App started → scan video immediately');
    // await this.SyncCampaignService();
    // await this.syncDailyAdsetInsights();
    // await this.syncDailyAdInsights();
  }

  @Cron(CronExpression.EVERY_DAY_AT_MIDNIGHT, { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncCampaignService() {
    this.logger.log('⏰ Sync Campaign Data...');
    await this.init();

    try {
      const accounts = await this.prisma.account.findMany({});

      for (const acc of accounts) {
        console.log('acc id', acc.name, acc.id);
        const adAccount = new AdAccount(acc.id);
        const campaignCursor = await adAccount.getCampaigns(
          [
            ...CAMPAIGN_FIELDS,
            `insights.date_preset(maximum).limit(1).level(campaign){${AD_INSIGHT_FIELDS.join(',')}}`,
          ],
          {
            limit: LIMIT_DATA,
            filtering: [
              {
                field: 'created_time',
                operator: 'GREATER_THAN',
                value: Math.floor(
                  (Date.now() - 60 * 24 * 60 * 60 * 1000) / 1000,
                ),
              },
            ],
          },
          true,
        );

        const campaigns = await fetchAll(campaignCursor, {
          context: { accountId: acc.id, step: 'FETCH_CAMPAIGNS', sleep: 60000 },
        });

        console.log('campaign length', campaigns?.length);

        // GET ALL ADSET
        const adsetCursor = await adAccount.getAdSets(
          [
            ...ADSET_FIELDS,
            `insights.date_preset(maximum).limit(1).level(adset){${AD_INSIGHT_FIELDS.join(',')}}`,
          ],
          {
            limit: LIMIT_DATA,
            filtering: [
              {
                field: 'created_time',
                operator: 'GREATER_THAN',
                value: Math.floor(
                  (Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000,
                ),
              },
            ],
          },
          true,
        );
        const adSets = await fetchAll(adsetCursor, {
          context: { accountId: acc.id, step: 'FETCH ADSETS', sleep: 60000 },
        });

        console.log('adSets length', adSets?.length);

        // GET ALL AD
        const adCursor = await adAccount.getAds(
          [
            Ad.Fields.id,
            Ad.Fields.account_id,
            Ad.Fields.campaign_id,
            Ad.Fields.adset_id,
            Ad.Fields.name,
            Ad.Fields.status,
            Ad.Fields.effective_status,
            Ad.Fields.creative_asset_groups_spec,
            Ad.Fields.bid_amount,
            Ad.Fields.priority,
            Ad.Fields.created_time,
            Ad.Fields.updated_time,
            `creative{${CREATIVE_FIELDS.join(',')}}`,
            `insights.date_preset(maximum).limit(1).level(ad){${AD_INSIGHT_FIELDS.join(',')}}`,
          ],
          {
            limit: LIMIT_DATA,
            filtering: [
              {
                field: 'created_time',
                operator: 'GREATER_THAN',
                value: Math.floor(
                  (Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000,
                ),
              },
            ],
          },
          true,
        );
        const ads = await fetchAll(adCursor, {
          context: { accountId: acc.id, step: 'FETCH Ads', sleep: 60000 },
        });

        console.log('ads length', ads?.length);

        const adSetsByCampaign = groupBy(adSets, (as) => as.campaign_id);
        const adsByAdSet = groupBy(ads, (ad) => ad.adset_id);

        for (const ad of ads) {
          await this.prisma.$transaction(async (tx) => {
            await this.upsertDataService.syncAdAssetsLegacy(
              tx,
              adAccount,
              acc.id,
              ad,
            );
          });

          await this.prisma.$transaction(async (tx) => {
            await this.upsertDataService.upsertCreativeLegacy(tx, acc.id, ad);
          });
        }

        for (const campaign of campaigns) {
          console.log('campaign id', campaign?.id);

          await this.prisma.$transaction(async (tx) => {
            const accountId = acc.id;
            await this.upsertDataService.upsertCampaign(
              tx,
              accountId,
              campaign,
            );

            const campaignAdSets = adSetsByCampaign[campaign.id] ?? [];

            console.log('campaignAdSets', campaignAdSets?.length);
            for (const adset of campaignAdSets) {
              await this.upsertDataService.upsertAdSet(
                tx,
                accountId,
                campaign.id,
                adset,
              );

              const adsetAds = adsByAdSet[adset.id] ?? [];
              console.log('adsetAds', adsetAds?.length);

              for (const ad of adsetAds) {
                await this.upsertDataService.syncCidLegacy(tx, accountId, ad);

                await this.upsertDataService.upsertAdLegacy(
                  tx,
                  accountId,
                  campaign.id,
                  adset.id,
                  ad,
                );
              }
            }
          });
        }
      }
      this.logger.log('--- END Campaign Data ---');
      return { status: 'DONE' };
    } catch (err) {
      throw new InternalServerErrorException(parseMetaError(err));
    }
  }

  @Cron('0 6,12,18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyCampaignInsights() {
    this.logger.log('⏰ Sync campaign daily insight');
    this.init();

    const today = dayjs().startOf('day');

    /**
     * 1️⃣ Lấy MAX insight (kèm accountId)
     */
    const maxInsights = await this.prisma.campaignInsight.findMany({
      where: {
        range: InsightRange.MAX,
        level: LevelInsight.CAMPAIGN,
      },
      select: {
        campaignId: true,
        dateStart: true,
        dateStop: true,
        campaign: {
          select: { accountId: true },
        },
      },
    });

    if (!maxInsights.length) {
      this.logger.log('No MAX insight found');
      return;
    }

    /**
     * 2️⃣ Lấy DAILY mới nhất của từng campaign
     */
    const dailyMax = await this.prisma.campaignInsight.groupBy({
      by: ['campaignId'],
      where: {
        range: InsightRange.DAILY,
        level: LevelInsight.CAMPAIGN,
      },
      _max: {
        dateStop: true,
      },
    });

    const dailyMap = new Map<string, string>();
    for (const d of dailyMax) {
      if (d._max.dateStop) {
        dailyMap.set(d.campaignId, d._max.dateStop);
      }
    }

    /**
     * 3️⃣ Build fetch plan
     */
    const fetchPlans: {
      campaignId: string;
      accountId: string;
      since: dayjs.Dayjs;
      until: dayjs.Dayjs;
    }[] = [];

    for (const max of maxInsights) {
      if (!max.campaign?.accountId) continue;

      const maxStart = dayjs(max.dateStart);
      const maxStop = dayjs(max.dateStop);

      // MAX đã quá cũ → skip
      if (maxStop.isBefore(today.subtract(1, 'day'))) continue;

      let since: dayjs.Dayjs;
      const dailyStop = dailyMap.get(max.campaignId);

      if (dailyStop) {
        since = dayjs(dailyStop).subtract(5, 'day');
        if (since.isBefore(maxStart)) since = maxStart.clone();
      } else {
        since = maxStart.clone();
      }

      if (since.isAfter(maxStop, 'day')) continue;

      fetchPlans.push({
        campaignId: max.campaignId,
        accountId: max.campaign.accountId,
        since,
        until: maxStop,
      });
    }

    if (!fetchPlans.length) {
      this.logger.log('No campaign need daily sync');
      return;
    }

    /**
     * 4️⃣ Group theo accountId
     */
    const byAccount = fetchPlans.reduce<
      Record<
        string,
        {
          campaignId: string;
          since: dayjs.Dayjs;
          until: dayjs.Dayjs;
        }[]
      >
    >((acc, p) => {
      (acc[p.accountId] ||= []).push({
        campaignId: p.campaignId,
        since: p.since,
        until: p.until,
      });
      return acc;
    }, {});

    /**
     * 5️⃣ Loop từng account → chunk campaign (≤50)
     */
    for (const [accountId, plans] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      for (const chunkPlans of chunk(plans, 50)) {
        const campaignIds = chunkPlans.map((p) => p.campaignId);

        const since = dayjs(
          Math.min(...chunkPlans.map((p) => p.since.valueOf())),
        );
        const until = dayjs(
          Math.max(...chunkPlans.map((p) => p.until.valueOf())),
        );

        /**
         * 6️⃣ Fetch DAILY insight
         */
        const cursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'campaign',
            time_increment: 1,
            limit: LIMIT_DATA,
            filtering: [
              {
                field: 'campaign.id',
                operator: 'IN',
                value: campaignIds,
              },
            ],
            time_range: {
              since: since.format('YYYY-MM-DD'),
              until: until.format('YYYY-MM-DD'),
            },
          },
          true,
        );

        const insights = await fetchAll(cursor);

        if (!insights.length) continue;

        /**
         * 7️⃣ Upsert CHỈ những ngày Meta trả về
         */
        for (const i of insights) {
          const date = i.date_start;

          if (!i.campaign_id) continue;
          if (dayjs(date).isAfter(today, 'day')) continue;

          await this.prisma.campaignInsight.upsert({
            where: {
              campaignId_dateStart_dateStop_range: {
                campaignId: i.campaign_id,
                dateStart: date,
                dateStop: date,
                range: InsightRange.DAILY,
              },
            },
            update: {
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

        await sleep(800);
      }
    }

    this.logger.log('✅ Campaign Daily Insight DONE');
  }

  @Cron('30 6,12,18 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyAdsetInsights() {
    this.logger.log('⏰ Sync adset daily insight');
    this.init();

    const today = dayjs().startOf('day');

    /**
     * 1️⃣ Lấy MAX adset insight
     */
    const maxInsights = await this.prisma.adSetInsight.findMany({
      where: {
        range: InsightRange.MAX,
        level: LevelInsight.ADSET,
      },
      select: {
        adSetId: true,
        dateStart: true,
        dateStop: true,
        adSet: {
          select: { accountId: true },
        },
      },
    });
    console.log(maxInsights.length);
    if (!maxInsights.length) return;

    /**
     * 2️⃣ DAILY mới nhất
     */
    const dailyMax = await this.prisma.adSetInsight.groupBy({
      by: ['adSetId'],
      where: {
        range: InsightRange.DAILY,
        level: LevelInsight.ADSET,
      },
      _max: {
        dateStop: true,
      },
    });

    const dailyMap = new Map<string, string>();
    for (const d of dailyMax) {
      if (d._max.dateStop) {
        dailyMap.set(d.adSetId, d._max.dateStop);
      }
    }

    /**
     * 3️⃣ Build fetch plan
     */
    const fetchPlans: {
      adsetId: string;
      accountId: string;
      since: dayjs.Dayjs;
      until: dayjs.Dayjs;
    }[] = [];

    for (const max of maxInsights) {
      if (!max.adSet?.accountId) continue;

      const maxStart = dayjs(max.dateStart);
      const maxStop = dayjs(max.dateStop);

      if (maxStop.isBefore(today.subtract(1, 'day'))) continue;

      let since: dayjs.Dayjs;
      const dailyStop = dailyMap.get(max.adSetId);

      if (dailyStop) {
        since = dayjs(dailyStop).subtract(5, 'day');
        if (since.isBefore(maxStart)) since = maxStart.clone();
      } else {
        since = maxStart.clone();
      }

      if (since.isAfter(maxStop, 'day')) continue;

      fetchPlans.push({
        adsetId: max.adSetId,
        accountId: max.adSet.accountId,
        since,
        until: maxStop,
      });
    }

    if (!fetchPlans.length) return;

    /**
     * 4️⃣ Group theo account
     */
    const byAccount = fetchPlans.reduce<Record<string, any[]>>((acc, p) => {
      (acc[p.accountId] ||= []).push(p);
      return acc;
    }, {});

    /**
     * 5️⃣ Fetch theo chunk
     */
    for (const [accountId, plans] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);
      for (const chunkPlans of chunk(plans, 50)) {
        const adsetIds = chunkPlans.map((p) => p.adsetId);

        const since = dayjs(
          Math.min(...chunkPlans.map((p) => p.since.valueOf())),
        );
        const until = dayjs(
          Math.max(...chunkPlans.map((p) => p.until.valueOf())),
        );

        const cursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'adset',
            time_increment: 1,
            limit: LIMIT_DATA,
            filtering: [{ field: 'adset.id', operator: 'IN', value: adsetIds }],
            time_range: {
              since: since.format('YYYY-MM-DD'),
              until: until.format('YYYY-MM-DD'),
            },
          },
          true,
        );

        const insights = await fetchAll(cursor);
        if (!insights.length) continue;

        /**
         * 6️⃣ Upsert DAILY
         */
        for (const i of insights) {
          const date = i.date_start;

          if (!i.adset_id) continue;
          if (dayjs(date).isAfter(today, 'day')) continue;

          await this.prisma.adSetInsight.upsert({
            where: {
              adSetId_dateStart_dateStop_range: {
                adSetId: i.adset_id,
                dateStart: date,
                dateStop: date,
                range: InsightRange.DAILY,
              },
            },
            update: {
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

        await sleep(800);
      }
    }

    this.logger.log('✅ Adset Daily Insight DONE');
  }

  @Cron('0 7,13,19 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncDailyAdInsights() {
    this.logger.log('⏰ Sync ad daily insight');
    this.init();

    const today = dayjs().startOf('day');

    /**
     * 1️⃣ Lấy MAX ad insight
     */
    const maxInsights = await this.prisma.adInsight.findMany({
      where: {
        range: InsightRange.MAX,
        level: LevelInsight.AD,
      },
      select: {
        adId: true,
        dateStart: true,
        dateStop: true,
        ad: {
          select: { accountId: true },
        },
      },
    });

    if (!maxInsights.length) return;

    /**
     * 2️⃣ DAILY mới nhất của từng ad
     */
    const dailyMax = await this.prisma.adInsight.groupBy({
      by: ['adId'],
      where: {
        range: InsightRange.DAILY,
        level: LevelInsight.AD,
      },
      _max: {
        dateStop: true,
      },
    });

    const dailyMap = new Map<string, string>();
    for (const d of dailyMax) {
      if (d._max.dateStop) {
        dailyMap.set(d.adId, d._max.dateStop);
      }
    }

    /**
     * 3️⃣ Build fetch plan
     */
    const fetchPlans: {
      adId: string;
      accountId: string;
      since: dayjs.Dayjs;
      until: dayjs.Dayjs;
    }[] = [];

    for (const max of maxInsights) {
      if (!max.ad?.accountId) continue;

      const maxStart = dayjs(max.dateStart);
      const maxStop = dayjs(max.dateStop);

      // MAX đã quá cũ
      if (maxStop.isBefore(today.subtract(1, 'day'))) continue;

      let since: dayjs.Dayjs;
      const dailyStop = dailyMap.get(max.adId);

      if (dailyStop) {
        since = dayjs(dailyStop).subtract(5, 'day');
        if (since.isBefore(maxStart)) since = maxStart.clone();
      } else {
        since = maxStart.clone();
      }

      if (since.isAfter(maxStop, 'day')) continue;

      fetchPlans.push({
        adId: max.adId,
        accountId: max.ad.accountId,
        since,
        until: maxStop,
      });
    }

    if (!fetchPlans.length) return;

    /**
     * 4️⃣ Group theo accountId
     */
    const byAccount = fetchPlans.reduce<
      Record<
        string,
        {
          adId: string;
          since: dayjs.Dayjs;
          until: dayjs.Dayjs;
        }[]
      >
    >((acc, p) => {
      (acc[p.accountId] ||= []).push({
        adId: p.adId,
        since: p.since,
        until: p.until,
      });
      return acc;
    }, {});

    /**
     * 5️⃣ Loop từng account → chunk ads (≤50)
     */
    for (const [accountId, plans] of Object.entries(byAccount)) {
      const adAccount = new AdAccount(accountId);

      for (const chunkPlans of chunk(plans, 50)) {
        const adIds = chunkPlans.map((p) => p.adId);

        const since = dayjs(
          Math.min(...chunkPlans.map((p) => p.since.valueOf())),
        );
        const until = dayjs(
          Math.max(...chunkPlans.map((p) => p.until.valueOf())),
        );

        /**
         * 6️⃣ Fetch DAILY insight
         */
        const cursor = await adAccount.getInsights(
          AD_INSIGHT_FIELDS,
          {
            level: 'ad',
            time_increment: 1,
            limit: LIMIT_DATA,
            filtering: [
              {
                field: 'ad.id',
                operator: 'IN',
                value: adIds,
              },
            ],
            time_range: {
              since: since.format('YYYY-MM-DD'),
              until: until.format('YYYY-MM-DD'),
            },
          },
          true,
        );

        const insights = await fetchAll(cursor);
        if (!insights.length) continue;

        /**
         * 7️⃣ Upsert CHỈ những ngày Meta trả về
         */
        for (const i of insights) {
          const date = i.date_start;

          if (!i.ad_id) continue;
          if (dayjs(date).isAfter(today, 'day')) continue;

          await this.prisma.adInsight.upsert({
            where: {
              adId_dateStart_dateStop_range: {
                adId: i.ad_id,
                dateStart: date,
                dateStop: date,
                range: InsightRange.DAILY,
              },
            },
            update: {
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

        await sleep(800);
      }
    }

    this.logger.log('✅ Ad Daily Insight DONE');
  }
}
