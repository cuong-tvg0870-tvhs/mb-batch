import { BadRequestException, Injectable } from '@nestjs/common';
import axios from 'axios';
import 'dotenv/config';
import {
  Ad,
  AdAccount,
  AdCreative,
  AdSet,
  Campaign,
  FacebookAdsApi,
  User,
} from 'facebook-nodejs-business-sdk';
import {
  beautyFashionKeywords,
  chunkArray,
  CleanObjectOrArray,
  commonKeywords,
  daysAgo,
  fetchAll,
  parseMetaError,
} from '../../common/utils';

import { InsightRange, LevelInsight, SystemCampaign } from '@prisma/client';
import { MetaCampaignTree } from '../../common/dtos/types.dto';
import { sleep } from '../../common/utils';
import {
  AD_ACCOUNT_FIELDS,
  AD_FIELDS,
  AD_INSIGHT_FIELDS,
  AD_PIXEL_FIELDS,
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
  CREATIVE_FIELDS,
} from '../../common/utils/meta-field';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class MetaService {
  constructor(private prisma: PrismaService) {}

  private initialized = false;

  private init() {
    if (!this.initialized) {
      FacebookAdsApi.init(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
      this.initialized = true;
    }
  }

  async fetchInterestTargeting() {
    // FETCH INTEREST LIST:
    const queries = [
      ...'abcdefghijklmnopqrstuvwxyz0123456789'.split(''),
      ...commonKeywords,
      ...beautyFashionKeywords,
    ];
    for (const q of queries) {
      try {
        const res = await axios.get('https://graph.facebook.com/v24.0/search', {
          params: {
            type: 'adinterest',
            q: q,
            limit: 1000,
            access_token: process.env.SDK_FACEBOOK_ACCESS_TOKEN,
          },
        });

        const data = res.data.data || [];
        for (const interest of data) {
          await this.prisma.targetingInterest.upsert({
            where: { id: interest.id },
            create: interest,
            update: interest,
          });
        }

        console.log(`Fetched ${data.length} interests for query: ${q}`);
      } catch {
        // nếu có lỗi thì bỏ qua và tiếp tục query tiếp theo
      }

      await new Promise((r) => setTimeout(r, 300)); // delay tránh rate-limit
    }
    return true;
  }

  async fetchAdPreview(body: any) {
    this.init();
    const { adAccountId, format, creative } = body;
    const me = new AdAccount(adAccountId);

    const preview = await me.getGeneratePreviews([], {
      creative: creative,
      ad_format: format || 'DESKTOP_FEED_STANDARD',
    });
    return preview[0]._data.body;
  }
  // =====================================================
  // ENTRY POINT
  // =====================================================
  async syncAllAccount() {
    this.init();

    const me = new User('me');

    const [accounts, pages] = await Promise.all([
      me.getAdAccounts([...AD_ACCOUNT_FIELDS], { limit: 100 }),
      me.getAccounts(['id', 'name'], { limit: 100 }),
    ]);
    for (let index = 0; index < pages.length; index++) {
      const page = pages[index]?._data;
      await this.prisma.fanpage.upsert({
        where: { id: page?.id },
        create: { id: page?.id, name: page?.name },
        update: { name: page?.name },
      });
    }

    for await (const account of accounts) {
      const acc = account._data;
      const adAccount = new AdAccount(acc.id);

      const pixelCursor = await adAccount.getAdsPixels(AD_PIXEL_FIELDS);
      const pixels = await fetchAll(pixelCursor);

      await this.prisma.account.upsert({
        where: { id: acc.id },
        update: {
          name: acc.name,
          currency: acc.currency,
          timezone: acc.timezone_name,
          pages,
          pixels,
          rawPayload: acc,
          lastFetchedAt: new Date(),
        },
        create: {
          id: acc.id,
          name: acc.name,
          accountType: 'AD_ACCOUNT',
          currency: acc.currency,
          timezone: acc.timezone_name,
          pixels,
          pages,

          rawPayload: acc,
          lastFetchedAt: new Date(),
        },
      });

      await sleep(5000);
    }
    return { success: true, accounts };
  }

  async fetchCampaignData(campaignId: string): Promise<MetaCampaignTree> {
    this.init();
    try {
      const campaignService = new Campaign(campaignId);

      // 1. Lấy dữ liệu Campaign với các trường bắt buộc (như special_ad_categories)
      const campaign = await campaignService.get(CAMPAIGN_FIELDS);

      // 2. Lấy danh sách Ad Sets (phải bao gồm optimization_goal và promoted_object)
      const adsets = await campaignService.getAdSets(ADSET_FIELDS, {
        limit: 200,
      });

      // 3. Lấy danh sách Ads
      const ads = await campaignService.getAds(AD_FIELDS, { limit: 200 });

      // 4. XỬ LÝ QUAN TRỌNG: Await tất cả Ad Creative bằng Promise.all
      // Bạn cần fetch chi tiết Creative để biết bài viết đó dùng object_story_id hay object_story_spec
      const adsWithCreative = await Promise.all(
        ads?.map(async (ad) => {
          const creativeSv = new AdCreative(ad?.creative?.id);
          const creative = await creativeSv.get(CREATIVE_FIELDS);
          return { ...ad._data, creative: creative._data };
        }),
      );

      const tree: MetaCampaignTree = {
        ...campaign._data,
        adsets: adsets.map((as) => ({
          ...as._data,
          ads: adsWithCreative.filter((ad) => ad.adset_id === as.id),
        })),
      };

      // 5. Tổ chức lại cấu trúc dữ liệu theo phân cấp: Campaign -> AdSet -> Ads
      return tree;
    } catch (err) {
      const metaError = parseMetaError(err);
      throw new BadRequestException(metaError);
    }
  }

  // SYNC INSIGHT

  async getAdInsightRange(adIds: string[]) {
    const last = await this.prisma.adInsight.findFirst({
      where: { adId: { in: adIds } },
      orderBy: { dateStop: 'desc' },
      select: { dateStop: true, updatedAt: true, createdAt: true },
    });

    const until = new Date();

    // ❌ chưa có data → 90 ngày
    if (!last) {
      return {
        since: daysAgo(90, until),
        until,
      };
    }

    const THREE_HOURS = 3 * 60 * 60 * 1000;

    if (
      last.updatedAt &&
      until.getTime() - last.updatedAt.getTime() < THREE_HOURS
    ) {
      return null; // 👈 caller hiểu là KHÔNG fetch
    }
    // ✅ đã có → back 7 ngày để overlap
    return {
      since: daysAgo(7, new Date(last.dateStop)),
      until,
    };
  }

  async syncAdInsights({ accountId, adIds, since, until }) {
    this.init();
    const adAccount = new AdAccount(accountId);

    const adIdChunks = chunkArray(adIds, 50);

    for (const ids of adIdChunks) {
      const cursor = await adAccount.getInsights(
        AD_INSIGHT_FIELDS,
        {
          level: 'ad',
          time_increment: 1,
          since: since.toISOString().split('T')[0],
          until: until.toISOString().split('T')[0],
          filtering: [{ field: 'ad.id', operator: 'IN', value: ids }],
        },
        true,
      );

      const insights = await fetchAll(cursor);

      await this.prisma.$transaction(
        insights.map((i) =>
          this.prisma.adInsight.upsert({
            where: {
              adId_dateStart_dateStop_range: {
                range: InsightRange.DAILY,
                adId: i.ad_id,
                dateStart: i.date_start,
                dateStop: i.date_stop,
              },
            },
            update: {
              impressions: +i.impressions,
              clicks: +i.clicks,
              spend: +i.spend,
              ctr: +i.ctr,
              cpc: +i.cpc,
              actions: i.actions,
              uniqueClicks: +i.unique_clicks,
              uniqueCtr: +i.unique_ctr,
              cpm: +i.cpm,
              reach: +i.reach,
              results: +i.results,
              frequency: +i.frequency,
              costPerResult: +i.cost_per_result,

              purchaseRoas: i.purchase_roas,
              actionValues: i.action_values,

              qualityRanking: i.quality_ranking,
              engagementRateRanking: i.engagement_rate_ranking,
              conversionRateRanking: i.conversion_rate_ranking,

              rawPayload: i,

              updatedAt: new Date(),
            },
            create: {
              impressions: +i.impressions,
              clicks: +i.clicks,
              spend: +i.spend,
              ctr: +i.ctr,
              cpc: +i.cpc,
              actions: i.actions,
              uniqueClicks: +i.unique_clicks,
              uniqueCtr: +i.unique_ctr,
              cpm: +i.cpm,
              reach: +i.reach,
              results: +i.results,
              frequency: +i.frequency,
              costPerResult: +i.cost_per_result,

              purchaseRoas: i.purchase_roas,
              actionValues: i.action_values,

              qualityRanking: i.quality_ranking,
              engagementRateRanking: i.engagement_rate_ranking,
              conversionRateRanking: i.conversion_rate_ranking,

              level: LevelInsight.AD,
              adId: i.ad_id,
              dateStart: i.date_start,
              dateStop: i.date_stop,
              range: InsightRange.DAILY,

              campaignId: i?.campaign_id,
              adSetId: i?.adset_id,
              purchases: i.purchases,
              avgWatchTime: i?.video_avg_time_watched_actions,

              rawPayload: i,
              updatedAt: new Date(),
            },
          }),
        ),
      );
      if (adIdChunks.length > 10) await sleep(800);
      else await sleep(300);
    }
  }

  async upsertCampaignToMeta(
    adAccount: AdAccount,
    campaignSystem: SystemCampaign,
    payload: any,
  ): Promise<string> {
    let campaignMetaId = campaignSystem.meta_id;

    if (campaignMetaId) {
      const campaign = new Campaign(campaignMetaId);

      await campaign.update(CAMPAIGN_FIELDS, {
        [Campaign.Fields.objective]: payload.objective,
        [Campaign.Fields.daily_budget]: payload.daily_budget,
        [Campaign.Fields.lifetime_budget]: payload.lifetime_budget,
        [Campaign.Fields.status]: payload.status,
        [Campaign.Fields.name]: payload.name,
        [Campaign.Fields.bid_strategy]: payload.bid_strategy,
      });

      return campaignMetaId;
    }

    const campaign = await adAccount.createCampaign(CAMPAIGN_FIELDS, {
      ...payload,
      special_ad_categories: payload?.special_ad_categories ?? ['NONE'],
    });

    await this.prisma.systemCampaign.update({
      where: { id: campaignSystem.id },
      data: { meta_id: campaign.id, status: campaign._data?.status },
    });

    return campaign.id;
  }

  async upsertAdSetToMeta(
    adAccount: AdAccount,
    campaignMetaId: string,
    adSetSystem: any,
  ): Promise<string> {
    const payload = CleanObjectOrArray(adSetSystem.data || {});

    if (adSetSystem.meta_id) {
      const adSet = new AdSet(adSetSystem.meta_id);
      await adSet.update(ADSET_FIELDS, payload);
      return adSetSystem.meta_id;
    }

    const createdAdSet = await adAccount.createAdSet([], {
      ...payload,
      campaign_id: campaignMetaId,
    });

    await this.prisma.systemAdSet.update({
      where: { id: adSetSystem.id },
      data: {
        meta_id: createdAdSet._data?.id,
        status: createdAdSet._data?.status,
      },
    });

    return createdAdSet._data.id;
  }

  async syncAdsOfAdSet(
    adAccount: AdAccount,
    adSetSystem: any,
    adSetMetaId: string,
  ) {
    const ads = await this.prisma.systemAd.findMany({
      where: { adSetId: adSetSystem.id },
    });

    for (const adSystem of ads) {
      const adPayload = adSystem.data as any;

      if (adSystem.meta_id) {
        const ad = new Ad(adSystem.meta_id);
        await ad.update(AD_FIELDS, adPayload);
        continue;
      }

      const creative = await adAccount.createAdCreative(CREATIVE_FIELDS, {
        name: adPayload.name || 'Creative',
        ...adPayload?.creative,
      });

      const createdAd = await adAccount.createAd(AD_FIELDS, {
        name: adPayload.name,
        status: adPayload.status ?? 'PAUSED',
        adset_id: adSetMetaId,
        creative: { creative_id: creative.id },
      });

      await this.prisma.systemAd.update({
        where: { id: adSystem.id },
        data: {
          meta_id: createdAd.id,
          status: createdAd._data.status,
        },
      });
    }
  }
}
