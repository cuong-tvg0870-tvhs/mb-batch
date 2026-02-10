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
  extractCampaignMetrics,
  fetchAll,
  getDateRange,
  isFresh,
  LIMIT_DATA,
  parseMetaError,
} from '../../common/utils';

import { InsightRange, LevelInsight, SystemCampaign } from '@prisma/client';
import FormData from 'form-data';
import fs from 'fs';
import { MetaCampaignTree } from '../../common/dtos/types.dto';
import { sleep } from '../../common/utils';
import {
  AD_ACCOUNT_FIELDS,
  AD_FIELDS,
  AD_IMAGE_FIELDS,
  AD_INSIGHT_FIELDS,
  AD_PIXEL_FIELDS,
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
  CREATIVE_FIELDS,
  SUMMARY_AD_INSIGHT_FIELDS,
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
      me.getAdAccounts([...AD_ACCOUNT_FIELDS], { limit: LIMIT_DATA }),
      me.getAccounts(['id', 'name'], { limit: LIMIT_DATA }),
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

      const campaign = await campaignService.get(CAMPAIGN_FIELDS);

      const adsets = await campaignService.getAdSets(ADSET_FIELDS, {
        limit: LIMIT_DATA,
      });
      const ads = await campaignService.getAds(AD_FIELDS, {
        limit: LIMIT_DATA,
      });

      const adsWithCreative = await Promise.all(
        ads?.map(async (ad) => {
          const creativeSv = new AdCreative(ad?.creative?.id);
          const creative = await creativeSv.get(CREATIVE_FIELDS);
          return { ...ad._data, creative: creative._data };
        }),
      );

      const tree: MetaCampaignTree = {
        ...campaign,
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
  async getAdInsightRange(adIds: string[], startDate?: Date) {
    const last = await this.prisma.adInsight.findFirst({
      where: { adId: { in: adIds } },
      orderBy: { dateStop: 'desc' },
      select: {
        ad: { select: { campaign: { select: { createdAt: true } } } },
        dateStop: true,
        updatedAt: true,
        createdAt: true,
      },
    });

    const until = new Date();

    // ❌ chưa có data → 90 ngày
    if (!last) {
      return {
        since: startDate ? new Date(startDate) : daysAgo(90, until),
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

  //FETCH MANY ADS
  async syncAdInsights({
    accountId,
    adIds,
    since,
    until,
  }: {
    accountId: string;
    adIds: string[];
    since: Date;
    until: Date;
  }) {
    this.init();
    const adAccount = new AdAccount(accountId);

    const adIdChunks = chunkArray(adIds, 50);
    const dateRanges = getDateRange(since, until);
    for (const ids of adIdChunks) {
      const cursor = await adAccount.getInsights(
        AD_INSIGHT_FIELDS,
        {
          level: 'ad',
          time_increment: 1,
          since: since.toISOString().slice(0, 10),
          until: until.toISOString().slice(0, 10),
          filtering: [{ field: 'ad.id', operator: 'IN', value: ids }],
        },
        true,
      );

      const insights = await fetchAll(cursor);
      /**
       * Map để check row nào đã có data
       * key = adId|dateStart
       */
      const insightMap = new Map<string, any>();
      for (const i of insights) {
        insightMap.set(`${i.ad_id}|${i.date_start}`, i);
      }
      const operations: any[] = [];
      for (const adId of ids) {
        for (const date of dateRanges) {
          const key = `${adId}|${date}`;
          const i = insightMap.get(key);

          operations.push(
            this.prisma.adInsight.upsert({
              where: {
                adId_dateStart_dateStop_range: {
                  adId,
                  dateStart: date,
                  dateStop: date,
                  range: InsightRange.DAILY,
                },
              },
              update: {
                ...extractCampaignMetrics(i),
                rawPayload: i ?? null,
              },
              create: {
                level: LevelInsight.AD,
                range: InsightRange.DAILY,
                adId,
                dateStart: date,
                dateStop: date,
                ...extractCampaignMetrics(i),
                rawPayload: i ?? null,
              },
            }),
          );
        }
      }
      // ⚠️ Prisma transaction limit ~ 10k ops → chunk nếu lớn
      for (const chunk of chunkArray(operations, 500)) {
        await this.prisma.$transaction(chunk);
      }

      await sleep(adIdChunks.length > 10 ? 800 : 300);
    }
  }

  //FETCH ONE ADSET
  async syncAdSetInsight({
    accountId,
    adSetId,
    since,
    until,
  }: {
    accountId: string;
    adSetId: string;
    since: Date;
    until: Date;
  }) {
    /**
     * 🔥 TTL CHECK
     */
    const latest = await this.prisma.adSetInsight.findFirst({
      where: {
        adSetId,
        range: InsightRange.DAILY,
      },
      orderBy: { updatedAt: 'desc' },
      select: { updatedAt: true },
    });

    if (isFresh(latest?.updatedAt)) {
      console.log(`AdSet ${adSetId} insight still fresh`);
      return;
    }

    this.init();
    const adAccount = new AdAccount(accountId);

    const cursor = await adAccount.getInsights(
      AD_INSIGHT_FIELDS,
      {
        level: 'adset',
        time_increment: 1,
        summary: SUMMARY_AD_INSIGHT_FIELDS,
        filtering: [
          {
            field: 'adset.id',
            operator: 'EQUAL',
            value: adSetId,
          },
        ],
        time_range: {
          since: since.toISOString().slice(0, 10),
          until: until.toISOString().slice(0, 10),
        },
      },
      true,
    );

    const insights = await fetchAll(cursor);
    const summary = cursor.summary;

    if (!insights.length) return;

    for (const insight of insights) {
      await this.prisma.adSetInsight.upsert({
        where: {
          adSetId_dateStart_dateStop_range: {
            adSetId: adSetId,
            dateStart: insight.date_start,
            dateStop: insight.date_stop,
            range: InsightRange.DAILY,
          },
        },
        update: {
          ...extractCampaignMetrics(insight),
          rawPayload: insight,
        },
        create: {
          adSetId: adSetId,
          level: LevelInsight.ADSET,
          range: InsightRange.DAILY,
          dateStart: insight.date_start,
          dateStop: insight.date_stop,
          ...extractCampaignMetrics(insight),
          rawPayload: insight,
        },
      });
    }

    /**
     * 🔥 Update aggregated metrics vào adset
     */
    if (summary) {
      await this.prisma.adSet.update({
        where: { id: adSetId },
        data: {
          ...extractCampaignMetrics(summary),
        },
      });
    }
  }
  //FETCH ONE CAMPAIGN
  async syncCampaignInsight({
    accountId,
    campaignId,
    since,
    until,
  }: {
    accountId: string;
    campaignId: string;
    since: Date;
    until: Date;
  }) {
    /**
     * 🔥 TTL CHECK
     */
    const latest = await this.prisma.campaignInsight.findFirst({
      where: { campaignId, range: 'DAILY' },
      orderBy: { updatedAt: 'desc' },
      select: { updatedAt: true },
    });

    if (isFresh(latest?.updatedAt)) {
      console.log('campaign not change data');
      return;
    }

    this.init();

    const adAccount = new AdAccount(accountId);

    const cursorMax = await adAccount.getInsights(
      AD_INSIGHT_FIELDS,
      {
        level: 'campaign',
        time_increment: 1,
        summary: SUMMARY_AD_INSIGHT_FIELDS,
        filtering: [
          { field: 'campaign.id', operator: 'EQUAL', value: campaignId },
        ],
        time_range: {
          since: since.toISOString().slice(0, 10),
          until: until.toISOString().slice(0, 10),
        },
      },
      true,
    );

    const insights = await fetchAll(cursorMax);
    const insightSummary = cursorMax.summary;

    if (!insights.length) return;

    for (const insight of insights) {
      await this.prisma.campaignInsight.upsert({
        where: {
          campaignId_dateStart_dateStop_range: {
            campaignId,
            dateStart: insight?.date_start,
            dateStop: insight?.date_stop,
            range: 'DAILY',
          },
        },
        update: {
          ...extractCampaignMetrics(insight),
          rawPayload: insight,
        },
        create: {
          campaignId,
          level: 'CAMPAIGN',
          dateStart: insight?.date_start,
          dateStop: insight?.date_stop,
          range: 'DAILY',
          ...extractCampaignMetrics(insight[0]),
          rawPayload: insight,
        },
      });
    }

    await this.prisma.campaignInsight.upsert({
      where: {
        campaignId_dateStart_dateStop_range: {
          campaignId,
          dateStart: insightSummary?.date_start,
          dateStop: insightSummary?.date_stop,
          range: 'MAX',
        },
      },
      update: {
        ...extractCampaignMetrics(insightSummary),
        rawPayload: insightSummary,
      },
      create: {
        campaignId,
        level: 'CAMPAIGN',
        dateStart: insightSummary?.date_start,
        dateStop: insightSummary?.date_stop,
        range: 'MAX',
        ...extractCampaignMetrics(insightSummary),
        rawPayload: insightSummary,
      },
    });

    await this.prisma.campaign.update({
      where: { id: campaignId },
      data: { ...extractCampaignMetrics(insightSummary) },
    });
  }

  async syncCampaignAudienceInsight({
    accountId,
    campaignId,
    since,
    until,
  }: {
    accountId: string;
    campaignId: string;
    since: Date;
    until: Date;
  }) {
    /**
     * 🔥 TTL CHECK
     */
    const latest = await this.prisma.campaignAudienceInsight.findFirst({
      where: { campaignId, range: 'MAX' },
      orderBy: { updatedAt: 'desc' },
      select: { updatedAt: true },
    });

    if (isFresh(latest?.updatedAt)) {
      console.log('audient not change data');
      return;
    }
    await this.init();

    const adAccount = new AdAccount(accountId);

    const cursor = await adAccount.getInsights(
      AD_INSIGHT_FIELDS,
      {
        level: 'campaign',
        breakdowns: ['age', 'gender'],
        filtering: [
          {
            field: 'campaign.id',
            operator: 'EQUAL',
            value: campaignId,
          },
        ],
        time_range: {
          since: since.toISOString().slice(0, 10),
          until: until.toISOString().slice(0, 10),
        },
      },
      true,
    );

    const insights = await fetchAll(cursor);

    for (const insight of insights) {
      await this.prisma.campaignAudienceInsight.upsert({
        where: {
          campaignId_age_gender_range: {
            campaignId,
            age: insight.age,
            gender: insight.gender,
            range: 'MAX',
          },
        },
        update: {
          ...extractCampaignMetrics(insight),
          rawPayload: insight,
        },
        create: {
          campaignId,
          level: 'CAMPAIGN',
          age: insight.age,
          gender: insight.gender,
          range: 'MAX',
          ...extractCampaignMetrics(insight),
          rawPayload: insight,
        },
      });
    }
  }

  // =========================
  // UPLOAD VIDEO (STREAM)
  // =========================
  async pollVideoUntilReady(videoId: string) {
    const maxRetry = 30; // ví dụ: tối đa 1 phút (30 * 2s)

    for (let i = 0; i < maxRetry; i++) {
      await sleep(4000);
      const video = await axios
        .get(
          `https://graph.facebook.com/v24.0/${videoId}?fields=id,title,description,length,permalink_url,source,picture&access_token=${process.env.SDK_FACEBOOK_ACCESS_TOKEN}`,
        )
        .then((videoRes) => {
          return videoRes.data;
        })
        .catch((error) => {
          return error;
        });

      // ✅ Điều kiện ready
      if (video?.source) {
        return video;
      }

      console.log(
        `[VIDEO PROCESSING] retry ${i + 1}/${maxRetry}`,
        video?.status?.video_status,
      );
    }

    throw new Error('Video processing timeout');
  }

  async uploadVideoViaCurlLike(filePath: string, adAccountId: string) {
    const form = new FormData();

    form.append('source', fs.createReadStream(filePath));
    form.append('access_token', process.env.SDK_FACEBOOK_ACCESS_TOKEN);

    try {
      // 1️⃣ Upload video
      const uploadRes = await axios.post(
        `https://graph.facebook.com/v24.0/${adAccountId}/advideos`,
        form,
        {
          headers: form.getHeaders(),
          maxBodyLength: Infinity,
          maxContentLength: Infinity,
        },
      );

      const videoId = uploadRes.data?.id;
      if (!videoId) throw new Error('Upload failed: no video id');

      // 2️⃣ Poll video info mỗi 2s
      const video = await this.pollVideoUntilReady(videoId);

      return video;
    } catch (error) {
      return parseMetaError(error);
    }
  }

  // =========================
  // UPLOAD MEDIA
  // =========================
  async uploadMedia(
    file: Express.Multer.File,
    adAccountId: string,
    type: 'image' | 'video',
  ) {
    if (!file) throw new BadRequestException('File is required');
    if (!adAccountId) throw new BadRequestException('adAccountId is required');

    this.init();
    const account = new AdAccount(adAccountId);

    // Validate mime
    if (
      type === 'image' &&
      !['image/jpeg', 'image/png'].includes(file.mimetype)
    ) {
      throw new BadRequestException('Only JPG/PNG allowed');
    }
    if (type === 'video' && !file.mimetype.startsWith('video/')) {
      throw new BadRequestException('Only video files allowed');
    }

    try {
      // =========================
      // IMAGE (nhỏ → memory OK)
      // =========================
      if (type === 'image') {
        if (!file.buffer)
          throw new BadRequestException('Image buffer not found');

        const result = await account.createAdImage(AD_IMAGE_FIELDS, {
          bytes: file.buffer.toString('base64'),
          name: file.originalname,
        });

        const imageKey = Object.keys(result._data.images ?? {})[0];
        const imageHash = result._data.images[imageKey].hash;

        const imageCursor = await account.getAdImages(
          AD_IMAGE_FIELDS,
          { hashes: [imageHash] },
          true,
        );

        const image = (await fetchAll(imageCursor))[0];

        if (!image) throw new BadRequestException('Upload image failed');

        await this.prisma.adImage.upsert({
          where: {
            accountId_hash_id: {
              id: image.id,
              accountId: adAccountId,
              hash: image.hash,
            },
          },
          update: {
            name: image.name,
            createdTime: image.created_time
              ? new Date(image.created_time)
              : null,

            url: image.url,
            rawPayload: image,
            status: image?.status,
            createdAt: new Date(image.created_time),
            updatedAt: new Date(image.updated_time),
          },
          create: {
            id: image.id,
            name: image.name,
            accountId: adAccountId,
            createdTime: image.created_time
              ? new Date(image.created_time)
              : null,

            hash: image.hash,
            url: image.url,
            rawPayload: image,

            status: image?.status,
            createdAt: new Date(image.created_time),
            updatedAt: new Date(image.updated_time),
          },
        });

        return { ...image, raw: result._data.images };
      }

      // =========================
      // VIDEO (disk → stream)
      // =========================
      if (!file.path) {
        throw new BadRequestException('Video file must be stored on disk');
      }

      const uploadResult = await this.uploadVideoViaCurlLike(
        file.path,
        adAccountId,
      );

      // cleanup file sau upload
      fs.unlink(file.path, () => null);

      // lưu DB trạng thái PROCESSING
      await this.prisma.adVideo.upsert({
        where: { id: uploadResult.id, accountId: adAccountId },
        create: {
          id: uploadResult.id,
          title: uploadResult?.name,
          status: uploadResult?.status?.video_status,
          accountId: adAccountId,
          thumbnailUrl: uploadResult?.source || uploadResult?.picture,
          rawPayload: uploadResult,
          createdAt: uploadResult?.created_time,
          createdTime: uploadResult?.created_time,
        },
        update: {
          title: uploadResult?.name,
          accountId: adAccountId,
          status: uploadResult?.status?.video_status,
          thumbnailUrl: uploadResult?.source || uploadResult?.picture,
          rawPayload: uploadResult,
          createdAt: uploadResult?.created_time,
          createdTime: uploadResult?.created_time,
        },
      });
      return { type: 'video', ...uploadResult };
    } catch (err) {
      const metaError = parseMetaError(err);
      throw new BadRequestException(metaError);
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
