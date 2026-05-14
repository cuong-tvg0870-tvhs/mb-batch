import {
  Injectable,
  InternalServerErrorException,
  Logger,
} from '@nestjs/common';
import dayjs from 'dayjs';
import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import pLimit from 'p-limit';
import { MetaTransformHelper } from '../../common/helpers/meta-transform.helper';
import { PrismaBatchHelper } from '../../common/helpers/prisma-batch.helper';
import {
  chunk,
  executeMetaApiWithRetry,
  fetchAll,
  parseMetaError,
  sleep,
  toPrismaJson,
} from '../../common/utils';
import {
  AD_FIELDS,
  AD_IMAGE_FIELDS,
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
  CREATIVE_FIELDS,
} from '../../common/utils/meta-field';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class MetaSyncService {
  private readonly logger = new Logger(MetaSyncService.name);
  private initialized = false;

  constructor(private readonly prisma: PrismaService) {}

  private init() {
    if (!this.initialized) {
      const token = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
      if (!token) {
        throw new Error(
          '❌ SDK_FACEBOOK_ACCESS_TOKEN is missing in environment variables!',
        );
      }
      FacebookAdsApi.init(token);
      this.initialized = true;
      this.logger.log('✅ Meta SDK Initialized successfully');
    }
  }

  async syncCampaignData() {
    this.logger.log('⏰ Starting Batch Sync Campaign Data...');
    this.init();

    try {
      const accounts = await this.prisma.account.findMany({
        where: { needsReauth: false },
      });

      const limit = pLimit(10); // Reduced parallel accounts for safety

      const syncTasks = accounts.map((account) => {
        return limit(async () => {
          try {
            const adAccount = new AdAccount(account.id);
            const lastSyncUnix = Math.floor(
              (Date.now() - 5 * 24 * 60 * 60 * 1000) / 1000,
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

            const campaignsCursor = await executeMetaApiWithRetry(
              () => adAccount.getCampaigns(CAMPAIGN_FIELDS, baseFilter, true),
              { logger: this.logger },
            );
            const allCampaigns = await fetchAll(campaignsCursor);
            const campaignIds = allCampaigns.map((c) => c.id);

            const allAdSets: any[] = [];
            const allAds: any[] = [];

            if (campaignIds.length > 0) {
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

            const adsetIds = new Set(allAdSets.map((as) => as.id));
            for (const as of extraAdsets) {
              if (!adsetIds.has(as.id)) allAdSets.push(as);
            }

            const adIds = new Set(allAds.map((ad) => ad.id));
            for (const ad of extraAds) {
              if (!adIds.has(ad.id)) allAds.push(ad);
            }

            // Missing parent fetching logic
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
                for (const chunkIds of chunk(missingCampaignIds, 50)) {
                  const cursor = await executeMetaApiWithRetry(() =>
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
                  );
                  allCampaigns.push(...(await fetchAll(cursor)));
                }
              }
            }

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

            await this.upsertFlatStructure(
              allCampaigns,
              allAdSets,
              allAds,
              account.id,
            );

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

  async upsertFlatStructure(
    campaigns: any[],
    adsets: any[],
    ads: any[],
    accountId: string,
  ) {
    const prismaHelper = new PrismaBatchHelper(this.prisma);

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
      if (item.pageId && fanpageMap.has(item.pageId)) {
        item.systemPageId = item.pageId;
      }

      if (item.videoId && !videoMap.has(item.videoId)) {
        videoMap.set(item.videoId, {
          id: item.videoId,
          accountId: item.accountId,
          thumbnailUrl: item?.thumbnailUrl,
        });
      }

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

  async syncVideo(limit: number = 50) {
    this.logger.log('🔄 Sync Ad Video (optimized)');
    this.init();
    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

    try {
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
      if (!existingVideos.length) return;

      const batchRequests = existingVideos.map((video) => ({
        method: 'GET',
        relative_url: `${video.id}?fields=source,thumbnails`,
      }));

      let batchResponses: any[];
      try {
        batchResponses = await api.call('POST', [''], {
          batch: JSON.stringify(batchRequests),
        });
      } catch (err: any) {
        this.logger.error('[FB BATCH CRITICAL ERROR]', err?.message);
        return false;
      }

      for (let i = 0; i < existingVideos.length; i++) {
        const asset = existingVideos[i];
        const res = batchResponses[i];
        const statusCode = res.code;
        const body = JSON.parse(res.body);

        try {
          if (statusCode === 200) {
            const thumbnail = body.thumbnails?.data?.find(
              (th: any) => !!th?.is_preferred,
            )?.uri;

            await this.prisma.adVideo.update({
              where: { id: asset.id },
              data: {
                thumbnailUrl: thumbnail || asset.thumbnailUrl,
                source: body.source ?? asset.source ?? null,
                status: 'READY',
                updatedAt: new Date(),
              },
            });
          } else {
            this.logger.warn(
              `[ID ERROR] ${asset.id} - Status: ${statusCode} - Msg: ${body?.error?.message}`,
            );
            await this.prisma.adVideo.update({
              where: { id: asset.id },
              data: {
                status: 'ERROR',
                updatedAt: new Date(),
              },
            });
          }
        } catch (dbErr: any) {
          this.logger.error(`[DB UPDATE ERROR] ID: ${asset.id}`, dbErr.message);
        }
      }
      return true;
    } catch (err: any) {
      this.logger.error('[CRON ERROR]', err?.message);
      return false;
    }
  }

  async syncImage(limit: number = 50) {
    this.logger.log('🔄 Sync AdImage (optimized)');
    this.init();
    const prismaHelper = new PrismaBatchHelper(this.prisma);

    try {
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

      const byAccount: Record<string, string[]> = {};
      for (const img of existingImages) {
        if (!byAccount[img.accountId]) byAccount[img.accountId] = [];
        byAccount[img.accountId].push(img.hash);
      }

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
    } catch (err) {
      this.logger.error(`❌ syncImage fatal: ${parseMetaError(err).message}`);
    }
  }

  async syncFolderVideo(limit: number = 50) {
    this.init();
    try {
      const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

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
      const videoIds = assets.map((v) => v.video_id).filter(Boolean);
      if (!videoIds.length) return true;

      let response: any = {};
      try {
        response = await api.call('GET', [''], {
          ids: videoIds.join(','),
          fields: 'source,thumbnails',
        });
      } catch (err: any) {
        this.logger.error('[FB API ERROR]', err?.message);
        return false;
      }

      const videosMap = response || {};

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
          this.logger.error('[UPDATE ERROR]', {
            assetId: asset.id,
            error: err?.message,
          });
        }
      }
      const total = Number(totalResult[0]?.total || 0);
      this.logger.log(
        `[SYNC DONE] processed=${assets.length} - total ${total}`,
      );
      return true;
    } catch (err: any) {
      this.logger.error('[CRON ERROR]', err?.message);
      return false;
    }
  }

  async syncFolderImage(limit: number = 50) {
    this.init();
    try {
      const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

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

      const imageIds = assets.map((v) => v.id);

      let response: any = {};
      try {
        response = await api.call('GET', [''], {
          ids: imageIds.join(','),
          fields: ['hash', 'url', 'name', 'creation_time', 'id'],
        });
      } catch (err: any) {
        this.logger.error('[FB API ERROR]', err?.message);
        return false;
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
          this.logger.error('[UPDATE ERROR]', {
            assetId: asset.id,
            error: err?.message,
          });
        }
      }

      const total = Number(totalResult[0]?.total || 0);
      this.logger.log(
        `[SYNC DONE] processed=${assets.length} - total ${total}`,
      );
      return true;
    } catch (err: any) {
      this.logger.error('[CRON ERROR]', err?.message);
      return false;
    }
  }
}
