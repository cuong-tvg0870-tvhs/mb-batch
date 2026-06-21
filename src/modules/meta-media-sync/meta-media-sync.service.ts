import { Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { AdAccount, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import { PrismaBatchHelper } from '../../common/helpers/prisma-batch.helper';
import {
  chunk,
  executeMetaApiWithRetry,
  fetchAll,
  parseMetaError,
  parseMetaUrlExpireTime,
  sleep,
  toPrismaJson,
} from '../../common/utils';
import {
  AD_IMAGE_FIELDS,
  AD_VIDEO_FIELDS,
} from '../../common/utils/meta-field';
import { MetaApiService } from '../meta-api/meta-api.service';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class MetaMediaSyncService {
  private readonly logger = new Logger(MetaMediaSyncService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaApi: MetaApiService,
  ) {}

  private pickFirstString(...values: unknown[]) {
    for (const value of values) {
      if (typeof value === 'string' && value.trim()) return value.trim();
    }

    return null;
  }

  private initMetaSdk() {
    const token = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
    if (!token) {
      throw new Error('SDK_FACEBOOK_ACCESS_TOKEN missing');
    }

    FacebookAdsApi.init(token);
  }

  private normalizeNullableInt(value: unknown) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      return Math.trunc(value);
    }

    if (typeof value === 'string' && value.trim()) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return Math.trunc(parsed);
    }

    return null;
  }

  private normalizeNullableDate(value: unknown) {
    if (!value) return null;
    const date = new Date(value as string | number | Date);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  private resolveMetaVideoStatus(videoData: any) {
    return (
      this.pickFirstString(
        videoData?.status?.video_status,
        videoData?.status?.status,
        videoData?.video_status,
        videoData?.status,
      ) || (videoData?.source ? 'READY' : 'PROCESSING')
    );
  }

  private normalizeAccountId(accountId?: string | null) {
    return accountId?.replaceAll('act_', '') || null;
  }

  private getThumbnailList(thumbnails: any) {
    if (Array.isArray(thumbnails?.data)) return thumbnails.data;
    if (Array.isArray(thumbnails)) return thumbnails;
    return [];
  }

  private getPreferredThumbnailUrl(thumbnails: any) {
    const list = this.getThumbnailList(thumbnails);
    const preferred =
      list.find((thumbnail: any) => !!thumbnail?.is_preferred) || list[0];

    return this.pickFirstString(
      preferred?.uri,
      preferred?.url,
      preferred?.image_url,
    );
  }

  private resolveAdVideoPreview(video?: any) {
    if (!video) {
      return { previewUrl: null, thumbnailUrl: null };
    }

    const thumbnailUrl = this.pickFirstString(
      video.thumbnailUrl,
      this.getPreferredThumbnailUrl(video.rawPayload?.thumbnails),
      video.rawPayload?.picture,
    );

    return {
      previewUrl: this.pickFirstString(video.source, thumbnailUrl),
      thumbnailUrl,
    };
  }

  private resolveAdImageUrl(image?: any) {
    if (!image) return null;

    return this.pickFirstString(image.url, image.permalink_url);
  }

  private resolveAssetPreview(asset?: any) {
    if (!asset) {
      return { previewUrl: null, thumbnailUrl: null, imageUrl: null };
    }

    if (asset.type === 'VIDEO') {
      const thumbnailUrl = this.pickFirstString(
        asset.thumbnail,
        this.getPreferredThumbnailUrl(asset.video_thumbnails),
        asset.imageUrl,
      );

      return {
        previewUrl: this.pickFirstString(asset.video_source, thumbnailUrl),
        thumbnailUrl,
        imageUrl: thumbnailUrl,
      };
    }

    const imageUrl = this.pickFirstString(asset.imageUrl, asset.thumbnail);

    return {
      previewUrl: imageUrl,
      thumbnailUrl: imageUrl,
      imageUrl,
    };
  }

  private getFirstArrayItem(value: unknown) {
    return Array.isArray(value) && value.length > 0 ? value[0] : undefined;
  }

  private getAssetImageUrl(asset?: any) {
    return this.pickFirstString(
      asset?.previewUrl,
      asset?.preview_url,
      asset?.imageUrl,
      asset?.image_url,
      asset?.thumbnailUrl,
      asset?.thumbnail_url,
      asset?.picture,
      asset?.url,
    );
  }

  private getAssetVideoThumbnailUrl(asset?: any) {
    return this.pickFirstString(
      asset?.thumbnailUrl,
      asset?.thumbnail_url,
      asset?.imageUrl,
      asset?.image_url,
      asset?.previewUrl,
      asset?.preview_url,
      asset?.picture,
      this.getPreferredThumbnailUrl(asset?.thumbnails),
      this.getPreferredThumbnailUrl(asset?.list_thumbnails),
      this.getPreferredThumbnailUrl(asset?.video_thumbnails),
      asset?.selected_thumbnail?.image_url,
      asset?.selected_thumbnail?.uri,
    );
  }

  private resolveRawCreativePreview(raw?: any) {
    if (!raw) {
      return { previewUrl: null, thumbnailUrl: null, imageUrl: null };
    }

    const story = raw.object_story_spec;
    const linkData = story?.link_data;
    const videoData = story?.video_data;
    const photoData = story?.photo_data;
    const assetImage = this.getFirstArrayItem(raw.asset_feed_spec?.images);
    const assetVideo = this.getFirstArrayItem(raw.asset_feed_spec?.videos);
    const childAttachment = this.getFirstArrayItem(
      linkData?.child_attachments || raw.child_attachments,
    );

    const thumbnailUrl = this.pickFirstString(
      raw.thumbnailUrl,
      raw.thumbnail_url,
      raw.picture,
      this.getAssetVideoThumbnailUrl(assetVideo),
      this.getAssetImageUrl(assetImage),
      videoData?.image_url,
      videoData?.thumbnail_url,
      videoData?.picture,
      linkData?.picture,
      linkData?.image_url,
      linkData?.thumbnail_url,
      photoData?.image_url,
      photoData?.picture,
      photoData?.url,
      this.getAssetImageUrl(childAttachment),
    );

    const imageUrl = this.pickFirstString(
      raw.imageUrl,
      raw.image_url,
      this.getAssetImageUrl(assetImage),
      linkData?.image_url,
      linkData?.picture,
      photoData?.image_url,
      photoData?.url,
      thumbnailUrl,
    );

    return {
      previewUrl: this.pickFirstString(thumbnailUrl, imageUrl),
      thumbnailUrl,
      imageUrl,
    };
  }

  private datesEqual(a?: Date | null, b?: Date | null) {
    if (!a && !b) return true;
    if (!a || !b) return false;
    return a.getTime() === b.getTime();
  }

  private getImageKey(accountId?: string | null, hash?: string | null) {
    const normalizedAccountId = this.normalizeAccountId(accountId);
    if (!normalizedAccountId || !hash) return null;
    return `${normalizedAccountId}:${hash}`;
  }

  private async refreshCreativePreviews(options?: {
    videoIds?: string[];
    imageHashes?: string[];
    imageIds?: string[];
  }) {
    const videoIds = [...new Set(options?.videoIds?.filter(Boolean) || [])];
    const imageHashes = [
      ...new Set(options?.imageHashes?.filter(Boolean) || []),
    ];
    const imageIds = [...new Set(options?.imageIds?.filter(Boolean) || [])];

    const relationFilters: Prisma.CreativeWhereInput[] = [
      videoIds.length ? { videoId: { in: videoIds } } : undefined,
      imageHashes.length ? { imageHash: { in: imageHashes } } : undefined,
      imageIds.length ? { imageId: { in: imageIds } } : undefined,
    ].filter(Boolean) as Prisma.CreativeWhereInput[];

    if (!relationFilters.length) return 0;

    const creatives = await this.prisma.creative.findMany({
      where: { OR: relationFilters },
      select: {
        id: true,
        accountId: true,
        imageHash: true,
        imageId: true,
        imageUrl: true,
        thumbnailUrl: true,
        previewUrl: true,
        rawPayload: true,
        urlExpiredAt: true,
        videoId: true,
        adImage: {
          select: {
            id: true,
            hash: true,
            accountId: true,
            url: true,
            permalink_url: true,
          },
        },
        adVideo: {
          select: {
            id: true,
            source: true,
            thumbnailUrl: true,
            rawPayload: true,
          },
        },
        assetMappings: {
          select: {
            creativeAsset: {
              select: {
                id: true,
                type: true,
                imageHash: true,
                imageUrl: true,
                thumbnail: true,
                video_id: true,
                video_source: true,
                video_thumbnails: true,
              },
            },
          },
        },
      },
    });

    if (!creatives.length) return 0;

    const creativeImageKeys = creatives
      .map((creative) =>
        creative.accountId && creative.imageHash
          ? { accountId: creative.accountId, hash: creative.imageHash }
          : null,
      )
      .filter(Boolean) as Array<{ accountId: string; hash: string }>;
    const creativeImageIds = creatives
      .map((creative) => creative.imageId)
      .filter(Boolean) as string[];
    const creativeVideoIds = creatives
      .map((creative) => creative.videoId)
      .filter(Boolean) as string[];
    const creativeImageHashes = creatives
      .map((creative) => creative.imageHash)
      .filter(Boolean) as string[];

    const [images, directAssets] = await Promise.all([
      creativeImageKeys.length || creativeImageIds.length
        ? this.prisma.adImage.findMany({
            where: {
              OR: [
                creativeImageIds.length
                  ? { id: { in: creativeImageIds } }
                  : undefined,
                ...creativeImageKeys,
              ].filter(Boolean) as Prisma.AdImageWhereInput[],
            },
            select: {
              id: true,
              accountId: true,
              hash: true,
              url: true,
              permalink_url: true,
            },
          })
        : [],
      creativeImageHashes.length || creativeVideoIds.length
        ? this.prisma.creativeAsset.findMany({
            where: {
              OR: [
                creativeImageHashes.length
                  ? { imageHash: { in: creativeImageHashes } }
                  : undefined,
                creativeVideoIds.length
                  ? { video_id: { in: creativeVideoIds } }
                  : undefined,
              ].filter(Boolean) as Prisma.CreativeAssetWhereInput[],
            },
            select: {
              id: true,
              type: true,
              imageHash: true,
              imageUrl: true,
              thumbnail: true,
              video_id: true,
              video_source: true,
              video_thumbnails: true,
            },
          })
        : [],
    ]);

    const imageById = new Map(
      images.map((image) => [image.id, image] as [string, (typeof images)[0]]),
    );
    const imageByKey = new Map(
      images
        .map((image) => [this.getImageKey(image.accountId, image.hash), image])
        .filter(([key]) => !!key) as Array<[string, (typeof images)[0]]>,
    );
    const assetByImageHash = new Map(
      directAssets
        .filter((asset) => asset.imageHash)
        .map(
          (asset) =>
            [asset.imageHash as string, asset] as [
              string,
              (typeof directAssets)[0],
            ],
        ),
    );
    const assetByVideoId = new Map(
      directAssets
        .filter((asset) => asset.video_id)
        .map(
          (asset) =>
            [asset.video_id as string, asset] as [
              string,
              (typeof directAssets)[0],
            ],
        ),
    );

    const updates: Array<{ id: string; data: Prisma.CreativeUpdateInput }> = [];

    for (const creative of creatives) {
      const mappedAssets = (creative.assetMappings || [])
        .map((mapping) => mapping.creativeAsset)
        .filter(Boolean);
      const image = this.pickFirstString(creative.imageId)
        ? imageById.get(creative.imageId) || creative.adImage
        : creative.adImage;
      const imageByHash =
        image ||
        imageByKey.get(
          this.getImageKey(creative.accountId, creative.imageHash),
        );

      let previewUrl: string | null = null;
      let thumbnailUrl: string | null = null;
      let imageUrl: string | null = null;
      const expiryUrls: string[] = [];
      const rawPreview = this.resolveRawCreativePreview(creative.rawPayload);

      if (creative.videoId) {
        const videoPreview = this.resolveAdVideoPreview(creative.adVideo);
        const mappedVideoAsset =
          mappedAssets.find(
            (asset) =>
              asset.type === 'VIDEO' &&
              (!creative.videoId || asset.video_id === creative.videoId),
          ) || assetByVideoId.get(creative.videoId);
        const assetPreview = this.resolveAssetPreview(mappedVideoAsset);

        thumbnailUrl = this.pickFirstString(
          videoPreview.thumbnailUrl,
          assetPreview.thumbnailUrl,
          rawPreview.thumbnailUrl,
          creative.thumbnailUrl,
          creative.imageUrl,
        );
        previewUrl = this.pickFirstString(
          assetPreview.previewUrl,
          rawPreview.previewUrl,
          thumbnailUrl,
          videoPreview.previewUrl,
          creative.previewUrl,
        );
        imageUrl = this.pickFirstString(
          thumbnailUrl,
          rawPreview.imageUrl,
          creative.imageUrl,
        );
        expiryUrls.push(
          ...[
            videoPreview.previewUrl,
            videoPreview.thumbnailUrl,
            assetPreview.previewUrl,
            assetPreview.thumbnailUrl,
            rawPreview.previewUrl,
            rawPreview.thumbnailUrl,
            rawPreview.imageUrl,
          ].filter(Boolean),
        );
      } else {
        const mappedImageAsset =
          mappedAssets.find(
            (asset) =>
              asset.type === 'IMAGE' &&
              (!creative.imageHash || asset.imageHash === creative.imageHash),
          ) || assetByImageHash.get(creative.imageHash);
        const assetPreview = this.resolveAssetPreview(mappedImageAsset);
        const syncedImageUrl = this.resolveAdImageUrl(imageByHash);

        imageUrl = this.pickFirstString(
          syncedImageUrl,
          assetPreview.imageUrl,
          rawPreview.imageUrl,
          creative.imageUrl,
          creative.thumbnailUrl,
          creative.previewUrl,
        );
        thumbnailUrl = this.pickFirstString(
          imageUrl,
          assetPreview.thumbnailUrl,
          rawPreview.thumbnailUrl,
        );
        previewUrl = this.pickFirstString(
          imageUrl,
          assetPreview.previewUrl,
          rawPreview.previewUrl,
        );
        expiryUrls.push(
          ...[
            syncedImageUrl,
            assetPreview.imageUrl,
            assetPreview.thumbnailUrl,
            rawPreview.previewUrl,
            rawPreview.thumbnailUrl,
            rawPreview.imageUrl,
          ].filter(Boolean),
        );
      }

      if (!previewUrl && !thumbnailUrl && !imageUrl) continue;

      const calculatedUrlExpiredAt = parseMetaUrlExpireTime(expiryUrls);
      const data: Prisma.CreativeUpdateInput = {};

      if (previewUrl && creative.previewUrl !== previewUrl) {
        data.previewUrl = previewUrl;
      }
      if (thumbnailUrl && creative.thumbnailUrl !== thumbnailUrl) {
        data.thumbnailUrl = thumbnailUrl;
      }
      if (imageUrl && creative.imageUrl !== imageUrl) {
        data.imageUrl = imageUrl;
      }
      if (
        calculatedUrlExpiredAt &&
        !this.datesEqual(creative.urlExpiredAt, calculatedUrlExpiredAt)
      ) {
        data.urlExpiredAt = calculatedUrlExpiredAt;
      }

      if (Object.keys(data).length) {
        data.updatedAt = new Date();
        updates.push({ id: creative.id, data });
      }
    }

    for (const updateChunk of chunk(updates, 50)) {
      await Promise.all(
        updateChunk.map((item) =>
          this.prisma.creative.update({
            where: { id: item.id },
            data: item.data,
          }),
        ),
      );
    }

    if (updates.length) {
      this.logger.log(
        `[refreshCreativePreviews] Updated ${updates.length}/${creatives.length} creatives`,
      );
    }

    return updates.length;
  }

  async syncAdVideo(limit: number = 200) {
    this.logger.log('🔄 Sync Ad Video (fully optimized)');

    try {
      this.initMetaSdk();

      const where: Prisma.AdVideoWhereInput = {
        account: { needsReauth: false },
        AND: [
          {
            OR: [{ status: null }, { status: { notIn: ['ERROR', 'error'] } }],
          },
          {
            OR: [
              { source: null },
              { thumbnailUrl: null },
              { urlExpiredAt: null },
              {
                urlExpiredAt: {
                  lte: new Date(Date.now() + 24 * 60 * 60 * 1000),
                },
              },
              { rawPayload: { equals: Prisma.DbNull } },
              { rawPayload: { equals: Prisma.JsonNull } },
            ],
          },
        ],
      };

      const existingVideos = await this.prisma.adVideo.findMany({
        where,
        orderBy: { urlExpiredAt: 'asc' },
        take: limit,
        select: { id: true, accountId: true, thumbnailUrl: true },
      });

      this.logger.log(
        `[syncAdVideo] Found ${existingVideos.length} videos to sync`,
      );

      if (!existingVideos.length) {
        return true;
      }

      // Gom nhóm theo accountId
      const byAccount: Record<string, string[]> = {};
      for (const v of existingVideos) {
        if (!byAccount[v.accountId]) byAccount[v.accountId] = [];
        byAccount[v.accountId].push(v.id);
      }

      for (const [accountId, videoIds] of Object.entries(byAccount)) {
        try {
          const adAccount = new AdAccount(accountId);
          const cursor = await executeMetaApiWithRetry(
            () =>
              adAccount.getAdVideos(AD_VIDEO_FIELDS, {
                filtering: [{ field: 'id', operator: 'IN', value: videoIds }],
                limit: 50,
              }),
            { logger: this.logger },
          );

          const videos = await fetchAll(cursor);
          const returnedIds = new Set(videos.map((v) => v.id));
          const missingIds = videoIds.filter((id) => !returnedIds.has(id));

          // Đánh dấu ERROR cho các ID không tìm thấy
          if (missingIds.length > 0) {
            this.logger.warn(
              `[syncAdVideo] Account ${accountId}: ${missingIds.length} videos not found on Meta. Marking as ERROR.`,
            );
            await this.prisma.adVideo.updateMany({
              where: { id: { in: missingIds } },
              data: { status: 'ERROR', updatedAt: new Date() },
            });
          }

          if (!videos.length) continue;

          // Cập nhật các video thành công
          const updatePromises = videos.map(async (videoData) => {
            try {
              const thumbnail =
                videoData.thumbnails?.data?.find(
                  (th: any) => !!th?.is_preferred,
                )?.uri ||
                videoData.thumbnails?.data?.[0]?.uri ||
                videoData.picture;

              await this.prisma.adVideo.update({
                where: { id: videoData.id },
                data: {
                  thumbnailUrl: thumbnail || null,
                  source: videoData.source || null,
                  title: videoData.title || null,
                  description: videoData.description || null,
                  length: this.normalizeNullableInt(videoData.length),
                  createdTime: this.normalizeNullableDate(
                    videoData.created_time,
                  ),
                  rawPayload: toPrismaJson(videoData),
                  urlExpiredAt: parseMetaUrlExpireTime([
                    videoData.source,
                    thumbnail,
                    videoData.picture,
                    ...(videoData.thumbnails?.data?.map((t: any) => t.uri) ||
                      []),
                  ]),
                  status: this.resolveMetaVideoStatus(videoData),
                  updatedAt: new Date(),
                },
              });
              this.logger.debug(
                `[syncAdVideo] Updated video ${videoData.id} successfully`,
              );
            } catch (err: any) {
              this.logger.error(
                `[syncAdVideo] DB Error updating ${videoData.id}: ${err.message}`,
              );
            }
          });

          await Promise.all(updatePromises);
          await this.refreshCreativePreviews({
            videoIds: videos.map((video) => video.id).filter(Boolean),
          });
          await sleep(5000);
        } catch (err: any) {
          this.logger.error(
            `[syncAdVideo] Error processing Account ${accountId}: ${err.message}`,
          );
        }
      }

      return true;
    } catch (err: any) {
      this.logger.error('[CRON ERROR]', err?.message);
      return false;
    }
  }

  async syncAdImage(limit: number = 50) {
    this.logger.log('🔄 Sync AdImage (optimized)');

    const prismaHelper = new PrismaBatchHelper(this.prisma);

    try {
      const where: Prisma.AdImageWhereInput = {
        account: { needsReauth: false },
        OR: [
          { urlExpiredAt: null },
          { urlExpiredAt: { lte: new Date(Date.now() + 24 * 60 * 60 * 1000) } },
          { rawPayload: { equals: Prisma.DbNull } },
          { rawPayload: { equals: Prisma.JsonNull } },
        ],
      };

      const existingImages = await this.prisma.adImage.findMany({
        where,
        orderBy: { urlExpiredAt: 'asc' },
        take: limit,
        select: { hash: true, url: true, accountId: true },
      });

      this.logger.log(
        `[syncAdImage] Found ${existingImages.length} images to sync`,
      );

      if (!existingImages.length) {
        return;
      }

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

            // Tìm các hash không được Meta trả về để đánh dấu ERROR
            const returnedHashes = new Set(images.map((img) => img.hash));
            const missingHashes = hashChunk.filter(
              (h) => !returnedHashes.has(h),
            );

            if (missingHashes.length > 0) {
              this.logger.warn(
                `[syncAdImage] Account ${accountId}: ${missingHashes.length} images not found on Meta. Marking as ERROR.`,
              );
              await this.prisma.adImage.updateMany({
                where: {
                  hash: { in: missingHashes },
                  accountId,
                },
                data: { status: 'ERROR', updatedAt: new Date() },
              });
            }

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
                status: img?.status || 'READY',
                createdTime: img?.created_time
                  ? new Date(img.created_time)
                  : undefined,
                createdAt: img?.created_time
                  ? new Date(img.created_time)
                  : undefined,
                urlExpiredAt: parseMetaUrlExpireTime([
                  img?.permalink_url,
                  img?.url,
                ]),
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
            this.logger.log(
              `[syncAdImage] Account ${accountId}: Synced ${updateData.length} images`,
            );

            await this.refreshCreativePreviews({
              imageHashes: images.map((image) => image.hash).filter(Boolean),
            });
            await sleep(800);
          } catch (error) {
            this.logger.error(
              `❌ syncAdImage ${accountId}: ${parseMetaError(error).message}`,
            );
          }
        }
      }
    } catch (err) {
      this.logger.error(`❌ syncAdImage fatal: ${parseMetaError(err).message}`);
    }
  }

  async recalculateLocalUrlExpired() {
    this.logger.log('🔄 Starting local URL Expiration recalculation...');
    let adImageUpdated = 0;
    let adVideoUpdated = 0;
    let creativeAssetUpdated = 0;

    // 1. Recalculate AdImage
    const adImages = await this.prisma.adImage.findMany({
      select: { id: true, url: true, permalink_url: true, urlExpiredAt: true },
    });
    for (const img of adImages) {
      const calculated = parseMetaUrlExpireTime([img.permalink_url, img.url]);
      if (
        calculated &&
        (!img.urlExpiredAt ||
          img.urlExpiredAt.getTime() !== calculated.getTime())
      ) {
        await this.prisma.adImage.update({
          where: { id: img.id },
          data: { urlExpiredAt: calculated },
        });
        adImageUpdated++;
      }
    }

    // 2. Recalculate AdVideo
    const adVideos = await this.prisma.adVideo.findMany({
      select: {
        id: true,
        source: true,
        thumbnailUrl: true,
        rawPayload: true,
        urlExpiredAt: true,
      },
    });
    for (const vid of adVideos) {
      const thumbnails =
        (vid.rawPayload as any)?.thumbnails?.data?.map((t: any) => t.uri) || [];
      const calculated = parseMetaUrlExpireTime([
        vid.source,
        vid.thumbnailUrl,
        ...thumbnails,
      ]);
      if (
        calculated &&
        (!vid.urlExpiredAt ||
          vid.urlExpiredAt.getTime() !== calculated.getTime())
      ) {
        await this.prisma.adVideo.update({
          where: { id: vid.id },
          data: { urlExpiredAt: calculated },
        });
        adVideoUpdated++;
      }
    }

    // 3. Recalculate CreativeAsset (Folder Images/Videos)
    const assets = await this.prisma.creativeAsset.findMany({
      select: {
        id: true,
        imageUrl: true,
        thumbnail: true,
        video_source: true,
        video_thumbnails: true,
        urlExpiredAt: true,
      },
    });
    for (const asset of assets) {
      const thumbnails =
        (asset.video_thumbnails as any)?.data?.map((t: any) => t.uri) || [];
      const calculated = parseMetaUrlExpireTime([
        asset.imageUrl,
        asset.thumbnail,
        asset.video_source,
        ...thumbnails,
      ]);
      if (
        calculated &&
        (!asset.urlExpiredAt ||
          asset.urlExpiredAt.getTime() !== calculated.getTime())
      ) {
        await this.prisma.creativeAsset.update({
          where: { id: asset.id },
          data: { urlExpiredAt: calculated },
        });
        creativeAssetUpdated++;
      }
    }

    this.logger.log(
      `✅ Local URL Expiration recalculation completed: AdImage updated: ${adImageUpdated}, AdVideo updated: ${adVideoUpdated}, CreativeAsset updated: ${creativeAssetUpdated}`,
    );
  }
}
