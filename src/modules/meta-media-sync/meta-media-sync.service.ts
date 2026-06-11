import { Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { AdAccount } from 'facebook-nodejs-business-sdk';
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

  async syncAdVideo(limit: number = 50) {
    this.logger.log('🔄 Sync Ad Video (fully optimized)');

    try {
      const where: Prisma.AdVideoWhereInput = {
        account: { needsReauth: false },
        status: { not: 'ERROR' },
        OR: [
          { source: null },
          { urlExpiredAt: null },
          { urlExpiredAt: { lte: new Date(Date.now() + 24 * 60 * 60 * 1000) } },
          { rawPayload: { equals: Prisma.DbNull } },
          { rawPayload: { equals: Prisma.JsonNull } },
        ],
      };

      const [existingVideos, totalCount] = await Promise.all([
        this.prisma.adVideo.findMany({
          where,
          orderBy: { urlExpiredAt: 'asc' },
          take: limit,
          select: { id: true, accountId: true, thumbnailUrl: true },
        }),
        this.prisma.adVideo.count({ where }),
      ]);

      this.logger.log(
        `[syncAdVideo] Found ${existingVideos.length} videos to sync (Total pending: ${totalCount})`,
      );
      if (!existingVideos.length) return;

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
                )?.uri || videoData.picture;

              await this.prisma.adVideo.update({
                where: { id: videoData.id },
                data: {
                  thumbnailUrl: thumbnail || null,
                  source: videoData.source || null,
                  title: videoData.title || null,
                  description: videoData.description || null,
                  length: videoData.length || null,
                  createdTime: videoData.created_time
                    ? new Date(videoData.created_time)
                    : null,
                  rawPayload: toPrismaJson(videoData),
                  urlExpiredAt: parseMetaUrlExpireTime([
                    videoData.source,
                    ...(videoData.thumbnails?.data?.map((t: any) => t.uri) ||
                      []),
                  ]),
                  status: 'READY',
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

      const [existingImages, total] = await Promise.all([
        this.prisma.adImage.findMany({
          where,
          orderBy: { urlExpiredAt: 'asc' },
          take: limit,
          select: { hash: true, url: true, accountId: true },
        }),
        this.prisma.adImage.count({ where }),
      ]);
      const totalCount = total;

      this.logger.log(
        `[syncAdImage] Found ${existingImages.length} images to sync (Total pending: ${totalCount})`,
      );

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
