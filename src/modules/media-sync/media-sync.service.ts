import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { AssetType, FolderStatus, Prisma } from '@prisma/client';
import {
  parseMetaError,
  parseMetaUrlExpireTime,
  sleep,
} from '../../common/utils';
import { MetaApiService } from '../meta-api/meta-api.service';
import { PrismaService } from '../prisma/prisma.service';

const parseEnvInteger = (
  value: string | undefined,
  defaultValue: number,
  minValue: number,
) => {
  const parsed = Number(value ?? defaultValue);
  return Number.isFinite(parsed) ? Math.max(minValue, parsed) : defaultValue;
};

@Injectable()
export class MediaSyncService implements OnModuleInit {
  private readonly logger = new Logger(MediaSyncService.name);
  private businessId = process.env.SDK_FACEBOOK_BUSINESS;
  private readonly metaAssetFetchDelayMs = parseEnvInteger(
    process.env.META_ASSET_FETCH_DELAY_MS,
    2000,
    0,
  );
  private readonly videoSourceBatchSize = parseEnvInteger(
    process.env.META_VIDEO_SOURCE_BATCH_SIZE,
    10,
    1,
  );
  private readonly videoSourceBatchSleepMs = parseEnvInteger(
    process.env.META_VIDEO_SOURCE_CHUNK_SLEEP_MS,
    8000,
    0,
  );
  private readonly expiredUrlAssetSleepMs = parseEnvInteger(
    process.env.META_EXPIRED_URL_ASSET_SLEEP_MS,
    2000,
    0,
  );
  private nextMetaAssetFetchAt = Date.now();

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaApi: MetaApiService,
  ) {}

  private async waitForMetaAssetFetchSlot() {
    if (this.metaAssetFetchDelayMs <= 0) return;

    const now = Date.now();
    const waitMs = Math.max(0, this.nextMetaAssetFetchAt - now);
    this.nextMetaAssetFetchAt =
      Math.max(now, this.nextMetaAssetFetchAt) + this.metaAssetFetchDelayMs;

    if (waitMs > 0) await sleep(waitMs);
  }

  private getPreferredThumbnail(thumbnails?: any) {
    return (
      thumbnails?.data?.find((item: any) => item?.is_preferred) ||
      thumbnails?.data?.[0] ||
      null
    );
  }

  private getVideoThumbnailCount(thumbnails?: any) {
    if (!thumbnails) return 0;
    if (Array.isArray(thumbnails?.data)) return thumbnails.data.length;
    if (Array.isArray(thumbnails)) return thumbnails.length;
    return 0;
  }

  private hasGoodVideoThumbnailSet(thumbnails?: any) {
    return this.getVideoThumbnailCount(thumbnails) > 2;
  }

  private needsVideoSourceRefresh(asset: {
    video_source?: string | null;
    video_thumbnails?: any;
  }) {
    return (
      !asset.video_source ||
      !this.hasGoodVideoThumbnailSet(asset.video_thumbnails)
    );
  }

  private async findVideoAssetsNeedingSourceRefresh() {
    const rows = await this.prisma.$queryRaw<Array<{ id: string }>>(
      Prisma.sql`
        SELECT id
        FROM "CreativeAsset"
        WHERE type = ${AssetType.VIDEO}
          AND (
            "video_source" IS NULL
            OR "video_thumbnails" IS NULL
            OR CASE
              WHEN jsonb_typeof("video_thumbnails"->'data') = 'array'
                THEN jsonb_array_length("video_thumbnails"->'data')
              WHEN jsonb_typeof("video_thumbnails") = 'array'
                THEN jsonb_array_length("video_thumbnails")
              ELSE 0
            END <= 2
          )
        ORDER BY "updatedAt" ASC
      `,
    );

    if (rows.length === 0) return [];

    return this.prisma.creativeAsset.findMany({
      where: { id: { in: rows.map((row) => row.id) } },
      orderBy: { updatedAt: 'asc' },
    });
  }

  private formatMetaError(err: any) {
    const metaError = parseMetaError(err);
    return [
      `message="${metaError.message}"`,
      `code=${metaError.code ?? '-'}`,
      `subcode=${metaError.subcode ?? '-'}`,
      `type=${metaError.type ?? '-'}`,
      `fbtrace=${metaError.fbtrace_id ?? '-'}`,
    ].join(' ');
  }

  private shouldDeleteMissingMetaAsset(err: any) {
    const metaError = parseMetaError(err);
    return Number(metaError.subcode) === 33;
  }

  private async refreshVideoCreativeAsset(asset: {
    id: string;
    name: string | null;
    creation_time: string | null;
    folderId: string;
    video_id: string | null;
    thumbnail: string | null;
    height: number | null;
    width: number | null;
    duration: number | null;
    video_source: string | null;
    video_thumbnails: any;
  }) {
    let assetPayload: any = null;
    let videoPayload: any = null;

    if (asset.video_id) {
      await this.waitForMetaAssetFetchSlot();
      videoPayload = await this.metaApi.request('get', asset.video_id, {
        fields: 'id,source,length,thumbnails',
      });
    } else {
      await this.waitForMetaAssetFetchSlot();
      assetPayload = await this.metaApi.request('get', asset.id, {
        fields: [
          'id',
          'name',
          'last_updated_time',
          'parent_folder_id',
          'video{id,source,length,thumbnails}',
        ].join(','),
      });
      videoPayload = assetPayload?.video;
    }

    if (!videoPayload?.id) {
      throw new Error(`Missing video payload for creative asset ${asset.id}`);
    }

    const thumbnail = this.getPreferredThumbnail(videoPayload.thumbnails);
    await this.prisma.creativeAsset.update({
      where: { id: asset.id },
      data: {
        name: assetPayload?.name || asset.name,
        creation_time: assetPayload?.last_updated_time || asset.creation_time,
        folderId: assetPayload?.parent_folder_id || asset.folderId,
        video_id: videoPayload.id || asset.video_id,
        thumbnail: thumbnail?.uri || asset.thumbnail,
        height: thumbnail?.height || asset.height,
        width: thumbnail?.width || asset.width,
        duration: videoPayload.length || asset.duration,
        video_source: videoPayload.source || asset.video_source,
        video_thumbnails: videoPayload.thumbnails || asset.video_thumbnails,
        urlExpiredAt: parseMetaUrlExpireTime([
          videoPayload.source,
          ...(videoPayload.thumbnails?.data?.map((t: any) => t.uri) || []),
        ]),
      },
    });
  }

  async onModuleInit() {
    this.logger.log('Module initialized. Starting automatic sync...');
    // Chạy ngầm để không block quá trình khởi động của NestJS
    setTimeout(async () => {
      try {
        // await this.syncVideoSources();
        // await this.syncMetaFolders();
        // await this.syncMetaAssets();
        // await this.syncVideoSources();
        this.logger.log('✅ Automatic sync on module init completed.');
      } catch (err) {
        this.logger.error(
          '❌ Error during automatic sync on module init:',
          err,
        );
      }
    }, 3000); // Delay 3s để đảm bảo DB và các module khác đã sẵn sàng
  }

  // Removed helper methods (getMetaAuthConfig, getHeaders, handleMetaError, fetchAllPages)
  async syncMetaFolders() {
    this.logger.log('📁 Starting Folders Sync...');
    const authConfig = await this.metaApi.getMetaAuthConfig();
    const token = authConfig.accessToken;
    if (!token) {
      this.logger.error('Chưa cấu hình Meta Auth');
      return { success: false, error: 'Chưa cấu hình Meta Auth' };
    }

    const rootId = '4303729193176038';
    if (!rootId) {
      this.logger.error('Thiếu Root Folder ID hoặc Business ID để sync');
      return {
        success: false,
        error: 'Thiếu Root Folder ID hoặc Business ID để sync',
      };
    }

    const fields = [
      'id',
      'name',
      'description',
      'creation_time',
      'parent_folder',
      `
      subfolders.limit(200){
        id,
        name,
        description,
        creation_time,
        parent_folder,
        subfolders.limit(200){
          id,
          name,
          description,
          creation_time,
          parent_folder,
          subfolders.limit(200){
            id,
            name,
            description,
            creation_time,
            parent_folder,
            subfolders.limit(200){
              id,
              name,
              description,
              creation_time,
              parent_folder,
              subfolders.limit(200){
                id,
                name,
                description,
                creation_time,
                parent_folder
              }
            }
          }
        }
      }`.replace(/\s+/g, ''),
    ];

    const params = new URLSearchParams({
      _reqName: 'object:creative_folder/subfolders',
      _reqSrc: 'AssetLibraryBizCreativeDataLoader.brands',
      fields: fields.join(','),
      locale: 'en_US',
      method: 'get',
      pretty: '0',
      recursive: 'false',
      suppress_http_code: '1',
      xref: 'f9f90a01c10abe369',
      metadata: '1',
    });

    const url = `https://graph.facebook.com/v24.0/${rootId}/subfolders?access_token=${token}&${params.toString()}`;
    const allFolders = await this.metaApi.fetchAllPages(url, authConfig);

    // Identify top-level folders in DB under this root that are MISSING from Meta
    const topLevelMetaIds = allFolders.map((f) => f.id);
    await this.prisma.creativeFolder.deleteMany({
      where: {
        parentId: rootId === authConfig.businessId ? null : rootId,
        id: { notIn: topLevelMetaIds },
      },
    });

    const processFolder = async (
      folder: any,
      parentId: string | null = null,
    ) => {
      this.logger.debug(
        `processFolder: Upserting folder ${folder.id} (${folder.name})`,
      );
      await this.prisma.creativeFolder.upsert({
        where: { id: folder.id },
        update: {
          name: folder.name,
          description: folder.description || null,
          creation_time: folder.creation_time || null,
          parentId: folder.parent_folder?.id || parentId || null,
          status: FolderStatus.ACTIVE,
          updatedAt: new Date(),
        },
        create: {
          id: folder.id,
          name: folder.name,
          description: folder.description || null,
          creation_time: folder.creation_time || null,
          parentId: folder.parent_folder?.id || parentId || null,
          status: FolderStatus.ACTIVE,
        },
      });

      if (folder.subfolders?.data) {
        const subMetaIds = folder.subfolders.data.map((f: any) => f.id);
        await this.prisma.creativeFolder.deleteMany({
          where: {
            parentId: folder.id,
            id: { notIn: subMetaIds },
          },
        });

        for (const sub of folder.subfolders.data) {
          await processFolder(sub, folder.id);
        }
      }
    };

    for (const folder of allFolders) {
      await processFolder(
        folder,
        rootId === authConfig.businessId ? null : rootId,
      );
    }

    this.logger.log(
      `✅ Folders sync DONE. Total processed: ${allFolders.length}`,
    );
    return { success: true, count: allFolders.length, allFolders };
  }

  async syncMetaAssets(folderId?: string) {
    this.logger.log('🎨 Starting Creatives Sync...');
    const authConfig = await this.metaApi.getMetaAuthConfig();
    const token = authConfig.accessToken;
    const businessId = this.businessId || '1916878948527753';

    if (!token || !businessId) {
      this.logger.error('Chưa cấu hình Meta Auth');
      return { success: false, error: 'Chưa cấu hình Meta Auth' };
    }

    const fields = [
      'id',
      'name',
      'creation_time',
      'duration',
      'hash',
      'height',
      'width',
      'thumbnail',
      'type',
      'url',
      'video_id',
      'video{source, thumbnails}',
      'parent_folder_id',
    ];

    let nextUrl: string | null =
      `https://graph.facebook.com/v24.0/${businessId}/creatives?access_token=${token}&fields=${fields.join(',')}&limit=50&method=get&pretty=0&suppress_http_code=1&xref=fe47908523b96c1c2`;
    let totalSynced = 0;
    let shouldStop = false;
    let pageCount = 0;

    // Định nghĩa mốc thời gian cutoff: 2 ngày cách đây
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - 2); // Lùi lại đúng 2 ngày

    while (nextUrl && !shouldStop) {
      pageCount++;
      this.logger.debug(`syncMetaAssets: Fetching page ${pageCount}...`);
      try {
        const response = await this.metaApi.request('get', nextUrl);
        const data = response.data || [];
        this.logger.debug(
          `syncMetaAssets: Fetched ${data.length} assets on page ${pageCount}`,
        );

        if (data.length === 0) break;

        const candidateAssets: any[] = [];
        for (const asset of data) {
          // Parse thời gian tạo của asset
          if (asset.creation_time) {
            const assetCreationTime = new Date(asset.creation_time);

            // Dừng hoàn toàn nếu gặp asset cũ hơn 2 ngày
            if (assetCreationTime < cutoffDate) {
              this.logger.debug(
                `Reached asset older than cutoff date (${cutoffDate.toISOString()}). Stopping sync.`,
              );
              shouldStop = true;
              break;
            }
          }

          candidateAssets.push(asset);
        }

        if (candidateAssets.length) {
          const existingIds = new Set(
            (
              await this.prisma.creativeAsset.findMany({
                where: { id: { in: candidateAssets.map((asset) => asset.id) } },
                select: { id: true },
              })
            ).map((item) => item.id),
          );

          const newAssets = candidateAssets.filter(
            (asset) => !existingIds.has(asset.id),
          );
          if (!newAssets.length) {
            nextUrl = response.paging?.next;
            if (data.length < 50) nextUrl = null;
            if (nextUrl && !shouldStop) {
              await sleep(Number(process.env.META_ASSET_PAGE_SLEEP_MS || 5000));
            }
            continue;
          }

          const folderIds = [
            ...new Set(
              newAssets.map((asset) => asset.parent_folder_id).filter(Boolean),
            ),
          ];

          if (folderIds.length) {
            const existingFolderIds = new Set(
              (
                await this.prisma.creativeFolder.findMany({
                  where: { id: { in: folderIds } },
                  select: { id: true },
                })
              ).map((folder) => folder.id),
            );

            const missingFolders = folderIds.filter(
              (id) => !existingFolderIds.has(id),
            );
            if (missingFolders.length) {
              await this.prisma.creativeFolder.createMany({
                data: missingFolders.map((id) => ({
                  id,
                  name: 'Unknown Folder (Synced)',
                  status: FolderStatus.ACTIVE,
                })),
                skipDuplicates: true,
              });
            }
          }

          await this.prisma.creativeAsset.createMany({
            data: newAssets.map((asset) => ({
              id: asset.id,
              name: asset.name,
              type: asset.video_id ? AssetType.VIDEO : AssetType.IMAGE,
              width: asset.width,
              height: asset.height,
              thumbnail: asset.thumbnail,
              imageUrl: asset.url,
              imageHash: asset.hash,
              video_id: asset.video_id,
              video_source: asset.video?.source,
              video_thumbnails: asset.video?.thumbnails,
              duration: asset.duration || asset.video?.length,
              creation_time: asset.creation_time,
              folderId: asset.parent_folder_id,
              urlExpiredAt: parseMetaUrlExpireTime([
                asset.thumbnail,
                asset.url,
                asset.video?.source,
                ...(asset.video?.thumbnails?.data?.map((t: any) => t.uri) ||
                  []),
              ]),
            })),
            skipDuplicates: true,
          });

          totalSynced += newAssets.length;
          if (newAssets.length) {
            this.logger.debug(
              `syncMetaAssets: Saved ${newAssets.length} new assets on page ${pageCount}`,
            );
          }
        }

        nextUrl = response.paging?.next;
        if (data.length < 50) nextUrl = null;

        if (nextUrl && !shouldStop) {
          await sleep(Number(process.env.META_ASSET_PAGE_SLEEP_MS || 5000));
        }
      } catch (err: any) {
        this.logger.error(
          `Asset Sync Error: ${err.response?.data || err.message}`,
        );
        break;
      }
    }

    this.logger.log(`✅ Creatives sync DONE. Total new assets: ${totalSynced}`);
    return { success: true, count: totalSynced };
  }

  async syncVideoSources() {
    this.logger.log('🎥 Starting Video Sources Sync...');
    const authConfig = await this.metaApi.getMetaAuthConfig();
    const token = authConfig.accessToken;
    if (!token) {
      this.logger.error('Chưa cấu hình Meta Auth');
      return { success: false, error: 'Chưa cấu hình Meta Auth' };
    }

    // Find videos missing source or with an incomplete thumbnail set (<= 2).
    const videos = (await this.findVideoAssetsNeedingSourceRefresh()).filter(
      (asset) => this.needsVideoSourceRefresh(asset),
    );

    this.logger.log(`Starting to sync sources for ${videos.length} videos...`);

    const chunkSize = this.videoSourceBatchSize;
    let totalUpdated = 0;

    for (let i = 0; i < videos.length; i += chunkSize) {
      const chunk = videos.slice(i, i + chunkSize);
      this.logger.debug(
        `syncVideoSources: Processing chunk ${i / chunkSize + 1} (${chunk.length} videos)`,
      );

      const videoIds = chunk.map((v) => v.video_id).filter(Boolean) as string[];
      if (!videoIds.length) continue;

      try {
        await this.waitForMetaAssetFetchSlot();
        const response = await this.metaApi.request(
          'get',
          'https://graph.facebook.com/v24.0/',
          {
            ids: videoIds.join(','),
            fields: 'id,source,length,thumbnails',
          },
        );
        const videosById = response || {};

        await Promise.all(
          chunk.map(async (v) => {
            const res = videosById[v.video_id!];
            if (!res?.id) return;

            const thumbnail =
              res?.thumbnails?.data?.find((d: any) => d?.is_preferred) ||
              res?.thumbnails?.data?.[0];
            await this.prisma.creativeAsset.update({
              where: { video_id: res.id },
              data: {
                thumbnail: thumbnail?.uri || v.thumbnail,
                height: thumbnail?.height || v.height,
                width: thumbnail?.width || v.width,
                duration: res?.length || v.duration,
                video_source: res?.source || v.video_source,
                video_thumbnails: res?.thumbnails || v.video_thumbnails,
                urlExpiredAt: parseMetaUrlExpireTime([
                  res?.source,
                  ...(res?.thumbnails?.data?.map((t: any) => t.uri) || []),
                ]),
              },
            });
            totalUpdated++;
          }),
        );
      } catch (err: any) {
        this.logger.error(
          `Failed to sync source chunk ${i / chunkSize + 1}: ${err.message}`,
        );
      }

      this.logger.log(
        `Synced ${Math.min(i + chunkSize, videos.length)}/${videos.length} videos...`,
      );

      if (i + chunkSize < videos.length) {
        await sleep(this.videoSourceBatchSleepMs);
      }
    }

    this.logger.log(
      `✅ Video sources sync DONE. Total updated: ${totalUpdated}`,
    );
    return { success: true, count: totalUpdated };
  }

  async syncExpiredUrls() {
    this.logger.log('🔄 Starting Expired URLs Sync...');
    const authConfig = await this.metaApi.getMetaAuthConfig();
    const token = authConfig.accessToken;
    if (!token) {
      this.logger.error('Chưa cấu hình Meta Auth');
      return { success: false, error: 'Chưa cấu hình Meta Auth' };
    }

    const assets = await this.prisma.creativeAsset.findMany({
      where: {
        urlExpiredAt: {
          lte: new Date(Date.now() + 24 * 60 * 60 * 1000), // Within 24 hours
        },
      },
      take: 500,
    });

    if (assets.length === 0) {
      this.logger.log('✅ No expired or expiring URLs found.');
      return { success: true, count: 0 };
    }

    this.logger.log(`Refreshing URLs for ${assets.length} assets...`);

    const fieldsImage = [
      'id',
      'name',
      'last_updated_time',
      'parent_folder_id',
      'url',
      'hash',
      'height',
      'width',
    ];

    let totalUpdated = 0;
    let totalDeleted = 0;

    for (const [index, asset] of assets.entries()) {
      this.logger.log(
        `[${index + 1}/${assets.length}] Đang xử lý asset: ${asset.id} (Type: ${asset.type})...`,
      );

      const isVideo = asset.type === AssetType.VIDEO;

      try {
        if (isVideo) {
          await this.refreshVideoCreativeAsset(asset);
          totalUpdated++;
          this.logger.log(
            `[${index + 1}/${assets.length}] ✅ Cập nhật thành công URL mới cho asset ${asset.id}`,
          );
          await sleep(this.expiredUrlAssetSleepMs);
          continue;
        }

        await this.waitForMetaAssetFetchSlot();
        const res = await this.metaApi.request('get', asset.id, {
          fields: fieldsImage.join(','),
        });

        if (res.id) {
          await this.prisma.creativeAsset.update({
            where: { id: asset.id },
            data: {
              name: res.name || asset.name,
              creation_time: res.last_updated_time || asset.creation_time,
              folderId: res.parent_folder_id || asset.folderId,
              imageUrl: res.url || asset.imageUrl,
              thumbnail: res.url || asset.thumbnail,
              imageHash: res.hash || asset.imageHash,
              height: res.height || asset.height,
              width: res.width || asset.width,
              urlExpiredAt: parseMetaUrlExpireTime(res.url),
            },
          });
          totalUpdated++;
          this.logger.log(
            `[${index + 1}/${assets.length}] ✅ Cập nhật thành công URL mới cho asset ${asset.id}`,
          );
        }
      } catch (err: any) {
        if (this.shouldDeleteMissingMetaAsset(err)) {
          this.logger.log(
            `Asset ${asset.id} not found on Meta. Deleting... (${this.formatMetaError(err)})`,
          );
          await this.prisma.creativeAsset.delete({ where: { id: asset.id } });
          totalDeleted++;
        } else {
          this.logger.error(
            `Failed to refresh asset ${asset.id} (type=${asset.type}, video_id=${asset.video_id || '-'}): ${this.formatMetaError(err)}`,
          );
        }
      }

      await sleep(this.expiredUrlAssetSleepMs);
    }

    this.logger.log(
      `✅ Expired URLs sync DONE. Updated: ${totalUpdated}, Deleted: ${totalDeleted}`,
    );
    return { success: true, count: totalUpdated, deleted: totalDeleted };
  }
}
