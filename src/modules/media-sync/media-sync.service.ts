import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { AssetType, FolderStatus } from '@prisma/client';
import { parseMetaUrlExpireTime, sleep } from '../../common/utils';
import { MetaApiService } from '../meta-api/meta-api.service';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class MediaSyncService implements OnModuleInit {
  private readonly logger = new Logger(MediaSyncService.name);
  private businessId = process.env.SDK_FACEBOOK_BUSINESS;

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaApi: MetaApiService,
  ) {}

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

          const exists = await this.prisma.creativeAsset.findUnique({
            where: { id: asset.id },
          });

          // Nếu asset đã tồn tại, BỎ QUA và check tiếp, KHÔNG DỪNG lại
          if (exists) {
            continue;
          }

          if (asset.parent_folder_id) {
            await this.prisma.creativeFolder.upsert({
              where: { id: asset.parent_folder_id },
              update: {},
              create: {
                id: asset.parent_folder_id,
                name: 'Unknown Folder (Synced)',
                status: FolderStatus.ACTIVE,
              },
            });
          }

          await this.prisma.creativeAsset.create({
            data: {
              id: asset.id,
              name: asset.name,
              type: asset.video_id ? AssetType.VIDEO : AssetType.IMAGE,
              width: asset.width,
              height: asset.height,
              thumbnail: asset.thumbnail,
              imageUrl: asset.url,
              imageHash: asset.hash,
              video_id: asset.video_id,
              duration: asset.duration,
              creation_time: asset.creation_time,
              folderId: asset.parent_folder_id,
              urlExpiredAt: parseMetaUrlExpireTime([
                asset.thumbnail,
                asset.url,
              ]),
            },
          });
          totalSynced++;
          this.logger.debug(
            `syncMetaAssets: Saved new asset ${asset.id} (${asset.name})`,
          );
        }

        nextUrl = response.paging?.next;
        if (data.length < 50) nextUrl = null;

        if (nextUrl && !shouldStop) {
          await sleep(3 * 60 * 1000);
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

    // Find videos that missing source
    const videos = await this.prisma.creativeAsset.findMany({
      where: { type: AssetType.VIDEO, video_source: null },
      take: 20,
    });

    this.logger.log(`Starting to sync sources for ${videos.length} videos...`);

    const chunkSize = 20;
    let totalUpdated = 0;

    const fields = [
      'id',
      'name',
      'last_updated_time',
      'parent_folder_id',
      'video{id,source,length,thumbnails}',
    ];

    for (let i = 0; i < videos.length; i += chunkSize) {
      const chunk = videos.slice(i, i + chunkSize);
      this.logger.debug(
        `syncVideoSources: Processing chunk ${i / chunkSize + 1} (${chunk.length} videos)`,
      );

      await Promise.all(
        chunk.map(async (v) => {
          try {
            this.logger.debug(
              `syncVideoSources: Fetching source for video ID: ${v.id}`,
            );
            const res = await this.metaApi.request('get', v.video_id!, {
              fields: 'id,source,length,thumbnails',
            });
            if (res.id) {
              const thumbnail = res?.thumbnails?.data?.find(
                (d: any) => d?.is_preferred,
              );
              await this.prisma.creativeAsset.update({
                where: { video_id: res.id },
                data: {
                  // name: res.name,
                  // creation_time: res.last_updated_time,
                  // folderId: res.parent_folder_id,
                  // video_id: res.video.id,
                  thumbnail: thumbnail?.uri,
                  height: thumbnail?.height,
                  width: thumbnail?.width,
                  duration: res?.length,
                  video_source: res?.source,
                  video_thumbnails: res?.thumbnails,
                  urlExpiredAt: parseMetaUrlExpireTime([
                    res?.source,
                    ...(res?.thumbnails?.data?.map((t: any) => t.uri) || []),
                  ]),
                },
              });
              totalUpdated++;
            }
          } catch (err: any) {
            this.logger.error(
              `Failed to sync source for video ${v.video_id || v.id}: ${err.message}`,
            );
          }
        }),
      );

      this.logger.log(
        `Synced ${Math.min(i + chunkSize, videos.length)}/${videos.length} videos...`,
      );

      if (i + chunkSize < videos.length) {
        // Sleep 30 seconds between batches of 20 to avoid rate limits
        await sleep(100000);
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
      take: 50,
    });

    if (assets.length === 0) {
      this.logger.log('✅ No expired or expiring URLs found.');
      return { success: true, count: 0 };
    }

    this.logger.log(`Refreshing URLs for ${assets.length} assets...`);

    const fieldsVideo = [
      'id',
      'name',
      'last_updated_time',
      'parent_folder_id',
      'video{id,source,length,thumbnails}',
    ];
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
        const res = await this.metaApi.request('get', asset.id, {
          fields: (isVideo ? fieldsVideo : fieldsImage).join(','),
        });

        if (res.id) {
          if (isVideo) {
            const thumbnail = res.video?.thumbnails?.data?.find(
              (d: any) => d?.is_preferred,
            );
            await this.prisma.creativeAsset.update({
              where: { id: asset.id },
              data: {
                name: res.name || asset.name,
                creation_time: res.last_updated_time || asset.creation_time,
                folderId: res.parent_folder_id || asset.folderId,
                video_id: res.video?.id || asset.video_id,
                thumbnail: thumbnail?.uri || asset.thumbnail,
                height: thumbnail?.height || asset.height,
                width: thumbnail?.width || asset.width,
                duration: res.video?.length || asset.duration,
                video_source: res.video?.source || asset.video_source,
                video_thumbnails:
                  res.video?.thumbnails || asset.video_thumbnails,
                urlExpiredAt: parseMetaUrlExpireTime([
                  res.video?.source,
                  ...(res.video?.thumbnails?.data?.map((t: any) => t.uri) ||
                    []),
                ]),
              },
            });
          } else {
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
          }
          totalUpdated++;
          this.logger.log(
            `[${index + 1}/${assets.length}] ✅ Cập nhật thành công URL mới cho asset ${asset.id}`,
          );
        }
      } catch (err: any) {
        const errData =
          err.metaError || err.response?.data?.error || err.response?.data;
        if (errData && (errData.code === 100 || errData.error_subcode === 33)) {
          this.logger.log(`Asset ${asset.id} not found on Meta. Deleting...`);
          await this.prisma.creativeAsset.delete({ where: { id: asset.id } });
          totalDeleted++;
        } else {
          this.logger.error(
            `Failed to refresh asset ${asset.id}: ${err.message}`,
          );
        }
      }

      await sleep(1000); // Tạm dừng 1s giữa mỗi request
    }

    this.logger.log(
      `✅ Expired URLs sync DONE. Updated: ${totalUpdated}, Deleted: ${totalDeleted}`,
    );
    return { success: true, count: totalUpdated, deleted: totalDeleted };
  }
}
