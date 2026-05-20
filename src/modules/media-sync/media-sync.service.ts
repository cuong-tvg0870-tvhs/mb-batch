import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { AssetType, FolderStatus } from '@prisma/client';
import axios from 'axios';
import { parseMetaUrlExpireTime, sleep } from '../../common/utils';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class MediaSyncService implements OnModuleInit {
  private readonly logger = new Logger(MediaSyncService.name);
  private businessId = process.env.SDK_FACEBOOK_BUSINESS;

  constructor(private readonly prisma: PrismaService) {}

  async onModuleInit() {
    this.logger.log('Module initialized. Starting automatic sync...');
    // Chạy ngầm để không block quá trình khởi động của NestJS
    setTimeout(async () => {
      try {
        await this.syncMetaFolders();
        await this.syncMetaAssets();
        await this.syncVideoSources();
        this.logger.log('✅ Automatic sync on module init completed.');
      } catch (err) {
        this.logger.error(
          '❌ Error during automatic sync on module init:',
          err,
        );
      }
    }, 3000); // Delay 3s để đảm bảo DB và các module khác đã sẵn sàng
  }

  async getMetaAuthConfig() {
    const config = await this.prisma.systemConfig.findUnique({
      where: { key: 'META_AUTH_CONFIG' },
    });
    return (config?.value as any) || {};
  }

  private getHeaders(authConfig: any) {
    return {
      accept: '*/*',
      'accept-language': 'en,vi;q=0.9,en-US;q=0.8,vi-VN;q=0.7',
      'content-type': 'application/x-www-form-urlencoded',
      origin: 'https://business.facebook.com',
      referer: 'https://business.facebook.com/',
      'sec-ch-ua':
        '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"macOS"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-site',
      'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      Cookie: authConfig?.cookie || '',
    };
  }

  private async handleMetaError(errorResponse: any) {
    console.log(errorResponse);
    if (!errorResponse) return;
    const error = errorResponse.error || errorResponse;
    if (!error || !error.code) return;

    const code = error.code;
    const type = error.type;

    // OAuthException (190, 102) or Rate Limits (17, 4, 32, 613)
    const isAuthError =
      type === 'OAuthException' || code === 190 || code === 102;
    const isLimitError =
      code === 17 || code === 4 || code === 32 || code === 613;

    if (isAuthError || isLimitError) {
      this.logger.warn(
        `Meta API Error [${code}]: ${error.message}. Clearing META_AUTH_CONFIG.`,
      );
      await this.prisma.systemConfig.deleteMany({
        where: { key: 'META_AUTH_CONFIG' },
      });
    }
  }

  private async fetchAllPages(initialUrl: string, authConfig: any) {
    this.logger.debug(`fetchAllPages: Starting fetch from initialUrl`);
    let results: any[] = [];
    let nextUrl = initialUrl;
    let pageCount = 0;

    while (nextUrl) {
      pageCount++;
      this.logger.debug(`fetchAllPages: Fetching page ${pageCount}...`);
      try {
        const response = await axios.get(nextUrl, {
          headers: this.getHeaders(authConfig),
        });
        if (response.data.error) {
          this.logger.debug(
            `fetchAllPages: Meta error received on page ${pageCount}`,
          );
          await this.handleMetaError(response.data);
          break;
        }
        const data = response.data.data || [];
        this.logger.debug(
          `fetchAllPages: Fetched ${data.length} items on page ${pageCount}`,
        );
        results = results.concat(data);
        nextUrl = response.data.paging?.next;
      } catch (err: any) {
        await this.handleMetaError(err.response?.data);
        this.logger.error(
          `Fetch All Pages Error: ${err.response?.data || err.message}`,
        );
        break;
      }
    }
    this.logger.debug(
      `fetchAllPages: Finished. Total items: ${results.length}`,
    );
    return results;
  }

  async syncMetaFolders() {
    this.logger.log('📁 Starting Folders Sync...');
    const authConfig = await this.getMetaAuthConfig();
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
      access_token: token,
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

    const url = `https://graph.facebook.com/v24.0/${rootId}/subfolders?${params.toString()}`;
    const allFolders = await this.fetchAllPages(url, authConfig);

    // Identify top-level folders in DB under this root that are MISSING from Meta
    const topLevelMetaIds = allFolders.map((f) => f.id);
    await this.prisma.creativeFolder.updateMany({
      where: {
        parentId: rootId === authConfig.businessId ? null : rootId,
        id: { notIn: topLevelMetaIds },
      },
      data: { status: FolderStatus.DEACTIVE },
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
        await this.prisma.creativeFolder.updateMany({
          where: {
            parentId: folder.id,
            id: { notIn: subMetaIds },
          },
          data: { status: FolderStatus.DEACTIVE },
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
    const authConfig = await this.getMetaAuthConfig();
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
        const response = await axios.get(nextUrl, {
          headers: this.getHeaders(authConfig),
        });
        if (response.data.error) {
          this.logger.debug(
            `syncMetaAssets: Meta error received on page ${pageCount}`,
          );
          await this.handleMetaError(response.data);
          break;
        }
        const data = response.data.data || [];
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
              urlExpiredAt: parseMetaUrlExpireTime(asset.thumbnail),
            },
          });
          totalSynced++;
          this.logger.debug(
            `syncMetaAssets: Saved new asset ${asset.id} (${asset.name})`,
          );
        }

        nextUrl = response.data.paging?.next;
        if (data.length < 50) nextUrl = null;

        if (nextUrl && !shouldStop) {
          await sleep(3 * 60 * 1000);
        }
      } catch (err: any) {
        await this.handleMetaError(err.response?.data || err);
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
    const authConfig = await this.getMetaAuthConfig();
    const token = authConfig.accessToken;
    if (!token) {
      this.logger.error('Chưa cấu hình Meta Auth');
      return { success: false, error: 'Chưa cấu hình Meta Auth' };
    }

    // Find videos that missing source
    const videos = await this.prisma.creativeAsset.findMany({
      where: {
        type: AssetType.VIDEO,
        video_source: null,
      },
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
          const videoUrl = `https://graph.facebook.com/v24.0/${v.id}`;
          const videoParams = new URLSearchParams({
            access_token: token,
            fields: fields.join(','),
            method: 'get',
            pretty: '0',
            suppress_http_code: '1',
            xref: 'fe47908523b96c1c2',
          });

          try {
            this.logger.debug(
              `syncVideoSources: Fetching source for video ID: ${v.id}`,
            );
            const res = await axios
              .get(`${videoUrl}?${videoParams.toString()}`, {
                headers: this.getHeaders(authConfig),
              })
              .then((r) => r.data);
            if (res.id) {
              const thumbnail = res.video?.thumbnails?.data?.find(
                (d: any) => d?.is_preferred,
              );
              await this.prisma.creativeAsset.update({
                where: { id: v.id },
                data: {
                  name: res.name,
                  creation_time: res.last_updated_time,
                  folderId: res.parent_folder_id,
                  video_id: res.video.id,
                  thumbnail: thumbnail?.uri,
                  height: thumbnail?.height,
                  width: thumbnail?.width,
                  duration: res?.video?.length,
                  video_source: res?.video?.source,
                  video_thumbnails: res?.video?.thumbnails,
                  urlExpiredAt: parseMetaUrlExpireTime(res?.video?.source),
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
        await sleep(30000);
      }
    }

    this.logger.log(
      `✅ Video sources sync DONE. Total updated: ${totalUpdated}`,
    );
    return { success: true, count: totalUpdated };
  }
}
