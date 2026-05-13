import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { AssetType } from '@prisma/client';
import { toPrismaJson } from 'src/common/utils';
import { PrismaService } from 'src/modules/prisma/prisma.service';

@Injectable()
export class MediaCron implements OnModuleInit {
  private readonly logger = new Logger(MediaCron.name);

  // Biến lưu cấu hình tạm thời cho mỗi phiên chạy
  private currentConfig: {
    token: string;
    cookie: string;
    businessId: string;
    rootFolderId: string;
  } | null = null;

  constructor(private readonly prisma: PrismaService) {}

  /**
   * Lấy cấu hình từ Database
   */
  private async loadConfig() {
    try {
      const configRecord = await (this.prisma as any).systemConfig.findUnique({
        where: { key: 'META_MEDIA_SYNC_CONFIG' },
      });

      if (!configRecord || !configRecord.value) {
        return null;
      }

      // Đảm bảo value là object (nếu user chèn nhầm string vào DB kiểu Json)
      let value = configRecord.value;
      if (typeof value === 'string') {
        try {
          value = JSON.parse(value);
        } catch (e) {
          this.logger.error('❌ Config value in DB is not a valid JSON object');
          return null;
        }
      }

      return value as any;
    } catch (error) {
      this.logger.error(`❌ Failed to load config from DB`);
      return null;
    }
  }

  private getHeaders() {
    const cookie = this.currentConfig?.cookie || '';
    return {
      accept: '*/*',
      'accept-language': 'en,vi;q=0.9,en-US;q=0.8,vi-VN;q=0.7',
      'content-type': 'application/x-www-form-urlencoded',
      origin: 'https://business.facebook.com',
      priority: 'u=1, i',
      referer: 'https://business.facebook.com/',
      'sec-ch-ua':
        '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"macOS"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-site',
      'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
      Cookie: cookie,
    };
  }

  async fetchAllPages(initialUrl: string) {
    let results: any[] = [];
    let nextUrl = initialUrl;

    while (nextUrl) {
      this.logger.log(`📡 API Request: ${nextUrl.substring(0, 150)}...`);
      const rawResponse = await fetch(nextUrl, {
        headers: this.getHeaders(),
        method: 'GET',
        redirect: 'follow',
      }).then((res) => res.text());

      if (!rawResponse || rawResponse === 'undefined') {
        this.logger.error('❌ Meta API returned empty or undefined response');
        break;
      }

      let response: any;
      try {
        response = JSON.parse(rawResponse);
      } catch (e) {
        this.logger.error('❌ Failed to parse JSON response');
        break;
      }

      if (response.error) {
        this.logger.error(`❌ Meta API Error: ${response.error.message}`);
        this.logger.log(`Full URL causing error: ${nextUrl}`);
        break;
      }

      const data = response.data || [];
      results = results.concat(data);

      if (data.length > 0) {
        this.logger.log(
          `📥 Received ${data.length} items (Current batch total: ${results.length})`,
        );
      }

      nextUrl = response.paging?.next;

      if (nextUrl) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
    return results;
  }

  /**
   * Cron Job: Chạy mỗi 30 phút
   */
  @Cron('*/30 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async handleMediaSyncJob() {
    this.logger.log('⏰ Starting scheduled Media Sync...');

    // 1. Tải cấu hình từ DB
    const config = await this.loadConfig();
    if (!config || !config.token || !config.cookie) {
      this.logger.error(
        '❌ Missing or Invalid META_MEDIA_SYNC_CONFIG (token/cookie) in SystemConfig table. Skipping sync.',
      );
      this.logger.log(`Current config in DB: ${JSON.stringify(config)}`);
      return;
    }

    // 2. Lưu vào biến tạm để dùng cho các hàm fetch bên dưới
    this.currentConfig = config;

    const { rootFolderId, businessId } = config;

    try {
      if (rootFolderId) {
        await this.syncFolders(rootFolderId);
      } else {
        this.logger.warn(
          '⚠️ No rootFolderId found in config, skipping folders sync',
        );
      }

      if (businessId) {
        await this.syncCreatives(businessId);
      } else {
        this.logger.warn(
          '⚠️ No businessId found in config, skipping creatives sync',
        );
      }

      this.logger.log('✅ Scheduled Media Sync completed');
    } catch (error: any) {
      this.logger.error(`❌ Scheduled Sync failed: ${error.message}`);
    } finally {
      // 3. Xóa biến tạm sau khi chạy xong
      this.currentConfig = null;
    }
  }

  async onModuleInit() {
    this.logger.log('🚀 MediaCron initialized');
    // Chạy lần đầu ngay khi khởi động server
    this.handleMediaSyncJob();
  }

  /**
   * Đồng bộ Folder vào Database
   */
  async syncFolders(folderId: string) {
    this.logger.log(`📁 Processing Folders sync starting from: ${folderId}`);

    const fields = [
      'id',
      'name',
      'description',
      'creation_time',
      'parent_folder',
      'subfolders{id,name,description,creation_time,parent_folder,subfolders{id,name,description,creation_time,parent_folder,subfolders{id,name,description,creation_time,parent_folder,subfolders{id,name,description,creation_time,parent_folder}}}}',
    ];

    const params = new URLSearchParams({
      access_token: this.currentConfig!.token,
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

    try {
      const url = `https://graph.facebook.com/v17.0/${folderId}/subfolders?${params.toString()}`;
      const allFolders = await this.fetchAllPages(url);

      let totalUpserted = 0;

      const processFolder = async (
        folder: any,
        parentId: string | null = null,
      ) => {
        await this.prisma.creativeFolder.upsert({
          where: { id: folder.id },
          update: {
            name: folder.name,
            description: folder.description || null,
            creation_time: folder.creation_time || null,
            parentId: folder.parent_folder?.id || parentId || null,
            updatedAt: new Date(),
          },
          create: {
            id: folder.id,
            name: folder.name,
            description: folder.description || null,
            creation_time: folder.creation_time || null,
            parentId: folder.parent_folder?.id || parentId || null,
          },
        });

        totalUpserted++;
        this.logger.log(
          `   └─ ✅ Saved Folder: ${folder.name} (ID: ${folder.id})`,
        );

        if (folder.subfolders?.data && folder.subfolders.data.length > 0) {
          for (const sub of folder.subfolders.data) {
            await processFolder(sub, folder.id);
          }
        }
      };

      for (const folder of allFolders) {
        await processFolder(folder, folderId);
      }

      this.logger.log(
        `✅ Folders sync DONE. Total processed: ${totalUpserted}`,
      );
    } catch (error: any) {
      this.logger.error(`❌ syncFolders failed: ${error.message}`);
    }
  }

  /**
   * Đồng bộ Creatives vào Database
   */
  async syncCreatives(businessId: string) {
    this.logger.log(`🎨 Processing Creatives sync for business: ${businessId}`);

    const rawFields =
      '%5B%22id%22%2C%22name%22%2C%22creation_time%22%2C%22last_updated_time%22%2C%22creative_folders%7Bid%2Cname%7D%22%2C%22width%22%2C%22height%22%2C%22duration%22%2C%22type%22%2C%22thumbnail%22%2C%22video_id%22%2C%22hash%22%2C%22url%22%2C%22fragment_id%22%2C%22content%22%2C%22text_type%22%2C%22label%22%2C%22fragment_status%22%2C%22can_create_mockup_ad%22%2C%22ad_account_id%22%2C%22ad_id%22%2C%22parent_folder_id%22%5D';

    const url =
      `https://graph.facebook.com/v17.0/${businessId}/creatives` +
      `?access_token=${this.currentConfig!.token}` +
      `&__business_id=${businessId}` +
      `&_reqName=object%3Abusiness%2Fcreatives` +
      `&_reqSrc=AssetLibraryBizCreativeRecentViewDataLoader` +
      `&fields=${rawFields}` +
      `&limit=25` + // Trả về limit=25 cho ổn định theo snippet gốc
      `&locale=en_US` +
      `&method=get` +
      `&pretty=0` +
      `&suppress_http_code=1` +
      `&xref=fe47908523b96c1c2`;

    try {
      const allCreatives = await this.fetchAllPages(url);
      let creativeCount = 0;

      for (const creative of allCreatives) {
        const folderId = creative.parent_folder_id;

        if (!folderId) {
          continue;
        }

        const exists = await this.prisma.creativeAsset.findUnique({
          where: { id: creative.id },
        });

        if (!exists) {
          await this.prisma.creativeFolder.upsert({
            where: { id: folderId },
            update: {},
            create: {
              id: folderId,
              name:
                creative.creative_folders?.data?.[0]?.name || 'Unknown Folder',
            },
          });

          await this.prisma.creativeAsset.create({
            data: {
              id: creative.id,
              name: creative.name,
              type:
                creative.type === 'VIDEO' ? AssetType.VIDEO : AssetType.IMAGE,
              width: creative.width,
              height: creative.height,
              thumbnail: creative.thumbnail,
              imageUrl: creative.url,
              imageHash: creative.hash,
              video_id: creative.video_id,
              video_source: creative.source,
              duration: creative.duration,
              creation_time: creative.creation_time,
              folderId: folderId,
              status: creative.fragment_status
                ? toPrismaJson(creative.fragment_status)
                : null,
            },
          });
          creativeCount++;
          this.logger.log(
            `   └─ ✨ Created: ${creative.name} (ID: ${creative.id})`,
          );
        }
      }

      this.logger.log(
        `✅ Creatives sync DONE. Total new assets: ${creativeCount}`,
      );
    } catch (error: any) {
      this.logger.error(`❌ syncCreatives failed: ${error.message}`);
    }
  }
}
