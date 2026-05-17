import { Injectable, Logger } from '@nestjs/common';
import { LarkRecord } from '@prisma/client';
import axios, { AxiosRequestConfig } from 'axios';
import { drive_v3, google } from 'googleapis';
import { chunk } from '../../common/utils';
import { PrismaService } from '../prisma/prisma.service';
import { extractDriveId, mapRecord } from './lark-sync.utils';

@Injectable()
export class LarkSyncService {
  private readonly logger = new Logger(LarkSyncService.name);
  private driveSA: drive_v3.Drive;
  private readonly baseURL = 'https://open.larksuite.com/open-apis/bitable/v1';
  private accessToken: string | null = null;
  private expireAt = 0;

  constructor(private readonly prisma: PrismaService) {
    const credentials = JSON.parse(
      process.env.GOOGLE_SERVICE_ACCOUNT_JSON || '{}',
    );
    if (credentials.private_key) {
      credentials.private_key = credentials.private_key.replace(/\\n/g, '\n');
    }

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/drive'],
    });

    this.driveSA = google.drive({ version: 'v3', auth });
  }

  async syncLarkToDrive() {
    this.logger.log('🔄 Starting periodic Lark Sync & Google Drive Permission Audit...');
    try {
      // 1. Đồng bộ Lark (Sync Lark)
      this.logger.log('📥 [STEP 1] Fetching new/updated records from Lark...');
      const lastRecord = await this.prisma.larkRecord.findFirst({
        orderBy: { production_date: 'desc' },
        select: { production_date: true },
      });

      const startTime = lastRecord?.production_date
        ? new Date(lastRecord.production_date).getTime() - 24 * 60 * 60 * 1000
        : Date.now() - 30 * 24 * 60 * 60 * 1000;

      await this.searchRecords(
        'VsGGbP5wkaY7uTsTpZ2l9G9HgWc',
        'tblzTv9D1LoUcgcq',
        {
          conjunction: 'and',
          conditions: [
            {
              field_name: 'Ngày sản xuất',
              operator: 'isGreater',
              value: ['ExactDate', String(startTime)],
            },
            {
              field_name: 'Ngày sản xuất',
              operator: 'isLess',
              value: ['Tomorrow'],
            },
          ],
        },
      );

      // 2. Đồng bộ Google Drive Files (Check Permission / Sync Drive)
      this.logger.log('📁 [STEP 2] Scanning all Google Drive files in batch...');
      await this.syncDriveFiles();

      // 2.5. Map drive_id cho tất cả Lark records chưa được liên kết
      this.logger.log('🔗 [STEP 2.5] Mapping drive_id for Lark records...');
      const unmappedRecords = await this.prisma.larkRecord.findMany({
        where: {
          drive_url: { not: null },
          drive_id: null,
        },
        select: {
          id: true,
          drive_url: true,
        },
      });

      if (unmappedRecords.length > 0) {
        const mapping = unmappedRecords.map((r) => ({
          id: r.id,
          driveId: extractDriveId(r.drive_url),
        }));

        const driveIds = mapping
          .map((m) => m.driveId)
          .filter(Boolean) as string[];

        const existingDriveFiles = await this.prisma.driveFile.findMany({
          where: {
            id: { in: driveIds },
          },
          select: { id: true },
        });

        const validIds = new Set(existingDriveFiles.map((d) => d.id));

        const updateOps = mapping
          .map((m) => {
            if (!m.driveId || !validIds.has(m.driveId)) return null;
            return this.prisma.larkRecord.update({
              where: { id: m.id },
              data: { drive_id: m.driveId },
            });
          })
          .filter(Boolean) as Promise<any>[];

        if (updateOps.length > 0) {
          await Promise.all(updateOps);
          this.logger.log(
            `✅ Successfully mapped drive_id for ${updateOps.length} Lark records`,
          );
        }
      }

      // 3. Xử lý song song cho các record còn sót chưa có quyền đọc hoặc chưa map Meta
      this.logger.log('🔍 [STEP 3] Starting parallel check for pending/unverified records...');
      let totalChecked = 0;
      while (true) {
        const pendingRecords = await this.prisma.larkRecord.findMany({
          where: {
            OR: [
              { creative_asset_id: null },
              { drive_id: null },
              { drive: { drive_permission: { not: true } } },
              { drive: null },
            ],
          },
          select: { id: true },
          take: 50,
          orderBy: { id: 'asc' },
        });

        if (pendingRecords.length === 0) break;

        await this.checkPermissions(pendingRecords.map((r) => r.id));
        totalChecked += pendingRecords.length;
        this.logger.log(`... audited ${totalChecked} records in parallel`);

        if (pendingRecords.length < 50) break;
        // Tránh quá tải API bằng cách delay nhẹ giữa các batch song song
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      this.logger.log('✅ Background continuous sync and audit completed successfully');
    } catch (error: unknown) {
      this.logger.error(
        `❌ Sync failure: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }

  async checkPermissions(larkRecordIds: string[]) {
    const records = await this.prisma.larkRecord.findMany({
      where: { id: { in: larkRecordIds } },
    });

    // 1. Cập nhật trạng thái CHECKING cho toàn bộ records trước
    await Promise.all(
      records.map((record) =>
        this.prisma.larkRecord.update({
          where: { id: record.id },
          data: {
            raw: {
              ...(record.raw as any),
              permission_status: 'CHECKING',
              permission_error: null,
            },
          },
        }),
      ),
    );

    const now = new Date();
    const results = await Promise.all(
      records.map(async (record) => {
        let drive_permission = false;
        let driveFileResponse: any = null;
        const driveId = extractDriveId(record.drive_url);

        // 1. Kiểm tra quyền Drive
        if (driveId) {
          try {
            driveFileResponse = await this.driveSA.files.get({
              fileId: driveId,
              fields: 'id,name,mimeType,webViewLink,webContentLink,size',
              supportsAllDrives: true,
            });
            drive_permission = true;
          } catch (e: any) {
            drive_permission = false;
            // Cập nhật lỗi cụ thể vào DB
            await this.prisma.larkRecord.update({
              where: { id: record.id },
              data: {
                raw: {
                  ...(record.raw as any),
                  permission_status: 'FAILED',
                  permission_error: e.message || String(e),
                },
              },
            });
          }
        } else {
          await this.prisma.larkRecord.update({
            where: { id: record.id },
            data: {
              raw: {
                ...(record.raw as any),
                permission_status: 'FAILED',
                permission_error: 'Không tìm thấy Google Drive ID từ đường dẫn',
              },
            },
          });
        }

        // 2. Cập nhật trạng thái Drive trong DB & liên kết drive_id vào larkRecord
        if (driveId) {
          // Upsert DriveFile
          await this.prisma.driveFile.upsert({
            where: { id: driveId },
            update: {
              name: driveFileResponse?.data?.name || undefined, // Giữ tên cũ nếu không lấy được
              drive_permission,
              mimeType: driveFileResponse?.data?.mimeType || undefined,
              webViewLink: driveFileResponse?.data?.webViewLink || undefined,
              webContentLink: driveFileResponse?.data?.webContentLink || undefined,
              size: driveFileResponse?.data?.size || undefined,
              raw: driveFileResponse?.data ? JSON.stringify(driveFileResponse.data) : undefined,
              last_seen_at: now,
            },
            create: {
              id: driveId,
              name: driveFileResponse?.data?.name || record.drive_url || 'Unknown',
              drive_permission,
              mimeType: driveFileResponse?.data?.mimeType || null,
              webViewLink: driveFileResponse?.data?.webViewLink || null,
              webContentLink: driveFileResponse?.data?.webContentLink || null,
              size: driveFileResponse?.data?.size || null,
              raw: JSON.stringify(driveFileResponse?.data || {}),
              last_seen_at: now,
            },
          });

          // Cập nhật drive_id & permission_status cho LarkRecord
          const isSuccess = drive_permission;
          await this.prisma.larkRecord.update({
            where: { id: record.id },
            data: {
              drive_id: driveId,
              raw: {
                ...(record.raw as any),
                permission_status: isSuccess ? 'SUCCESS' : 'FAILED',
                permission_error: isSuccess ? null : 'Lỗi quyền truy cập Drive hoặc file không tồn tại',
              },
            },
          });
        }

        // 3. Tự động map nếu đã tồn tại trên Meta (theo path và tên file)
        let creative_asset_id = record.creative_asset_id;
        if (
          !creative_asset_id &&
          record.project_name &&
          record.brand_name &&
          record.product_code
        ) {
          // Lấy tên từ drive response vừa fetched được, hoặc từ DB cũ, hoặc fallback
          let targetName = driveFileResponse?.data?.name;
          if (!targetName && driveId) {
            const dbFile = await this.prisma.driveFile.findUnique({
              where: { id: driveId },
              select: { name: true },
            });
            targetName = dbFile?.name;
          }
          if (!targetName) {
            targetName = record.project_name; // Fallback
          }

          const extIndex = targetName.lastIndexOf('.');
          const targetNameWithoutExt = extIndex > 0 ? targetName.substring(0, extIndex) : targetName;

          const asset = await this.prisma.creativeAsset.findFirst({
            where: {
              OR: [
                { name: targetName },
                { name: targetNameWithoutExt },
                { name: { startsWith: targetNameWithoutExt } }
              ],
              folder: {
                name: record.product_code,
                parent: {
                  name: record.brand_name,
                  parent: {
                    name: record.project_name,
                  },
                },
              },
            },
          });

          if (asset) {
            creative_asset_id = asset.id;
            await this.prisma.larkRecord.update({
              where: { id: record.id },
              data: { creative_asset_id },
            });
          }
        }

        return {
          id: record.id,
          drive_permission,
          creative_asset_id,
        };
      })
    );

    return results;
  }

  async syncDriveFiles() {
    const now = new Date();
    let pageToken: string | undefined;

    this.logger.log('Scanning files on Google Drive...');

    do {
      const res = await this.driveSA.files.list({
        fields:
          'nextPageToken, files(id,name,mimeType,parents,webViewLink,webContentLink,size)',
        pageSize: 1000,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
        pageToken,
      });

      const files = res.data.files || [];

      const driveOps = files.map((file) => {
        const id = file.id || '';
        return this.prisma.driveFile.upsert({
          where: { id },
          update: {
            name: file.name || 'Untitled',
            last_seen_at: now,
            webViewLink: file.webViewLink || null,
            webContentLink: file.webContentLink || null,
            mimeType: file.mimeType || null,
            size: file.size || null,
            drive_permission: true,
          },
          create: {
            id,
            raw: JSON.stringify(file),
            name: file.name || 'Untitled',
            mimeType: file.mimeType || null,
            last_seen_at: now,
            webViewLink: file.webViewLink || null,
            webContentLink: file.webContentLink || null,
            size: file.size || null,
            drive_permission: true,
          },
        });
      });

      if (driveOps.length > 0) {
        // Chia nhỏ transaction để tránh timeout khi xử lý quá nhiều file cùng lúc
        const batches = chunk(driveOps, 200);
        for (const batchOps of batches) {
          await this.prisma.$transaction(batchOps);
        }
      }

      pageToken = res.data.nextPageToken || undefined;
    } while (pageToken);

    const disabledFiles = await this.prisma.driveFile.updateMany({
      where: { last_seen_at: { lt: now } },
      data: { drive_permission: false },
    });
    if (disabledFiles.count > 0) {
      this.logger.log(
        `🗑️ Marked ${disabledFiles.count} files as drive_permission = false.`,
      );
    }
  }

  private async getAccessToken(): Promise<string> {
    const now = Date.now();
    if (this.accessToken && now < this.expireAt - 60000)
      return this.accessToken;

    const res = await axios.post(
      'https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal',
      {
        app_id: process.env.LARK_APP_ID,
        app_secret: process.env.LARK_APP_SECRET,
      },
    );

    if (res.data.code !== 0) throw new Error('Lark auth failed');
    this.accessToken = res.data.tenant_access_token;
    this.expireAt = now + res.data.expire * 1000;
    return this.accessToken!;
  }

  private async request(config: AxiosRequestConfig): Promise<any> {
    const token = await this.getAccessToken();
    const res = await axios({
      baseURL: this.baseURL,
      ...config,
      headers: { Authorization: `Bearer ${token}`, ...(config.headers || {}) },
    });
    return res.data;
  }

  async searchRecords(
    appToken: string,
    tableId: string,
    filter: Record<string, any>,
  ) {
    let hasMore = true;
    let pageToken: string | undefined;

    while (hasMore) {
      const res = await this.request({
        method: 'POST',
        url: `/apps/${appToken}/tables/${tableId}/records/search`,
        params: { page_size: 500, page_token: pageToken },
        data: { automatic_fields: false, filter },
      });

      if (res.code !== 0) break;
      const items = res.data.items || [];
      if (items.length > 0) {
        const mapped = items.map((item: any) => mapRecord(item) as any);
        await this.upsertLarkBatch(mapped);
      }

      hasMore = res.data.has_more;
      pageToken = res.data.page_token;
    }
  }

  private async upsertLarkBatch(records: LarkRecord[]) {
    const validRecords = records.filter((r) => r.id);
    const ids = validRecords.map((r) => String(r.id));

    const uniqueDriveIds = Array.from(
      new Set(
        validRecords.map((r) => extractDriveId(r.drive_url)).filter(Boolean),
      ),
    ) as string[];

    const existing = await this.prisma.larkRecord.findMany({
      where: { id: { in: ids } },
      select: { id: true },
    });
    const existingIds = new Set(existing.map((e) => e.id));

    const toCreate = validRecords.filter((r) => !existingIds.has(String(r.id)));
    const toUpdate = validRecords.filter((r) => existingIds.has(String(r.id)));

    await this.prisma.$transaction(
      async (tx) => {
        if (toCreate.length > 0) {
          await tx.larkRecord.createMany({
            data: toCreate as any[],
            skipDuplicates: true,
          });
        }

        if (uniqueDriveIds.length > 0) {
          const driveData = uniqueDriveIds.map((id) => ({
            id,
            name: 'Pending Sync...',
            raw: '{}',
            drive_permission: false,
          }));

          await tx.driveFile.createMany({
            data: driveData as any[],
            skipDuplicates: true,
          });
        }
        for (const r of toUpdate) {
          await tx.larkRecord.update({
            where: { id: String(r.id) },
            data: r,
          });
        }
      },
      { timeout: 30000 },
    );
  }
}
