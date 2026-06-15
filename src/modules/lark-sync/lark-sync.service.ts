import { Injectable, Logger } from '@nestjs/common';
import { LarkRecord } from '@prisma/client';
import axios, { AxiosRequestConfig } from 'axios';
import { drive_v3, google } from 'googleapis';
import { chunk } from '../../common/utils';
import { PrismaService } from '../prisma/prisma.service';
import {
  PUBLIC_ONLY_PERMISSION_ERROR,
  buildPermissionRawUpdate,
  extractDriveId,
  hasExplicitDriveAccess,
  isPermissionCheckDue,
  mapRecord,
  parseAllowedSharedDriveIds,
} from './lark-sync.utils';

@Injectable()
export class LarkSyncService {
  private readonly logger = new Logger(LarkSyncService.name);
  private driveSA: drive_v3.Drive;
  private readonly baseURL = 'https://open.larksuite.com/open-apis/bitable/v1';
  private readonly allowedSharedDriveIds = parseAllowedSharedDriveIds(
    process.env.GOOGLE_ALLOWED_SHARED_DRIVE_IDS,
  );
  private serviceAccountEmail: string | null = null;
  private accessToken: string | null = null;
  private expireAt = 0;

  constructor(private readonly prisma: PrismaService) {
    const credentials = JSON.parse(
      process.env.GOOGLE_SERVICE_ACCOUNT_JSON || '{}',
    );
    if (credentials.private_key) {
      credentials.private_key = credentials.private_key.replace(/\\n/g, '\n');
    }
    this.serviceAccountEmail = credentials.client_email || null;

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/drive'],
    });

    this.driveSA = google.drive({ version: 'v3', auth });
  }

  async syncLarkToDrive() {
    this.logger.log(
      '🔄 Starting periodic Lark Sync & Google Drive Permission Audit...',
    );
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

      // 2. Map legacy records and create DriveFile placeholders without full Drive scan.
      this.logger.log('🔗 [STEP 2] Mapping drive_id from Lark URLs...');
      await this.mapMissingDriveIds();

      // 3. Audit only permission-pending records that are due for retry.
      const fourteenDaysAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);
      this.logger.log(
        '🔍 [STEP 3] Checking permission-pending records with backoff...',
      );
      const pendingRecords = await this.prisma.larkRecord.findMany({
        where: {
          production_date: { gte: fourteenDaysAgo },
          drive_url: { not: null },
          OR: [
            { drive_id: null },
            { drive: { drive_permission: { not: true } } },
            { drive: null },
            {
              NOT: {
                raw: {
                  path: ['permission_status'],
                  equals: 'SUCCESS',
                },
              },
            },
            {
              NOT: {
                raw: {
                  path: ['permission_access_verified'],
                  equals: true,
                },
              },
            },
          ],
        },
        select: { id: true, raw: true },
        orderBy: { id: 'asc' },
      });
      const dueRecords = pendingRecords.filter((r) =>
        isPermissionCheckDue(r.raw as any),
      );

      let totalChecked = 0;
      for (const batch of chunk(dueRecords, 50)) {
        await this.checkPermissions(batch.map((r) => r.id));
        totalChecked += batch.length;
        this.logger.log(`... audited ${totalChecked} records`);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      // 4. Map assets for permission-success records without rechecking Drive.
      this.logger.log(
        '🎯 [STEP 4] Mapping Meta assets for verified records...',
      );
      await this.mapVerifiedRecordsToAssets(fourteenDaysAgo);

      this.logger.log(
        '✅ Background continuous sync and audit completed successfully',
      );
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
      include: { drive: true },
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
    const results: Array<{
      id: string;
      success: boolean;
      drive_permission: boolean;
      creative_asset_id: string | null;
    }> = [];

    // Chạy từng cụm 10 bản ghi song song để tránh Rate Limit API của Google
    const chunks = chunk(records, 10);
    for (const recordChunk of chunks) {
      const chunkResults = await Promise.all(
        recordChunk.map(async (record) => {
          let drive_permission = false;
          let driveFileResponse: any = null;
          let permission_error: string | null = null;
          let driveId = record.drive_id || extractDriveId(record.drive_url);

          const alreadyVerified = !!(
            record.drive_id &&
            record.drive?.drive_permission === true &&
            (record.raw as any)?.permission_access_verified === true
          );

          if (alreadyVerified) {
            drive_permission = true;
            driveId = record.drive_id;
            driveFileResponse = {
              data: {
                id: record.drive_id,
                name: record.drive?.name,
                mimeType: record.drive?.mimeType,
                webViewLink: record.drive?.webViewLink,
                webContentLink: record.drive?.webContentLink,
                size: record.drive?.size,
              },
            };
          } else if (driveId) {
            try {
              driveFileResponse = await this.driveSA.files.get({
                fileId: driveId,
                fields:
                  'id,name,mimeType,webViewLink,webContentLink,size,ownedByMe,sharedWithMeTime,driveId',
                supportsAllDrives: true,
              });
              drive_permission = hasExplicitDriveAccess(
                driveFileResponse.data,
                this.allowedSharedDriveIds,
                this.serviceAccountEmail,
              );
              if (!drive_permission) {
                const permissions = await this.getFilePermissions(driveId);
                drive_permission = hasExplicitDriveAccess(
                  driveFileResponse.data,
                  this.allowedSharedDriveIds,
                  this.serviceAccountEmail,
                  permissions,
                );
              }
              if (!drive_permission) {
                permission_error = PUBLIC_ONLY_PERMISSION_ERROR;
              }
            } catch (e: any) {
              drive_permission = false;
              permission_error = e.message || String(e);
            }
          } else {
            permission_error = 'Không tìm thấy Google Drive ID từ đường dẫn';
          }

          if (driveId && !alreadyVerified) {
            const file = driveFileResponse?.data;
            await this.prisma.driveFile.upsert({
              where: { id: driveId },
              update: {
                name: file?.name || undefined,
                drive_permission,
                mimeType: file?.mimeType || undefined,
                webViewLink: file?.webViewLink || undefined,
                webContentLink: file?.webContentLink || undefined,
                size: file?.size || undefined,
                raw: file ? JSON.stringify(file) : undefined,
                last_seen_at: now,
              },
              create: {
                id: driveId,
                name: file?.name || record.drive_url || 'Unknown File',
                drive_permission,
                mimeType: file?.mimeType || null,
                webViewLink: file?.webViewLink || null,
                webContentLink: file?.webContentLink || null,
                size: file?.size || null,
                raw: JSON.stringify(file || {}),
                last_seen_at: now,
              },
            });
          }

          // Cập nhật trạng thái Drive trong DB & liên kết drive_id vào larkRecord
          const updatedRaw = buildPermissionRawUpdate(
            record.raw as any,
            drive_permission,
            permission_error,
            now,
          );

          const updateData: any = {
            raw: updatedRaw,
          };

          if (driveId) {
            updateData.drive_id = driveId;
          }

          let creative_asset_id = record.creative_asset_id;
          if (drive_permission && !creative_asset_id) {
            creative_asset_id = await this.findCreativeAssetId(
              record,
              driveFileResponse?.data,
              driveId,
            );
            if (creative_asset_id)
              updateData.creative_asset_id = creative_asset_id;
          }

          await this.prisma.larkRecord.update({
            where: { id: record.id },
            data: updateData,
          });

          return {
            id: record.id,
            success: drive_permission,
            drive_permission,
            creative_asset_id,
          };
        }),
      );

      results.push(...chunkResults);
    }

    return results;
  }

  private async getFilePermissions(driveId: string): Promise<any[]> {
    try {
      const res = await this.driveSA.permissions.list({
        fileId: driveId,
        fields:
          'permissions(id,type,role,emailAddress,deleted,permissionDetails(permissionType,role,inherited,inheritedFrom))',
        supportsAllDrives: true,
      });

      return res.data.permissions || [];
    } catch {
      return [];
    }
  }

  private async mapMissingDriveIds() {
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

    const mapping = unmappedRecords
      .map((r) => ({
        id: r.id,
        driveId: extractDriveId(r.drive_url),
      }))
      .filter((m): m is { id: string; driveId: string } => !!m.driveId);

    const uniqueDriveIds = Array.from(new Set(mapping.map((m) => m.driveId)));
    if (uniqueDriveIds.length > 0) {
      await this.prisma.driveFile.createMany({
        data: uniqueDriveIds.map((id) => ({
          id,
          name: 'Pending Permission Check',
          raw: '{}',
          drive_permission: false,
        })),
        skipDuplicates: true,
      });
    }

    for (const batch of chunk(mapping, 50)) {
      await Promise.all(
        batch.map((m) =>
          this.prisma.larkRecord.update({
            where: { id: m.id },
            data: { drive_id: m.driveId },
          }),
        ),
      );
    }

    if (mapping.length > 0) {
      this.logger.log(`✅ Mapped drive_id for ${mapping.length} Lark records`);
    }
  }

  private async mapVerifiedRecordsToAssets(since: Date) {
    const records = await this.prisma.larkRecord.findMany({
      where: {
        production_date: { gte: since },
        creative_asset_id: null,
        drive: { drive_permission: true },
        raw: {
          path: ['permission_access_verified'],
          equals: true,
        },
      },
      include: { drive: true },
    });

    let mappedCount = 0;
    for (const batch of chunk(records, 50)) {
      await Promise.all(
        batch.map(async (record) => {
          const creativeAssetId = await this.findCreativeAssetId(
            record,
            record.drive,
            record.drive_id,
          );
          if (!creativeAssetId) return;

          await this.prisma.larkRecord.update({
            where: { id: record.id },
            data: { creative_asset_id: creativeAssetId },
          });
          mappedCount++;
        }),
      );
    }

    if (mappedCount > 0) {
      this.logger.log(
        `✅ Mapped ${mappedCount} verified Lark records to Meta assets`,
      );
    }
  }

  private async findCreativeAssetId(
    record: Pick<
      LarkRecord,
      | 'project_name'
      | 'brand_name'
      | 'product_code'
      | 'creative_asset_id'
      | 'drive_url'
    >,
    driveFile: any,
    driveId?: string | null,
  ): Promise<string | null> {
    if (
      record.creative_asset_id ||
      !record.project_name ||
      !record.brand_name ||
      !record.product_code
    ) {
      return null;
    }

    let targetName = driveFile?.name;
    if (!targetName && driveId) {
      const dbFile = await this.prisma.driveFile.findUnique({
        where: { id: driveId },
        select: { name: true },
      });
      targetName = dbFile?.name;
    }
    if (!targetName) targetName = record.project_name;

    const extIndex = targetName.lastIndexOf('.');
    const targetNameWithoutExt =
      extIndex > 0 ? targetName.substring(0, extIndex) : targetName;

    const asset = await this.prisma.creativeAsset.findFirst({
      where: {
        OR: [
          { name: targetName },
          { name: targetNameWithoutExt },
          { name: { startsWith: targetNameWithoutExt } },
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

    return asset?.id || null;
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
        validRecords
          .map((r) => r.drive_id || extractDriveId(r.drive_url))
          .filter(Boolean),
      ),
    ) as string[];

    const existing = await this.prisma.larkRecord.findMany({
      where: { id: { in: ids } },
      select: { id: true },
    });
    const existingIds = new Set(existing.map((e) => e.id));

    const toCreate = validRecords.filter((r) => !existingIds.has(String(r.id)));
    const toUpdate = validRecords.filter((r) => existingIds.has(String(r.id)));

    // 1. Tạo DriveFile placeholders trước để thỏa mãn FK LarkRecord.drive_id.
    if (uniqueDriveIds.length > 0) {
      const driveData = uniqueDriveIds.map((id) => ({
        id,
        name: 'Pending Permission Check',
        raw: '{}',
        drive_permission: false,
      }));

      await this.prisma.driveFile.createMany({
        data: driveData as any[],
        skipDuplicates: true,
      });
    }

    // 2. Thực hiện CreateMany trực tiếp cho LarkRecord
    if (toCreate.length > 0) {
      await this.prisma.larkRecord.createMany({
        data: toCreate as any[],
        skipDuplicates: true,
      });
    }

    // 3. Cập nhật các LarkRecord cũ song song theo cụm 50 để tránh quá tải
    if (toUpdate.length > 0) {
      const batches = chunk(toUpdate, 50);
      for (const batch of batches) {
        await Promise.all(
          batch.map((r) =>
            this.prisma.larkRecord.update({
              where: { id: String(r.id) },
              data: r,
            }),
          ),
        );
      }
    }
  }

  async checkOnlyMetaUploadStatus() {
    this.logger.log('🔍 [TEST] Starting upload status check only...');

    // Lấy tất cả records chưa có creative_asset_id
    const records = await this.prisma.larkRecord.findMany({
      where: {
        creative_asset_id: null,
        project_name: { not: null },
        brand_name: { not: null },
        product_code: { not: null },
      },
      select: {
        id: true,
        drive_url: true,
        project_name: true,
        brand_name: true,
        product_code: true,
        drive_id: true,
        creative_asset_id: true,
      },
    });

    this.logger.log(
      `Found ${records.length} Lark records without creative_asset_id to check.`,
    );
    let matchedCount = 0;

    const batches = chunk(records, 100);
    for (const batch of batches) {
      await Promise.all(
        batch.map(async (record) => {
          let driveId = record.drive_id;
          if (!driveId) {
            const extractDriveId = (url?: string | null): string | null => {
              if (!url) return null;
              const match =
                url.match(/\/d\/([a-zA-Z0-9_-]+)/) ||
                url.match(/[?&]id=([a-zA-Z0-9_-]+)/);
              return match ? match[1] : null;
            };
            driveId = extractDriveId(record.drive_url);
          }

          let targetName = null;
          if (driveId) {
            const dbFile = await this.prisma.driveFile.findUnique({
              where: { id: driveId },
              select: { name: true },
            });
            targetName = dbFile?.name;
          }
          if (!targetName) {
            targetName = record.project_name;
          }

          const extIndex = targetName.lastIndexOf('.');
          const targetNameWithoutExt =
            extIndex > 0 ? targetName.substring(0, extIndex) : targetName;

          const asset = await this.prisma.creativeAsset.findFirst({
            where: {
              OR: [
                { name: { equals: targetName, mode: 'insensitive' } },
                { name: { equals: targetNameWithoutExt, mode: 'insensitive' } },
                {
                  name: {
                    startsWith: targetNameWithoutExt,
                    mode: 'insensitive',
                  },
                },
              ],
              folder: {
                name: { equals: record.product_code!, mode: 'insensitive' },
                parent: {
                  name: { equals: record.brand_name!, mode: 'insensitive' },
                  parent: {
                    name: { equals: record.project_name!, mode: 'insensitive' },
                  },
                },
              },
            },
          });

          if (asset) {
            await this.prisma.larkRecord.update({
              where: { id: record.id },
              data: { creative_asset_id: asset.id },
            });
            matchedCount++;
          }
        }),
      );
    }

    this.logger.log(
      `🔍 [TEST] Upload status check done. Matched and linked: ${matchedCount} items.`,
    );
  }
}
