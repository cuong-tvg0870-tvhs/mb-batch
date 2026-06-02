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

      // 3. Xử lý song song cho các record còn sót chưa có quyền đọc hoặc chưa map Meta (chỉ quét 14 ngày gần đây)
      const fourteenDaysAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);
      this.logger.log('🔍 [STEP 3] Starting parallel check for pending/unverified records (recent 14 days)...');
      let totalChecked = 0;
      while (true) {
        const pendingRecords = await this.prisma.larkRecord.findMany({
          where: {
            production_date: { gte: fourteenDaysAgo },
            OR: [
              { creative_asset_id: null },
              { drive_id: null },
              { drive: { drive_permission: { not: true } } },
              { drive: null },
            ],
            NOT: {
              raw: {
                path: ['permission_status'],
                equals: 'FAILED',
              },
            },
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
    const results: Array<{ id: string; success: boolean; drive_permission: boolean; creative_asset_id: string | null }> = [];

    // Chạy từng cụm 10 bản ghi song song để tránh Rate Limit API của Google
    const chunks = chunk(records, 10);
    for (const recordChunk of chunks) {
      const chunkResults = await Promise.all(
        recordChunk.map(async (record) => {
          let drive_permission = false;
          let driveFileResponse: any = null;
          let permission_error: string | null = null;
          let driveId = extractDriveId(record.drive_url);

          const alreadyVerified = !!(record.drive_id && record.drive?.drive_permission === true);

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
                fields: 'id,name,mimeType,webViewLink,webContentLink,size',
                supportsAllDrives: true,
              });
              drive_permission = true;
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
          const updatedRaw = {
            ...(record.raw as any),
            permission_status: drive_permission ? 'SUCCESS' : 'FAILED',
            permission_error: permission_error,
          };

          const updateData: any = {
            raw: updatedRaw,
          };

          if (driveId) {
            updateData.drive_id = driveId;
          }

          // Tự động map nếu đã tồn tại trên Meta (theo path và tên file)
          let creative_asset_id = record.creative_asset_id;
          if (
            !creative_asset_id &&
            record.project_name &&
            record.brand_name &&
            record.product_code
          ) {
            let targetName = driveFileResponse?.data?.name;
            if (!targetName && driveId) {
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
              updateData.creative_asset_id = creative_asset_id;
            }
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

  async syncDriveFiles() {
    const now = new Date();
    let pageToken: string | undefined;

    this.logger.log('Scanning files on Google Drive...');

    // 1. Quét hàng loạt các tệp trong Shared Drive / Chia sẻ trực tiếp
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

      if (files.length > 0) {
        const fileIds = files.map((f) => f.id).filter(Boolean) as string[];

        // 1. Lấy thông tin các file đã tồn tại trong DB để so sánh
        const existingDriveFiles = await this.prisma.driveFile.findMany({
          where: { id: { in: fileIds } },
          select: {
            id: true,
            name: true,
            mimeType: true,
            webViewLink: true,
            size: true,
            drive_permission: true,
          },
        });

        const existingMap = new Map(existingDriveFiles.map((f) => [f.id, f]));
        const unchangedIds: string[] = [];
        const filesToUpsert: any[] = [];

        for (const file of files) {
          const id = file.id;
          if (!id) continue;

          const existing = existingMap.get(id);
          if (existing) {
            // Kiểm tra xem có thay đổi gì quan trọng không
            const hasChanged =
              existing.name !== (file.name || 'Untitled') ||
              existing.mimeType !== (file.mimeType || null) ||
              existing.webViewLink !== (file.webViewLink || null) ||
              existing.size !== (file.size || null) ||
              existing.drive_permission !== true;

            if (hasChanged) {
              filesToUpsert.push(file);
            } else {
              unchangedIds.push(id);
            }
          } else {
            filesToUpsert.push(file);
          }
        }

        // 2. Cập nhật last_seen_at hàng loạt cho các file không đổi (chỉ mất 1 query!)
        if (unchangedIds.length > 0) {
          await this.prisma.driveFile.updateMany({
            where: { id: { in: unchangedIds } },
            data: { last_seen_at: now },
          });
        }

        // 3. Chỉ upsert các file mới hoặc có thay đổi (chạy theo chunk 50)
        if (filesToUpsert.length > 0) {
          const batches = chunk(filesToUpsert, 50);
          for (const batch of batches) {
            await Promise.all(
              batch.map((file) => {
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
              }),
            );
          }
        }
      }

      pageToken = res.data.nextPageToken || undefined;
    } while (pageToken);

    // 2. Lấy danh sách tất cả drive_id từ LarkRecord để kiểm tra ngoại lệ (file Public)
    const records = await this.prisma.larkRecord.findMany({
      where: {
        drive_url: { not: null },
      },
      select: {
        id: true,
        drive_url: true,
      },
    });

    const mapping = records.map((r) => ({
      id: r.id,
      driveId: extractDriveId(r.drive_url),
    })).filter((m) => m.driveId);

    const driveIdsToCheck = Array.from(new Set(mapping.map((m) => m.driveId) as string[]));

    // Lọc ra các driveId đã được quét thành công ở bước 1
    const scannedDriveFiles = await this.prisma.driveFile.findMany({
      where: {
        id: { in: driveIdsToCheck },
        last_seen_at: now,
      },
      select: { id: true },
    });

    const scannedIdsSet = new Set(scannedDriveFiles.map((d) => d.id));
    const exceptionalIds = driveIdsToCheck.filter((id) => !scannedIdsSet.has(id));

    // Kiểm tra trực tiếp các tệp ngoại lệ (hỗ trợ file Public)
    // Để tối ưu tốc độ và tránh Rate Limit của Google Drive API:
    // Bỏ qua và giữ nguyên drive_permission = true đối với các tệp đã được xác thực thành công trong vòng 6 giờ qua.
    const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);
    const recentlyVerifiedFiles = await this.prisma.driveFile.findMany({
      where: {
        id: { in: exceptionalIds },
        drive_permission: true,
        last_seen_at: { gte: sixHoursAgo },
      },
      select: { id: true },
    });

    const recentlyVerifiedIds = new Set(recentlyVerifiedFiles.map((d) => d.id));

    // Gia hạn trực tiếp trong DB cho các tệp đã xác thực gần đây (Mất 0ms, 0 API calls)
    if (recentlyVerifiedFiles.length > 0) {
      await this.prisma.driveFile.updateMany({
        where: {
          id: { in: Array.from(recentlyVerifiedIds) },
        },
        data: {
          last_seen_at: now,
        },
      });
    }

    const finalIdsToQuery = exceptionalIds.filter((id) => !recentlyVerifiedIds.has(id));

    if (finalIdsToQuery.length > 0) {
      this.logger.log(`Checking ${finalIdsToQuery.length} exceptional/public drive files directly...`);
      const chunks = chunk(finalIdsToQuery, 20);
      for (const exceptionalChunk of chunks) {
        await Promise.all(
          exceptionalChunk.map(async (driveId) => {
            try {
              const res = await this.driveSA.files.get({
                fileId: driveId,
                fields: 'id,name,mimeType,parents,webViewLink,webContentLink,size',
                supportsAllDrives: true,
              });
              const file = res.data;
              await this.prisma.driveFile.upsert({
                where: { id: driveId },
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
                  id: driveId,
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
            } catch (err) {
              // Ghi nhận tệp thực sự không có quyền hoặc bị xóa
              await this.prisma.driveFile.upsert({
                where: { id: driveId },
                update: {
                  drive_permission: false,
                  last_seen_at: now,
                },
                create: {
                  id: driveId,
                  name: 'Unknown File',
                  drive_permission: false,
                  last_seen_at: now,
                  raw: '{}',
                },
              });
            }
          })
        );
      }
    }

    // 3. Đánh dấu các tệp không còn thấy (và không phải file ngoại lệ thành công) là false
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

    // 1. Thực hiện CreateMany trực tiếp cho LarkRecord
    if (toCreate.length > 0) {
      await this.prisma.larkRecord.createMany({
        data: toCreate as any[],
        skipDuplicates: true,
      });
    }

    // 2. Thực hiện CreateMany trực tiếp cho DriveFile
    if (uniqueDriveIds.length > 0) {
      const driveData = uniqueDriveIds.map((id) => ({
        id,
        name: 'Pending Sync...',
        raw: '{}',
        drive_permission: false,
      }));

      await this.prisma.driveFile.createMany({
        data: driveData as any[],
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

    this.logger.log(`Found ${records.length} Lark records without creative_asset_id to check.`);
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
          const targetNameWithoutExt = extIndex > 0 ? targetName.substring(0, extIndex) : targetName;

          const asset = await this.prisma.creativeAsset.findFirst({
            where: {
              OR: [
                { name: { equals: targetName, mode: 'insensitive' } },
                { name: { equals: targetNameWithoutExt, mode: 'insensitive' } },
                { name: { startsWith: targetNameWithoutExt, mode: 'insensitive' } },
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

    this.logger.log(`🔍 [TEST] Upload status check done. Matched and linked: ${matchedCount} items.`);
  }
}
