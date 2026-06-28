import { Injectable, Logger } from '@nestjs/common';
import { LarkRecord, Prisma } from '@prisma/client';
import axios, { AxiosRequestConfig } from 'axios';
import { drive_v3, google } from 'googleapis';
import { chunk } from '../../common/utils';
import { fileMatchesRecordCid } from '../../common/utils/cid.util';
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

const parseEnvInteger = (
  value: string | undefined,
  defaultValue: number,
  minValue: number,
) => {
  const parsed = Number(value ?? defaultValue);
  return Number.isFinite(parsed) ? Math.max(minValue, parsed) : defaultValue;
};

@Injectable()
export class LarkSyncService {
  private readonly logger = new Logger(LarkSyncService.name);
  private readonly runtimeRawKeys = [
    'permission_status',
    'permission_error',
    'permission_access_verified',
    'retry_count',
    'last_checked_at',
    'sync_status',
    'sync_error',
    'sync_error_code',
    'sync_error_status',
    'sync_error_reason',
    'sync_error_detail',
    'sync_error_at',
    'sync_uploaded_count',
    'sync_skipped_count',
    'sync_unsupported_count',
    'sync_creative_asset_ids',
    'sync_creative_asset_count',
    'sync_limit_reached',
    'sync_meta_cooldown',
    'sync_meta_cooldown_until',
    'sync_upload_batch_id',
    'sync_claimed_at',
    'sync_stale_released_at',
    'last_meta_upload_checked_at',
  ];
  private driveSA: drive_v3.Drive;
  private readonly driveFolderMimeType = 'application/vnd.google-apps.folder';
  private readonly baseURL = 'https://open.larksuite.com/open-apis/bitable/v1';
  private readonly allowedSharedDriveIds = parseAllowedSharedDriveIds(
    process.env.GOOGLE_ALLOWED_SHARED_DRIVE_IDS,
  );
  private readonly allowedDriveFolderIds = parseAllowedSharedDriveIds(
    process.env.GOOGLE_ALLOWED_DRIVE_FOLDER_IDS ||
      process.env.GOOGLE_ALLOWED_DRIVE_PARENT_FOLDER_IDS,
  );
  private readonly maxParentTraversalDepth = parseEnvInteger(
    process.env.GOOGLE_DRIVE_PARENT_CHECK_MAX_DEPTH,
    20,
    1,
  );
  private readonly verifiedParentAccessCache = new Set<string>();
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

  private normalizeRaw(raw: unknown): Record<string, any> {
    return raw && typeof raw === 'object' && !Array.isArray(raw)
      ? (raw as Record<string, any>)
      : {};
  }

  private addFolderIdsFromConfigValue(target: Set<string>, value: unknown) {
    if (!value) return;

    if (typeof value === 'string') {
      parseAllowedSharedDriveIds(value).forEach((id) => target.add(id));
      return;
    }

    if (Array.isArray(value)) {
      value
        .map((item) => String(item).trim())
        .filter(Boolean)
        .forEach((id) => target.add(id));
      return;
    }

    if (typeof value === 'object') {
      const config = value as Record<string, unknown>;
      this.addFolderIdsFromConfigValue(target, config.ids);
      this.addFolderIdsFromConfigValue(target, config.folderIds);
      this.addFolderIdsFromConfigValue(target, config.driveFolderIds);
    }
  }

  private async getAllowedDriveFolderIds() {
    const folderIds = new Set(this.allowedDriveFolderIds);
    const configs = await this.prisma.systemConfig.findMany({
      where: {
        key: {
          in: [
            'GOOGLE_ALLOWED_DRIVE_FOLDER_IDS',
            'GOOGLE_ALLOWED_DRIVE_PARENT_FOLDER_IDS',
          ],
        },
      },
      select: { value: true },
    });

    configs.forEach((config) =>
      this.addFolderIdsFromConfigValue(folderIds, config.value),
    );
    return folderIds;
  }

  private mergeRuntimeRawFields(previousRaw: unknown, nextRaw: unknown) {
    const merged = { ...this.normalizeRaw(nextRaw) };
    const previous = this.normalizeRaw(previousRaw);

    for (const key of this.runtimeRawKeys) {
      if (previous[key] !== undefined) {
        merged[key] = previous[key];
      }
    }

    return merged;
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
              raw: {
                path: ['permission_status'],
                equals: Prisma.DbNull,
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
            {
              raw: {
                path: ['permission_access_verified'],
                equals: Prisma.DbNull,
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
          let permissions: any[] = [];
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
            this.logger.log(
              `[DriveCheck] LarkRecord ${record.id} already verified. Using cached metadata.`,
            );
          } else if (driveId) {
            try {
              this.logger.log(
                `[DriveCheck] Calling driveSA.files.get for fileId: ${driveId} (LarkRecord: ${record.id})`,
              );
              driveFileResponse = await this.driveSA.files.get({
                fileId: driveId,
                fields:
                  'id,name,mimeType,parents,webViewLink,webContentLink,size,ownedByMe,sharedWithMeTime,driveId,capabilities/canDownload',
                supportsAllDrives: true,
              });
              this.logger.log(
                `[DriveCheck] driveSA.files.get SUCCESS for fileId: ${driveId}. Data: ${JSON.stringify(
                  driveFileResponse.data,
                )}`,
              );

              drive_permission = hasExplicitDriveAccess(
                driveFileResponse.data,
                this.allowedSharedDriveIds,
                this.serviceAccountEmail,
              );
              if (!drive_permission) {
                this.logger.log(
                  `[DriveCheck] File ${driveId} has no explicit access from metadata. Fetching file permissions...`,
                );
                permissions = await this.getFilePermissions(driveId);
                this.logger.log(
                  `[DriveCheck] File ${driveId} permissions list: ${JSON.stringify(permissions)}`,
                );

                drive_permission = hasExplicitDriveAccess(
                  driveFileResponse.data,
                  this.allowedSharedDriveIds,
                  this.serviceAccountEmail,
                  permissions,
                );
              }
              if (!drive_permission) {
                this.logger.log(
                  `[DriveCheck] File ${driveId} has no explicit file sharing. Checking inherited parent folder/drive access...`,
                );
                drive_permission = await this.checkInheritedAccess(
                  driveFileResponse.data,
                );
              }
              if (!drive_permission) {
                permission_error = PUBLIC_ONLY_PERMISSION_ERROR;
                this.logger.warn(
                  `[DriveCheck] Drive permission check FAILED for fileId: ${driveId}. Error: ${permission_error}`,
                );
              } else {
                this.logger.log(
                  `[DriveCheck] Drive permission check SUCCESS for fileId: ${driveId}`,
                );
              }
            } catch (e: any) {
              drive_permission = false;
              permission_error = e.message || String(e);
              this.logger.error(
                `[DriveCheck] Drive API call failed for fileId: ${driveId}. Error: ${permission_error}`,
                e.stack || e,
              );
            }
          } else {
            permission_error = 'Không tìm thấy Google Drive ID từ đường dẫn';
            this.logger.warn(
              `[DriveCheck] LarkRecord ${record.id} check failed: ${permission_error}`,
            );
          }

          if (driveId && !alreadyVerified) {
            const file = driveFileResponse?.data;
            await this.prisma.driveFile.upsert({
              where: { id: driveId },
              update: {
                name: file?.name || undefined,
                drive_permission,
                parentId: file?.parents?.[0] || undefined,
                mimeType: file?.mimeType || undefined,
                webViewLink: file?.webViewLink || undefined,
                webContentLink: file?.webContentLink || undefined,
                size: file?.size || undefined,
                raw: file || undefined,
                last_seen_at: now,
              },
              create: {
                id: driveId,
                name: file?.name || record.drive_url || 'Unknown File',
                drive_permission,
                parentId: file?.parents?.[0] || null,
                mimeType: file?.mimeType || null,
                webViewLink: file?.webViewLink || null,
                webContentLink: file?.webContentLink || null,
                size: file?.size || null,
                raw: file || {},
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

          await this.prisma.larkRecord.update({
            where: { id: record.id },
            data: updateData,
          });

          return {
            id: record.id,
            success: drive_permission,
            drive_permission,
            creative_asset_id: record.creative_asset_id,
          };
        }),
      );

      results.push(...chunkResults);
    }

    return results;
  }

  private async getFilePermissions(driveId: string): Promise<any[]> {
    try {
      this.logger.log(
        `[DriveCheck] Calling driveSA.permissions.list for fileId: ${driveId}`,
      );
      const res = await this.driveSA.permissions.list({
        fileId: driveId,
        fields:
          'permissions(id,type,role,emailAddress,deleted,permissionDetails(permissionType,role,inherited,inheritedFrom))',
        supportsAllDrives: true,
      });

      const list = res.data.permissions || [];
      this.logger.log(
        `[DriveCheck] driveSA.permissions.list SUCCESS for fileId: ${driveId}. Count: ${list.length}`,
      );
      return list;
    } catch (e: any) {
      this.logger.error(
        `[DriveCheck] driveSA.permissions.list FAILED for fileId: ${driveId}. Error: ${e.message || String(e)}`,
        e.stack || e,
      );
      return [];
    }
  }

  private async checkParentAccessRecursive(
    parentId: string,
    depth = 0,
    visited = new Set<string>(),
  ): Promise<boolean> {
    if (this.verifiedParentAccessCache.has(parentId)) return true;

    const allowedDriveFolderIds = await this.getAllowedDriveFolderIds();
    if (allowedDriveFolderIds.has(parentId)) {
      this.logger.log(
        `[DriveCheck] Parent folder access verified via GOOGLE_ALLOWED_DRIVE_FOLDER_IDS at depth ${depth}: ${parentId}`,
      );
      this.verifiedParentAccessCache.add(parentId);
      return true;
    }

    const dbParent = await this.prisma.driveFile.findUnique({
      where: { id: parentId },
      select: { drive_permission: true },
    });
    if (dbParent?.drive_permission === true) {
      this.logger.log(
        `[DriveCheck] Parent folder access verified from DriveFile cache at depth ${depth}: ${parentId}`,
      );
      this.verifiedParentAccessCache.add(parentId);
      return true;
    }

    if (visited.has(parentId)) {
      this.logger.warn(
        `[DriveCheck] Skipping circular parent traversal for folder: ${parentId}`,
      );
      return false;
    }
    visited.add(parentId);

    if (depth >= this.maxParentTraversalDepth) {
      this.logger.warn(
        `[DriveCheck] Reached maximum parent traversal depth of ${this.maxParentTraversalDepth} for parent folder: ${parentId}`,
      );
      return false;
    }

    try {
      this.logger.log(
        `[DriveCheck] Traversing parent folder (depth ${depth}): ${parentId}`,
      );
      const parentMetadata = await this.driveSA.files.get({
        fileId: parentId,
        fields: 'id,name,ownedByMe,sharedWithMeTime,parents,driveId',
        supportsAllDrives: true,
      });

      if (
        hasExplicitDriveAccess(
          parentMetadata.data,
          this.allowedSharedDriveIds,
          this.serviceAccountEmail,
        )
      ) {
        this.logger.log(
          `[DriveCheck] Parent folder access verified at depth ${depth} from metadata: ${parentId}`,
        );
        this.verifiedParentAccessCache.add(parentId);
        return true;
      }

      const parentPermissions = await this.getFilePermissions(parentId);
      if (
        hasExplicitDriveAccess(
          parentMetadata.data,
          this.allowedSharedDriveIds,
          this.serviceAccountEmail,
          parentPermissions,
        )
      ) {
        this.logger.log(
          `[DriveCheck] Parent folder access verified at depth ${depth} via explicit permissions: ${parentId}`,
        );
        this.verifiedParentAccessCache.add(parentId);
        return true;
      }

      const nextParents = parentMetadata.data.parents || [];
      if (nextParents.length > 0) {
        for (const nextParentId of nextParents) {
          const verified = await this.checkParentAccessRecursive(
            nextParentId,
            depth + 1,
            visited,
          );
          if (verified) return true;
        }
      }
    } catch (err: any) {
      this.logger.log(
        `[DriveCheck] Parent folder ${parentId} at depth ${depth} is not accessible: ${err.message || err}`,
      );
    }
    return false;
  }

  private async checkInheritedAccess(file: any): Promise<boolean> {
    // 1. If in an explicitly allowed Shared Drive, access is valid for that scope.
    if (file.driveId && this.allowedSharedDriveIds.has(file.driveId)) {
      this.logger.log(
        `[DriveCheck] Shared Drive access verified by GOOGLE_ALLOWED_SHARED_DRIVE_IDS for driveId: ${file.driveId}`,
      );
      return true;
    }

    // 2. If parent folders are present, check if the service account has access to the parent folder recursively
    if (file.parents && file.parents.length > 0) {
      try {
        for (const parentId of file.parents) {
          const verified = await this.checkParentAccessRecursive(parentId, 0);
          if (verified) return true;
        }
      } catch (e: any) {
        this.logger.warn(
          `[DriveCheck] Recursive parent verification failed: ${e.message || e}`,
        );
      }
    }
    return false;
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
          raw: {},
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
        AND: [
          {
            raw: {
              path: ['permission_access_verified'],
              equals: true,
            },
          },
          {
            NOT: {
              raw: {
                path: ['sync_status'],
                equals: 'SUCCESS',
              },
            },
          },
        ],
      },
      include: { drive: true },
    });

    let mappedCount = 0;
    for (const batch of chunk(records, 50)) {
      await Promise.all(
        batch.map(async (record) => {
          const assetLink = await this.findCreativeAssetLink(
            record,
            record.drive,
            record.drive_id,
          );
          if (assetLink.assetIds.length === 0) return;

          const rawPatch: Record<string, any> = {
            sync_creative_asset_ids: assetLink.assetIds,
            sync_creative_asset_count: assetLink.assetIds.length,
            sync_status: 'SUCCESS',
            sync_error: null,
            last_meta_upload_checked_at: new Date().toISOString(),
          };

          await this.prisma.larkRecord.update({
            where: { id: record.id },
            data: {
              creative_asset_id: assetLink.primaryAssetId,
              raw: {
                ...this.normalizeRaw(record.raw),
                ...rawPatch,
              },
            },
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

  private async findCreativeAssetLink(
    record: Pick<
      LarkRecord,
      | 'project_name'
      | 'brand_name'
      | 'product_code'
      | 'creative_asset_id'
      | 'drive_url'
      | 'cid'
    >,
    driveFile: any,
    driveId?: string | null,
  ): Promise<{ primaryAssetId: string | null; assetIds: string[] }> {
    if (
      record.creative_asset_id ||
      !record.project_name ||
      !record.brand_name ||
      !record.product_code
    ) {
      return {
        primaryAssetId: record.creative_asset_id || null,
        assetIds: record.creative_asset_id ? [record.creative_asset_id] : [],
      };
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

    if (driveFile?.mimeType === this.driveFolderMimeType) {
      const folderAssetIds = await this.findFolderCreativeAssetIds(
        record,
        targetName,
        record.cid,
      );

      return {
        primaryAssetId: null,
        assetIds: folderAssetIds,
      };
    }

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

    // STRICT CID RULE: chỉ link khi tên asset chứa ĐÚNG CID của record — mirror
    // luồng upload (meta-media-upload). Tránh việc khớp theo tên gắn nhầm creative
    // của CID khác vào content này. Không có CID thì giữ hành vi khớp tên cũ.
    if (record.cid && asset && !fileMatchesRecordCid(asset.name, record.cid)) {
      this.logger.warn(
        `CID mismatch: record ${record.cid} khớp tên với asset ${asset.id} ("${asset.name}") nhưng CID không trùng → bỏ qua link.`,
      );
      return { primaryAssetId: null, assetIds: [] };
    }

    return {
      primaryAssetId: asset?.id || null,
      assetIds: asset?.id ? [asset.id] : [],
    };
  }

  private async findFolderCreativeAssetIds(
    record: Pick<LarkRecord, 'project_name' | 'brand_name' | 'product_code'>,
    driveFolderName: string,
    recordCid?: string | null,
  ) {
    const productFolder = await this.prisma.creativeFolder.findFirst({
      where: {
        name: record.product_code!,
        parent: {
          name: record.brand_name!,
          parent: {
            name: record.project_name!,
          },
        },
      },
      select: { id: true, name: true },
    });

    if (!productFolder) return [];

    let targetFolderId = productFolder.id;
    if (
      this.cleanName(driveFolderName) !== this.cleanName(productFolder.name)
    ) {
      const driveNamedFolder = await this.prisma.creativeFolder.findFirst({
        where: {
          parentId: productFolder.id,
          name: driveFolderName,
        },
        select: { id: true },
      });
      if (driveNamedFolder) targetFolderId = driveNamedFolder.id;
    }

    const folderIds = await this.collectCreativeFolderIds(targetFolderId);
    const assets = await this.prisma.creativeAsset.findMany({
      where: { folderId: { in: folderIds } },
      select: { id: true, name: true },
      orderBy: { createdAtLocal: 'asc' },
    });

    // STRICT CID RULE: chỉ giữ asset có tên chứa đúng CID của record. Một folder
    // sản phẩm có thể chứa nhiều content (nhiều CID) nên không lọc sẽ gắn nhầm.
    // Không có CID thì giữ toàn bộ (không lọc được).
    const matched = recordCid
      ? assets.filter((a) => fileMatchesRecordCid(a.name, recordCid))
      : assets;
    if (recordCid && matched.length !== assets.length) {
      this.logger.warn(
        `CID filter (folder): record ${recordCid} — giữ ${matched.length}/${assets.length} asset khớp CID.`,
      );
    }

    return matched.map((asset) => asset.id);
  }

  private async collectCreativeFolderIds(rootFolderId: string) {
    const folderIds = [rootFolderId];
    let cursor = [rootFolderId];

    while (cursor.length > 0) {
      const children = await this.prisma.creativeFolder.findMany({
        where: { parentId: { in: cursor } },
        select: { id: true },
      });
      cursor = children.map((child) => child.id);
      folderIds.push(...cursor);
    }

    return folderIds;
  }

  private cleanName(name: string) {
    return name
      .toLowerCase()
      .replace(/\.[^/.]+$/, '')
      .trim();
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
      select: {
        id: true,
        raw: true,
        drive_url: true,
        drive_id: true,
        creative_asset_id: true,
      },
    });
    const existingById = new Map(existing.map((e) => [e.id, e]));
    const existingIds = new Set(existingById.keys());

    const toCreate = validRecords.filter((r) => !existingIds.has(String(r.id)));
    const toUpdate = validRecords.filter((r) => existingIds.has(String(r.id)));

    // 1. Tạo DriveFile placeholders trước để thỏa mãn FK LarkRecord.drive_id.
    if (uniqueDriveIds.length > 0) {
      const driveData = uniqueDriveIds.map((id) => ({
        id,
        name: 'Pending Permission Check',
        raw: {},
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
          batch.map((r) => {
            const existingRecord = existingById.get(String(r.id));
            const nextDriveId = r.drive_id || extractDriveId(r.drive_url);
            const previousDriveId =
              existingRecord?.drive_id ||
              extractDriveId(existingRecord?.drive_url);
            const isSameDrive =
              !!existingRecord &&
              (previousDriveId && nextDriveId
                ? previousDriveId === nextDriveId
                : existingRecord.drive_url === r.drive_url);

            const data: any = {
              ...r,
              drive_id: nextDriveId,
              raw: isSameDrive
                ? this.mergeRuntimeRawFields(existingRecord.raw, r.raw)
                : r.raw,
            };

            if (isSameDrive) {
              data.creative_asset_id = existingRecord.creative_asset_id;
            }

            return this.prisma.larkRecord.update({
              where: { id: String(r.id) },
              data,
            });
          }),
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
        cid: true,
        drive_url: true,
        raw: true,
        project_name: true,
        brand_name: true,
        product_code: true,
        drive_id: true,
        creative_asset_id: true,
        drive: true,
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
          const driveId = record.drive_id || extractDriveId(record.drive_url);
          const assetLink = await this.findCreativeAssetLink(
            record,
            record.drive,
            driveId,
          );

          if (assetLink.assetIds.length > 0) {
            await this.prisma.larkRecord.update({
              where: { id: record.id },
              data: {
                creative_asset_id: assetLink.primaryAssetId,
                raw: {
                  ...this.normalizeRaw(record.raw),
                  sync_status: 'SUCCESS',
                  sync_error: null,
                  sync_creative_asset_ids: assetLink.assetIds,
                  sync_creative_asset_count: assetLink.assetIds.length,
                  last_meta_upload_checked_at: new Date().toISOString(),
                },
              },
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
