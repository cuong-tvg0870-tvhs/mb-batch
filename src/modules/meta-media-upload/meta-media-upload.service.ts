import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { AssetType, Prisma } from '@prisma/client';
import { randomUUID } from 'crypto';
import * as fs from 'fs';
import { drive_v3, google } from 'googleapis';
import * as path from 'path';
import { parseMetaUrlExpireTime } from '../../common/utils';
import { MetaApiService } from '../meta-api/meta-api.service';
import { PrismaService } from '../prisma/prisma.service';

type DriveFileEntry = {
  id?: string | null;
  name?: string | null;
  mimeType?: string | null;
  parents?: string[] | null;
  webContentLink?: string | null;
  webViewLink?: string | null;
  size?: string | null;
};

type UploadBudget = {
  uploaded: number;
  limit: number;
};

type FolderUploadStats = {
  uploadedCount: number;
  skippedCount: number;
  unsupportedCount: number;
  limitReached: boolean;
};

@Injectable()
export class MetaMediaUploadService {
  private readonly logger = new Logger(MetaMediaUploadService.name);
  private readonly maxFilesPerRun = 20;
  private readonly uploadingClaimTtlMs = 6 * 60 * 60 * 1000;
  private readonly driveFolderMimeType = 'application/vnd.google-apps.folder';
  private readonly rootFolderId =
    process.env.META_CREATIVE_ROOT_FOLDER_ID || '4303729193176038';
  private readonly driveSA: drive_v3.Drive;

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaApi: MetaApiService,
  ) {
    const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON!);
    credentials.private_key = credentials.private_key.replace(/\\n/g, '\n');

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/drive'],
    });

    this.driveSA = google.drive({ version: 'v3', auth });
  }

  async autoUpload() {
    await this.releaseStaleUploadingRecords();

    const budget: UploadBudget = {
      uploaded: 0,
      limit: this.maxFilesPerRun,
    };

    const records = await this.prisma.larkRecord.findMany({
      where: this.buildEligibleRecordWhere(),
      include: { drive: true },
      orderBy: [{ production_date: 'desc' }, { id: 'asc' }],
      take: this.maxFilesPerRun * 5,
    });
    const uploadCandidates = records
      .filter(
        (record) => this.normalizeRaw(record.raw).sync_status !== 'UPLOADING',
      )
      .slice(0, this.maxFilesPerRun);

    this.logger.log(
      `Found ${uploadCandidates.length} eligible Lark records for Meta auto-upload`,
    );

    const uploadBatchId = randomUUID();
    const claimedRecords = await this.claimRecordsForUpload(
      uploadCandidates,
      uploadBatchId,
    );

    this.logger.log(
      `Claimed ${claimedRecords.length}/${uploadCandidates.length} Lark records as UPLOADING for Meta auto-upload batch ${uploadBatchId}`,
    );

    for (const record of claimedRecords) {
      if (budget.uploaded >= budget.limit) {
        await this.markRecordStatus(record, 'PENDING', null, {
          sync_limit_reached: true,
          sync_upload_batch_id: uploadBatchId,
        });
        continue;
      }

      try {
        const result = await this.uploadRecord(record, budget);
        await this.markRecordStatus(record, result.status, null, result.raw);
      } catch (err: any) {
        this.logger.error(
          `Meta auto-upload failed for LarkRecord ${record.id}: ${err.message}`,
          err.stack,
        );
        await this.markRecordStatus(
          record,
          'FAILED',
          err.message || String(err),
        );
      }
    }

    this.logger.log(
      `Meta auto-upload completed. Uploaded files this run: ${budget.uploaded}/${budget.limit}`,
    );

    return { uploaded: budget.uploaded, scanned: uploadCandidates.length };
  }

  private async uploadRecord(record: any, budget: UploadBudget) {
    if (!record.drive_url) {
      throw new Error('Lark record missing drive_url');
    }
    if (!record.drive?.drive_permission) {
      throw new Error('Drive permission is not verified');
    }
    if (!record.project_name || !record.brand_name || !record.product_code) {
      throw new Error(
        'Lark record missing project_name, brand_name, or product_code',
      );
    }

    const productFolder = await this.ensureFolderForRecord(
      record.project_name,
      record.brand_name,
      record.product_code,
      record.product_name || record.product_code,
    );

    const driveFile: DriveFileEntry = {
      id: record.drive_id || record.drive.id,
      name: record.drive.name,
      mimeType: record.drive.mimeType,
      webContentLink: record.drive.webContentLink,
      webViewLink: record.drive.webViewLink || record.drive_url,
      size: record.drive.size,
    };

    if (driveFile.mimeType === this.driveFolderMimeType) {
      if (!driveFile.id) throw new Error('Missing Drive folder ID');
      const stats = await this.uploadDriveFolder(
        driveFile.id,
        productFolder.id,
        budget,
      );

      return {
        status: stats.limitReached ? 'PENDING' : 'SUCCESS',
        raw: {
          sync_uploaded_count: stats.uploadedCount,
          sync_skipped_count: stats.skippedCount,
          sync_unsupported_count: stats.unsupportedCount,
          sync_limit_reached: stats.limitReached,
        },
      };
    }

    const asset = await this.uploadDriveFile(
      driveFile,
      productFolder.id,
      budget,
    );
    if (!asset.asset) {
      throw new Error(
        `Loại file Drive chưa được hỗ trợ để upload Meta: ${
          driveFile.mimeType || driveFile.name || 'unknown'
        }`,
      );
    }

    await this.prisma.larkRecord.update({
      where: { id: record.id },
      data: { creative_asset_id: asset.asset.id },
    });

    return {
      status: 'SUCCESS',
      raw: { sync_uploaded_count: asset.uploaded ? 1 : 0 },
    };
  }

  private async uploadDriveFolder(
    driveFolderId: string,
    metaFolderId: string,
    budget: UploadBudget,
  ): Promise<FolderUploadStats> {
    const stats: FolderUploadStats = {
      uploadedCount: 0,
      skippedCount: 0,
      unsupportedCount: 0,
      limitReached: false,
    };

    const walk = async (folderId: string, targetFolderId: string) => {
      if (budget.uploaded >= budget.limit) {
        stats.limitReached = true;
        return;
      }

      const children = await this.listFolderChildren(folderId);

      for (const child of children) {
        if (!child.id) continue;

        if (child.mimeType === this.driveFolderMimeType) {
          await this.upsertDriveFile(child);
          const subFolder = await this.ensureCreativeFolder(
            child.name || child.id,
            targetFolderId,
          );
          await walk(child.id, subFolder.id);
        } else {
          const result = await this.uploadDriveFile(
            child,
            targetFolderId,
            budget,
          );

          if (result.uploaded) stats.uploadedCount += 1;
          else if (result.reason === 'UNSUPPORTED_TYPE')
            stats.unsupportedCount += 1;
          else stats.skippedCount += 1;
        }

        if (budget.uploaded >= budget.limit) {
          stats.limitReached = true;
          return;
        }
      }
    };

    await walk(driveFolderId, metaFolderId);
    return stats;
  }

  private async uploadDriveFile(
    file: DriveFileEntry,
    folderId: string,
    budget: UploadBudget,
  ) {
    if (!file.id) throw new Error(`Drive file missing id: ${file.name}`);

    await this.upsertDriveFile(file);

    const type = this.getAssetType(file.mimeType, file.name);
    if (!type) {
      return { uploaded: false, reason: 'UNSUPPORTED_TYPE' as const };
    }

    const existingAsset = await this.findExistingAssetForDriveFile(
      file,
      folderId,
      type,
    );

    if (existingAsset) {
      if (!existingAsset.drive_id) {
        await this.linkAssetToDriveFile(existingAsset.id, file);
      }
      return { uploaded: false, asset: existingAsset };
    }

    const metaAssetId = await this.findMetaAssetIdByName(
      file.name || file.id,
      folderId,
    );
    if (metaAssetId) {
      const syncedAsset = await this.pollAndSaveAsset({
        assetId: metaAssetId,
        type,
        folderId,
        driveUrl: file.webViewLink || this.buildDriveWebViewUrl(file.id),
        driveId: file.id,
      });
      return { uploaded: false, asset: syncedAsset };
    }

    if (budget.uploaded >= budget.limit) {
      return { uploaded: false, reason: 'LIMIT_REACHED' as const };
    }

    const buffer = await this.downloadDriveFile(file.id);
    const asset = await this.uploadBufferToMeta({
      buffer,
      name: file.name || file.id,
      type,
      folderId,
      driveUrl: file.webViewLink || this.buildDriveWebViewUrl(file.id),
      driveId: file.id,
    });
    budget.uploaded += 1;

    return { uploaded: true, asset };
  }

  private async ensureFolderForRecord(
    projectName: string,
    brandName: string,
    productCode: string,
    productName: string,
  ) {
    const projectFolder = await this.ensureCreativeFolder(
      projectName,
      this.rootFolderId,
      projectName,
    );
    const brandFolder = await this.ensureCreativeFolder(
      brandName,
      projectFolder.id,
      brandName,
    );

    return this.ensureCreativeFolder(productCode, brandFolder.id, productName);
  }

  private async ensureCreativeFolder(
    name: string,
    parentId: string | null,
    description = name,
  ) {
    const existingFolder = await this.prisma.creativeFolder.findFirst({
      where: { name, parentId },
    });
    if (existingFolder) return existingFolder;

    const authConfig = await this.metaApi.getMetaAuthConfig();
    const businessId =
      authConfig?.businessId || this.metaApi.businessId || this.rootFolderId;

    const params: any = {
      name,
      description,
      fields: 'id,name,description,creation_time',
    };
    if (parentId) params.parent_folder_id = parentId;
    if (authConfig?.fb_dtsg) params.fb_dtsg = authConfig.fb_dtsg;

    const created = await this.metaApi.request(
      'post',
      `${businessId}/creative_folders`,
      {},
      new URLSearchParams(params).toString(),
    );

    const folder = {
      id: created.id,
      name: created.name || name,
      description: created.description || description,
      creation_time: created.creation_time || null,
      parentId,
    };

    return this.prisma.creativeFolder.upsert({
      where: { id: folder.id },
      update: {
        name: folder.name,
        description: folder.description,
        creation_time: folder.creation_time,
        parentId,
      },
      create: folder,
    });
  }

  private async listFolderChildren(
    folderId: string,
  ): Promise<drive_v3.Schema$File[]> {
    const files: drive_v3.Schema$File[] = [];
    let pageToken: string | undefined;

    do {
      const response = await this.driveSA.files.list({
        q: `'${folderId}' in parents and trashed = false`,
        fields:
          'nextPageToken, files(id,name,mimeType,parents,webViewLink,webContentLink,size)',
        pageSize: 1000,
        pageToken,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
      });

      files.push(...(response.data.files || []));
      pageToken = response.data.nextPageToken || undefined;
    } while (pageToken);

    return files.sort((a, b) => {
      const aIsFolder = a.mimeType === this.driveFolderMimeType;
      const bIsFolder = b.mimeType === this.driveFolderMimeType;
      if (aIsFolder !== bIsFolder) return aIsFolder ? -1 : 1;
      return (a.name || '').localeCompare(b.name || '');
    });
  }

  private async downloadDriveFile(fileId: string) {
    const driveRes = await this.driveSA.files.get(
      { fileId, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer' },
    );

    return Buffer.from(driveRes.data as ArrayBuffer);
  }

  private async uploadBufferToMeta(params: {
    buffer: Buffer;
    name: string;
    type: AssetType;
    folderId: string;
    driveUrl: string;
    driveId: string;
  }) {
    const { buffer, name, type, folderId, driveUrl, driveId } = params;
    const authConfig = await this.metaApi.getMetaAuthConfig();
    if (!authConfig?.accessToken) {
      throw new Error('Chưa cấu hình Meta Access Token');
    }

    const businessId =
      authConfig?.businessId || this.metaApi.businessId || this.rootFolderId;
    const ext =
      path.extname(name) || (type === AssetType.IMAGE ? '.png' : '.mp4');
    const filename = `${Date.now()}-${Math.random()
      .toString(36)
      .substring(2, 8)}${ext}`;

    const baseDir = fs.existsSync('/app')
      ? '/app/files'
      : path.join(process.cwd(), 'files');
    if (!fs.existsSync(baseDir)) fs.mkdirSync(baseDir, { recursive: true });

    const filePath = path.join(baseDir, filename);
    fs.writeFileSync(filePath, buffer);

    let creativeAsset: any;
    try {
      if (type === AssetType.IMAGE) {
        const requestParams: any = {
          name,
          bytes: buffer.toString('base64'),
          creative_folder_id: folderId,
          locale: 'en_US',
        };
        if (authConfig?.fb_dtsg) requestParams.fb_dtsg = authConfig.fb_dtsg;

        creativeAsset = await this.metaApi.request(
          'post',
          `${businessId}/images`,
          {},
          new URLSearchParams(requestParams).toString(),
        );
      } else {
        const publicUrl = `${process.env.FRONT_END_DOMAIN || 'https://ads.3fastvn.com'}/cdn/${filename}`;
        const requestParams: any = {
          title: name,
          file_url: publicUrl,
          creative_folder_id: folderId,
          locale: 'en_US',
        };
        if (authConfig?.fb_dtsg) requestParams.fb_dtsg = authConfig.fb_dtsg;

        creativeAsset = await this.metaApi.request(
          'post',
          `${businessId}/videos`,
          {},
          new URLSearchParams(requestParams).toString(),
        );
      }

      if (creativeAsset?.error) {
        throw new Error(creativeAsset.error.message || 'Meta API Error');
      }
    } catch (err: any) {
      await this.metaApi.handleMetaError(err.response?.data || err);
      throw new Error(
        err.response?.data?.error?.message ||
          err.message ||
          'Không thể upload lên Meta',
      );
    } finally {
      if (type === AssetType.VIDEO && fs.existsSync(filePath)) {
        setTimeout(() => {
          try {
            if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
          } catch {}
        }, 10000);
      } else if (type === AssetType.IMAGE && fs.existsSync(filePath)) {
        try {
          fs.unlinkSync(filePath);
        } catch {}
      }
    }

    await this.sleep(2000);

    const assetId =
      type === AssetType.VIDEO
        ? creativeAsset?.business_video_id
        : (Object.values(creativeAsset.images || {})[0] as any)?.id;

    if (!assetId) {
      throw new NotFoundException('Không lấy được asset id từ Meta');
    }

    return this.pollAndSaveAsset({
      assetId,
      type,
      folderId,
      driveUrl,
      driveId,
    });
  }

  private async pollAndSaveAsset(params: {
    assetId: string;
    type: AssetType;
    folderId: string;
    driveUrl?: string | null;
    driveId?: string | null;
  }) {
    const { assetId, type, folderId, driveUrl = null, driveId = null } = params;
    const fields =
      type === AssetType.VIDEO
        ? [
            'id',
            'name',
            'last_updated_time',
            'parent_folder_id',
            'video{id,source,length,thumbnails}',
          ]
        : [
            'id',
            'name',
            'last_updated_time',
            'parent_folder_id',
            'url',
            'hash',
            'height',
            'width',
          ];

    let asset: any = null;
    let latestReadyAsset: any = null;
    const maxRetries = type === AssetType.VIDEO ? 15 : 5;
    let errorCount = 0;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const response = await this.metaApi.request('get', assetId, {
          fields: fields.join(','),
        });

        if (response?.id) {
          if (type === AssetType.VIDEO && response.video?.source) {
            latestReadyAsset = response;
            const hasThumbnails =
              Array.isArray(response.video?.thumbnails?.data) &&
              response.video.thumbnails.data.length > 0;
            if (hasThumbnails || attempt === maxRetries - 1) {
              asset = response;
              break;
            }
          }
          if (type === AssetType.IMAGE && response.url) {
            asset = response;
            break;
          }
        }
      } catch (err: any) {
        errorCount++;
        this.logger.warn(
          `Asset details polling try ${attempt + 1} failed: ${err.message}`,
        );
        if (errorCount >= 3) break;
      }

      await this.sleep(type === AssetType.VIDEO ? 5000 : 4000);
    }

    if (!asset) {
      asset = latestReadyAsset;
    }

    if (!asset) {
      throw new NotFoundException(
        'Không lấy được thông tin chi tiết tài nguyên từ Meta sau khi upload',
      );
    }

    let videoData: any = null;
    let preferredThumbnail: any = null;

    if (type === AssetType.VIDEO) {
      videoData = asset.video;
      preferredThumbnail =
        videoData?.thumbnails?.data?.find((item: any) => item?.is_preferred) ||
        videoData?.thumbnails?.data?.[0] ||
        null;
    }

    const dbData = {
      id: asset.id,
      name: asset.name,
      type,
      width:
        type === AssetType.IMAGE
          ? asset.width
          : preferredThumbnail?.width || videoData?.width || null,
      height:
        type === AssetType.IMAGE
          ? asset.height
          : preferredThumbnail?.height || videoData?.height || null,
      thumbnail:
        type === AssetType.IMAGE ? asset.url : preferredThumbnail?.uri || null,
      imageUrl: type === AssetType.IMAGE ? asset.url : null,
      imageHash: type === AssetType.IMAGE ? asset.hash : null,
      video_id: type === AssetType.VIDEO ? videoData?.id : null,
      video_source: type === AssetType.VIDEO ? videoData?.source : null,
      video_thumbnails: type === AssetType.VIDEO ? videoData?.thumbnails : null,
      duration: type === AssetType.VIDEO ? videoData?.length : null,
      creation_time: asset.last_updated_time || new Date().toISOString(),
      folderId: asset.parent_folder_id || folderId,
      drive_url: driveUrl,
      drive_id: driveId,
      urlExpiredAt: parseMetaUrlExpireTime(
        type === AssetType.IMAGE
          ? asset.url
          : [
              videoData?.source,
              ...(videoData?.thumbnails?.data?.map((t: any) => t.uri) || []),
            ],
      ),
    };

    return this.prisma.creativeAsset.upsert({
      where: { id: asset.id },
      create: dbData,
      update: dbData,
    });
  }

  private async findExistingAssetForDriveFile(
    file: DriveFileEntry,
    folderId: string,
    type: AssetType,
  ) {
    const driveUrl = file.id
      ? file.webViewLink || this.buildDriveWebViewUrl(file.id)
      : file.webViewLink || null;

    return this.prisma.creativeAsset.findFirst({
      where: {
        OR: [
          file.id ? { drive_id: file.id } : undefined,
          driveUrl ? { drive_url: driveUrl } : undefined,
          file.name ? { folderId, type, name: file.name } : undefined,
        ].filter(Boolean) as any[],
      },
    });
  }

  private async findMetaAssetIdByName(name: string, folderId: string) {
    const authConfig = await this.metaApi.getMetaAuthConfig();
    const businessId =
      authConfig?.businessId || this.metaApi.businessId || this.rootFolderId;

    try {
      const filtering = [
        {
          field: 'name_or_content_filter',
          operator: 'CONTAIN',
          value: name,
        },
        {
          field: 'is_valid',
          operator: 'EQUAL',
          value: true,
        },
      ];

      const response = await this.metaApi.request(
        'get',
        `${businessId}/creatives`,
        {
          creative_folder_id: folderId,
          fields: JSON.stringify(['id', 'name']),
          filtering: JSON.stringify(filtering),
          recursive: true,
        },
      );

      const cleanTarget = this.cleanName(name);
      const exactMatch = (response.data || []).find((item: any) => {
        if (!item.name) return false;
        return (
          item.name.toLowerCase().trim() === name.toLowerCase().trim() ||
          this.cleanName(item.name) === cleanTarget
        );
      });

      return exactMatch?.id || null;
    } catch (err: any) {
      this.logger.warn(`Failed to check Meta creatives: ${err.message}`);
      return null;
    }
  }

  private async linkAssetToDriveFile(assetId: string, file: DriveFileEntry) {
    if (!file.id) return;

    const driveUrl = file.webViewLink || this.buildDriveWebViewUrl(file.id);
    await this.upsertDriveFile({ ...file, webViewLink: driveUrl });
    await this.prisma.creativeAsset.update({
      where: { id: assetId },
      data: {
        drive_id: file.id,
        drive_url: driveUrl,
      },
    });
  }

  private async upsertDriveFile(file: DriveFileEntry) {
    if (!file.id) return;

    await this.prisma.driveFile.upsert({
      where: { id: file.id },
      update: {
        parentId: file.parents?.[0] || undefined,
        name: file.name || undefined,
        mimeType: file.mimeType || undefined,
        webContentLink: file.webContentLink || undefined,
        webViewLink: file.webViewLink || undefined,
        size: file.size || undefined,
        raw: file as any,
        drive_permission: true,
        last_seen_at: new Date(),
      },
      create: {
        id: file.id,
        parentId: file.parents?.[0] || null,
        name: file.name || 'Unknown File',
        mimeType: file.mimeType || null,
        webContentLink: file.webContentLink || null,
        webViewLink: file.webViewLink || null,
        size: file.size || null,
        raw: file as any,
        drive_permission: true,
        last_seen_at: new Date(),
      },
    });
  }

  private async markRecordStatus(
    record: { id: string; raw: any },
    syncStatus: string,
    syncError: string | null = null,
    extraRaw: Record<string, any> = {},
  ) {
    await this.prisma.larkRecord.update({
      where: { id: record.id },
      data: {
        raw: this.buildRecordRaw(record.raw, syncStatus, syncError, extraRaw),
      },
    });
  }

  private buildEligibleRecordWhere(): Prisma.LarkRecordWhereInput {
    return {
      AND: [
        { drive: { drive_permission: true } },
        {
          raw: {
            path: ['permission_access_verified'],
            equals: true,
          },
        },
        { creative_asset_id: null },
        {
          OR: [
            { NOT: { raw: { path: ['sync_status'], equals: 'SUCCESS' } } },
            { raw: { path: ['sync_status'], equals: Prisma.DbNull } },
          ],
        },
      ],
    };
  }

  private async claimRecordsForUpload(records: any[], uploadBatchId: string) {
    if (records.length === 0) return [];

    const claimedAt = new Date().toISOString();
    const claimedRaws = records.map((record) =>
      this.buildRecordRaw(record.raw, 'UPLOADING', null, {
        sync_upload_batch_id: uploadBatchId,
        sync_claimed_at: claimedAt,
      }),
    );

    const results = await this.prisma.$transaction(
      records.map((record, index) =>
        this.prisma.larkRecord.updateMany({
          where: {
            id: record.id,
            ...this.buildEligibleRecordWhere(),
          },
          data: {
            raw: claimedRaws[index],
          },
        }),
      ),
    );

    return records
      .map((record, index) =>
        results[index].count > 0
          ? {
              ...record,
              raw: claimedRaws[index],
            }
          : null,
      )
      .filter(Boolean);
  }

  private async releaseStaleUploadingRecords() {
    const uploadingRecords = await this.prisma.larkRecord.findMany({
      where: {
        raw: {
          path: ['sync_status'],
          equals: 'UPLOADING',
        },
      },
      select: {
        id: true,
        raw: true,
      },
      take: this.maxFilesPerRun * 5,
    });

    const now = Date.now();
    const staleRecords = uploadingRecords.filter((record) => {
      const raw = this.normalizeRaw(record.raw);
      const checkedAt = raw.sync_claimed_at || raw.last_meta_upload_checked_at;
      const checkedAtMs = checkedAt ? new Date(checkedAt).getTime() : 0;

      return !checkedAtMs || now - checkedAtMs > this.uploadingClaimTtlMs;
    });

    if (staleRecords.length === 0) return;

    await this.prisma.$transaction(
      staleRecords.map((record) =>
        this.prisma.larkRecord.update({
          where: { id: record.id },
          data: {
            raw: this.buildRecordRaw(
              record.raw,
              'PENDING',
              'Upload claim expired before completion',
              { sync_stale_released_at: new Date().toISOString() },
            ),
          },
        }),
      ),
    );

    this.logger.warn(
      `Released ${staleRecords.length} stale UPLOADING Lark records back to PENDING`,
    );
  }

  private buildRecordRaw(
    recordRaw: unknown,
    syncStatus: string,
    syncError: string | null = null,
    extraRaw: Record<string, any> = {},
  ) {
    const raw =
      recordRaw && typeof recordRaw === 'object' && !Array.isArray(recordRaw)
        ? (recordRaw as Record<string, any>)
        : {};

    return {
      ...raw,
      ...extraRaw,
      sync_status: syncStatus,
      sync_error: syncError,
      last_meta_upload_checked_at: new Date().toISOString(),
    };
  }

  private normalizeRaw(raw: unknown): Record<string, any> {
    return raw && typeof raw === 'object' && !Array.isArray(raw)
      ? (raw as Record<string, any>)
      : {};
  }

  private getAssetType(mimeType?: string | null, name?: string | null) {
    const lowerName = (name || '').toLowerCase();
    if (
      mimeType?.startsWith('image/') ||
      /\.(png|jpe?g|gif|webp)$/i.test(lowerName)
    ) {
      return AssetType.IMAGE;
    }
    if (
      mimeType?.startsWith('video/') ||
      /\.(mp4|mov|m4v|webm)$/i.test(lowerName)
    ) {
      return AssetType.VIDEO;
    }
    return null;
  }

  private buildDriveWebViewUrl(fileId: string) {
    return `https://drive.google.com/file/d/${fileId}/view`;
  }

  private cleanName(name: string) {
    return name
      .toLowerCase()
      .replace(/\.[^/.]+$/, '')
      .trim();
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
