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
  capabilities?: { canDownload?: boolean | null } | null;
  raw?: any;
};

type UploadBudget = {
  uploaded: number;
  limit: number;
  reserved: number;
};

type FolderUploadStats = {
  uploadedCount: number;
  skippedCount: number;
  unsupportedCount: number;
  limitReached: boolean;
  assetIds: string[];
};

type UploadErrorInfo = {
  message: string;
  raw: Record<string, string | null>;
};

type UploadControl = {
  paused: boolean;
  cooldown?: any;
};

const parseEnvInteger = (
  value: string | undefined,
  defaultValue: number,
  minValue: number,
) => {
  const parsed = Number(value ?? defaultValue);
  return Number.isFinite(parsed) ? Math.max(minValue, parsed) : defaultValue;
};

@Injectable()
export class MetaMediaUploadService {
  private readonly logger = new Logger(MetaMediaUploadService.name);
  private readonly maxFilesPerRun = 20;
  private readonly metaUploadConcurrency = parseEnvInteger(
    process.env.META_MEDIA_UPLOAD_CONCURRENCY,
    5,
    1,
  );
  private readonly metaUploadStartDelayMs = parseEnvInteger(
    process.env.META_MEDIA_UPLOAD_START_DELAY_MS,
    1500,
    0,
  );
  private readonly metaUploadChunkDelayMs = parseEnvInteger(
    process.env.META_MEDIA_UPLOAD_CHUNK_DELAY_MS,
    5000,
    0,
  );
  private readonly metaAssetFetchDelayMs = parseEnvInteger(
    process.env.META_ASSET_FETCH_DELAY_MS,
    2000,
    0,
  );
  private readonly metaImagePollRetryDelayMs = parseEnvInteger(
    process.env.META_IMAGE_POLL_RETRY_DELAY_MS,
    5000,
    0,
  );
  private readonly metaVideoPollRetryDelayMs = parseEnvInteger(
    process.env.META_VIDEO_POLL_RETRY_DELAY_MS,
    7000,
    0,
  );
  private readonly metaCooldownHardBlock =
    process.env.META_API_COOLDOWN_HARD_BLOCK === 'true';
  private readonly checkMetaExistingAssetBeforeUpload =
    process.env.META_MEDIA_UPLOAD_CHECK_META_EXISTING_ASSET === 'true';
  private readonly uploadingClaimTtlMs = 6 * 60 * 60 * 1000;
  private readonly maxFailedUploadRetries = parseEnvInteger(
    process.env.META_MEDIA_UPLOAD_FAILED_RETRY_MAX,
    10,
    1,
  );
  private readonly failedUploadRetryBaseMs = parseEnvInteger(
    process.env.META_MEDIA_UPLOAD_FAILED_RETRY_BASE_MS,
    20 * 60 * 1000,
    60 * 1000,
  );
  private readonly failedUploadRetryMaxDelayMs = parseEnvInteger(
    process.env.META_MEDIA_UPLOAD_FAILED_RETRY_MAX_DELAY_MS,
    6 * 60 * 60 * 1000,
    60 * 1000,
  );
  private readonly driveFolderMimeType = 'application/vnd.google-apps.folder';
  private readonly rootFolderId =
    process.env.META_CREATIVE_ROOT_FOLDER_ID || '4303729193176038';
  private readonly folderEnsurePromises = new Map<string, Promise<any>>();
  private nextMetaAssetFetchAt = Date.now();
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

  private async getBlockingMetaApiCooldown() {
    if (!this.metaCooldownHardBlock) return null;
    return this.metaApi.getActiveMetaApiCooldown();
  }

  async autoUpload() {
    await this.releaseStaleUploadingRecords();

    const activeUploadingCount = await this.countActiveUploadingRecords();
    if (activeUploadingCount > 0) {
      this.logger.warn(
        `Skipping Meta auto-upload because ${activeUploadingCount} Lark records are still UPLOADING`,
      );
      return {
        uploaded: 0,
        scanned: 0,
        skipped: true,
        reason: 'ACTIVE_UPLOAD_IN_PROGRESS',
        activeUploadingCount,
      };
    }

    const activeCooldown = await this.getBlockingMetaApiCooldown();
    if (activeCooldown) {
      this.logger.warn(
        `Skipping Meta auto-upload because Meta API is cooling down until ${activeCooldown.blockedUntil}`,
      );
      return {
        uploaded: 0,
        scanned: 0,
        skipped: true,
        reason: 'META_API_COOLDOWN',
        cooldownUntil: activeCooldown.blockedUntil,
      };
    }

    const budget: UploadBudget = {
      uploaded: 0,
      limit: this.maxFilesPerRun,
      reserved: 0,
    };

    const uploadBatchId = randomUUID();
    let scanned = 0;
    let claimed = 0;
    let queueNumber = 0;

    while (this.getRemainingUploadSlots(budget) > 0) {
      const activeLoopCooldown = await this.getBlockingMetaApiCooldown();
      if (activeLoopCooldown) {
        this.logger.warn(
          `Stopping Meta auto-upload because Meta API is cooling down until ${activeLoopCooldown.blockedUntil}`,
        );
        break;
      }

      const remainingSlots = this.getRemainingUploadSlots(budget);
      const records = await this.prisma.larkRecord.findMany({
        where: this.buildEligibleRecordWhere(),
        include: { drive: true },
        orderBy: [{ production_date: 'desc' }, { id: 'asc' }],
        take: remainingSlots,
      });
      const uploadCandidates = records.filter(
        (record) => this.normalizeRaw(record.raw).sync_status !== 'UPLOADING',
      );

      if (uploadCandidates.length === 0) break;

      scanned += uploadCandidates.length;
      queueNumber += 1;
      this.logger.log(
        `Found ${uploadCandidates.length} eligible Lark records for Meta auto-upload queue ${queueNumber} (${budget.uploaded}/${budget.limit} uploaded so far)`,
      );

      const claimedRecords = await this.claimRecordsForUpload(
        uploadCandidates,
        uploadBatchId,
      );
      claimed += claimedRecords.length;

      this.logger.log(
        `Claimed ${claimedRecords.length}/${uploadCandidates.length} Lark records as UPLOADING for Meta auto-upload batch ${uploadBatchId}`,
      );

      if (claimedRecords.length === 0) break;

      await this.uploadClaimedRecords(claimedRecords, budget, uploadBatchId);
    }

    this.logger.log(
      `Meta auto-upload completed. Uploaded files this run: ${budget.uploaded}/${budget.limit}`,
    );

    return { uploaded: budget.uploaded, scanned, claimed };
  }

  private async uploadClaimedRecords(
    records: any[],
    budget: UploadBudget,
    uploadBatchId: string,
  ) {
    const fileRecords: any[] = [];
    const queueState = { number: 0 };

    const flushFileRecords = async () => {
      await this.uploadFileRecordQueue(
        fileRecords,
        budget,
        uploadBatchId,
        queueState,
      );
    };

    for (const record of records) {
      const cooldown = await this.getBlockingMetaApiCooldown();
      if (cooldown) {
        await this.markRecordsMetaCooldownPending(
          [record, ...fileRecords, ...records.slice(records.indexOf(record) + 1)],
          uploadBatchId,
          cooldown,
        );
        return;
      }

      if (this.isDriveFolderRecord(record)) {
        await flushFileRecords();

        const cooldownAfterFlush = await this.getBlockingMetaApiCooldown();
        if (cooldownAfterFlush) {
          await this.markRecordsMetaCooldownPending(
            [record, ...records.slice(records.indexOf(record) + 1)],
            uploadBatchId,
            cooldownAfterFlush,
          );
          return;
        }

        if (this.getRemainingUploadSlots(budget) <= 0) {
          await this.markRecordUploadLimitReached(record, uploadBatchId);
          continue;
        }

        const result = await this.processClaimedRecord(
          record,
          budget,
          uploadBatchId,
        );
        if (result === 'COOLDOWN') {
          await this.markRecordsMetaCooldownPending(
            records.slice(records.indexOf(record) + 1),
            uploadBatchId,
          );
          return;
        }
        continue;
      }

      fileRecords.push(record);
    }

    await flushFileRecords();
  }

  private async uploadFileRecordQueue(
    fileRecords: any[],
    budget: UploadBudget,
    uploadBatchId: string,
    queueState: { number: number },
  ) {
    while (fileRecords.length > 0) {
      const remainingSlots = this.getRemainingUploadSlots(budget);
      if (remainingSlots <= 0) {
        const remainingRecords = fileRecords.splice(0);
        await Promise.all(
          remainingRecords.map((record) =>
            this.markRecordUploadLimitReached(record, uploadBatchId),
          ),
        );
        return;
      }

      const queue = fileRecords.splice(
        0,
        Math.min(remainingSlots, fileRecords.length),
      );
      if (queue.length === 0) return;

      queueState.number += 1;
      this.logger.log(
        `Uploading Lark file-record queue ${queueState.number}: ${queue.length} records (${budget.uploaded}/${budget.limit} uploaded this job, concurrency=${this.metaUploadConcurrency}, startDelay=${this.metaUploadStartDelayMs}ms)`,
      );

      const control: UploadControl = { paused: false };
      const results = await this.mapWithMetaUploadThrottle(
        queue,
        async (record) => {
          const result = await this.processClaimedRecord(
            record,
            budget,
            uploadBatchId,
          );
          if (result === 'COOLDOWN') control.paused = true;
          return result;
        },
        control,
      );

      const cooldown = await this.getBlockingMetaApiCooldown();
      if (control.paused || cooldown) {
        const skippedQueueRecords = queue.filter(
          (_, index) => results[index] === undefined,
        );
        await this.markRecordsMetaCooldownPending(
          [...skippedQueueRecords, ...fileRecords.splice(0)],
          uploadBatchId,
          control.cooldown || cooldown,
        );
        return;
      }

      if (
        fileRecords.length > 0 &&
        this.getRemainingUploadSlots(budget) > 0 &&
        this.metaUploadChunkDelayMs > 0
      ) {
        await this.sleep(this.metaUploadChunkDelayMs);
      }
    }
  }

  private async mapWithMetaUploadThrottle<T, R>(
    items: T[],
    task: (item: T, index: number) => Promise<R>,
    control?: UploadControl,
  ): Promise<Array<R | undefined>> {
    const results = new Array<R | undefined>(items.length);
    let nextIndex = 0;
    let nextStartAt = Date.now();
    const workerCount = Math.min(this.metaUploadConcurrency, items.length);

    await Promise.all(
      Array.from({ length: workerCount }, async () => {
        while (true) {
          if (control?.paused) return;
          const cooldown = await this.getBlockingMetaApiCooldown();
          if (cooldown) {
            if (control) {
              control.paused = true;
              control.cooldown = cooldown;
            }
            return;
          }

          const index = nextIndex++;
          if (index >= items.length) return;

          if (this.metaUploadStartDelayMs > 0) {
            const now = Date.now();
            const waitMs = Math.max(0, nextStartAt - now);
            nextStartAt =
              Math.max(now, nextStartAt) + this.metaUploadStartDelayMs;
            if (waitMs > 0) await this.sleep(waitMs);
          }

          if (control?.paused) return;
          results[index] = await task(items[index], index);
        }
      }),
    );

    return results;
  }

  private getRemainingUploadSlots(budget: UploadBudget) {
    return Math.max(0, budget.limit - budget.uploaded - budget.reserved);
  }

  private tryReserveUploadSlot(budget: UploadBudget) {
    if (this.getRemainingUploadSlots(budget) <= 0) return false;
    budget.reserved += 1;
    return true;
  }

  private releaseUploadSlot(budget: UploadBudget, uploaded: boolean) {
    budget.reserved = Math.max(0, budget.reserved - 1);
    if (uploaded) budget.uploaded += 1;
  }

  private normalizeUploadError(error: any): UploadErrorInfo {
    const responseData = error?.response?.data || null;
    const responseError =
      error?.metaError || responseData?.error || error?.error || null;
    const nestedErrors =
      responseError?.errors || responseData?.errors || error?.errors || [];
    const firstNestedError = Array.isArray(nestedErrors)
      ? nestedErrors[0]
      : null;

    const message =
      responseError?.message ||
      firstNestedError?.message ||
      responseData?.message ||
      error?.message ||
      String(error);
    const code =
      responseError?.code ??
      error?.code ??
      error?.status ??
      error?.response?.status ??
      null;
    const status = error?.status ?? error?.response?.status ?? null;
    const reason =
      firstNestedError?.reason ||
      responseError?.error_subcode ||
      responseError?.reason ||
      null;
    const type = responseError?.type || firstNestedError?.domain || null;
    const detailParts = [type, reason ? `reason=${reason}` : null].filter(
      Boolean,
    );

    return {
      message,
      raw: {
        sync_error_code: code === null ? null : String(code),
        sync_error_status: status === null ? null : String(status),
        sync_error_reason: reason === null ? null : String(reason),
        sync_error_detail:
          detailParts.length > 0 ? detailParts.join(' | ') : null,
        sync_error_at: new Date().toISOString(),
      },
    };
  }

  private async processClaimedRecord(
    record: any,
    budget: UploadBudget,
    uploadBatchId: string,
  ): Promise<'DONE' | 'COOLDOWN'> {
    try {
      const result = await this.uploadRecord(record, budget);
      await this.markRecordStatus(record, result.status, null, {
        ...result.raw,
        ...this.buildUploadRetryResetRaw(),
      });
      return 'DONE';
    } catch (err: any) {
      this.logger.error(
        `Meta auto-upload failed for LarkRecord ${record.id}: ${err.message}`,
        err.stack,
      );
      const errorInfo = this.normalizeUploadError(err);
      if (this.isMetaCooldownError(err)) {
        await this.markRecordMetaCooldownPending(
          record,
          uploadBatchId,
          errorInfo,
        );
        return 'COOLDOWN';
      }

      await this.markRecordStatus(record, 'FAILED', errorInfo.message, {
        ...errorInfo.raw,
        ...this.buildFailedUploadRetryRaw(record.raw),
      });
      return 'DONE';
    }
  }

  private isMetaCooldownError(error: any) {
    const responseData = error?.response?.data || null;
    const responseError =
      error?.metaError || responseData?.error || error?.error || null;
    const code = Number(responseError?.code ?? error?.code);
    const subcode = Number(responseError?.error_subcode);
    const message = String(
      responseError?.message || responseData?.message || error?.message || '',
    ).toLowerCase();

    return (
      [4, 17, 32, 368, 613, 80004].includes(code) ||
      subcode === 2446079 ||
      message.includes('meta api đang tạm cooldown') ||
      message.includes('temporarily blocked') ||
      message.includes('rate limit') ||
      message.includes('too many') ||
      message.includes('try again later')
    );
  }

  private async markRecordMetaCooldownPending(
    record: { id: string; raw: any },
    uploadBatchId: string | null,
    errorInfo?: UploadErrorInfo,
    cooldown?: any,
  ) {
    const activeCooldown =
      cooldown || (await this.metaApi.getActiveMetaApiCooldown());
    await this.markRecordStatus(
      record,
      'PENDING',
      errorInfo?.message || null,
      {
        ...(errorInfo?.raw || {}),
        sync_meta_cooldown: true,
        sync_meta_cooldown_until: activeCooldown?.blockedUntil || null,
        ...(uploadBatchId ? { sync_upload_batch_id: uploadBatchId } : {}),
      },
    );
  }

  private async markRecordsMetaCooldownPending(
    records: Array<{ id: string; raw: any }>,
    uploadBatchId: string,
    cooldown?: any,
  ) {
    const uniqueRecords = Array.from(
      new Map(records.filter(Boolean).map((record) => [record.id, record]))
        .values(),
    );
    if (uniqueRecords.length === 0) return;

    await Promise.all(
      uniqueRecords.map((record) =>
        this.markRecordMetaCooldownPending(
          record,
          uploadBatchId,
          undefined,
          cooldown,
        ),
      ),
    );
  }

  private async markRecordUploadLimitReached(
    record: { id: string; raw: any },
    uploadBatchId: string,
  ) {
    await this.markRecordStatus(record, 'PENDING', null, {
      sync_limit_reached: true,
      sync_upload_batch_id: uploadBatchId,
    });
  }

  private isDriveFolderRecord(record: any) {
    return record.drive?.mimeType === this.driveFolderMimeType;
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
      capabilities: (record.drive.raw as any)?.capabilities || null,
      raw: record.drive.raw,
    };

    if (driveFile.mimeType === this.driveFolderMimeType) {
      if (!driveFile.id) throw new Error('Missing Drive folder ID');
      await this.upsertDriveFile(driveFile);

      const driveFolderName =
        driveFile.name ||
        record.product_name ||
        record.product_code ||
        driveFile.id;
      const targetFolder =
        this.cleanName(driveFolderName) === this.cleanName(productFolder.name)
          ? productFolder
          : await this.ensureCreativeFolder(
              driveFolderName,
              productFolder.id,
              driveFolderName,
            );

      const stats = await this.uploadDriveFolder(
        driveFile.id,
        targetFolder.id,
        budget,
      );
      if (stats.assetIds[0]) {
        await this.prisma.larkRecord.update({
          where: { id: record.id },
          data: { creative_asset_id: stats.assetIds[0] },
        });
      }

      return {
        status: stats.limitReached ? 'PENDING' : 'SUCCESS',
        raw: {
          sync_meta_folder_id: targetFolder.id,
          sync_meta_folder_name: targetFolder.name,
          sync_uploaded_count: stats.uploadedCount,
          sync_skipped_count: stats.skippedCount,
          sync_unsupported_count: stats.unsupportedCount,
          sync_creative_asset_ids: stats.assetIds,
          sync_creative_asset_count: stats.assetIds.length,
          sync_limit_reached: stats.limitReached,
        },
      };
    }

    const asset = await this.uploadDriveFile(
      driveFile,
      productFolder.id,
      budget,
    );
    if (asset.reason === 'LIMIT_REACHED') {
      return {
        status: 'PENDING',
        raw: { sync_limit_reached: true },
      };
    }

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
      assetIds: [],
    };

    const walk = async (folderId: string, targetFolderId: string) => {
      if (await this.getBlockingMetaApiCooldown()) {
        stats.limitReached = true;
        return;
      }

      if (this.getRemainingUploadSlots(budget) <= 0) {
        stats.limitReached = true;
        return;
      }

      const children = await this.listFolderChildren(folderId);
      const childFolders = children.filter(
        (child) => child.mimeType === this.driveFolderMimeType,
      );
      const mediaFiles = children.filter(
        (child) => child.mimeType !== this.driveFolderMimeType,
      );

      for (const child of childFolders) {
        if (!child.id) continue;
        if (await this.getBlockingMetaApiCooldown()) {
          stats.limitReached = true;
          return;
        }

        await this.upsertDriveFile(child);
        const subFolder = await this.ensureCreativeFolder(
          child.name || child.id,
          targetFolderId,
        );
        await walk(child.id, subFolder.id);

        if (this.getRemainingUploadSlots(budget) <= 0) {
          stats.limitReached = true;
          return;
        }
      }

      let queueNumber = 0;
      for (let i = 0; i < mediaFiles.length; ) {
        const remainingSlots = this.getRemainingUploadSlots(budget);
        if (remainingSlots <= 0) {
          stats.limitReached = true;
          return;
        }

        const queue = mediaFiles.slice(
          i,
          i + Math.min(remainingSlots, mediaFiles.length - i),
        );
        if (queue.length === 0) {
          stats.limitReached = this.getRemainingUploadSlots(budget) <= 0;
          return;
        }

        i += queue.length;
        queueNumber += 1;

        this.logger.log(
          `Uploading media queue ${queueNumber} in Meta folder ${targetFolderId}: ${queue.length} files (${budget.uploaded}/${budget.limit} uploaded this job, concurrency=${this.metaUploadConcurrency}, startDelay=${this.metaUploadStartDelayMs}ms)`,
        );

        const control: UploadControl = { paused: false };
        const results = await this.mapWithMetaUploadThrottle(
          queue,
          async (child) => {
            try {
              return await this.uploadDriveFile(child, targetFolderId, budget);
            } catch (err) {
              if (this.isMetaCooldownError(err)) {
                control.paused = true;
                return {
                  uploaded: false,
                  reason: 'META_COOLDOWN' as const,
                };
              }
              throw err;
            }
          },
          control,
        );

        for (const result of results) {
          if (!result) continue;
          const resultAsset = 'asset' in result ? result.asset : null;
          if (resultAsset?.id && !stats.assetIds.includes(resultAsset.id)) {
            stats.assetIds.push(resultAsset.id);
          }
          if (result.uploaded) stats.uploadedCount += 1;
          else if (result.reason === 'UNSUPPORTED_TYPE')
            stats.unsupportedCount += 1;
          else if (result.reason === 'LIMIT_REACHED') stats.limitReached = true;
          else if (result.reason === 'META_COOLDOWN') stats.limitReached = true;
          else stats.skippedCount += 1;
        }

        if (control.paused || (await this.getBlockingMetaApiCooldown())) {
          stats.limitReached = true;
          return;
        }

        if (this.getRemainingUploadSlots(budget) <= 0) {
          stats.limitReached = true;
          return;
        }

        if (
          i < mediaFiles.length &&
          this.getRemainingUploadSlots(budget) > 0 &&
          this.metaUploadChunkDelayMs > 0
        ) {
          await this.sleep(this.metaUploadChunkDelayMs);
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

    if (this.isDriveDownloadBlocked(file)) {
      throw new Error(
        `Google Drive không cho phép tải file này để upload Meta: ${
          file.name || file.id
        } (capabilities.canDownload=false)`,
      );
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

    if (this.checkMetaExistingAssetBeforeUpload) {
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
    }

    if (!this.tryReserveUploadSlot(budget)) {
      return { uploaded: false, reason: 'LIMIT_REACHED' as const };
    }

    try {
      const buffer = await this.downloadDriveFile(file.id);
      const asset = await this.uploadBufferToMeta({
        buffer,
        name: file.name || file.id,
        type,
        folderId,
        driveUrl: file.webViewLink || this.buildDriveWebViewUrl(file.id),
        driveId: file.id,
      });
      this.releaseUploadSlot(budget, true);

      return { uploaded: true, asset };
    } catch (err) {
      this.releaseUploadSlot(budget, false);
      throw err;
    }
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
    const folderKey = `${parentId || 'root'}:${name}`;
    const existingFolder = await this.prisma.creativeFolder.findFirst({
      where: { name, parentId },
    });
    if (existingFolder) return existingFolder;

    const pendingFolder = this.folderEnsurePromises.get(folderKey);
    if (pendingFolder) return pendingFolder;

    const folderPromise = this.createCreativeFolder(
      name,
      parentId,
      description,
    ).finally(() => {
      this.folderEnsurePromises.delete(folderKey);
    });
    this.folderEnsurePromises.set(folderKey, folderPromise);

    return folderPromise;
  }

  private async createCreativeFolder(
    name: string,
    parentId: string | null,
    description = name,
  ) {
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
          'nextPageToken, files(id,name,mimeType,parents,webViewLink,webContentLink,size,capabilities/canDownload)',
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

  private isDriveDownloadBlocked(file: DriveFileEntry) {
    const canDownload =
      file.capabilities?.canDownload ??
      file.raw?.capabilities?.canDownload ??
      null;
    return canDownload === false;
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
        }, 30000);
      } else if (type === AssetType.IMAGE && fs.existsSync(filePath)) {
        try {
          fs.unlinkSync(filePath);
        } catch {}
      }
    }

    const initialPollDelayMs = type === AssetType.VIDEO ? 15000 : 2000;
    await this.sleep(initialPollDelayMs);

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
    const retryDelayMs =
      type === AssetType.VIDEO
        ? this.metaVideoPollRetryDelayMs
        : this.metaImagePollRetryDelayMs;
    let errorCount = 0;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        await this.waitForMetaAssetFetchSlot();
        const response = await this.metaApi.request('get', assetId, {
          fields: fields.join(','),
        });

        if (response?.id) {
          if (type === AssetType.VIDEO && response.video?.source) {
            latestReadyAsset = response;
            const hasGoodThumbnailSet =
              Array.isArray(response.video?.thumbnails?.data) &&
              response.video.thumbnails.data.length > 2;
            if (hasGoodThumbnailSet || attempt === maxRetries - 1) {
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

      await this.sleep(retryDelayMs);
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

      await this.waitForMetaAssetFetchSlot();
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

  private buildUploadRetryResetRaw() {
    return {
      meta_upload_retry_count: 0,
      meta_upload_next_retry_at: null,
      meta_upload_last_failed_at: null,
      meta_upload_retry_exhausted: null,
    };
  }

  private buildFailedUploadRetryRaw(recordRaw: unknown) {
    const raw = this.normalizeRaw(recordRaw);
    const previousRetryCount = Number(raw.meta_upload_retry_count || 0);
    const retryCount = previousRetryCount + 1;
    const exhausted =
      this.maxFailedUploadRetries > 0 &&
      retryCount >= this.maxFailedUploadRetries;
    const delayMs = Math.min(
      this.failedUploadRetryBaseMs * 2 ** Math.max(0, retryCount - 1),
      this.failedUploadRetryMaxDelayMs,
    );
    const now = new Date();

    return {
      meta_upload_retry_count: retryCount,
      meta_upload_last_failed_at: now.toISOString(),
      meta_upload_next_retry_at: exhausted
        ? null
        : new Date(now.getTime() + delayMs).toISOString(),
      meta_upload_retry_exhausted: exhausted,
    };
  }

  private buildRetryableFailedUploadWhere(): Prisma.LarkRecordWhereInput {
    return {
      AND: [
        {
          raw: {
            path: ['sync_status'],
            equals: 'FAILED',
          },
        },
        {
          OR: [
            {
              raw: {
                path: ['meta_upload_retry_count'],
                equals: Prisma.DbNull,
              },
            },
            {
              raw: {
                path: ['meta_upload_retry_count'],
                equals: Prisma.JsonNull,
              },
            },
            {
              raw: {
                path: ['meta_upload_retry_count'],
                lt: this.maxFailedUploadRetries,
              },
            },
          ],
        },
        {
          OR: [
            {
              raw: {
                path: ['meta_upload_next_retry_at'],
                equals: Prisma.DbNull,
              },
            },
            {
              raw: {
                path: ['meta_upload_next_retry_at'],
                equals: Prisma.JsonNull,
              },
            },
            {
              raw: {
                path: ['meta_upload_next_retry_at'],
                lte: new Date().toISOString(),
              },
            },
          ],
        },
      ],
    };
  }

  private buildAutoUploadStatusEligibleWhere(): Prisma.LarkRecordWhereInput {
    return {
      OR: [
        {
          AND: [
            this.buildSyncStatusNotWhere('UPLOADING'),
            this.buildSyncStatusNotWhere('FAILED'),
            this.buildSyncStatusNotWhere('SUCCESS'),
          ],
        },
        this.buildRetryableFailedUploadWhere(),
      ],
    };
  }

  private buildEligibleRecordWhere(): Prisma.LarkRecordWhereInput {
    return {
      AND: [
        { drive: { drive_permission: true } },
        this.buildNotUploadedRecordWhere(),
        this.buildAutoUploadStatusEligibleWhere(),
      ],
    };
  }

  private buildNotUploadedRecordWhere(): Prisma.LarkRecordWhereInput {
    return {
      AND: [
        { creative_asset_id: null },
        {
          OR: [
            {
              raw: {
                path: ['sync_creative_asset_count'],
                equals: Prisma.DbNull,
              },
            },
            {
              raw: {
                path: ['sync_creative_asset_count'],
                equals: Prisma.JsonNull,
              },
            },
            {
              raw: {
                path: ['sync_creative_asset_count'],
                lte: 0,
              },
            },
          ],
        },
      ],
    };
  }

  private buildSyncStatusNotWhere(status: string): Prisma.LarkRecordWhereInput {
    return {
      OR: [
        {
          raw: {
            path: ['sync_status'],
            equals: Prisma.DbNull,
          },
        },
        {
          raw: {
            path: ['sync_status'],
            equals: Prisma.JsonNull,
          },
        },
        {
          raw: {
            path: ['sync_status'],
            not: status,
          },
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

  private async countActiveUploadingRecords() {
    return this.prisma.larkRecord.count({
      where: {
        raw: {
          path: ['sync_status'],
          equals: 'UPLOADING',
        },
      },
    });
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
    const clearErrorRaw =
      syncError === null
        ? {
            sync_error_code: null,
            sync_error_status: null,
            sync_error_reason: null,
            sync_error_detail: null,
            sync_error_at: null,
          }
        : {};

    return {
      ...raw,
      ...clearErrorRaw,
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

  private async waitForMetaAssetFetchSlot() {
    if (this.metaAssetFetchDelayMs <= 0) return;

    const now = Date.now();
    const waitMs = Math.max(0, this.nextMetaAssetFetchAt - now);
    this.nextMetaAssetFetchAt =
      Math.max(now, this.nextMetaAssetFetchAt) + this.metaAssetFetchDelayMs;

    if (waitMs > 0) await this.sleep(waitMs);
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
