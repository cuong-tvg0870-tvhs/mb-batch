import { Injectable, Logger } from '@nestjs/common';
import { AssetType, LarkRecord, Prisma } from '@prisma/client';
import axios, { AxiosRequestConfig } from 'axios';
import { FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import * as fs from 'fs';
import { drive_v3, google } from 'googleapis';
import path from 'path';
import { PrismaService } from '../prisma/prisma.service';
import { pipeline } from 'stream/promises';
import { extractDriveId, mapRecord } from './lark-sync.utils';
import pLimit from 'p-limit';
import { chunk } from '../../common/utils';
import { MetaFolderResponse, FolderRequest } from './lark-sync.constants';

interface LogicalProduct {
  product_code: string;
  product_name: string;
}

interface LogicalBrand {
  brand_name: string;
  products: LogicalProduct[];
}

interface LogicalProject {
  project_name: string;
  brands: LogicalBrand[];
}

@Injectable()
export class LarkSyncService {
  private readonly logger = new Logger(LarkSyncService.name);
  private driveSA: drive_v3.Drive;
  private readonly baseURL = 'https://open.larksuite.com/open-apis/bitable/v1';
  private readonly BASE_DIR = '/app/files';
  private readonly BUSINESS_ID = '1916878948527753';
  private readonly ROOT_META_FOLDER = '4303729193176038';

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

  async cleanupDuplicateFolders() {
    this.logger.log('🔍 Starting Batch Cleanup for duplicate folders...');
    const LIMIT_DELETE = 100;

    const allFolders = await this.prisma.creativeFolder.findMany({
      orderBy: { creation_time: 'asc' },
    });

    const reportGroup = new Map<
      string,
      {
        keeper: { folder: any; assetCount: number };
        duplicates: Array<{
          folder: any;
          assetCount: number;
          reason: string;
          status: 'DELETE' | 'SKIP';
        }>;
      }
    >();

    for (const folder of allFolders) {
      const parentId = folder.parentId || 'root';
      const uniqueKey = `${parentId} | ${folder.name.toLowerCase().trim()}`;

      const assetCount = await this.prisma.creativeAsset.count({
        where: { folderId: folder.id },
      });

      if (!reportGroup.has(uniqueKey)) {
        reportGroup.set(uniqueKey, {
          keeper: { folder, assetCount },
          duplicates: [],
        });
      } else {
        const group = reportGroup.get(uniqueKey)!;
        let status: 'DELETE' | 'SKIP' = 'DELETE';
        let reason = 'Empty folder, duplicate name/level.';

        if (assetCount > 0) {
          status = 'SKIP';
          reason = `⚠️ SKIP: Contains ${assetCount} assets.`;
        }
        group.duplicates.push({ folder, assetCount, reason, status });
      }
    }

    const pendingDeleteList = Array.from(reportGroup.values())
      .flatMap((g) => g.duplicates)
      .filter((d) => d.status === 'DELETE');

    if (pendingDeleteList.length === 0) {
      this.logger.log('✅ No duplicate folders eligible for deletion.');
      return;
    }

    const batchToDelete = pendingDeleteList.slice(0, LIMIT_DELETE);
    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

    for (const item of batchToDelete) {
      const folderId = item.folder.id;
      try {
        await api.call('DELETE', [folderId]);
        await this.prisma.creativeFolder.delete({ where: { id: folderId } });
        this.logger.log(`✅ Deleted: ${folderId}`);
      } catch (error: any) {
        this.logger.error(`❌ Failed to delete ${folderId}: ${error.message}`);
      }
    }
  }

  async syncLarkToDrive() {
    this.logger.log('🔄 Starting periodic data sync (Lark <-> Drive)...');
    try {
      // 1. Lấy ngày sản xuất gần nhất từ DB để fetch incremental
      const lastRecord = await this.prisma.larkRecord.findFirst({
        where: { production_date: { not: null } },
        orderBy: { production_date: 'desc' },
        select: { production_date: true },
      });

      const conditions: any[] = [];
      if (lastRecord?.production_date) {
        // Lùi lại 1 ngày để đảm bảo không sót record nào trong cùng ngày
        const since = new Date(lastRecord.production_date);
        since.setDate(since.getDate() - 1);
        conditions.push({
          field_name: 'Ngày sản xuất',
          operator: 'isGreaterEqual',
          value: [since.getTime()],
        });
      } else {
        // Nếu DB trống, lấy của ngày hôm nay
        conditions.push({
          field_name: 'Ngày sản xuất',
          operator: 'is',
          value: ['Today'],
        });
      }

      await this.searchRecords('VsGGbP5wkaY7uTsTpZ2l9G9HgWc', 'tblzTv9D1LoUcgcq', {
        conjunction: 'and',
        conditions,
      });

      // 2. Sync metadata từ Drive
      await this.syncDriveFiles();

      // 3. Check permissions & auto-map cho TẤT CẢ các record chưa có creative_asset_id
      this.logger.log('🔍 Starting full audit for unmapped records...');
      let totalChecked = 0;
      while (true) {
        const pendingRecords = await this.prisma.larkRecord.findMany({
          where: { creative_asset_id: null },
          select: { id: true },
          take: 100,
          orderBy: { id: 'asc' },
        });

        if (pendingRecords.length === 0) break;

        await this.checkPermissions(pendingRecords.map((r) => r.id));
        totalChecked += pendingRecords.length;
        this.logger.log(`... audited ${totalChecked} records`);

        // Nếu số lượng trả về ít hơn take, chắc chắn là hết rồi
        if (pendingRecords.length < 100) break;
      }

      this.logger.log('✅ Data sync completed successfully');
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

    const results = [];
    for (const record of records) {
      let drive_permission = false;
      const driveId = extractDriveId(record.drive_url);

      // 1. Kiểm tra quyền Drive
      if (driveId) {
        try {
          await this.driveSA.files.get({
            fileId: driveId,
            fields: 'id',
            supportsAllDrives: true,
          });
          drive_permission = true;
        } catch (e) {
          drive_permission = false;
        }
      }

      // 2. Cập nhật trạng thái Drive trong DB
      if (driveId) {
        await this.prisma.driveFile.upsert({
          where: { id: driveId },
          update: { drive_permission },
          create: {
            id: driveId,
            name: record.drive_url || 'Unknown',
            drive_permission,
            raw: '{}',
          },
        });
      }

      // 3. Tự động map nếu đã tồn tại trên Meta (theo path và tên file)
      let creative_asset_id = record.creative_asset_id;
      if (!creative_asset_id && record.project_name && record.brand_name && record.product_code) {
        const driveFileName = record.drive_url?.split('/').pop() || ''; // Đây là fallback nếu không lấy được từ DriveFile
        const fileNameInDb = await this.prisma.driveFile
          .findUnique({ where: { id: driveId || '' } })
          .then((f) => f?.name);

        const targetName = fileNameInDb || record.project_name; // Cần logic lấy tên file chính xác hơn nếu có

        const asset = await this.prisma.creativeAsset.findFirst({
          where: {
            name: targetName,
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

      results.push({
        id: record.id,
        drive_permission,
        creative_asset_id,
      });
    }

    return results;
  }

  private async getMetaAuthConfig() {
    const config = await this.prisma.systemConfig.findUnique({
      where: { key: 'META_AUTH_CONFIG' },
    });
    return config?.value as {
      accessToken?: string;
      cookie?: string;
      fb_dtsg?: string;
      businessId?: string;
    };
  }

  private getHeaders(authConfig: any) {
    const headers: any = {
      'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      accept: '*/*',
    };
    if (authConfig?.cookie) headers.cookie = authConfig.cookie;
    return headers;
  }

  async uploadDriveToMeta(takeLimit: number = 30) {
    this.logger.log(`🚀 Starting Meta upload (Limit: ${takeLimit} items)...`);
    const authConfig = await this.getMetaAuthConfig();
    const token = authConfig?.accessToken;
    const businessId = authConfig?.businessId || this.BUSINESS_ID;

    if (!token) {
      this.logger.error('❌ Meta Access Token not found in SystemConfig');
      return;
    }

    const limit = pLimit(2); // Giới hạn song song thấp để tránh rate limit

    const [folders, cidContents] = await Promise.all([
      this.prisma.creativeFolder.findFirst({
        where: { parentId: null },
        include: {
          children: { include: { children: { include: { children: true } } } },
        },
      }),
      this.prisma.larkRecord.findMany({
        where: {
          drive: { drive_permission: true },
          creative_asset_id: null,
          drive_id: { not: null },
        },
        take: takeLimit,
        include: { drive: true },
        orderBy: { id: 'asc' },
      }),
    ]);

    if (!folders || cidContents.length === 0) {
      this.logger.log('ℹ️ No new assets to upload.');
      return;
    }

    const tasks = cidContents.map((item) =>
      limit(async () => {
        if (!item.drive || !item.drive_id) return;

        // 1. Kiểm tra lần cuối xem đã có trong DB chưa (tránh trùng lặp do race condition)
        const existingAsset = await this.prisma.creativeAsset.findFirst({
          where: {
            OR: [{ id: item.creative_asset_id || undefined }, { drive_id: item.drive_id }],
          },
        });

        if (existingAsset) {
          await this.prisma.larkRecord.update({
            where: { id: item.id },
            data: { creative_asset_id: existingAsset.id },
          });
          return;
        }

        // 2. Tìm thư mục đích (Product folder)
        const project = folders.children.find((f) => f.name === item.project_name);
        const brand = project?.children?.find((b) => b.name === item.brand_name);
        const product = brand?.children?.find((p) => p.name === item.product_code);

        if (!product) {
          this.logger.warn(`Folder path not found for record: ${item.id} (${item.product_code})`);
          return;
        }

        const fileType = item.drive.mimeType?.startsWith('image') ? AssetType.IMAGE : AssetType.VIDEO;
        let creativeAsset: any;
        let filePath: string | null = null;

        try {
          if (fileType === AssetType.IMAGE) {
            // --- IMAGE UPLOAD ---
            const driveRes = await this.driveSA.files.get(
              { fileId: item.drive_id, alt: 'media', supportsAllDrives: true },
              { responseType: 'arraybuffer' },
            );
            const buffer = Buffer.from(driveRes.data as ArrayBuffer);

            const url = `https://graph.facebook.com/v24.0/${businessId}/images`;
            const params: any = {
              name: item.drive.name,
              bytes: buffer.toString('base64'),
              creative_folder_id: product.id,
              access_token: token,
              method: 'post',
              pretty: '0',
              suppress_http_code: '1',
              xref: 'fe47908523b96c1c2',
            };
            if (authConfig?.fb_dtsg) params.fb_dtsg = authConfig.fb_dtsg;

            const res = await axios.post(url, new URLSearchParams(params).toString(), {
              headers: this.getHeaders(authConfig),
            });
            creativeAsset = res.data;
          } else {
            // --- VIDEO UPLOAD ---
            filePath = path.join(this.BASE_DIR, `${item.drive_id}.mp4`);
            if (!fs.existsSync(this.BASE_DIR)) fs.mkdirSync(this.BASE_DIR, { recursive: true });

            const driveRes = await this.driveSA.files.get(
              { fileId: item.drive_id, alt: 'media', supportsAllDrives: true },
              { responseType: 'stream' },
            );
            await pipeline(driveRes.data as any, fs.createWriteStream(filePath));

            const cdnUrl = `https://mb-ads.tvhs.asia/cdn/${item.drive_id}.mp4`;

            const url = `https://graph.facebook.com/v24.0/${businessId}/videos`;
            const params: any = {
              title: item.drive.name,
              file_url: cdnUrl,
              creative_folder_id: product.id,
              access_token: token,
              method: 'post',
              pretty: '0',
              suppress_http_code: '1',
              xref: 'fe47908523b96c1c2',
            };
            if (authConfig?.fb_dtsg) params.fb_dtsg = authConfig.fb_dtsg;

            const res = await axios.post(url, new URLSearchParams(params).toString(), {
              headers: this.getHeaders(authConfig),
            });
            creativeAsset = res.data;
          }

          if (creativeAsset?.error) throw new Error(creativeAsset.error.message);

          const assetId =
            fileType === AssetType.VIDEO
              ? creativeAsset?.business_video_id
              : (Object.values(creativeAsset.images || {})[0] as any)?.id;

          if (assetId) {
            // Chờ một chút để Meta index asset
            await new Promise((r) => setTimeout(r, 2000));
            
            const dbData = {
              id: assetId,
              name: item.drive.name || 'Untitled',
              drive_url: item.drive.webViewLink || null,
              drive_id: item.drive_id,
              folderId: product.id,
              type: fileType,
            };

            await this.prisma.$transaction([
              this.prisma.creativeAsset.upsert({
                where: { id: assetId },
                update: dbData,
                create: dbData,
              }),
              this.prisma.larkRecord.update({
                where: { id: item.id },
                data: { creative_asset_id: assetId },
              }),
            ]);
            this.logger.log(`✅ Uploaded ${fileType}: ${item.drive.name}`);
          }
        } catch (err: any) {
          this.logger.error(`❌ Upload failed for ${item.drive_id}: ${err.message}`);
        } finally {
          if (filePath && fs.existsSync(filePath)) {
            await fs.promises.unlink(filePath).catch(() => {});
          }
        }
      }),
    );

    await Promise.allSettled(tasks);
  }

  async syncDriveFiles() {
    const now = new Date();
    let pageToken: string | undefined;

    this.logger.log('Scanning files on Google Drive...');

    do {
      const res = await this.driveSA.files.list({
        fields: 'nextPageToken, files(id,name,mimeType,parents,webViewLink,webContentLink,size)',
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
            drive_permission: true,
          },
          create: {
            id,
            raw: JSON.stringify(file),
            name: file.name || 'Untitled',
            mimeType: file.mimeType || null,
            last_seen_at: now,
            webViewLink: file.webViewLink || null,
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

    const deletedFiles = await this.prisma.driveFile.deleteMany({
      where: { last_seen_at: { lt: now } },
    });
    if (deletedFiles.count > 0) {
      this.logger.log(`🗑️ Cleaned up ${deletedFiles.count} files removed from Drive.`);
    }
  }

  private async getAccessToken(): Promise<string> {
    const now = Date.now();
    if (this.accessToken && now < this.expireAt - 60000) return this.accessToken;

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

  async searchRecords(appToken: string, tableId: string, filter: Record<string, any>) {
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
          .map((r) => extractDriveId(r.drive_url))
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

  async ensureFolderMeta() {
    const records = await this.prisma.larkRecord.findMany({
      where: { product_code: { contains: 'SP', mode: 'insensitive' } },
    });

    const tree = this.buildLogicalTree(records);
    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

    // Level 1: Projects
    const projectReqs: FolderRequest[] = tree.map((p) => ({
      name: p.project_name,
      parentId: this.ROOT_META_FOLDER,
    }));
    const projectFolders = await this.ensureFoldersInBatch(projectReqs, api);

    // Level 2: Brands
    const brandReqs: FolderRequest[] = [];
    tree.forEach((p) => {
      const parent = projectFolders.find((f) => f.name === p.project_name);
      if (parent) {
        p.brands.forEach((b) =>
          brandReqs.push({ name: b.brand_name, parentId: parent.id }),
        );
      }
    });
    const brandFolders = await this.ensureFoldersInBatch(brandReqs, api);

    // Level 3: Products
    const productReqs: FolderRequest[] = [];
    tree.forEach((p) => {
      p.brands.forEach((b) => {
        const parent = brandFolders.find((f) => f.name === b.brand_name);
        if (parent) {
          b.products.forEach((prod) =>
            productReqs.push({
              name: prod.product_code,
              description: prod.product_name,
              parentId: parent.id,
            }),
          );
        }
      });
    });
    await this.ensureFoldersInBatch(productReqs, api);
  }

  private async ensureFoldersInBatch(
    requests: FolderRequest[],
    api: FacebookAdsApi,
  ): Promise<MetaFolderResponse[]> {
    if (requests.length === 0) return [];

    const uniqueReqs = requests.filter(
      (v, i, a) =>
        a.findIndex((t) => t.name === v.name && t.parentId === v.parentId) === i,
    );

    const existedDb = await this.prisma.creativeFolder.findMany({
      where: {
        OR: uniqueReqs.flatMap((r) => {
          const conditions = [
            {
              name: { equals: r.name.trim(), mode: "insensitive" as Prisma.QueryMode },
              parentId: r.parentId || null,
            },
          ];
          // If parent is the root meta folder, also check for null parent in DB
          if (r.parentId === this.ROOT_META_FOLDER) {
            conditions.push({
              name: { equals: r.name.trim(), mode: "insensitive" as Prisma.QueryMode },
              parentId: null,
            });
          }
          return conditions;
        }),
      },
    });

    const results: MetaFolderResponse[] = existedDb.map((f) => ({
      id: f.id,
      name: f.name,
      description: f.description,
      parentId: f.parentId,
      creation_time: f.creation_time ? new Date(f.creation_time) : null,
    }));

    const toCreate = uniqueReqs.filter((r) => {
      const rName = r.name.toLowerCase().trim();
      return !existedDb.some((e) => {
        const eName = e.name.toLowerCase().trim();
        const eParentId = e.parentId === null ? this.ROOT_META_FOLDER : e.parentId;
        const rParentId = r.parentId === null ? this.ROOT_META_FOLDER : r.parentId;
        return eName === rName && eParentId === rParentId;
      });
    });

    if (toCreate.length === 0) return results;

    for (let i = 0; i < toCreate.length; i += 50) {
      const batch = toCreate.slice(i, i + 50);
      const metaBatch = batch.map((f) => ({
        method: 'POST',
        relative_url: `${this.BUSINESS_ID}/creative_folders`,
        body: `name=${encodeURIComponent(f.name)}&description=${encodeURIComponent(f.description || '')}&parent_folder_id=${f.parentId || ''}&fields=id,name,description`,
      }));

      try {
        const responses = (await api.call('POST', [''], {
          batch: JSON.stringify(metaBatch),
        })) as Array<any>;

        const dbCreations = responses
          .map((res, idx) => {
            if (res.code === 200 && res.body) {
              const folder = JSON.parse(res.body) as MetaFolderResponse;
              results.push({
                id: folder.id,
                name: folder.name,
                description: folder.description,
                parentId: batch[idx].parentId,
                creation_time: folder.creation_time ? new Date(folder.creation_time) : new Date(),
              });

              return this.prisma.creativeFolder.create({
                data: {
                  id: folder.id,
                  name: folder.name,
                  parentId: batch[idx].parentId || null,
                  description: folder.description,
                  creation_time: folder.creation_time?.toString(),
                },
              });
            }
            return null;
          })
          .filter(Boolean) as Prisma.PrismaPromise<any>[];

        if (dbCreations.length > 0) {
          await this.prisma.$transaction(dbCreations);
        }
      } catch (err) {
        this.logger.error('Failed to create folder batch in Meta', err);
      }
    }
    return results;
  }

  private buildLogicalTree(records: LarkRecord[]): LogicalProject[] {
    const projectMap = new Map<string, LogicalProject>();

    for (const r of records) {
      if (!r.project_code || !r.project_name) continue;

      if (!projectMap.has(r.project_code)) {
        projectMap.set(r.project_code, {
          project_name: r.project_name,
          brands: [],
        });
      }
      const project = projectMap.get(r.project_code)!;

      if (r.brand_code && r.brand_name) {
        let brand = project.brands.find((b) => b.brand_name === r.brand_name);
        if (!brand) {
          brand = { brand_name: r.brand_name, products: [] };
          project.brands.push(brand);
        }

        if (r.product_code && r.product_name) {
          const productExists = brand.products.some((p) => p.product_code === r.product_code);
          if (!productExists) {
            brand.products.push({
              product_code: r.product_code,
              product_name: r.product_name,
            });
          }
        }
      }
    }
    return Array.from(projectMap.values());
  }
}
