import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as fs from 'fs';

import { drive_v3, google } from 'googleapis';
import { PrismaService } from 'src/modules/prisma/prisma.service';

import { Cron } from '@nestjs/schedule';
import { AssetType, LarkRecord } from '@prisma/client';
import axios, { AxiosRequestConfig } from 'axios';
import { FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import path from 'path';
import { chunk } from 'src/common/utils';
import { pipeline } from 'stream/promises';
import { mapRecord } from './helper';

/* =====================================================
   CRON SERVICE
===================================================== */

@Injectable()
export class LarkCron implements OnModuleInit {
  constructor(private readonly prisma: PrismaService) {
    const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON!);

    credentials.private_key = credentials.private_key.replace(/\\n/g, '\n');

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/drive'],
    });

    this.driveSA = google.drive({ version: 'v3', auth });
  }

  private readonly logger = new Logger(LarkCron.name);
  private driveSA: drive_v3.Drive;

  private baseURL = 'https://open.larksuite.com/open-apis/bitable/v1';

  private accessToken: string | null = null;
  private expireAt = 0;

  async onModuleInit() {
    this.logger.log('🚀 Lark initialized');
    await this.uploadContentToMeta();
  }

  /**
   * ================================
   * 🔹 CORE DATA (1 lần / ngày)
   * ================================
   */

  @Cron('*/8 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async uploadContentToMeta() {
    this.logger.log('🔄 uploadContentToMeta BEGIN');
    // await this.uploadDriveToMeta();
    this.logger.log('✅ uploadContentToMeta DONE');
  }

  @Cron('*/5 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncLarkContent() {
    this.logger.log('🔄 Sync lark Core');
    await this.searchRecords(
      'VsGGbP5wkaY7uTsTpZ2l9G9HgWc',
      'tblzTv9D1LoUcgcq',
      {
        conjunction: 'and',
        conditions: [
          { field_name: 'Ngày sản xuất', operator: 'is', value: ['Today'] },
        ],
      },
      500,
    );
    await this.syncDriveAndLark();

    await this.createFolderMeta();

    this.logger.log('✅ Sync lark Core DONE');
  }

  // HELPER
  /* -------------------------------------------------------------------------- */
  /*                               SEARCH RECORDS                               */
  /* -------------------------------------------------------------------------- */

  /* -------------------------------------------------------------------------- */
  /*                              AUTH TOKEN                                    */
  /* -------------------------------------------------------------------------- */

  private async getAccessToken(): Promise<string> {
    const now = Date.now();

    // còn hạn thì dùng
    if (this.accessToken && now < this.expireAt - 60000) {
      return this.accessToken;
    }

    const res = await axios.post(
      'https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal',
      {
        app_id: process.env.LARK_APP_ID,
        app_secret: process.env.LARK_APP_SECRET,
      },
    );

    if (res.data.code !== 0) {
      this.logger.error('Lark auth failed', res.data);
      throw new Error('Cannot get Lark access token');
    }

    this.accessToken = res.data.tenant_access_token;
    this.expireAt = now + res.data.expire * 1000;

    this.logger.log('Lark token refreshed');

    return this.accessToken!;
  }

  /* -------------------------------------------------------------------------- */
  /*                              REQUEST WRAPPER                               */
  /* -------------------------------------------------------------------------- */

  private async request(config: AxiosRequestConfig): Promise<any> {
    const token = await this.getAccessToken();

    const res = await axios({
      baseURL: this.baseURL,
      ...config,
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json; charset=utf-8',
        ...(config.headers || {}),
      },
    });

    return res.data;
  }

  async uploadDriveToMeta() {
    const [folders, cidContents] = await Promise.all([
      this.prisma.creativeFolder.findMany({
        where: { parent: { parentId: null } },
        select: {
          name: true,
          id: true,
          children: {
            select: {
              name: true,
              id: true,
              children: {
                select: { name: true, id: true },
              },
            },
          },
        },
      }),
      this.prisma.larkRecord.findMany({
        orderBy: [{ production_date: 'asc' }],
        select: {
          production_date: true,
          brand_code: true,
          project_code: true,
          product_code: true,
          drive: {
            select: {
              id: true,
              webViewLink: true,
              webContentLink: true,
              mimeType: true,
              name: true,
            },
          },
        },
        where: {
          drive: { drive_permission: true },
          creative_asset_id: null,
        },
      }),
    ]);

    // =========================
    // 🔥 Normalize
    // =========================
    const normalizedData = cidContents
      .map((item) => {
        const { project_code, brand_code, product_code, drive } = item;

        if (!drive?.webViewLink) return null;

        const project = folders.find((f) => f.name === project_code);
        if (!project) return null;

        const brand = project.children?.find((b) => b.name === brand_code);
        if (!brand) return null;

        const product = brand.children?.find((p) => p.name === product_code);
        if (!product) return null;

        const isImage = drive.mimeType?.startsWith('image');

        return {
          name: drive.name,
          urlDownload: drive.webContentLink,
          urlView: drive.webViewLink,
          type: isImage ? AssetType.IMAGE : AssetType.VIDEO,
          folderId: product.id,
          drive_id: drive.id,
        };
      })
      .filter(Boolean);

    // =========================
    // 🔥 ensure folder exists
    // =========================
    const BASE_DIR = '/app/files';

    if (!fs.existsSync(BASE_DIR)) {
      fs.mkdirSync(BASE_DIR, { recursive: true });
    }

    // =========================
    // 🔥 Batch upload
    // =========================
    const BATCH_SIZE = 3;
    const uploadResults: any[] = [];

    for (let i = 0; i < normalizedData.length; i += BATCH_SIZE) {
      const batch = normalizedData.slice(i, i + BATCH_SIZE);

      const batchResults = await Promise.allSettled(
        batch.map(async (item) => {
          let needRevert = false;

          try {
            const api = new FacebookAdsApi(
              process.env.SDK_FACEBOOK_ACCESS_TOKEN!,
            );

            const ext = item.type === AssetType.IMAGE ? '.jpg' : '.mp4';

            const filePath = path.join(BASE_DIR, `${item.drive_id}_${ext}`);

            // =========================
            // 🔥 DRIVE API DOWNLOAD
            // =========================
            const driveRes = await this.driveSA.files.get(
              {
                fileId: item.drive_id,
                alt: 'media',
              },
              { responseType: 'stream' },
            );

            if (item.type === AssetType.IMAGE) {
              await pipeline(driveRes.data, fs.createWriteStream(filePath));
            } else {
              await pipeline(driveRes.data, fs.createWriteStream(filePath));
            }

            return filePath;
          } catch (err) {
            console.error('❌ Upload fail:', item.name, err);
            return { success: false, error: err, item };
          }
        }),
      );

      uploadResults.push(...batchResults);
    }

    // =========================
    // 🔥 SUCCESS FILTER
    // =========================
    const successUploads = uploadResults
      .filter((r: any) => r.value?.success)
      .map((r: any) => r.value);

    if (!successUploads.length) {
      return { uploadResults };
    }

    // =========================
    // 🔥 DB UPSERT
    // =========================
    const dbResults = await Promise.allSettled(
      successUploads.map(({ assetId, item }) =>
        this.prisma.creativeAsset.upsert({
          where: { id: assetId },
          update: {
            drive_url: item.urlView,
            name: item.name,
            folderId: item.folderId,
            type: item.type,
          },
          create: {
            id: assetId,
            drive_url: item.urlView,
            name: item.name,
            folderId: item.folderId,
            type: item.type,
          },
        }),
      ),
    );

    // =========================
    // 🔥 UPDATE LARK RECORD
    // =========================
    await Promise.allSettled(
      successUploads.map(({ assetId, item }) =>
        this.prisma.larkRecord.updateMany({
          where: { drive_id: item.drive_id },
          data: {
            creative_asset_id: assetId,
          },
        }),
      ),
    );

    console.log('✅ DONE ALL');

    return {
      uploadResults,
      dbResults,
    };
  }

  async syncDriveAndLark() {
    let pageToken: string | undefined;
    const now = new Date();

    // ========= HELPERS =========
    const extractDriveId = (url?: string | null): string | null => {
      if (!url) return null;

      const match1 = url.match(/\/d\/([a-zA-Z0-9_-]+)/);
      if (match1) return match1[1];

      const match2 = url.match(/[?&]id=([a-zA-Z0-9_-]+)/);
      if (match2) return match2[1];

      return null;
    };

    const mapDriveFile = (file: any) => ({
      id: file.id,
      raw: file,
      parentId: file.parents?.[0] || null,
      name: file.name,
      mimeType: file.mimeType,
      webContentLink: file.webContentLink,
      webViewLink: file.webViewLink,
      size: file.size,
      drive_permission: true,
      last_seen_at: now,
    });

    // ========= 1. SYNC DRIVE =========
    do {
      const res = await this.driveSA.files.list({
        fields:
          'nextPageToken, files(id,name,mimeType,parents,webViewLink,webContentLink,size)',
        pageSize: 100,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
        pageToken,
      });

      const files = res.data.files || [];
      // 🚀 batch upsert
      await Promise.all(
        files.map((file) =>
          this.prisma.driveFile.upsert({
            where: { id: file.id },
            update: mapDriveFile(file),
            create: mapDriveFile(file),
          }),
        ),
      );

      pageToken = res.data.nextPageToken || undefined;
    } while (pageToken);

    // ========= 2. MARK FILE DELETED =========
    await this.prisma.driveFile.updateMany({
      where: { last_seen_at: { lt: now } },
      data: { drive_permission: false },
    });

    // ========= 3. PRELOAD DATA =========
    const [records, driveFiles, creativeAssets] = await Promise.all([
      this.prisma.larkRecord.findMany({
        where: { drive_url: { not: null } },
        select: { id: true, drive_url: true },
      }),
      this.prisma.driveFile.findMany({
        select: { id: true, name: true, webViewLink: true },
      }),
      this.prisma.creativeAsset.findMany({
        select: {
          id: true,
          name: true,
          drive_id: true,
          folder: {
            select: {
              id: true,
              name: true,
              parent: {
                select: {
                  id: true,
                  name: true,
                  parent: { select: { id: true, name: true } },
                },
              },
            },
          },
        },
      }),
    ]);

    // ========= 4. BUILD MAP =========
    const driveMap = new Map(driveFiles.map((d) => [d.id, d]));
    const creativeMap = new Map(creativeAssets.map((c) => [c.name, c]));
    // ========= 5. PREPARE UPDATE =========
    const larkUpdates: any[] = [];
    const creativeUpdates: any[] = [];

    for (const r of records) {
      const driveId = extractDriveId(r.drive_url);
      if (!driveId) continue;

      const driveItem = driveMap.get(driveId);
      if (!driveItem) continue;

      // update Creative
      const creative = creativeMap.get(driveItem.name);

      larkUpdates.push({
        where: { id: r.id },
        data: {
          drive_id: driveId,
          ...(creative && { creative_asset_id: creative.id }), // 👈 thêm dòng này
        },
      });
      if (creative && creative.drive_id !== driveId) {
        creativeUpdates.push({
          where: { id: creative.id },
          data: {
            drive_id: driveItem.id,
            drive_url: driveItem.webViewLink,
          },
        });
      }
    }

    // ========= 6. EXECUTE BATCH =========
    const chunkSize = 100;

    const chunk = (arr: any[], size: number) =>
      Array.from({ length: Math.ceil(arr.length / size) }, (_, i) =>
        arr.slice(i * size, i * size + size),
      );

    for (const batch of chunk(larkUpdates, chunkSize)) {
      await Promise.all(batch.map((u) => this.prisma.larkRecord.update(u)));
    }

    for (const batch of chunk(creativeUpdates, chunkSize)) {
      await Promise.all(batch.map((u) => this.prisma.creativeAsset.update(u)));
    }

    return true;
  }

  async searchRecords(
    appToken: string,
    tableId: string,
    filter: any,
    pageSize = 500,
  ) {
    let hasMore = true;
    let pageToken: string | undefined;
    const seenTokens = new Set<string>();

    while (hasMore) {
      const res = await this.request({
        method: 'POST',
        url: `/apps/${appToken}/tables/${tableId}/records/search`,
        params: { page_size: pageSize, page_token: pageToken },
        data: { automatic_fields: false, filter },
      });

      if (res.code !== 0) {
        this.logger.error('Lark searchRecords error', res);
        break;
      }

      const data = res.data;
      const items = data.items || [];
      if (items.length === 0) break;

      // 🔥 map data
      const mapped = items.map((item) => mapRecord(item)) as LarkRecord[];
      await this.upsertBatch(mapped);

      this.logger.log(
        `Fetched ${data.items?.length || 0} filtered records (total: ${data.total} / ${data?.items?.length})`,
      );

      // 🛑 chống loop vô hạn
      if (pageToken && seenTokens.has(pageToken)) {
        this.logger.warn('Duplicate page_token → stop');
        break;
      }

      if (pageToken) seenTokens.add(pageToken);

      hasMore = data.has_more;
      pageToken = data.page_token;
    }
    return true;
  }

  async upsertBatch(records: LarkRecord[]) {
    const ids = records.map((r) => r.id);

    const existing = await this.prisma.larkRecord.findMany({
      where: { id: { in: ids } },
      select: { id: true },
    });

    const existingIds = new Set(existing.map((e) => e.id));

    const toCreate = [] as LarkRecord[];
    const toUpdate = [] as LarkRecord[];

    for (const r of records) {
      if (existingIds.has(r.id)) {
        toUpdate.push(r);
      } else {
        toCreate.push(r);
      }
    }
    await this.prisma.$transaction(
      async (tx) => {
        // CREATE MANY
        if (toCreate.length > 0) {
          await tx.larkRecord.createMany({
            data: toCreate as any,
            skipDuplicates: true,
          });
        }

        // UPDATE (loop)
        for (const r of toUpdate) {
          await tx.larkRecord.update({
            where: { id: r.id },
            data: r as any,
          });
        }
      },
      { timeout: 50000 },
    );
  }

  // Helper: Xử lý Batch cho một danh sách folder cùng cấp

  async createFolderMeta() {
    // 1. Lấy dữ liệu và xây dựng cây thư mục logic
    const records = await this.prisma.larkRecord.findMany({
      where: { product_code: { contains: 'SP', mode: 'insensitive' } },
    });
    const tree = await this.buildTree(records);
    const businessId = '1916878948527753';
    const rootParentId = '4303729193176038';

    // --- LEVEL 1: PROJECTS ---
    const projectFolders = await this.ensureFoldersInBatch(
      tree.map((p) => ({
        name: p.project_name,
        description: p.project_name,
        parentId: rootParentId,
      })),
      businessId,
    );
    // --- LEVEL 2: BRANDS ---
    const brandRequests = [] as FolderRequest[];

    for (const project of tree) {
      const parentFolder = projectFolders.find(
        (f) => f.name === project.project_name,
      );
      if (parentFolder) {
        project.brands.forEach((brand) => {
          brandRequests.push({
            name: brand.brand_name,
            description: brand.brand_name,
            parentId: parentFolder.id,
          });
        });
      }
    }
    const brandFolders = await this.ensureFoldersInBatch(
      brandRequests,
      businessId,
    );
    // --- LEVEL 3: PRODUCTS ---
    const productRequests = [] as FolderRequest[];
    for (const project of tree) {
      project.brands.forEach((brand) => {
        const parentFolder = brandFolders.find(
          (f) => f.name === brand.brand_name,
        );
        if (parentFolder) {
          brand.products.forEach((product) => {
            productRequests.push({
              name: product.product_code,
              description: product.product_name,
              parentId: parentFolder.id,
            });
          });
        }
      });
    }
    await this.ensureFoldersInBatch(productRequests, businessId);

    return { message: 'Done sync folders in batch' };
  }

  async ensureFoldersInBatch(
    folderRequests: FolderRequest[],
    businessId: string,
  ): Promise<MetaFolderResponse[]> {
    const results: MetaFolderResponse[] = [];

    // 1. Lấy danh sách đã tồn tại trong DB
    const existedInDb = await this.prisma.creativeFolder.findMany({
      where: {
        OR: folderRequests.map((f) => ({
          name: f.name,
          parentId: f.parentId,
        })),
      },
    });

    // Convert DB -> response format
    const existedFormatted: MetaFolderResponse[] = existedInDb.map((f) => ({
      id: f.id,
      name: f.name,
      description: f.description ?? undefined,
      creation_time: f.creation_time ?? undefined,
    }));

    results.push(...existedFormatted);

    // 2. Filter folder cần tạo
    const foldersToCreate = folderRequests.filter(
      (req) =>
        !existedInDb.some(
          (db) => db.name === req.name && db.parentId === req.parentId,
        ),
    );

    if (foldersToCreate.length === 0) return results;
    const apiChunks = chunk(foldersToCreate, 50);
    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

    for (const batch of apiChunks) {
      const metaRequests = batch.map((f) => ({
        method: 'POST',
        relative_url: `${businessId}/creative_folders`,
        body: `name=${encodeURIComponent(f.name)}&description=${encodeURIComponent(
          f.description || '',
        )}&parent_folder_id=${f.parentId ?? ''}&fields=id,name,description,creation_time`,
      }));

      try {
        const responses = (await api.call('POST', [''], {
          batch: JSON.stringify(metaRequests),
        })) as Array<{
          code: number;
          body: string;
        }>;
        const dbOps: any[] = [];

        responses.forEach((res, index) => {
          if (res.code === 200) {
            try {
              const folder: MetaFolderResponse = JSON.parse(res.body);

              results.push(folder);

              dbOps.push(
                this.prisma.creativeFolder.create({
                  data: {
                    id: folder.id,
                    name: folder.name,
                    description: folder.description ?? null,
                    parentId: batch[index].parentId ?? null,
                    creation_time: folder.creation_time ?? null,
                  },
                }),
              );
            } catch (err) {
              console.error('Parse folder error', err);
            }
          }
        });

        if (dbOps.length > 0) {
          await this.prisma.$transaction(dbOps);
        }
      } catch (error) {
        console.error('Batch creation failed', error);
      }
    }

    return results;
  }
  async buildTree(records: any[]): Promise<
    Array<{
      id: string | undefined;
      project_code: string;
      project_name: string;
      brands: Array<{
        id: string | undefined;
        brand_code: string;
        brand_name: string;
        products: Array<{
          id: string | undefined;
          product_code: string;
          product_name: string;
        }>;
      }>;
    }>
  > {
    const projectMap = new Map();
    const folders = await this.prisma.creativeFolder.findMany({
      select: { id: true, name: true },
    });
    for (const r of records) {
      if (!r.project_code) continue;

      // 🟢 PROJECT
      if (!projectMap.has(r.project_code)) {
        projectMap.set(r.project_code, {
          id: folders.find((f) => f.name == r.project_name)?.id,
          project_code: r.project_code,
          project_name: r.project_name,
          brands: new Map(),
        });
      }

      const project = projectMap.get(r.project_code);

      // 🔵 BRAND
      if (r.brand_code) {
        if (!project.brands.has(r.brand_code)) {
          project.brands.set(r.brand_code, {
            id: folders.find((f) => f.name == r.brand_name)?.id,
            brand_code: r.brand_code,
            brand_name: r.brand_name,
            products: new Map(),
          });
        }

        const brand = project.brands.get(r.brand_code);

        // 🟣 PRODUCT
        if (r.product_code) {
          if (!brand.products.has(r.product_code)) {
            brand.products.set(r.product_code, {
              id: folders.find((f) => f.name == r.product_name)?.id,
              product_code: r.product_code,
              product_name: r.product_name,
            });
          }
        }
      }
    }

    // 🔄 convert Map → Array
    return Array.from(projectMap.values()).map((p) => ({
      id: p.id,
      project_code: p.project_code,
      project_name: p.project_name,
      brands: Array.from(p.brands.values()).map((b: any) => ({
        id: b.id,
        brand_code: b.brand_code,
        brand_name: b.brand_name,
        products: Array.from(b.products.values()),
      })),
    }));
  }

  // HELPER DRIVE
  async addPublicPermission(fileId: string) {
    const res = await this.driveSA.permissions.create({
      fileId,
      requestBody: { role: 'reader', type: 'anyone' },
    });

    return res.data; // chứa permissionId
  }

  async removePublicPermission(fileId: string) {
    // 1. Lấy danh sách permission
    const res = await this.driveSA.permissions.list({
      fileId,
      fields: 'permissions(id,type)',
    });

    const anyonePermission = res.data.permissions?.find(
      (p) => p.type === 'anyone',
    );

    if (!anyonePermission) {
      return { message: 'No public permission found' };
    }

    // 2. Xóa permission
    await this.driveSA.permissions.delete({
      fileId,
      permissionId: anyonePermission.id!,
    });

    return { message: 'Public permission removed' };
  }
}
