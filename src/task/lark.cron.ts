import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { AssetType, LarkRecord, Prisma } from '@prisma/client';
import axios, { AxiosRequestConfig } from 'axios';
import { FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import * as fs from 'fs';
import { drive_v3, google } from 'googleapis';
import path from 'path';
import { PrismaService } from 'src/modules/prisma/prisma.service';
import { pipeline } from 'stream/promises';
import { extractDriveId, mapRecord } from './helper';

import pLimit from 'p-limit';

/* =====================================================
   TYPES & INTERFACES (STRICT TYPING)
===================================================== */
export interface FolderRequest {
  name: string;
  description?: string;
  parentId?: string | null;
}

export interface MetaFolderResponse {
  id: string;
  name: string;
  description?: string | null;
  parentId?: string | null;
  creation_time?: Date | null;
}

// Interfaces cho cây thư mục logic
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
export class LarkCron implements OnModuleInit {
  private readonly logger = new Logger(LarkCron.name);
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

  async onModuleInit() {
    if (!fs.existsSync(this.BASE_DIR)) {
      fs.mkdirSync(this.BASE_DIR, { recursive: true });
    }

    await this.handleMetaUploadWorkflow();
    this.logger.log('🚀 Lark Cron Service Initialized');
  }

  async cleanupDuplicateFolders() {
    this.logger.log(
      '🔍 Đang khởi tạo tiến trình dọn dẹp theo mẻ (Batch Cleanup)...',
    );

    const IS_DRY_RUN = false; // Đổi thành false để thực thi thật
    const LIMIT_DELETE = 100; // Mỗi lần chạy chỉ xử lý tối đa 5 folder

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

    // 1. Phân tích và gom nhóm dữ liệu
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
        let reason = 'Thư mục rỗng, trùng tên/cấp.';

        if (assetCount > 0) {
          status = 'SKIP';
          reason = `⚠️ KHÔNG XOÁ: Đang chứa ${assetCount} assets.`;
        }
        group.duplicates.push({ folder, assetCount, reason, status });
      }
    }

    // 2. Lọc danh sách thực sự cần xóa
    const pendingDeleteList = Array.from(reportGroup.values())
      .flatMap((g) => g.duplicates)
      .filter((d) => d.status === 'DELETE');

    if (pendingDeleteList.length === 0) {
      this.logger.log('✅ Không còn thư mục trùng lặp nào đủ điều kiện xóa.');
      return;
    }

    // Cắt mẻ 5 cái đầu tiên
    const batchToDelete = pendingDeleteList.slice(0, LIMIT_DELETE);

    // ====================================================================
    // BƯỚC 2: IN LOG BÁO CÁO MẺ HIỆN TẠI
    // ====================================================================
    this.logger.log(
      `📊 ====== BÁO CÁO MẺ XÓA (Batch: ${batchToDelete.length}/${pendingDeleteList.length}) ======`,
    );

    batchToDelete.forEach((item, index) => {
      const f = item.folder;
      this.logger.log(
        `${index + 1}. [SẼ XOÁ] Tên: "${f.name}" | ID: ${f.id} | Parent: ${f.parentId || 'root'} }`,
      );
    });

    if (IS_DRY_RUN) {
      this.logger.log(
        '\n🛑 DRY_RUN: ON. Vui lòng kiểm tra 5 folder trên. Đổi IS_DRY_RUN = false để xóa.',
      );
      return;
    }

    // ====================================================================
    // BƯỚC 3: THỰC THI XÓA MẺ 5 FOLDER
    // ====================================================================
    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
    this.logger.log(`🔥 Đang xóa mẻ ${batchToDelete.length} thư mục...`);

    for (const item of batchToDelete) {
      const folderId = item.folder.id;
      try {
        // 1. Xóa trên Meta
        await api.call('DELETE', [folderId]);
        // 2. Xóa trong DB
        await this.prisma.creativeFolder.delete({ where: { id: folderId } });

        this.logger.log(`✅ Thành công: ${folderId}`);
      } catch (error: any) {
        this.logger.error(`❌ Thất bại khi xóa ${folderId}: ${error.message}`);
      }
    }

    const remaining = pendingDeleteList.length - batchToDelete.length;
    this.logger.log(
      `\n✨ Hoàn tất mẻ này. Còn lại khoảng ${remaining} folder trùng lặp cần xử lý ở lần sau.`,
    );
  }
  /* -------------------------------------------------------------------------- */
  /* CRON JOBS                                                                  */
  /* -------------------------------------------------------------------------- */

  // 1. CRON SYNC LARK & DRIVE: Chạy liên tục mỗi 5 phút
  @Cron(CronExpression.EVERY_5_MINUTES, { timeZone: 'Asia/Ho_Chi_Minh' })
  async handleSyncWorkflow() {
    this.logger.log(
      '🔄 [Sync] Starting periodic data sync (Lark <-> Drive)...',
    );
    try {
      await this.searchRecords(
        'VsGGbP5wkaY7uTsTpZ2l9G9HgWc',
        'tblzTv9D1LoUcgcq',
        {
          conjunction: 'and',
          conditions: [
            { field_name: 'Ngày sản xuất', operator: 'is', value: ['Today'] },
          ],
        },
      );

      await this.syncDriveAndLark();
      this.logger.log('✅ [Sync] Data sync completed successfully');
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ [Sync] Critical failure: ${msg}`);
    }
  }

  // 2. CRON META UPLOAD: Chạy mỗi giờ (phút thứ 0 của mỗi giờ) để xử lý đúng 30 files
  @Cron('0 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async handleMetaUploadWorkflow() {
    this.logger.log(
      '🚀 [Meta] Starting hourly Meta upload (Limit: 30 items)...',
    );
    try {
      await this.createFolderMeta();
      // Truyền tham số 10 vào để giới hạn số lượng mỗi mẻ chạy
      await this.uploadDriveToMeta(10);
      this.logger.log('✅ [Meta] Hourly upload completed successfully');
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ [Meta] Critical failure during upload: ${msg}`);
    }
  }

  /* -------------------------------------------------------------------------- */
  /* CORE LOGIC                                                                 */
  /* -------------------------------------------------------------------------- */

  async uploadDriveToMeta(takeLimit: number = 30) {
    const api = new FacebookAdsApi(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);
    const limit = pLimit(3);

    const [folders, cidContents] = await Promise.all([
      this.prisma.creativeFolder.findFirst({
        where: { parentId: null }, // Lấy thư mục gốc (Project)
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
        orderBy: { id: 'asc' }, // Ưu tiên xử lý các bản ghi tạo trước (cũ nhất)
      }),
    ]);

    if (cidContents.length === 0) {
      this.logger.log('ℹ️ [Meta] No new assets to upload this hour.');
      return;
    }

    const tasks = cidContents.map((item) =>
      limit(async () => {
        if (!item.drive || !item.drive.webViewLink || !item.drive_id)
          return null;

        // ====================================================================
        // 🛡️ CHỐNG TRÙNG LẶP ASSET: Kiểm tra drive_id đã từng upload Meta chưa
        // ====================================================================
        const existingAsset = await this.prisma.creativeAsset.findFirst({
          where: { drive_id: item.drive_id },
        });

        if (existingAsset) {
          this.logger.log(
            `♻️ [Skip] File ${item.drive.name} already in Meta. Mapping ID only.`,
          );
          await this.prisma.larkRecord.update({
            where: { id: item.id },
            data: { creative_asset_id: existingAsset.id },
          });
          return; // Skip Meta upload
        }
        // ====================================================================

        const project = folders.children.find(
          (f) => f.name === item.project_name,
        );
        const brand = project?.children?.find(
          (b) => b.name === item.brand_name,
        );
        const product = brand?.children?.find(
          (p) => p.name === item.product_code,
        );

        if (!product) {
          this.logger.warn(
            `Folder path not found for record: ${item.id}, ${item.project_code} > ${item.brand_code} > ${item.product_code}`,
          );
          return null;
        }
        const fileType = item.drive.mimeType?.startsWith('image')
          ? AssetType.IMAGE
          : AssetType.VIDEO;
        let assetId: string | null = null;
        let filePath: string | null = null;

        try {
          if (fileType === AssetType.IMAGE) {
            const driveRes = await this.driveSA.files.get(
              { fileId: item.drive_id, alt: 'media', supportsAllDrives: true },
              { responseType: 'arraybuffer' },
            );
            const buffer = Buffer.from(driveRes.data as ArrayBuffer);

            const res = (await api.call('POST', [this.BUSINESS_ID, 'images'], {
              name: item.drive.name,
              bytes: buffer.toString('base64'),
              creative_folder_id: product.id,
            })) as Record<string, any>;

            assetId =
              (Object.values(res.images || {})?.[0] as { id?: string })?.id ||
              null;
          } else {
            filePath = path.join(this.BASE_DIR, `${item.drive_id}.mp4`);
            const driveRes = await this.driveSA.files.get(
              { fileId: item.drive_id, alt: 'media', supportsAllDrives: true },
              { responseType: 'stream' },
            );

            await pipeline(
              driveRes.data as NodeJS.ReadableStream,
              fs.createWriteStream(filePath),
            );

            const cdnUrl = `${process.env.FRONT_END_DOMAIN}/cdn/${item.drive_id}.mp4`;
            const res = (await api.call('POST', [this.BUSINESS_ID, 'videos'], {
              title: item.drive.name,
              file_url: cdnUrl,
              creative_folder_id: product.id,
            })) as Record<string, any>;

            assetId = res?.business_video_id || null;
          }

          if (assetId) {
            await this.prisma.$transaction([
              this.prisma.creativeAsset.upsert({
                where: { id: assetId },
                update: {
                  name: item.drive.name,
                  drive_url: item.drive.webViewLink,
                  folderId: product.id,
                  type: fileType,
                  drive_id: item.drive_id,
                },
                create: {
                  id: assetId,
                  name: item.drive.name,
                  drive_url: item.drive.webViewLink,
                  folderId: product.id,
                  type: fileType,
                  drive_id: item.drive_id,
                },
              }),
              this.prisma.larkRecord.update({
                where: { id: item.id },
                data: { creative_asset_id: assetId },
              }),
            ]);
            this.logger.log(`✅ Uploaded ${fileType}: ${item.drive.name}`);
          }
        } catch (err: unknown) {
          const msg = err instanceof Error ? err.message : String(err);
          this.logger.error(`❌ Upload failed for ${item.drive_id}: ${msg}`);
        } finally {
          if (filePath && fs.existsSync(filePath)) {
            await fs.promises.unlink(filePath).catch(() => {});
          }
        }
      }),
    );

    await Promise.allSettled(tasks);
  }

  async syncDriveAndLark() {
    const now = new Date();
    let pageToken: string | undefined;

    this.logger.log('Bắt đầu quét toàn bộ file trên Google Drive...');

    // =========================================================
    // 1. QUÉT & LƯU TOÀN BỘ FILE TRÊN DRIVE VÀO DB
    // =========================================================
    do {
      const res = await this.driveSA.files.list({
        fields:
          'nextPageToken, files(id,name,mimeType,parents,webViewLink,webContentLink,size)',
        pageSize: 1000, // Tăng lên 1000 (mức max của Google) để giảm số lần gọi API, chạy nhanh hơn
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
        pageToken,
      });

      const files = res.data.files || [];

      const driveOps = files.map((file) => {
        const id = file.id || '';
        const name = file.name || 'Untitled';
        const webViewLink = file.webViewLink || null;
        const mimeType = file.mimeType || null;

        return this.prisma.driveFile.upsert({
          where: { id },
          update: {
            name,
            last_seen_at: now, // Cập nhật thời gian nhìn thấy lần cuối
            webViewLink,
            drive_permission: true,
          },
          create: {
            id,
            raw: JSON.stringify(file),
            name,
            mimeType,
            last_seen_at: now,
            webViewLink,
            drive_permission: true,
          },
        });
      });

      if (driveOps.length > 0) {
        await this.prisma.$transaction(driveOps);
      }

      pageToken = res.data.nextPageToken || undefined;
    } while (pageToken);

    // =========================================================
    // 2. XÓA CÁC FILE ĐÃ BỊ THU HỒI QUYỀN / BỊ XÓA KHỎI DRIVE
    // =========================================================
    // Những file nào có last_seen_at bé hơn "now" nghĩa là trong lần quét vừa rồi Google không trả về -> Xóa luôn
    const deletedFiles = await this.prisma.driveFile.deleteMany({
      where: { last_seen_at: { lt: now } },
    });
    if (deletedFiles.count > 0) {
      this.logger.log(
        `🗑️ Đã dọn dẹp (xóa) ${deletedFiles.count} files bị thu hồi quyền hoặc đã xóa khỏi Drive.`,
      );
    }
  }

  /* -------------------------------------------------------------------------- */
  /* HELPERS                                                                    */
  /* -------------------------------------------------------------------------- */

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
      console.log(items.length + ' records fetched from Lark');
      if (items.length > 0) {
        const mapped = items.map(
          (item: any) =>
            mapRecord(item) as unknown as Prisma.LarkRecordCreateInput,
        );

        await this.upsertLarkBatch(mapped);
      }

      hasMore = res.data.has_more;
      pageToken = res.data.page_token;
    }
  }

  private async upsertLarkBatch(records: Prisma.LarkRecordCreateInput[]) {
    const validRecords = records.filter((r) => r.id);
    const ids = validRecords.map((r) => String(r.id));

    const uniqueDriveIds = Array.from(
      new Set(
        validRecords
          .map((r) => {
            const drive_id = extractDriveId(r.drive_url);
            return drive_id;
          })
          .filter(Boolean),
      ),
    ) as string[];

    console.log(
      uniqueDriveIds.length + ' unique drive IDs to map in Lark records...',
    );

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
            data: toCreate as Prisma.LarkRecordCreateManyInput[],
            skipDuplicates: true,
          });
        }

        if (uniqueDriveIds.length > 0) {
          const driveData = uniqueDriveIds.map((id) => ({
            id,
            name: 'Pending Sync...', // Tên tạm thời
            raw: '{}',
            drive_permission: false, // Tạm thời coi như chưa có quyền, chờ Google check
          }));

          await tx.driveFile.createMany({
            data: driveData as any[],

            skipDuplicates: true, // Nếu ID này đã có ở lần chạy trước rồi thì thôi, không ghi đè
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

  async createFolderMeta() {
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

    // ====================================================================
    // 🛡️ CHỐNG TRÙNG LẶP FOLDER TÊN CON TRONG FOLDER CHA
    // Dòng dưới đây đảm bảo: Nếu mảng truyền vào có 2 thư mục tên "SP01"
    // và có chung 1 parentId, nó sẽ gộp lại thành 1 request duy nhất.
    // ====================================================================
    const uniqueReqs = requests.filter(
      (v, i, a) =>
        a.findIndex((t) => t.name === v.name && t.parentId === v.parentId) ===
        i,
    );

    // Lấy những thư mục "Đã tồn tại trong DB theo điều kiện Name + ParentId"
    const existedDb = await this.prisma.creativeFolder.findMany({
      where: {
        OR: uniqueReqs.map((r) => ({
          name: r.name,
          parentId: r.parentId || null,
        })),
      },
    });

    const results: MetaFolderResponse[] = existedDb.map((f) => ({
      id: f.id,
      name: f.name,
      description: f.description,
      parentId: f.parentId,
      creation_time: f.creation_time ? new Date(f.creation_time) : null,
    }));

    // Bỏ qua những folder đã có, chỉ tạo folder MỚI
    const toCreate = uniqueReqs.filter(
      (r) =>
        !existedDb.some((e) => e.name === r.name && e.parentId === r.parentId),
    );
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
        })) as Array<Record<string, any>>;

        const dbCreations = responses
          .map((res, idx) => {
            if (res.code === 200 && res.body) {
              const folder = JSON.parse(res.body) as MetaFolderResponse;
              results.push({
                id: folder.id,
                name: folder.name,
                description: folder.description,
                parentId: batch[idx].parentId,
                creation_time: folder.creation_time
                  ? new Date(folder.creation_time)
                  : new Date(),
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
          const productExists = brand.products.some(
            (p) => p.product_code === r.product_code,
          );
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
