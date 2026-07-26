import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import { FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import { chunk, executeMetaApiWithRetry } from '../../common/utils';
import { ObjectStorageService } from '../object-storage/object-storage.service';
import { PrismaService } from '../prisma/prisma.service';

// Post đã chết → Meta không trả thumbnail, bỏ qua khỏi vòng re-host orphan cho đỡ tốn call.
const ORPHAN_SKIP_TYPES = ['POST_DELETED', 'PRIVACY_CHECK_FAIL'];

/**
 * Re-host THUMBNAIL VIDEO của Creative LIVE lên MinIO để hết vỡ ảnh ở list/detail mẫu QC.
 *
 * CHỈ video — ảnh Meta KHÔNG cần re-host:
 *   - Ảnh: AdImage.url là URL bền dạng facebook.com/ads/image/?d=... (redirect vĩnh viễn,
 *     không hết hạn) → không đụng tới, giữ nguyên dùng thẳng URL Meta.
 *   - Video: thumbnail lấy từ fbcdn kèm tham số oe= (hết hạn theo thời gian) → PHẢI re-host.
 * Chọn BẢN NÉT nhất đã có sẵn trong DB (khỏi gọi thêm Meta):
 *   uri có width lớn nhất trong AdVideo.rawPayload.thumbnails.data[] (~1080px), fallback
 *   AdVideo.thumbnailUrl → Creative.thumbnailUrl → Creative.previewUrl.
 * Dedup theo key `creative/vid/<videoId>` → mỗi video upload 1 lần; nhiều creative dùng
 * chung 1 video → cùng ownedImageUrl. Chỉ xử lý ownedImageUrl == null.
 * Độ tươi gate theo AdVideo.urlExpiredAt (KHÔNG phải Creative.urlExpiredAt) — AdVideo được
 * meta-media-sync.syncAdVideo() refresh mỗi 3h (source/rawPayload.thumbnails/urlExpiredAt),
 * nên video sắp/đã hết hạn tự có urlExpiredAt tương lai và quay lại hàng chờ đúng lúc.
 */
@Injectable()
export class CreativeMediaService {
  private readonly logger = new Logger(CreativeMediaService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly storage: ObjectStorageService,
  ) {}

  async rehostPendingCreatives(options?: { limit?: number }) {
    const limit = options?.limit ?? this.getLimit();
    const now = new Date();

    const candidates = await this.prisma.creative.findMany({
      where: {
        ownedImageUrl: null,
        videoId: { not: null },
        // Bỏ qua video có AdVideo.urlExpiredAt ĐÃ hết hạn (tải là 403/410) → job KHÔNG
        // quay vòng vô ích trên hàng chết mỗi giờ. AdVideo được meta-media-sync.syncAdVideo()
        // refresh mỗi 3h sẽ có urlExpiredAt tương lai → tự quay lại hàng chờ.
        adVideo: { is: { OR: [{ urlExpiredAt: null }, { urlExpiredAt: { gt: now } }] } },
      },
      select: {
        id: true,
        videoId: true,
        thumbnailUrl: true,
        previewUrl: true,
        adVideo: { select: { thumbnailUrl: true, rawPayload: true, urlExpiredAt: true } },
      },
      // ưu tiên creative mới cập nhật (URL còn sống → tải được), tránh URL đã chết
      orderBy: { updatedAt: 'desc' },
      take: limit,
    });

    if (candidates.length === 0) {
      this.logger.log('No creatives pending media re-host');
      return { candidates: 0, uploaded: 0, linked: 0, skipped: 0, failed: 0 };
    }

    // cache key → publicUrl trong 1 lượt (nhiều creative cùng video/ảnh) → khỏi HeadObject lặp
    const keyUrlCache = new Map<string, string>();
    let uploaded = 0;
    let linked = 0;
    let skipped = 0;
    let failed = 0;

    for (const creative of candidates) {
      const resolved = this.resolveBestThumb(creative);
      if (!resolved) {
        skipped++;
        continue;
      }

      try {
        let publicUrl = keyUrlCache.get(resolved.key);

        if (!publicUrl) {
          if (await this.storage.exists(resolved.key)) {
            // asset đã re-host ở lượt/creative trước → tái dùng, khỏi tải lại
            publicUrl = this.storage.publicUrl(resolved.key);
          } else {
            const media = await this.download(resolved.sourceUrl);
            publicUrl = await this.storage.put(
              resolved.key,
              media.buffer,
              media.contentType,
            );
            uploaded++;
          }
          keyUrlCache.set(resolved.key, publicUrl);
        }

        await this.prisma.creative.update({
          where: { id: creative.id },
          data: {
            ownedImageUrl: publicUrl,
            ownedImageKey: resolved.key,
            ownedImageAt: new Date(),
          },
        });
        linked++;
      } catch (error: any) {
        failed++;
        this.logger.warn(
          `[${creative.id}] re-host failed (${resolved.key}): ${error?.message || error}`,
        );
      }
    }

    this.logger.log(
      `Creative media re-host finished: candidates=${candidates.length}, uploaded=${uploaded}, linked=${linked}, skipped=${skipped}, failed=${failed}`,
    );

    return { candidates: candidates.length, uploaded, linked, skipped, failed };
  }

  /**
   * Re-host CREATIVE ORPHAN (SHARE/STATUS/PHOTO… không có videoId/imageId). URL trong DB của
   * nhóm này là fbcdn có oe= và ĐÃ chết (403) + không job nào refresh → phải gọi Meta lấy
   * `thumbnail_url` TƯƠI 1 lần rồi tải + đẩy MinIO. Sau khi ownedImageUrl set, filter
   * `ownedImageUrl IS NULL` đảm bảo KHÔNG bao giờ lấy lại (dù URL Meta có hết hạn tiếp).
   */
  async rehostPendingOrphans(options?: { limit?: number }) {
    const limit = options?.limit ?? this.getOrphanLimit();
    this.initMetaSdk();

    const candidates = await this.prisma.creative.findMany({
      where: {
        ownedImageUrl: null,
        videoId: null,
        imageId: null,
        AND: [
          {
            OR: [
              { creativeType: null },
              { creativeType: { notIn: ORPHAN_SKIP_TYPES } },
            ],
          },
          {
            OR: [
              { thumbnailUrl: { not: null } },
              { imageUrl: { not: null } },
              { previewUrl: { not: null } },
            ],
          },
        ],
      },
      select: { id: true },
      // ít-được-thử-nhất trước; ca Meta không trả ảnh sẽ bị mark lastFetchedAt → chìm xuống cuối
      orderBy: { lastFetchedAt: 'asc' },
      take: limit,
    });

    if (candidates.length === 0) {
      this.logger.log('No orphan creatives pending re-host');
      return { candidates: 0, uploaded: 0, linked: 0, skipped: 0, failed: 0 };
    }

    let uploaded = 0;
    let linked = 0;
    let skipped = 0;
    let failed = 0;
    const noMediaIds: string[] = []; // Meta không trả ảnh → mark để khỏi quay vòng

    for (const idsChunk of chunk(
      candidates.map((c) => c.id),
      50,
    )) {
      let response: any;
      try {
        const api = FacebookAdsApi.getDefaultApi();
        response = await executeMetaApiWithRetry(
          () =>
            api.call('GET', [], {
              ids: idsChunk.join(','),
              fields: 'thumbnail_url,image_url',
            }),
          { logger: this.logger, context: { ids: idsChunk } },
        );
      } catch (error: any) {
        failed += idsChunk.length;
        this.logger.warn(
          `orphan Meta fetch failed (${idsChunk.length} ids): ${error?.message || error}`,
        );
        continue;
      }

      for (const id of idsChunk) {
        const data = (response as any)?.[id];
        const freshUrl: string | undefined =
          data?.thumbnail_url || data?.image_url;
        if (!freshUrl) {
          skipped++;
          noMediaIds.push(id); // post chết/không media → đánh dấu đã thử
          continue;
        }

        const key = `creative/misc/${id}`;
        try {
          let publicUrl: string;
          if (await this.storage.exists(key)) {
            publicUrl = this.storage.publicUrl(key);
          } else {
            const media = await this.download(freshUrl);
            publicUrl = await this.storage.put(
              key,
              media.buffer,
              media.contentType,
            );
            uploaded++;
          }

          await this.prisma.creative.update({
            where: { id },
            data: {
              ownedImageUrl: publicUrl,
              ownedImageKey: key,
              ownedImageAt: new Date(),
              lastFetchedAt: new Date(),
            },
          });
          linked++;
        } catch (error: any) {
          failed++;
          this.logger.warn(
            `[${id}] orphan re-host failed: ${error?.message || error}`,
          );
        }
      }
    }

    if (noMediaIds.length > 0) {
      // updatedAt của Creative là @default(now()) (không @updatedAt) → chỉ đụng lastFetchedAt,
      // KHÔNG ảnh hưởng thứ tự job video (orderBy updatedAt).
      await this.prisma.creative.updateMany({
        where: { id: { in: noMediaIds } },
        data: { lastFetchedAt: new Date() },
      });
    }

    this.logger.log(
      `Orphan re-host finished: candidates=${candidates.length}, uploaded=${uploaded}, linked=${linked}, skipped=${skipped}, failed=${failed}`,
    );

    return { candidates: candidates.length, uploaded, linked, skipped, failed };
  }

  private initMetaSdk() {
    const token = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
    if (!token) {
      throw new Error('SDK_FACEBOOK_ACCESS_TOKEN missing');
    }
    FacebookAdsApi.init(token);
  }

  // ===== chọn nguồn thumbnail video ĐẸP nhất + key dedup =====

  private resolveBestThumb(
    creative: RehostRow,
  ): { sourceUrl: string; key: string } | null {
    // VIDEO: thumbnail width lớn nhất trong rawPayload; fallback thumbnailUrl bé
    if (!creative.videoId) return null;

    const largest = this.pickLargestVideoThumb(creative.adVideo?.rawPayload);
    const src =
      largest ||
      creative.adVideo?.thumbnailUrl ||
      creative.thumbnailUrl ||
      creative.previewUrl;
    if (!src) return null;

    return { sourceUrl: src, key: `creative/vid/${creative.videoId}` };
  }

  /** Chọn uri có width lớn nhất; tie-break is_preferred. */
  private pickLargestVideoThumb(rawPayload: unknown): string | null {
    const data = (rawPayload as any)?.thumbnails?.data;
    if (!Array.isArray(data) || data.length === 0) return null;

    let best: any = null;
    for (const thumb of data) {
      if (!thumb?.uri) continue;
      if (!best) {
        best = thumb;
        continue;
      }
      const w = Number(thumb.width) || 0;
      const bw = Number(best.width) || 0;
      if (w > bw || (w === bw && thumb.is_preferred && !best.is_preferred)) {
        best = thumb;
      }
    }
    return best?.uri || null;
  }

  private async download(
    url: string,
  ): Promise<{ buffer: Buffer; contentType: string }> {
    const response = await axios.get<ArrayBuffer>(url, {
      responseType: 'arraybuffer',
      timeout: this.getDownloadTimeoutMs(),
      maxContentLength: this.getMaxBytes(),
      maxRedirects: 3,
      // Chỉ nhận 2xx — fbcdn hết hạn trả 403/410 sẽ throw → KHÔNG lưu rác.
      validateStatus: (status) => status >= 200 && status < 300,
    });

    const buffer = Buffer.from(response.data);
    const headerType = String(response.headers['content-type'] || '')
      .split(';')[0]
      .trim()
      .toLowerCase();

    // XÁC THỰC thực sự là ảnh (magic bytes) — chặn body 200 dạng HTML/trang-lỗi bị lưu
    // thành "image/jpeg" (sẽ hỏng vĩnh viễn + đầu độc mọi sibling chung key).
    const sniffed = this.sniffImageType(buffer);
    if (!sniffed) {
      throw new Error(
        `not an image (bytes=${buffer.length}, header="${headerType || 'none'}")`,
      );
    }
    // Ưu tiên header nếu là image/*, ngược lại dùng loại nhận diện được.
    const contentType = headerType.startsWith('image/') ? headerType : sniffed;
    return { buffer, contentType };
  }

  /** Nhận diện ảnh qua magic bytes; null nếu không phải ảnh (JPEG/PNG/GIF/WebP). */
  private sniffImageType(buf: Buffer): string | null {
    if (buf.length < 12) return null;
    if (buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff) return 'image/jpeg';
    if (buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47)
      return 'image/png';
    if (buf[0] === 0x47 && buf[1] === 0x49 && buf[2] === 0x46) return 'image/gif';
    if (
      buf[0] === 0x52 &&
      buf[1] === 0x49 &&
      buf[2] === 0x46 &&
      buf[3] === 0x46 &&
      buf[8] === 0x57 &&
      buf[9] === 0x45 &&
      buf[10] === 0x42 &&
      buf[11] === 0x50
    )
      return 'image/webp';
    return null;
  }

  private getLimit() {
    return Number(process.env.CREATIVE_MEDIA_REHOST_LIMIT || 300);
  }

  private getOrphanLimit() {
    // Nhỏ hơn video vì mỗi lượt gọi Meta (tốn quota) — mặc định 150/lượt.
    return Number(process.env.CREATIVE_MEDIA_ORPHAN_LIMIT || 150);
  }

  private getDownloadTimeoutMs() {
    return Number(process.env.CREATIVE_MEDIA_DOWNLOAD_TIMEOUT_MS || 20000);
  }

  private getMaxBytes() {
    return Number(process.env.CREATIVE_MEDIA_MAX_BYTES || 15 * 1024 * 1024);
  }
}

type RehostRow = {
  id: string;
  videoId: string | null;
  thumbnailUrl: string | null;
  previewUrl: string | null;
  adVideo: {
    thumbnailUrl: string | null;
    rawPayload: unknown;
    urlExpiredAt: Date | null;
  } | null;
};
