import { Injectable, Logger } from '@nestjs/common';
import {
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';

/**
 * Wrapper S3-compatible cho MinIO tự host trên VPS (bucket `media`, serve công khai qua
 * Traefik: https://ads.3fastvn.com/media/<key>). Dùng @aws-sdk/client-s3 để sau này đổi
 * R2/S3 khỏi viết lại. MinIO => forcePathStyle=true.
 *
 * ENV (đặt trong mb-batch/.env trên VPS):
 *   MINIO_ENDPOINT           http://minio:9000   (DNS nội bộ network system_control)
 *   MINIO_ACCESS_KEY         = MINIO_ROOT_USER    (hoặc access key riêng)
 *   MINIO_SECRET_KEY         = MINIO_ROOT_PASSWORD
 *   MINIO_BUCKET             media
 *   MEDIA_PUBLIC_BASE_URL    https://ads.3fastvn.com/media
 */
@Injectable()
export class ObjectStorageService {
  private readonly logger = new Logger(ObjectStorageService.name);
  private readonly client: S3Client;
  private readonly bucket = process.env.MINIO_BUCKET || 'media';
  // base URL công khai, bỏ mọi dấu "/" cuối để nối key an toàn
  private readonly publicBase = (
    process.env.MEDIA_PUBLIC_BASE_URL || 'https://ads.3fastvn.com/media'
  ).replace(/\/+$/, '');

  constructor() {
    const endpoint = process.env.MINIO_ENDPOINT || 'http://minio:9000';
    const accessKeyId =
      process.env.MINIO_ACCESS_KEY || process.env.MINIO_ROOT_USER || '';
    const secretAccessKey =
      process.env.MINIO_SECRET_KEY || process.env.MINIO_ROOT_PASSWORD || '';

    if (!accessKeyId || !secretAccessKey) {
      this.logger.error(
        'MinIO credentials missing (MINIO_ACCESS_KEY/MINIO_SECRET_KEY hoặc MINIO_ROOT_USER/PASSWORD) — upload sẽ lỗi',
      );
    }

    this.client = new S3Client({
      endpoint,
      region: process.env.MINIO_REGION || 'us-east-1', // MinIO bỏ qua nhưng SDK bắt buộc
      forcePathStyle: true,
      credentials: { accessKeyId, secretAccessKey },
      // aws-sdk v3 mặc định gắn checksum (CRC) vào PutObject; vài bản MinIO từ chối →
      // chỉ tính khi thật sự cần để tương thích rộng.
      requestChecksumCalculation: 'WHEN_REQUIRED',
      responseChecksumValidation: 'WHEN_REQUIRED',
    });

    this.logger.log(
      `ObjectStorage ready: endpoint=${endpoint} bucket=${this.bucket} publicBase=${this.publicBase}`,
    );
  }

  /** URL công khai (qua Traefik) cho một key. */
  publicUrl(key: string): string {
    return `${this.publicBase}/${key.replace(/^\/+/, '')}`;
  }

  /** Object đã tồn tại chưa (để dedup, khỏi upload lại cùng asset). */
  async exists(key: string): Promise<boolean> {
    try {
      await this.client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: key }),
      );
      return true;
    } catch (err: any) {
      const status = err?.$metadata?.httpStatusCode;
      if (
        status === 404 ||
        err?.name === 'NotFound' ||
        err?.name === 'NoSuchKey'
      ) {
        return false;
      }
      throw err;
    }
  }

  /**
   * Upload object với Content-Type ĐÚNG (bắt buộc — nếu octet-stream thì browser tải về
   * thay vì hiện <img>). Trả về URL công khai.
   */
  async put(key: string, body: Buffer, contentType: string): Promise<string> {
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        ContentType: contentType,
        // ảnh re-host là bất biến (key theo hash/id) → cache lâu ở browser/CDN
        CacheControl: 'public, max-age=31536000, immutable',
      }),
    );
    return this.publicUrl(key);
  }
}
