import { Injectable, Logger } from '@nestjs/common';
import { AssetType, Status, TemplatePurpose } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { DraftAutomationMetaPublisherService } from './draft-automation-meta-publisher.service';
import {
  applyCidToAdName,
  extractCidFromName,
} from '../../common/utils/cid.util';
import {
  normalizeNameFilter,
  failsNameInclude,
  hitsNameExclude,
  normalizeCreatedRange,
  inCreatedRange,
  getAssetCreatedDate,
} from './name-term-match';
import { normalizePendingAutomation } from './pending-automation.util';
import {
  resolveAdSchedule,
  sanitizeAdSetScheduleFromTemplate,
  AdScheduleConfig,
  ResolvedAdSchedule,
} from './ad-schedule.util';
import {
  forEachMediaSlot,
  computeTemplateSlots,
  coerceCardToType,
  planContent,
  stampSlotToken,
  SlotRuleV2,
} from './draft-slot-count.util';

const DEFAULT_AUTOMATION_CRON = '*/30 * * * *';
const DEFAULT_AUTOMATION_TIMEZONE = 'Asia/Ho_Chi_Minh';

function formatDate(d: Date): string {
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${year}${month}${day}`;
}

function updateName(
  name: string,
  employeeId: string,
  employeeName: string,
): string {
  if (!name) return name;
  const parts = name.split('|').map((p) => p.trim());
  if (parts.length >= 6 && parts[5] === 'AUTO_ADS') {
    parts[2] = employeeId;
    parts[3] = employeeName;
    parts[4] = formatDate(new Date());
    return parts.join('|');
  }

  const currentDateStr = formatDate(new Date());
  const updatedParts = name.split('|').map((part) => {
    const trimmed = part.trim();
    if (/^20\d{6}$/.test(trimmed)) {
      return currentDateStr;
    }
    return part;
  });
  return updatedParts.join('|');
}

// Các key chứa định danh media THẬT của một asset (video/ảnh). Chỉ thu thập giá
// trị tại các key này để xác định asset "đã được dùng", tránh gom nhầm mọi token
// số/hex bất kỳ (page id, account id, budget, timestamp, post id...) như cách quét
// chuỗi cũ — vốn loại oan asset mới và gây skip vĩnh viễn.
const MEDIA_IDENTIFIER_KEYS = new Set([
  'video_id',
  'videoId',
  'image_hash',
  'imageHash',
  'selected_thumbnail_id',
  'image_id',
]);

function collectUsedMediaIdentifiers(
  node: any,
  keys: Set<string> = new Set(),
  scopedIds: string[] = [],
): Set<string> {
  const walk = (value: any) => {
    if (value === null || value === undefined) return;
    if (Array.isArray(value)) {
      for (const item of value) walk(item);
      return;
    }
    if (typeof value === 'object') {
      for (const key of Object.keys(value)) {
        const child = value[key];
        if (
          MEDIA_IDENTIFIER_KEYS.has(key) &&
          (typeof child === 'string' || typeof child === 'number')
        ) {
          const id = String(child).trim();
          if (id) keys.add(id);
        } else {
          walk(child);
        }
      }
    }
  };
  walk(node);

  // Vẫn quét asset.id (cuid) theo kiểu substring vì id có thể nằm trong mảng
  // automation_used_assets hoặc tham chiếu khác trong payload nháp.
  if (scopedIds.length) {
    const json = JSON.stringify(node);
    for (const id of scopedIds) {
      if (id && json.includes(id)) keys.add(id);
    }
  }

  return keys;
}

function parseJsonValue(value: any) {
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function summarizeAsset(asset: any) {
  return {
    id: asset.id,
    name: asset.name,
    type: asset.type,
    video_id: asset.video_id,
    imageHash: asset.imageHash,
    creation_time: asset.creation_time,
    employee_id: asset.larkRecords?.[0]?.employee_id,
  };
}

function parseValidDate(value: any): Date | undefined {
  if (!value) return undefined;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? undefined : date;
}

function getAssetCreationDate(asset: any): Date | undefined {
  return parseValidDate(asset?.creation_time || asset?.createdAtLocal);
}

// Nguồn CreativeAsset cho automation:
//  - NEW_ONLY  (mặc định): chỉ content chưa từng lên camp/publish (hành vi cũ)
//  - USED_ONLY: chỉ content ĐÃ từng lên camp/publish
//  - BOTH     : cả hai
function normalizeAssetReuseMode(
  value: any,
): 'NEW_ONLY' | 'USED_ONLY' | 'BOTH' {
  const v = String(value ?? '')
    .trim()
    .toUpperCase();
  if (v === 'USED_ONLY' || v === 'BOTH') return v;
  return 'NEW_ONLY';
}

function getThumbnailList(video: any): any[] {
  const thumbnails = parseJsonValue(video?.video_thumbnails);
  if (Array.isArray(thumbnails)) return thumbnails;
  if (Array.isArray(thumbnails?.data)) return thumbnails.data;
  if (Array.isArray(video?.list_thumbnails)) return video.list_thumbnails;
  if (Array.isArray(video?.list_thumbnails?.data))
    return video.list_thumbnails.data;
  return [];
}

function getPreferredThumbnail(video: any) {
  const thumbnails = getThumbnailList(video);
  return (
    thumbnails.find((thumbnail: any) => thumbnail?.is_preferred) ||
    thumbnails[0]
  );
}

function getVideoThumbnailId(video: any) {
  return (
    video?.selected_thumbnail_id ||
    video?.imageHash ||
    video?.thumbnailHash ||
    video?.thumbnail_hash ||
    getPreferredThumbnail(video)?.id ||
    undefined
  );
}

function getVideoThumbnailUrl(video: any) {
  const preferred = getPreferredThumbnail(video);
  return (
    video?.thumbnail ||
    video?.imageUrl ||
    video?.thumbnailUrl ||
    preferred?.uri ||
    preferred?.url ||
    undefined
  );
}

function inferMediaType(creative: any): string {
  if (!creative) return 'NONE';
  if (creative.mediaType) {
    return String(creative.mediaType).toUpperCase();
  }

  const spec = creative.object_story_spec || {};

  // Carousel check
  if (
    spec.link_data?.child_attachments?.length > 0 ||
    creative.carouselCards?.length > 0
  ) {
    return 'CAROUSEL';
  }

  // Video check
  if (spec.video_data?.video_id || creative.videoId || creative.video_id) {
    return 'VIDEO';
  }

  // Image check
  if (spec.link_data?.image_hash || creative.imageHash || creative.image_hash) {
    return 'IMAGE';
  }

  return 'NONE';
}

function enrichVideoPlaceholderObject(obj: any, video: any) {
  const thumbnailId = getVideoThumbnailId(video);
  const thumbnailUrl = getVideoThumbnailUrl(video);
  const thumbnails = parseJsonValue(video?.video_thumbnails);

  const enriched = {
    ...obj,
  };
  delete enriched.placeholder;

  enriched.videoId = video.video_id;
  enriched.video_id = video.video_id;
  enriched.source = video.video_source;
  enriched.video_source = video.video_source;
  enriched.previewUrl = thumbnailUrl;
  enriched.thumbnail = thumbnailUrl;
  enriched.image_url = thumbnailUrl;
  enriched.selected_thumbnail_id = thumbnailId;
  enriched.imageHash = thumbnailId;
  enriched.image_hash = thumbnailId;
  enriched.image_id = thumbnailId;

  if (thumbnails) {
    enriched.list_thumbnails = thumbnails;
    enriched.video_thumbnails = thumbnails;
  }

  if (enriched.object_story_spec?.video_data) {
    enriched.object_story_spec.video_data.video_id = video.video_id;
    enriched.object_story_spec.video_data.image_id = thumbnailId;
    enriched.object_story_spec.video_data.image_hash = thumbnailId;
  }

  return enriched;
}

function enrichImagePlaceholderObject(obj: any, img: any) {
  const enriched = {
    ...obj,
  };
  delete enriched.placeholder;

  const url = img.imageUrl || img.thumbnail;
  enriched.imageHash = img.imageHash;
  enriched.image_hash = img.imageHash;
  enriched.previewUrl = url;
  enriched.thumbnail = url;
  enriched.image_url = url;

  if (enriched.object_story_spec?.link_data) {
    enriched.object_story_spec.link_data.image_hash = img.imageHash;
  }

  return enriched;
}

function replacePlaceholders(obj: any, videos: any[], images: any[]): any {
  if (!obj) return obj;

  if (typeof obj === 'string') {
    const videoMatch = obj.match(/^VIDEO_(\d+)$/);
    if (videoMatch) {
      const idx = parseInt(videoMatch[1], 10) - 1;
      if (idx >= 0 && idx < videos.length && videos[idx]) {
        return videos[idx];
      }
    }
    const imageMatch = obj.match(/^IMAGE_(\d+)$/);
    if (imageMatch) {
      const idx = parseInt(imageMatch[1], 10) - 1;
      if (idx >= 0 && idx < images.length && images[idx]) {
        return images[idx];
      }
    }
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map((item) => replacePlaceholders(item, videos, images));
  }

  if (typeof obj === 'object') {
    const newObj: any = {};
    let matchedVideo: any;
    let matchedImage: any;
    for (const key of Object.keys(obj)) {
      const val = obj[key];
      if (typeof val === 'string') {
        const videoMatch = val.match(/^VIDEO_(\d+)$/);
        const imageMatch = val.match(/^IMAGE_(\d+)$/);
        if (videoMatch) {
          const idx = parseInt(videoMatch[1], 10) - 1;
          if (idx >= 0 && idx < videos.length && videos[idx]) {
            const v = videos[idx];
            matchedVideo = v;
            if (key === 'id') {
              newObj[key] = v.id;
            } else if (key === 'video_id' || key === 'videoId') {
              newObj[key] = v.video_id;
            } else if (key === 'video_source' || key === 'source') {
              newObj[key] = v.video_source;
            } else if (
              key === 'thumbnail' ||
              key === 'previewUrl' ||
              key === 'preview_url' ||
              key === 'image_url'
            ) {
              newObj[key] = getVideoThumbnailUrl(v);
            } else if (
              key === 'selected_thumbnail_id' ||
              key === 'image_id' ||
              key === 'image_hash' ||
              key === 'imageHash'
            ) {
              newObj[key] = getVideoThumbnailId(v);
            } else {
              newObj[key] = v.video_id;
            }
            continue;
          }
        } else if (imageMatch) {
          const idx = parseInt(imageMatch[1], 10) - 1;
          if (idx >= 0 && idx < images.length && images[idx]) {
            const img = images[idx];
            matchedImage = img;
            if (key === 'id') {
              newObj[key] = img.id;
            } else if (
              key === 'imageHash' ||
              key === 'image_hash' ||
              key === 'selected_thumbnail_id'
            ) {
              newObj[key] = img.imageHash;
            } else if (
              key === 'thumbnail' ||
              key === 'previewUrl' ||
              key === 'preview_url' ||
              key === 'image_url'
            ) {
              newObj[key] = img.imageUrl || img.thumbnail;
            } else {
              newObj[key] = img.imageHash;
            }
            continue;
          }
        }
      }
      newObj[key] = replacePlaceholders(val, videos, images);
    }
    const finalMediaType = inferMediaType(newObj);
    if (matchedVideo || (obj.placeholder && finalMediaType === 'VIDEO')) {
      const video =
        matchedVideo ||
        // Mảng fill có thể THƯA (ô trống giữ marker không có entry) → bỏ qua lỗ.
        videos.find(
          (item) =>
            item &&
            (item.video_id === newObj.videoId ||
              item.video_id === newObj.video_id ||
              item.id === newObj.id),
        );
      if (video) {
        return enrichVideoPlaceholderObject(newObj, video);
      }
    }
    if (matchedImage || (obj.placeholder && finalMediaType === 'IMAGE')) {
      const img =
        matchedImage ||
        // Mảng fill có thể THƯA (ô trống giữ marker không có entry) → bỏ qua lỗ.
        images.find(
          (item) =>
            item &&
            (item.imageHash === newObj.imageHash ||
              item.image_hash === newObj.image_hash ||
              item.id === newObj.id),
        );
      if (img) {
        return enrichImagePlaceholderObject(newObj, img);
      }
    }
    return newObj;
  }

  return obj;
}

// ============================================================================
// Ô TRUNG TÍNH LOẠI MEDIA (media-neutral slots) — Finding Q1 "template cố định
// ảnh không nhét được video → không bao giờ đủ". Cho phép một ô (single/card/
// attachment) nhận content KHÁC loại: content thực là video mà ô là ảnh → morph ô
// sang video (và ngược lại), giữ text/CTA/link/page. Ô dynamic (DOF) KHÔNG morph.
// Việc DUYỆT & ĐẾM ô nay dùng CHUNG forEachMediaSlot/computeTemplateSlots trong
// draft-slot-count.util.ts (mọi loại mẫu, phủ carousel/child_attachments/dynamic —
// KHÔNG còn chia nhánh "mẫu phức tạp"). PARITY: file util giữ giống nhau 2 repo.
// ----------------------------------------------------------------------------

// Loại media của một creativeAsset row (từ folder): 'VIDEO' | 'IMAGE'.
function assetMediaType(asset: any): 'VIDEO' | 'IMAGE' {
  const t = String(asset?.type || asset?.mediaType || '').toUpperCase();
  if (t === 'VIDEO') return 'VIDEO';
  if (t === 'IMAGE') return 'IMAGE';
  return asset?.video_id || asset?.videoId ? 'VIDEO' : 'IMAGE';
}

// Chuyển đổi (morph) một creative đơn-media sang loại đích. Giữ message/CTA/link/
// title/description + page-level fields (page_id, page_welcome_message, asset_feed_spec).
// Xoá token/ID media cũ để builder (stampSlotToken) đóng lại token ĐÚNG loại. Trả về
// true nếu có đổi loại.
function coerceCreativeToType(
  creative: any,
  targetType: 'VIDEO' | 'IMAGE',
): boolean {
  const current = inferMediaType(creative);
  if (current === targetType) return false;
  if (current === 'CAROUSEL' || current === 'NONE') return false;
  const spec = creative.object_story_spec || (creative.object_story_spec = {});

  if (targetType === 'VIDEO') {
    const link = spec.link_data || {};
    spec.video_data = {
      ...(spec.video_data || {}),
      message: link.message ?? spec.video_data?.message,
      call_to_action: link.call_to_action ?? spec.video_data?.call_to_action,
      title: link.name ?? spec.video_data?.title,
      link_description: link.description ?? spec.video_data?.link_description,
    };
    delete spec.link_data;
    delete spec.video_data.video_id;
    delete spec.video_data.image_hash;
    delete spec.video_data.image_id;
    delete creative.imageHash;
    delete creative.image_hash;
    delete creative.videoId;
    delete creative.video_id;
    delete creative.selected_thumbnail_id;
    creative.mediaType = 'VIDEO';
  } else {
    const vd = spec.video_data || {};
    spec.link_data = {
      ...(spec.link_data || {}),
      link:
        creative.link ||
        vd.call_to_action?.value?.link ||
        spec.link_data?.link,
      message: vd.message ?? spec.link_data?.message,
      call_to_action: vd.call_to_action ?? spec.link_data?.call_to_action,
      name: vd.title ?? spec.link_data?.name,
      description: vd.link_description ?? spec.link_data?.description,
    };
    delete spec.link_data.image_hash;
    delete spec.video_data;
    delete creative.videoId;
    delete creative.video_id;
    delete creative.selected_thumbnail_id;
    delete creative.imageHash;
    delete creative.image_hash;
    creative.mediaType = 'IMAGE';
  }
  return true;
}

function findExistingSlots(
  obj: any,
  usedVideos: Set<number>,
  usedImages: Set<number>,
) {
  if (!obj) return;
  if (typeof obj === 'string') {
    const videoMatch = obj.match(/^VIDEO_(\d+)$/);
    if (videoMatch) {
      usedVideos.add(parseInt(videoMatch[1], 10));
    }
    const imageMatch = obj.match(/^IMAGE_(\d+)$/);
    if (imageMatch) {
      usedImages.add(parseInt(imageMatch[1], 10));
    }
    return;
  }
  if (Array.isArray(obj)) {
    for (const item of obj) {
      findExistingSlots(item, usedVideos, usedImages);
    }
    return;
  }
  if (typeof obj === 'object') {
    for (const key of Object.keys(obj)) {
      findExistingSlots(obj[key], usedVideos, usedImages);
    }
  }
}

// Kết quả một lượt chạy engine cho MỘT automation (template legacy hoặc row
// DraftAutomation). Nhánh legacy bỏ qua; nhánh DraftAutomation dùng để cập nhật
// run-tracking (lastRunStatus/lastRunReason) và quyết định COMPLETED / lịch kế tiếp.
export interface AutomationRunResult {
  status: 'SUCCESS' | 'SKIPPED' | 'FAILED';
  reason?: string;
  // isComplete = bản nháp đã lấp đủ mọi slot của mẫu (đủ điều kiện coi là "xong").
  isComplete: boolean;
  // published = đã đăng lên Meta ở lượt này (chỉ khi publishMode=PUBLISH_IMMEDIATELY
  // và không bị khóa chống trùng bỏ qua).
  published: boolean;
  generatedCampaignId?: string;
}

@Injectable()
export class DraftAutomationScheduler {
  private readonly logger = new Logger(DraftAutomationScheduler.name);

  // Số lần SKIPPED LIÊN TIẾP với lý do "chưa có asset đủ điều kiện" trong khi bản
  // nháp còn DỞ DANG thì tự TẠM DỪNG automation (cảnh báo kẹt). Pool asset trong
  // thư mục cạn mà mẫu vẫn cần thêm ô → mọi lượt sau đều skip vô ích và bản nháp
  // kẹt vĩnh viễn (publish sẽ đẩy token IMAGE_n lên Meta gây lỗi). Đếm bằng chính
  // DraftAutomationHistory gần nhất — KHÔNG thêm cột/counter/migration.
  private static readonly STUCK_SKIP_PAUSE_THRESHOLD = 3;

  // Dấu hiệu nhận biết một lượt SKIPPED thuộc nhóm "kẹt vì thiếu asset đủ điều kiện"
  // (khớp cả 2 câu ở nhánh có/không có nháp đang xử lý). Dùng để đếm chuỗi skip.
  private isStuckNoAssetsReason(reason?: string | null): boolean {
    const r = String(reason || '');
    return (
      r.startsWith('Chưa có asset mới đủ điều kiện') ||
      r.startsWith('Chưa có asset đủ điều kiện')
    );
  }

  // Đếm số lượt SKIPPED-vì-thiếu-asset LIÊN TIẾP gần nhất của một DraftAutomation
  // (chỉ tính chuỗi ở đầu lịch sử: gặp lượt KHÔNG-phải-stuck thì dừng đếm). Trả về
  // số lượt "kẹt" đã ghi TRƯỚC lượt hiện tại. Lỗi query → 0 (fail-open, không PAUSE oan).
  private async countConsecutiveStuckSkips(
    draftAutomationId: string,
  ): Promise<number> {
    try {
      const recent = await this.prisma.draftAutomationHistory.findMany({
        where: { draftAutomationId },
        orderBy: { createdAt: 'desc' },
        select: { status: true, reason: true },
        take: DraftAutomationScheduler.STUCK_SKIP_PAUSE_THRESHOLD,
      });
      let count = 0;
      for (const row of recent) {
        if (row.status === 'SKIPPED' && this.isStuckNoAssetsReason(row.reason)) {
          count += 1;
        } else {
          break;
        }
      }
      return count;
    } catch {
      return 0;
    }
  }

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaPublisher: DraftAutomationMetaPublisherService,
  ) {}

  private formatError(err: any) {
    return err?.stack || err?.message || String(err);
  }

  // Ngưỡng "tối thiểu N mẫu nội dung/chiến dịch" — cấu hình runtime qua
  // SystemConfig[min_publish_contents] (parity mb-ads DraftCampaignService). value =
  // số (vd 5) hoặc { value: 5 }. FAIL-OPEN về mặc định 5 khi thiếu row/lỗi → không cần
  // migration/seed. Đọc mỗi lượt cron (không phải đường nóng).
  private async getMinPublishContents(): Promise<number> {
    try {
      const cfg = await this.prisma.systemConfig.findUnique({
        where: { key: 'min_publish_contents' },
        select: { value: true },
      });
      const raw: any = cfg?.value;
      const n = Number(
        raw !== null && typeof raw === 'object' ? raw.value : raw,
      );
      return Number.isFinite(n) && n >= 0 ? Math.floor(n) : 5;
    } catch {
      return 5;
    }
  }

  private async recordAutomationHistory(input: {
    template: any;
    // Liên kết history với row DraftAutomation (nhánh mới). undefined ở nhánh legacy
    // → Prisma bỏ qua cột → history cũ không đổi.
    draftAutomationId?: string;
    startedAt: Date;
    status: 'SUCCESS' | 'SKIPPED' | 'FAILED';
    reason?: string;
    automation?: any;
    creator?: any;
    folderId?: string;
    conditionSummary?: any;
    steps?: any[];
    selectedAssets?: any;
    generatedCampaignId?: string;
    publishRequested?: boolean;
    publishMode?: 'DRAFT_ONLY' | 'PUBLISH_IMMEDIATELY';
    publishResult?: any;
    error?: string;
  }) {
    try {
      await this.prisma.draftAutomationHistory.create({
        data: {
          templateId: input.template.id,
          draftAutomationId: input.draftAutomationId,
          templateName: input.template.name,
          creatorId: input.creator?.id || input.template.createdById,
          creatorName: input.creator?.name,
          creatorEmployeeId: input.creator?.employee_id,
          folderId: input.folderId || input.automation?.folderId,
          status: input.status,
          reason: input.reason,
          publishRequested: input.publishRequested || false,
          publishMode: input.publishMode || 'DRAFT_ONLY',
          publishResult: input.publishResult || undefined,
          conditionSummary: input.conditionSummary || undefined,
          steps: input.steps || [],
          selectedAssets: input.selectedAssets || undefined,
          generatedCampaignId: input.generatedCampaignId,
          error: input.error,
          startedAt: input.startedAt,
          finishedAt: new Date(),
        },
      });
    } catch (err: any) {
      this.logger.error(
        `Failed to save draft automation history for template "${input.template.name}":`,
        this.formatError(err),
      );
    }
  }

  private shouldPublishImmediately(automation: any) {
    return (
      automation?.publishToMeta === true ||
      automation?.autoPublish === true ||
      automation?.publishImmediately === true ||
      automation?.publishMode === 'PUBLISH_IMMEDIATELY'
    );
  }

  normalizeAutomation(automation: any = {}) {
    const publishMode = this.shouldPublishImmediately(automation)
      ? 'PUBLISH_IMMEDIATELY'
      : 'DRAFT_ONLY';
    const runMode = automation.runMode === 'ONCE' ? 'ONCE' : 'LOOP';
    const cronExpression =
      typeof automation.cronExpression === 'string' &&
      automation.cronExpression.trim()
        ? automation.cronExpression.trim()
        : DEFAULT_AUTOMATION_CRON;
    const timezone =
      typeof automation.timezone === 'string' && automation.timezone.trim()
        ? automation.timezone.trim()
        : DEFAULT_AUTOMATION_TIMEZONE;

    return {
      ...automation,
      intervalMinutes: 30,
      cronExpression,
      timezone,
      publishMode,
      publishToMeta: publishMode === 'PUBLISH_IMMEDIATELY',
      runMode,
    };
  }

  private async getAssetsByIds(ids: string[]) {
    const uniqueIds = [...new Set(ids.filter(Boolean))];
    if (uniqueIds.length === 0) return [];

    const assets = await this.prisma.creativeAsset.findMany({
      where: { id: { in: uniqueIds } },
      include: { larkRecords: true },
    });
    const assetById = new Map(assets.map((asset) => [asset.id, asset]));
    return uniqueIds.map((id) => assetById.get(id)).filter(Boolean);
  }

  private splitAssetsByType(assets: any[]) {
    return {
      videos: assets.filter((asset) => asset.type === AssetType.VIDEO),
      images: assets.filter((asset) => asset.type === AssetType.IMAGE),
    };
  }

  private async getInProgressDraft(
    template: any,
    creatorId: string,
    automation: any,
  ) {
    const inProgressDraftId = automation?.inProgressDraftId;
    if (!inProgressDraftId) return null;

    return this.prisma.systemCampaign.findFirst({
      where: {
        id: inProgressDraftId,
        createdById: creatorId,
        createdByAutomation: true,
        automationTemplateId: template.id,
        status: Status.DRAFT,
        deletedAt: null,
        meta_id: null,
        appendOnly: false,
      },
      include: {
        ad_sets: {
          orderBy: { createdAt: 'asc' },
          include: {
            ads: { orderBy: { createdAt: 'asc' } },
          },
        },
      },
    });
  }

  private buildSubstitutedValues(input: {
    template: any;
    creator: any;
    videos: any[];
    images: any[];
    publishMode: 'DRAFT_ONLY' | 'PUBLISH_IMMEDIATELY';
    requiredVideos: number;
    requiredImages: number;
    isComplete: boolean;
    automation: any;
    // Ô trung tính loại: danh sách content gán cho TỪNG Ô (mọi loại: single/card/
    // attachment/dynamic) theo thứ tự forEachMediaSlot. Nếu có → morph ô sang đúng
    // loại content trước khi gán token.
    contentPlan?: any[];
    // Tổng số ô media của mẫu (computeTemplateSlots.totalSlots) — cho automation_progress.
    totalSlots?: number;
  }) {
    const {
      template,
      creator,
      publishMode,
      requiredVideos,
      requiredImages,
      isComplete,
      automation,
      contentPlan,
      totalSlots,
    } = input;
    const templateData = template.data as any;

    // Deep clone the template campaign data to avoid mutating database/in-memory template object
    const clonedTemplateData = JSON.parse(JSON.stringify(templateData));

    // TOKEN MẪU HẾT VAI TRÒ GÁN CONTENT: định danh ô = VỊ TRÍ trong thứ tự duyệt cố
    // định forEachMediaSlot; ô nào nhận content nào do contentPlan (planContent — đọc
    // conditions.slotRules của bộ rule) quyết. Engine TỰ đánh token MỚI tuần tự theo
    // loại cho từng ô rồi để replacePlaceholders thay như cũ — token sẵn có trong mẫu
    // (kể cả trùng/rác do nhân bản ad đã slotify) bị GHI ĐÈ, không bao giờ được đọc
    // lại → hết lớp bug "token trùng dồn CÙNG 1 asset vào nhiều ad". Ô thiếu content
    // (null trong plan) nhận token + placeholder làm DẤU Ô TRỐNG — không có entry
    // trong fill nên replacePlaceholders giữ nguyên → publish gate EMPTY_SLOT và lượt
    // gom sau vẫn hoạt động; SỐ trên dấu này không consumer nào đọc (chỉ regex
    // VIDEO_\d+/placeholder=true).
    if (!Array.isArray(contentPlan)) {
      throw new Error(
        'buildSubstitutedValues cần contentPlan (kế hoạch content theo từng ô) — nhánh gán theo token của mẫu đã bị loại bỏ.',
      );
    }
    const fillVideos: any[] = [];
    const fillImages: any[] = [];
    let videoOrdinal = 0;
    let imageOrdinal = 0;
    let planCursor = 0;
    for (const slot of forEachMediaSlot(clonedTemplateData)) {
      const asset = contentPlan[planCursor++];
      if (asset) {
        const targetType = assetMediaType(asset);
        if (slot.kind === 'single') {
          coerceCreativeToType(slot.ref, targetType);
        } else if (slot.kind === 'card' || slot.kind === 'attachment') {
          coerceCardToType(slot.ref, targetType, asset, slot.kind);
        }
        // Ô dynamic (DOF): crossTypeAllowed=false → planner chỉ gán đúng loại → không morph.
        if (targetType === 'VIDEO') {
          videoOrdinal += 1;
          stampSlotToken(slot.ref, slot.kind, 'VIDEO', `VIDEO_${videoOrdinal}`);
          fillVideos[videoOrdinal - 1] = asset;
        } else {
          imageOrdinal += 1;
          stampSlotToken(slot.ref, slot.kind, 'IMAGE', `IMAGE_${imageOrdinal}`);
          fillImages[imageOrdinal - 1] = asset;
        }
      } else if (slot.currentType === 'VIDEO') {
        videoOrdinal += 1;
        stampSlotToken(slot.ref, slot.kind, 'VIDEO', `VIDEO_${videoOrdinal}`);
      } else {
        imageOrdinal += 1;
        stampSlotToken(slot.ref, slot.kind, 'IMAGE', `IMAGE_${imageOrdinal}`);
      }
    }

    const substitutedValues = replacePlaceholders(
      clonedTemplateData,
      fillVideos,
      fillImages,
    );

    // Gắn folderId của automation vào draft.data.automation để engine tự tìm lại
    // "bản nháp đang gom dở" ở lượt sau (fallback theo template+folder+creator ở
    // processAutomation). NHÁNH LEGACY: no-op — substitutedValues.automation.folderId
    // vốn đã bằng automation.folderId (cùng nguồn template.data.automation). NHÁNH
    // DraftAutomation: template có thể KHÔNG có data.automation → đây là chỗ gắn
    // folderId để chế độ LOOP gom nhiều asset vào cùng một nháp hoạt động đúng.
    if (automation?.folderId) {
      substitutedValues.automation = {
        ...(substitutedValues.automation || {}),
        folderId: automation.folderId,
      };
    }

    // Map mỗi ad -> CID của asset đã lấp slot của ad đó, để đặt tên CID theo TỪNG
    // ad (clonedTemplateData vẫn giữ placeholder VIDEO_n/IMAGE_n vì replacePlaceholders
    // không mutate input). Slot VIDEO_n -> videos[n-1], IMAGE_n -> images[n-1].
    const adCidByPosition = new Map<string, string>();
    if (Array.isArray(clonedTemplateData.ad_sets)) {
      clonedTemplateData.ad_sets.forEach((adset: any, ai: number) => {
        if (!Array.isArray(adset.ads)) return;
        adset.ads.forEach((ad: any, di: number) => {
          if (!ad?.creative) return;
          const slotVideos = new Set<number>();
          const slotImages = new Set<number>();
          findExistingSlots(ad.creative, slotVideos, slotImages);
          const firstVideo = [...slotVideos].sort((a, b) => a - b)[0];
          const firstImage = [...slotImages].sort((a, b) => a - b)[0];
          let asset: any;
          if (firstVideo && fillVideos[firstVideo - 1]) {
            asset = fillVideos[firstVideo - 1];
          } else if (firstImage && fillImages[firstImage - 1]) {
            asset = fillImages[firstImage - 1];
          }
          const cid = asset ? extractCidFromName(asset.name) : null;
          if (cid) adCidByPosition.set(`${ai}:${di}`, cid);
        });
      });
    }

    const employeeId = creator.employee_id;
    const employeeName = creator.name;

    if (substitutedValues.campaign?.name) {
      substitutedValues.campaign.name = updateName(
        substitutedValues.campaign.name,
        employeeId,
        employeeName,
      );
    }

    if (Array.isArray(substitutedValues.ad_sets)) {
      substitutedValues.ad_sets = substitutedValues.ad_sets.map(
        (adset: any, ai: number) => {
          if (adset.name) {
            adset.name = updateName(adset.name, employeeId, employeeName);
          }
          if (Array.isArray(adset.ads)) {
            adset.ads = adset.ads.map((ad: any, di: number) => {
              if (ad.name) {
                let nextName = updateName(ad.name, employeeId, employeeName);
                const cid = adCidByPosition.get(`${ai}:${di}`);
                if (cid) nextName = applyCidToAdName(nextName, cid);
                ad.name = nextName;
              }
              return ad;
            });
          }
          return adset;
        },
      );
    }

    const filledVideos = fillVideos.filter(Boolean);
    const filledImages = fillImages.filter(Boolean);
    substitutedValues.automation_used_assets = [
      ...filledVideos.map((v) => v.id),
      ...filledImages.map((i) => i.id),
    ];
    substitutedValues.automation_progress = {
      templateId: template.id,
      templateName: template.name,
      requiredVideos,
      requiredImages,
      // requiredTotal = tổng số ô media của mẫu (nhất quán mọi loại ô); currentTotal =
      // số ô đã lấp content ở lần dựng này.
      requiredTotal: totalSlots ?? requiredVideos + requiredImages,
      currentVideos: filledVideos.length,
      currentImages: filledImages.length,
      currentTotal: filledVideos.length + filledImages.length,
      isComplete,
      runMode: automation.runMode,
      publishMode,
      updatedAt: new Date().toISOString(),
    };

    return substitutedValues;
  }

  private async saveAutomationDraft(input: {
    existingDraft?: any;
    substitutedValues: any;
    template: any;
    creator: any;
    publishMode: 'DRAFT_ONLY' | 'PUBLISH_IMMEDIATELY';
    // Lịch chạy QC user chọn (conditions.adSchedule). Absent ⇒ chỉ dọn mốc quá khứ từ mẫu.
    adSchedule?: AdScheduleConfig | null;
  }) {
    const {
      existingDraft,
      substitutedValues,
      template,
      creator,
      publishMode,
      adSchedule,
    } = input;

    // NGOÀI transaction: resolve lịch chạy QC theo MÚI GIỜ TKQC (Meta chạy theo tz tài
    // khoản). Chỉ lookup timezone khi user thực sự cấu hình adSchedule — automation cũ
    // (không adSchedule) đi nhánh sanitize dọn-mốc-quá-khứ, không cần tz.
    let adSched: ResolvedAdSchedule | null = null;
    if (adSchedule) {
      const account = substitutedValues.ad_account_id
        ? await this.prisma.account.findUnique({
            where: { id: substitutedValues.ad_account_id },
            select: { timezone: true },
          })
        : null;
      adSched = resolveAdSchedule(
        adSchedule,
        Math.floor(Date.now() / 1000),
        account?.timezone,
      );
    }

    // Dọn LỊCH CHẠY của MỌI ad set clone từ mẫu TRƯỚC khi ghi DB. Chạy cho cả automation
    // KHÔNG có adSchedule (truyền null) để dọn mốc start_time/end_time quá khứ mẫu bê theo
    // — port fix a65bd18 từ mb-ads. Sanitize TRƯỚC khi tạo campaign để bản ad_sets lưu trong
    // SystemCampaign.data (substitutedValues) KHỚP với bản lưu trong từng SystemAdSet.data
    // (cùng tham chiếu object). Xem sanitizeAdSetScheduleFromTemplate.
    for (const adset of substitutedValues.ad_sets || []) {
      sanitizeAdSetScheduleFromTemplate(adset, adSched);
    }

    // CBO lifetime (ngân sách trọn đời ở cấp CHIẾN DỊCH): Meta bắt buộc stop_time trong
    // tương lai. Ép stop_time/start_time cấp camp theo lịch đã hẹn (belt-and-suspenders,
    // song song với backstop ở publisher buildCampaignCreatePayload).
    const campaignCfg = substitutedValues.campaign;
    if (
      campaignCfg &&
      Number(campaignCfg.lifetime_budget) > 0 &&
      adSched?.endUnix
    ) {
      campaignCfg.stop_time = String(adSched.endUnix);
      if (adSched.startUnix)
        campaignCfg.start_time = String(adSched.startUnix);
    }

    return this.prisma.$transaction(async (tx) => {
      const campaignData = {
        accountId: substitutedValues.ad_account_id,
        createdById: creator.id,
        status: Status.DRAFT,
        createdByAutomation: true,
        automationTemplateId: template.id,
        automationTemplateName: template.name,
        automationPublishMode: publishMode,
        // NHÂN config "Tự động hoá sau khi lên Camp" từ MẪU xuống camp automation sinh ra →
        // đi theo luồng publish (publishDraftCampaign) để tự áp lên Meta. ABO áp cho MỌI ad set.
        pendingAutomation:
          (normalizePendingAutomation(
            (template as any)?.pendingAutomation,
          ) as any) ?? undefined,
        cid: substitutedValues.cid,
        data: substitutedValues as any,
        campaign_bidAmount:
          Number(substitutedValues.campaign?.bid_amount) || undefined,
        campaign_bidStrategy:
          substitutedValues.campaign?.bid_strategy || undefined,
        campaign_budget:
          Number(
            substitutedValues.campaign?.daily_budget ||
              substitutedValues.campaign?.lifetime_budget,
          ) || undefined,
        campaign_budgetType:
          (substitutedValues.campaign?.daily_budget && 'DAILY') ||
          (substitutedValues.campaign?.lifetime_budget && 'LIFETIME') ||
          'undefined',
        campaign_CBO:
          !!substitutedValues.campaign?.daily_budget ||
          !!substitutedValues.campaign?.lifetime_budget ||
          false,
        campaign_name: substitutedValues.campaign?.name,
        campaign_objective: substitutedValues.campaign?.objective,
      };

      const campaign = existingDraft
        ? await tx.systemCampaign.update({
            where: { id: existingDraft.id },
            data: campaignData,
          })
        : await tx.systemCampaign.create({
            data: campaignData,
          });

      if (existingDraft) {
        // Loại bỏ id undefined/null trước khi query — Prisma ném lỗi nếu mảng `in`
        // chứa undefined (nguồn gốc 313 lần FAILED "Có lỗi không mong muốn").
        // Parity với mb-ads draft-campaign.service.ts (.filter(Boolean)).
        const adSetIds = existingDraft.ad_sets
          .map((adset: any) => adset.id)
          .filter(Boolean);
        if (adSetIds.length > 0) {
          await tx.systemAd.deleteMany({
            where: { adSetId: { in: adSetIds } },
          });
          await tx.systemAdSet.deleteMany({
            where: { id: { in: adSetIds } },
          });
        }
      }

      for (const adset of substitutedValues.ad_sets || []) {
        const createdAdSet = await tx.systemAdSet.create({
          data: {
            accountId: substitutedValues.ad_account_id,
            campaignId: campaign.id,
            createdById: creator.id,
            status: Status.DRAFT,
            data: adset as any,
            adset_bidAmount: Number(adset.bid_amount) || undefined,
            adset_bidStrategy: adset.bid_strategy || undefined,
            adset_budget:
              Number(adset.daily_budget || adset.lifetime_budget) || undefined,
            adset_budgetType:
              (adset.daily_budget && 'DAILY') ||
              (adset.lifetime_budget && 'LIFETIME') ||
              'undefined',
            adset_name: adset.name,
            adset_optimization: adset.optimization_goal,
          },
        });

        if (Array.isArray(adset.ads)) {
          await tx.systemAd.createMany({
            data: adset.ads.map((ad: any) => ({
              accountId: substitutedValues.ad_account_id,
              createdById: creator.id,
              data: ad,
              status: Status.DRAFT,
              adSetId: createdAdSet.id,
            })),
          });
        }
      }

      return campaign.id;
    });
  }

  async processAutomation(override: {
    template: any;
    rawAutomation: any;
    draftAutomationId?: string;
  }): Promise<AutomationRunResult> {
    // NHÁNH DraftAutomation (hệ mới) — luôn chạy đúng 1 template do caller cấp; cấu
    // hình automation lấy từ override.rawAutomation (map từ row DraftAutomation). Hệ
    // "automation cấu hình trong template" (cũ) đã được gỡ bỏ hoàn toàn.
    let lastResult: AutomationRunResult = {
      status: 'SKIPPED',
      reason: 'Không có template tự động hóa nào để xử lý.',
      isComplete: false,
      published: false,
    };

    if (override.template?.purpose !== TemplatePurpose.TESTING_CONTENT) {
      return {
        status: 'SKIPPED',
        reason:
          'Tự động hóa lấy asset mới chỉ chạy với mẫu có mục đích “Test content mới”.',
        isComplete: false,
        published: false,
      };
    }

    const activeTemplates = [override.template];

    for (const template of activeTemplates) {
      const startedAt = new Date();
      let automation: any;
      let creator: any;
      let conditionSummary: any;
      let generatedCampaignId: string | undefined;
      let publishRequested = false;
      let publishMode: 'DRAFT_ONLY' | 'PUBLISH_IMMEDIATELY' = 'DRAFT_ONLY';
      try {
        automation = this.normalizeAutomation(override.rawAutomation);
        if (
          automation.enabled !== true ||
          automation.status === 'PAUSED' ||
          automation.status === 'DISABLED' ||
          // Thiếu thư mục nội dung thì không chạy.
          !automation.folderId
        ) {
          lastResult = {
            status: 'SKIPPED',
            reason: 'Tự động hóa chưa bật hoặc thiếu thư mục nội dung.',
            isComplete: false,
            published: false,
          };
          continue;
        }
        publishRequested = this.shouldPublishImmediately(automation);
        publishMode = publishRequested ? 'PUBLISH_IMMEDIATELY' : 'DRAFT_ONLY';
        const creatorId = template.createdById;
        if (!creatorId) {
          this.logger.warn(
            `Template ${template.name} (${template.id}) has no creator ID. Skipping.`,
          );
          await this.recordAutomationHistory({
            template,
            startedAt,
            status: 'SKIPPED',
            reason: 'Mẫu chưa có ID người tạo.',
            automation,
            folderId: automation.folderId,
            publishRequested,
            publishMode,
            draftAutomationId: override?.draftAutomationId,
            steps: [
              {
                key: 'creator',
                label: 'Kiểm tra người tạo mẫu',
                status: 'skipped',
                reason: 'ID người tạo đang trống',
              },
            ],
          });
          lastResult = {
            status: 'SKIPPED',
            reason: 'Mẫu chưa có ID người tạo.',
            isComplete: false,
            published: false,
          };
          continue;
        }

        creator = await this.prisma.user.findUnique({
          where: { id: creatorId },
        });

        if (!creator || !creator.employee_id) {
          this.logger.warn(
            `Creator of template ${template.name} has no employee ID. Skipping.`,
          );
          const creatorSkipReason = !creator
            ? 'Không tìm thấy người tạo mẫu.'
            : 'Người tạo template chưa có employee ID.';
          await this.recordAutomationHistory({
            template,
            startedAt,
            status: 'SKIPPED',
            reason: creatorSkipReason,
            automation,
            creator,
            folderId: automation.folderId,
            publishRequested,
            publishMode,
            draftAutomationId: override?.draftAutomationId,
            steps: [
              {
                key: 'creator',
                label: 'Kiểm tra người tạo mẫu',
                status: 'skipped',
                creatorId,
                reason: !creator
                  ? 'Không tìm thấy bản ghi người tạo'
                  : 'creator.employee_id đang trống',
              },
            ],
          });
          lastResult = {
            status: 'SKIPPED',
            reason: creatorSkipReason,
            isComplete: false,
            published: false,
          };
          continue;
        }

        this.logger.log(
          `Processing template "${template.name}" for user ${creator.name} (${creator.employee_id}). folderId: ${automation.folderId}`,
        );

        // 2. Fetch all assets in the target folder(s)
        // Gom folder nguồn: folder được chọn + con cháu (nếu bật). Legacy chỉ có
        // folderId + không includeSubfolders => set = [folderId] (y hệt hành vi cũ).
        const folderRoots =
          Array.isArray(automation.folderIds) && automation.folderIds.length
            ? automation.folderIds
            : automation.folderId
              ? [automation.folderId]
              : [];
        const folderIdSet = await this.resolveAutomationFolderIds(
          folderRoots,
          automation.includeSubfolders === true,
        );
        const allFolderAssets = await this.prisma.creativeAsset.findMany({
          where: {
            folderId: { in: folderIdSet },
          },
          orderBy: {
            createdAtLocal: 'asc', // Oldest first
          },
        });
        // Lọc theo KHOẢNG thời gian tạo content (mở 2 đầu tuỳ ý). Ưu tiên
        // assetCreatedRange mới; absent ⇒ fold legacy 1 mốc "sau ngày"
        // (assetCreatedAfter || assetCreationTimeFrom || creationTimeFrom) → from.
        // Dùng helper parity với mb-ads (từ ./name-term-match) để "Chạy thử" và lịch
        // chạy thật lọc GIỐNG HỆT nhau.
        const createdRange = normalizeCreatedRange(
          automation.assetCreatedRange,
          automation.assetCreatedAfter,
          automation.assetCreationTimeFrom,
          automation.creationTimeFrom,
        );
        const hasCreatedRange =
          createdRange.from != null || createdRange.to != null;
        const folderAssets = hasCreatedRange
          ? allFolderAssets.filter((asset) =>
              inCreatedRange(getAssetCreatedDate(asset), createdRange),
            )
          : allFolderAssets;
        const folderAssetIds = folderAssets.map((asset) => asset.id);

        // 3. Query all active drafts to extract assets that are currently reserved.
        // Deleted drafts are ignored so their assets can become eligible again.
        const activeDrafts = await this.prisma.systemCampaign.findMany({
          where: {
            status: Status.DRAFT,
            deletedAt: null,
            meta_id: null,
            appendOnly: false,
            OR: [{ is_template: false }, { is_template: null }],
          },
          select: {
            id: true,
            createdById: true,
            automationTemplateId: true,
            data: true,
            // id của ad_sets/ads BẮT BUỘC phải select (mirror mb-ads): inProgressDraft
            // lấy từ list này được truyền làm existingDraft cho saveAutomationDraft —
            // thiếu id thì adSetIds=[undefined].filter(Boolean)=[] → deleteMany con cũ
            // thành no-op → nháp gom dở bị NHÂN ĐÔI ad set mỗi lượt chạy.
            ad_sets: {
              select: {
                id: true,
                data: true,
                ads: {
                  select: {
                    id: true,
                    data: true,
                  },
                },
              },
            },
          },
        });

        let inProgressDraft = automation?.inProgressDraftId
          ? activeDrafts.find(
              (draft) => draft.id === automation.inProgressDraftId,
            )
          : null;
        if (
          inProgressDraft &&
          (inProgressDraft.data as any)?.automation_progress?.isComplete ===
            true
        ) {
          inProgressDraft = null;
        }
        if (!inProgressDraft) {
          inProgressDraft = activeDrafts.find((draft) => {
            const draftAutomation = (draft.data as any)?.automation;
            const isDraftComplete =
              (draft.data as any)?.automation_progress?.isComplete === true;
            return (
              !isDraftComplete &&
              draft.createdById === creator.id &&
              draft.automationTemplateId === template.id &&
              draftAutomation?.folderId === automation.folderId
            );
          });
        }
        const reservedDrafts = inProgressDraft
          ? activeDrafts.filter((draft) => draft.id !== inProgressDraft.id)
          : activeDrafts;

        const usedDraftIdentifiers = new Set<string>();
        for (const draft of reservedDrafts) {
          collectUsedMediaIdentifiers(
            draft,
            usedDraftIdentifiers,
            folderAssetIds,
          );
        }

        const existingAssetIds = Array.isArray(
          (inProgressDraft?.data as any)?.automation_used_assets,
        )
          ? ((inProgressDraft?.data as any).automation_used_assets as string[])
          : [];
        const existingAssets = await this.getAssetsByIds(existingAssetIds);
        const existingAssetsByType = this.splitAssetsByType(existingAssets);

        const launchedSystemCampaigns = folderAssetIds.length
          ? await this.prisma.systemCampaign.findMany({
              where: {
                createdByAutomation: true,
                deletedAt: null,
                appendOnly: false,
                OR: [
                  { meta_id: { not: null } },
                  { status: { not: Status.DRAFT } },
                ],
              },
              select: {
                id: true,
                data: true,
              },
            })
          : [];
        const usedSystemCampaignIdentifiers = new Set<string>();
        for (const campaign of launchedSystemCampaigns) {
          collectUsedMediaIdentifiers(
            campaign,
            usedSystemCampaignIdentifiers,
            folderAssetIds,
          );
        }

        const publishedAssetMappings = folderAssetIds.length
          ? await this.prisma.creativeAssetMapping.findMany({
              where: {
                creativeAssetId: { in: folderAssetIds },
              },
              select: { creativeAssetId: true },
            })
          : [];
        const publishedAssetIds = new Set(
          publishedAssetMappings.map((m) => m.creativeAssetId),
        );
        const folderImageHashes = folderAssets
          .map((asset) => asset.imageHash)
          .filter(Boolean) as string[];
        const folderVideoIds = folderAssets
          .map((asset) => asset.video_id)
          .filter(Boolean) as string[];
        const directlyPublishedCreatives =
          folderImageHashes.length || folderVideoIds.length
            ? await this.prisma.creative.findMany({
                where: {
                  ads: { some: {} },
                  OR: [
                    folderImageHashes.length
                      ? { imageHash: { in: folderImageHashes } }
                      : undefined,
                    folderVideoIds.length
                      ? { videoId: { in: folderVideoIds } }
                      : undefined,
                  ].filter(Boolean) as any,
                },
                select: {
                  imageHash: true,
                  videoId: true,
                },
              })
            : [];
        const publishedCreativeIdentifiers = new Set<string>();
        for (const creative of directlyPublishedCreatives) {
          if (creative.imageHash) {
            publishedCreativeIdentifiers.add(creative.imageHash);
          }
          if (creative.videoId) {
            publishedCreativeIdentifiers.add(creative.videoId);
          }
        }

        // 4. Filter assets based on published/system usage, draft state,
        // and naming rules. Usage checks are system-wide, not scoped by creator.
        const exclusionCounts = {
          alreadyUsedBySystemOrPublished: 0,
          usedInDraft: 0,
          nameRuleMismatch: 0,
          nameRuleExcluded: 0,
          noCid: 0,
        };

        // Chuẩn hoá bộ lọc TÊN đúng 1 LẦN cho cả pool: ưu tiên nameFilter mới
        // (Bao gồm OR + Loại trừ); absent/null ⇒ fold nameRule CSV legacy → include,
        // exclude = []. Cùng helper parity với mb-ads.
        const nameFilter = normalizeNameFilter(
          automation.nameFilter,
          automation.nameRule,
        );

        // Nguồn nội dung: NEW_ONLY (mặc định, chỉ content chưa từng lên camp),
        // USED_ONLY (chỉ content đã từng lên camp), BOTH (cả hai). Chỉ đổi hành vi
        // của điều kiện "đã dùng ở hệ thống/đã publish" — vẫn luôn loại content
        // đang bị bản nháp khác giữ (isUsedInDraft).
        const assetReuseMode = normalizeAssetReuseMode(
          (automation as any).assetReuseMode,
        );

        const eligibleAssets = folderAssets.filter((asset) => {
          const isUsedInDraft =
            usedDraftIdentifiers.has(asset.id) ||
            (asset.video_id && usedDraftIdentifiers.has(asset.video_id)) ||
            (asset.imageHash && usedDraftIdentifiers.has(asset.imageHash));
          const isUsedBySystemOrPublished =
            publishedAssetIds.has(asset.id) ||
            (asset.video_id &&
              publishedCreativeIdentifiers.has(asset.video_id)) ||
            (asset.imageHash &&
              publishedCreativeIdentifiers.has(asset.imageHash)) ||
            usedSystemCampaignIdentifiers.has(asset.id) ||
            (asset.video_id &&
              usedSystemCampaignIdentifiers.has(asset.video_id)) ||
            (asset.imageHash &&
              usedSystemCampaignIdentifiers.has(asset.imageHash));
          const passesReuse =
            assetReuseMode === 'BOTH'
              ? true
              : assetReuseMode === 'USED_ONLY'
                ? !!isUsedBySystemOrPublished
                : !isUsedBySystemOrPublished; // NEW_ONLY (mặc định)
          // Bộ lọc tên: fail Bao gồm (không dính từ khoá include) và dính Loại trừ
          // đếm ĐỘC LẬP thành 2 bucket. Đạt = qua Bao gồm VÀ không dính Loại trừ
          // (= matchesNameFilter). Include rỗng ⇒ luôn qua; Exclude thắng include.
          const failsInclude = failsNameInclude(asset.name, nameFilter);
          const hitsExclude = hitsNameExclude(asset.name, nameFilter);
          // Yêu cầu: chỉ lấy content có chứa mã CID trong tên (vd CID00046478).
          const hasCid = !!extractCidFromName(asset.name);

          if (!passesReuse) {
            exclusionCounts.alreadyUsedBySystemOrPublished += 1;
          }
          if (isUsedInDraft) exclusionCounts.usedInDraft += 1;
          if (failsInclude) exclusionCounts.nameRuleMismatch += 1;
          if (hitsExclude) exclusionCounts.nameRuleExcluded += 1;
          if (!hasCid) exclusionCounts.noCid += 1;

          return (
            passesReuse &&
            !isUsedInDraft &&
            !failsInclude &&
            !hitsExclude &&
            hasCid
          );
        });

        const eligibleVideos = eligibleAssets.filter(
          (a) => a.type === AssetType.VIDEO,
        );
        const eligibleImages = eligibleAssets.filter(
          (a) => a.type === AssetType.IMAGE,
        );

        // Đếm ô mẫu qua computeTemplateSlots (nguồn DUY NHẤT — phủ single/carousel/
        // child_attachments/dynamic). requiredVideos/Images GIỮ semantic cũ =
        // max(số cấu hình legacy, số ô theo loại) để bảo tồn shouldCreateEmptyDraft +
        // hiển thị. totalSlots = tổng ô (dùng cho isComplete/requiredTotal).
        const {
          slots: templateSlots,
          totalSlots,
          videoSlots,
          imageSlots,
        } = computeTemplateSlots(template.data as any);
        let requiredVideos = Number(automation.videoCount) || 0;
        let requiredImages = Number(automation.imageCount) || 0;
        if (requiredVideos < videoSlots) requiredVideos = videoSlots;
        if (requiredImages < imageSlots) requiredImages = imageSlots;

        // Tự TẠM DỪNG khi tự-đăng KHÔNG THỂ đạt chuẩn ≥5 nội dung: mẫu chỉ tạo
        // projectedContents < ngưỡng creative/chiến dịch → gom bao nhiêu content cũng
        // vô ích, chỉ tốn tài nguyên mỗi 30'. PAUSE row DraftAutomation + ghi lý do
        // (parity với gate lúc lưu ở mb-ads). Chỉ PUBLISH_IMMEDIATELY; miễn purpose
        // của mẫu là TESTING_CONTENT. Chỉ áp dụng cho nhánh mới (có draftAutomationId)
        // — nhánh legacy không có row để tạm dừng.
        // projectedContents = max(totalSlots, videoCount+imageCount legacy): automation
        // MỚI (không có 2 field legacy) = totalSlots; automation CŨ giữ số cấu hình cũ.
        const MIN_AUTO_PUBLISH_CONTENTS = await this.getMinPublishContents();
        const legacyContentCount =
          (Number(automation.videoCount) || 0) +
          (Number(automation.imageCount) || 0);
        const projectedContents = Math.max(totalSlots, legacyContentCount);
        const templateCampaignName = String(
          (template.data as any)?.campaign?.name || '',
        );
        const isTestingTemplate = template.purpose
          ? template.purpose === TemplatePurpose.TESTING_CONTENT
          : templateCampaignName.toLowerCase().includes('testingcontent');
        if (
          publishRequested &&
          override?.draftAutomationId &&
          !isTestingTemplate &&
          projectedContents < MIN_AUTO_PUBLISH_CONTENTS
        ) {
          const pauseReason =
            `Tạm dừng tự động: mẫu chỉ tạo ${projectedContents} mẫu nội dung/chiến dịch ` +
            `(cần tối thiểu ${MIN_AUTO_PUBLISH_CONTENTS} để đăng tự động lên Meta). Hãy chọn ` +
            `mẫu có ≥${MIN_AUTO_PUBLISH_CONTENTS} ô nội dung, tăng số lượng video/ảnh, hoặc ` +
            `dùng mẫu có mục đích “Test content mới”.`;
          this.logger.warn(
            `DraftAutomation ${override.draftAutomationId} (mẫu "${template.name}") tạm dừng: chỉ tạo ${projectedContents} < ${MIN_AUTO_PUBLISH_CONTENTS} nội dung.`,
          );
          await this.prisma.draftAutomation
            .update({
              where: { id: override.draftAutomationId },
              data: {
                status: 'PAUSED',
                nextRunAt: null,
                runLockedAt: null,
                lastRunAt: startedAt,
                lastRunStatus: 'SKIPPED',
                lastRunReason: pauseReason.slice(0, 1000),
              },
            })
            .catch(() => undefined);
          await this.recordAutomationHistory({
            template,
            startedAt,
            status: 'SKIPPED',
            reason: pauseReason,
            automation,
            creator,
            folderId: automation.folderId,
            publishRequested,
            publishMode,
            draftAutomationId: override?.draftAutomationId,
            steps: [
              {
                key: 'min_contents',
                label: 'Đủ số nội dung tối thiểu để đăng tự động',
                status: 'skipped',
                reason: pauseReason,
              },
            ],
          });
          lastResult = {
            status: 'SKIPPED',
            reason: pauseReason,
            isComplete: false,
            published: false,
          };
          continue;
        }

        // Rule theo TỪNG slot: { VIDEO_1: 'hook', IMAGE_2: 'demo' }. Mỗi slot lấy
        // content có TÊN chứa dấu hiệu của slot; không có dấu hiệu thì lấy content
        // đủ điều kiện cũ nhất bất kỳ; không có content khớp thì để trống slot.
        // Rule hiệu lực của ô K = slotRulesV2[K] (Nâng cao: Bao gồm/Loại trừ/khoảng
        // thời gian) nếu có, ngược lại slotRules[K] (string legacy 1 substring). Gộp
        // mọi K xuất hiện ở 1 trong 2 nguồn — planContent hiểu cả 2 dạng. Parity mb-ads.
        const slotRulesLegacy =
          automation.slotRules && typeof automation.slotRules === 'object'
            ? (automation.slotRules as Record<string, string>)
            : {};
        const slotRulesV2 =
          automation.slotRulesV2 && typeof automation.slotRulesV2 === 'object'
            ? (automation.slotRulesV2 as Record<string, SlotRuleV2>)
            : {};
        const slotRules: Record<string, string | SlotRuleV2> = {};
        for (const k of new Set([
          ...Object.keys(slotRulesLegacy),
          ...Object.keys(slotRulesV2),
        ])) {
          slotRules[k] = slotRulesV2[k] ?? slotRulesLegacy[k];
        }
        // PLANNER THỐNG NHẤT (planContent thuần, mọi loại mẫu): mỗi Ô lấy content
        // ĐÚNG loại trước, thiếu VÀ ô cho phép cross-type (single/card/attachment) thì
        // lấy chéo. Ô dynamic (DOF) chỉ nhận đúng loại. pool đã orderBy createdAtLocal
        // ASC → FIFO. contentPlan.length = totalSlots (KHÔNG bơm theo videoCount/
        // imageCount legacy → hết đốt content). Asset đang nằm trong nháp dở vẫn
        // eligible nên tự được chọn lại đúng ô ở lượt sau.
        const contentPlan: any[] = planContent(
          templateSlots,
          eligibleVideos,
          eligibleImages,
          slotRules,
        );
        const filledPlan = contentPlan.filter(Boolean);
        const slotVideos: any[] = filledPlan.filter(
          (a) => assetMediaType(a) === 'VIDEO',
        );
        const slotImages: any[] = filledPlan.filter(
          (a) => assetMediaType(a) === 'IMAGE',
        );

        const existingAssetIdSet = new Set(existingAssetIds);
        const selectedVideos = slotVideos.filter(Boolean);
        const selectedImages = slotImages.filter(Boolean);
        const selectedNewVideos = selectedVideos.filter(
          (asset) => !existingAssetIdSet.has(asset.id),
        );
        const selectedNewImages = selectedImages.filter(
          (asset) => !existingAssetIdSet.has(asset.id),
        );
        const remainingVideos = Math.max(
          0,
          requiredVideos - selectedVideos.length,
        );
        const remainingImages = Math.max(
          0,
          requiredImages - selectedImages.length,
        );
        const selectedAssetsWithLark = await this.getAssetsByIds([
          ...selectedVideos.map((asset) => asset.id),
          ...selectedImages.map((asset) => asset.id),
        ]);
        const selectedAssetWithLarkById = new Map(
          selectedAssetsWithLark.map((asset) => [asset.id, asset]),
        );
        const withLark = (asset: any) =>
          selectedAssetWithLarkById.get(asset.id) || asset;
        const selectedNewVideosForHistory = selectedNewVideos.map(withLark);
        const selectedNewImagesForHistory = selectedNewImages.map(withLark);
        const selectedVideosForHistory = selectedVideos.map(withLark);
        const selectedImagesForHistory = selectedImages.map(withLark);
        // "Đủ" = MỌI ô đã có content (bất kể loại). Mẫu 0 ô media (toàn pinnedPost) →
        // totalSlots===0 → coi là đủ (đường shouldCreateEmptyDraft xử lý tạo nháp rỗng).
        const isComplete =
          contentPlan.every(Boolean) &&
          (contentPlan.length > 0 || totalSlots === 0);
        const hasNewAssets =
          selectedNewVideos.length > 0 || selectedNewImages.length > 0;
        const shouldCreateEmptyDraft =
          !inProgressDraft && requiredVideos === 0 && requiredImages === 0;
        conditionSummary = {
          automation: {
            enabled: automation.enabled,
            folderId: automation.folderId,
            nameRule: automation.nameRule || null,
            requiredVideos,
            requiredImages,
            // requiredTotal = tổng số ô media của mẫu (nhất quán mọi loại ô).
            requiredTotal: totalSlots,
            publishToMeta: publishRequested,
            publishMode,
            scheduleCheckIntervalMinutes: 30,
            nextRunAt: automation.nextRunAt || null,
            cronExpression: automation.cronExpression,
            timezone: automation.timezone,
            runMode: automation.runMode,
            assetCreatedAfter: createdRange.from?.toISOString() || null,
          },
          creator: {
            id: creator.id,
            name: creator.name,
            employee_id: creator.employee_id,
          },
          draft: {
            inProgressDraftId: inProgressDraft?.id || null,
            existingAssetIds,
            currentVideos: existingAssetsByType.videos.length,
            currentImages: existingAssetsByType.images.length,
            currentTotal:
              existingAssetsByType.videos.length +
              existingAssetsByType.images.length,
            remainingVideos,
            remainingImages,
            selectedNewVideos: selectedNewVideos.length,
            selectedNewImages: selectedNewImages.length,
            totalSelectedVideos: selectedVideos.length,
            totalSelectedImages: selectedImages.length,
            selectedTotal: selectedVideos.length + selectedImages.length,
            isComplete,
          },
          counts: {
            folderAssets: allFolderAssets.length,
            folderAssetsAfterCreationTime: folderAssets.length,
            activeDrafts: activeDrafts.length,
            reservedDrafts: reservedDrafts.length,
            usedDraftIdentifiers: usedDraftIdentifiers.size,
            launchedSystemCampaigns: launchedSystemCampaigns.length,
            usedSystemCampaignIdentifiers: usedSystemCampaignIdentifiers.size,
            publishedAssetMappingsKnown: publishedAssetIds.size,
            publishedCreativeIdentifiersKnown:
              publishedCreativeIdentifiers.size,
            eligibleAssets: eligibleAssets.length,
            eligibleVideos: eligibleVideos.length,
            eligibleImages: eligibleImages.length,
            eligibleTotal: eligibleAssets.length,
          },
          exclusions: exclusionCounts,
          checks: [
            {
              key: 'creator',
              label: 'Người tạo có employee ID',
              status: 'passed',
            },
            {
              key: 'folder_assets',
              label: 'Tải asset từ thư mục tự động hóa',
              status: 'passed',
              count: allFolderAssets.length,
            },
            {
              key: 'asset_creation_time',
              label: 'Thời gian tạo content nằm trong khoảng cấu hình',
              status: hasCreatedRange ? 'passed' : 'not_configured',
              from: createdRange.from?.toISOString() || null,
              excluded: allFolderAssets.length - folderAssets.length,
            },
            {
              key: 'not_used_by_system_or_published',
              label: 'Asset chưa từng được chạy bằng hệ thống hoặc publish',
              status: 'passed',
              excluded: exclusionCounts.alreadyUsedBySystemOrPublished,
            },
            {
              key: 'not_used_in_active_draft',
              label: 'Asset chưa được dùng trong bản nháp đang hoạt động',
              status: 'passed',
              excluded: exclusionCounts.usedInDraft,
            },
            {
              key: 'name_rule',
              label: 'Tên asset khớp quy tắc tên của tự động hóa',
              status: automation.nameRule ? 'passed' : 'not_configured',
              rule: automation.nameRule || null,
              excluded: exclusionCounts.nameRuleMismatch,
            },
            {
              key: 'name_rule_exclude',
              label: 'Tên không dính từ khoá loại trừ',
              status: nameFilter.exclude.length ? 'passed' : 'not_configured',
              excluded: exclusionCounts.nameRuleExcluded,
            },
            {
              key: 'has_cid',
              label: 'Tên content có chứa mã CID',
              status: 'passed',
              excluded: exclusionCounts.noCid,
            },
          ],
        };

        this.logger.log(
          `Template "${template.name}": eligible videos: ${eligibleVideos.length}/${requiredVideos}, eligible images: ${eligibleImages.length}/${requiredImages}`,
        );

        if (!hasNewAssets && !isComplete && !shouldCreateEmptyDraft) {
          this.logger.log(
            `No new eligible assets for template "${template.name}". Skipping this run.`,
          );
          // CẢNH BÁO KẸT + AUTO-PAUSE: khi bản nháp đang DỞ DANG (inProgressDraft,
          // isComplete=false) mà đã SKIPPED-vì-thiếu-asset liên tiếp đủ ngưỡng thì
          // tạm dừng automation với lý do actionable — thay vì skip vô hạn mỗi 30'
          // (pool thư mục cạn) và để nháp kẹt token IMAGE_n. Chỉ nhánh mới (có
          // draftAutomationId) mới có row để tạm dừng. Lượt hiện tại tính là 1 →
          // priorConsecutive + 1 >= ngưỡng ⇒ PAUSE.
          if (override?.draftAutomationId && inProgressDraft) {
            const priorStuckSkips = await this.countConsecutiveStuckSkips(
              override.draftAutomationId,
            );
            if (
              priorStuckSkips + 1 >=
              DraftAutomationScheduler.STUCK_SKIP_PAUSE_THRESHOLD
            ) {
              const filledContents = filledPlan.length;
              const stuckPauseReason =
                `Tạm dừng: thư mục chỉ còn ${eligibleAssets.length}/${allFolderAssets.length} ` +
                `content đủ điều kiện, bản nháp đang dở dang (${filledContents}/${totalSlots} nội dung). ` +
                `Hãy bổ sung content vào thư mục hoặc nới điều kiện lọc ` +
                `(tên/khoảng ngày tạo/nguồn nội dung) rồi bật lại.`;
              this.logger.warn(
                `DraftAutomation ${override.draftAutomationId} (mẫu "${template.name}") tạm dừng: SKIPPED ${priorStuckSkips + 1} lần liên tiếp vì thiếu asset, nháp dở ${filledContents}/${totalSlots}.`,
              );
              await this.prisma.draftAutomation
                .update({
                  where: { id: override.draftAutomationId },
                  data: {
                    status: 'PAUSED',
                    nextRunAt: null,
                    runLockedAt: null,
                    lastRunAt: startedAt,
                    lastRunStatus: 'SKIPPED',
                    lastRunReason: stuckPauseReason.slice(0, 1000),
                  },
                })
                .catch(() => undefined);
              await this.recordAutomationHistory({
                template,
                startedAt,
                status: 'SKIPPED',
                reason: stuckPauseReason,
                automation,
                creator,
                folderId: automation.folderId,
                publishRequested,
                publishMode,
                draftAutomationId: override?.draftAutomationId,
                conditionSummary,
                steps: [
                  {
                    key: 'scan_assets',
                    label: 'Quét và lọc creative asset',
                    status: 'success',
                  },
                  {
                    key: 'stuck_pause',
                    label: 'Tạm dừng vì bản nháp kẹt (thiếu asset đủ điều kiện)',
                    status: 'skipped',
                    reason: stuckPauseReason,
                    eligibleAssets: eligibleAssets.length,
                    folderAssets: allFolderAssets.length,
                    filledContents,
                    totalSlots,
                  },
                ],
              });
              lastResult = {
                status: 'SKIPPED',
                reason: stuckPauseReason,
                isComplete: false,
                published: false,
              };
              continue;
            }
          }
          const noAssetsReason = inProgressDraft
            ? 'Chưa có asset mới đủ điều kiện cho bản nháp tự động hóa đang xử lý.'
            : 'Chưa có asset đủ điều kiện để bắt đầu bản nháp tự động hóa.';
          await this.recordAutomationHistory({
            template,
            startedAt,
            status: 'SKIPPED',
            reason: noAssetsReason,
            automation,
            creator,
            folderId: automation.folderId,
            publishRequested,
            publishMode,
            draftAutomationId: override?.draftAutomationId,
            conditionSummary: {
              ...conditionSummary,
              checks: [
                ...conditionSummary.checks,
                {
                  key: 'required_assets',
                  label: 'Đủ asset đã chọn cho placeholder của mẫu',
                  status: 'pending',
                  requiredVideos,
                  requiredImages,
                  selectedVideos: selectedVideos.length,
                  selectedImages: selectedImages.length,
                },
              ],
            },
            steps: [
              {
                key: 'scan_assets',
                label: 'Quét và lọc creative asset',
                status: 'success',
              },
              {
                key: 'select_assets',
                label: 'Chọn asset mới đủ điều kiện',
                status: 'skipped',
                reason: 'Chưa có asset mới đủ điều kiện',
              },
            ],
          });
          // Trạng thái lượt chạy được ghi vào row DraftAutomation ở scheduler/runNow.
          lastResult = {
            status: 'SKIPPED',
            reason: noAssetsReason,
            isComplete: false,
            published: false,
          };
          continue;
        }

        this.logger.log(
          `Generating/updating draft campaign for template "${template.name}" using videos: [${selectedVideos
            .map((v) => v.name)
            .join(
              ', ',
            )}], images: [${selectedImages.map((i) => i.name).join(', ')}]`,
        );

        const substitutedValues = this.buildSubstitutedValues({
          template,
          creator,
          videos: slotVideos,
          images: slotImages,
          publishMode,
          // Số ô cần gán token = số content THỰC mỗi loại sau khi phân bổ (có thể lệch
          // template gốc do morph), đủ chỉ số cho getNext*Slot.
          requiredVideos: slotVideos.length,
          requiredImages: slotImages.length,
          isComplete,
          automation,
          contentPlan,
          totalSlots,
        });

        generatedCampaignId = await this.saveAutomationDraft({
          existingDraft: inProgressDraft,
          substitutedValues,
          template,
          creator,
          publishMode,
          // Lịch chạy QC user chọn; absent ⇒ chỉ dọn mốc quá khứ từ mẫu.
          adSchedule: (automation?.adSchedule as AdScheduleConfig) ?? null,
        });

        let publishResult: any;
        const successSteps: any[] = [
          {
            key: 'scan_assets',
            label: 'Quét và lọc creative asset',
            status: 'success',
          },
          {
            key: 'select_assets',
            label: 'Chọn asset đủ điều kiện cũ nhất',
            status: 'success',
            existingVideos: existingAssetsByType.videos.map(summarizeAsset),
            existingImages: existingAssetsByType.images.map(summarizeAsset),
            newVideos: selectedNewVideosForHistory.map(summarizeAsset),
            newImages: selectedNewImagesForHistory.map(summarizeAsset),
            videos: selectedVideosForHistory.map(summarizeAsset),
            images: selectedImagesForHistory.map(summarizeAsset),
          },
          {
            key: inProgressDraft ? 'update_draft' : 'create_draft',
            label: inProgressDraft
              ? 'Cập nhật bản nháp chiến dịch tự động'
              : 'Tạo bản nháp chiến dịch',
            status: 'success',
            campaignId: generatedCampaignId,
            isComplete,
          },
        ];

        if (publishRequested && isComplete) {
          successSteps.push({
            key: 'publish_meta',
            label: 'Đăng bản nháp chiến dịch lên Meta',
            status: 'processing',
          });

          publishResult =
            await this.metaPublisher.publishDraftCampaign(generatedCampaignId);

          if (publishResult?.skipped) {
            // Bị khóa chống trùng bỏ qua (tiến trình khác đang publish hoặc đã
            // publish) — KHÔNG đánh dấu thành công và KHÔNG ghi nhận asset đã dùng
            // ở lần chạy này, để tránh báo nhầm.
            successSteps[successSteps.length - 1] = {
              key: 'publish_meta',
              label: 'Đăng bản nháp chiến dịch lên Meta',
              status: 'skipped',
              reason:
                'Đang được publish bởi tiến trình khác hoặc đã được publish',
            };
          } else {
            successSteps[successSteps.length - 1] = {
              key: 'publish_meta',
              label: 'Đăng bản nháp chiến dịch lên Meta',
              status: 'success',
              metaCampaignId: publishResult.campaignId,
              publishHistoryId: publishResult.publishHistoryId,
            };

            for (const asset of [...selectedVideos, ...selectedImages]) {
              publishedAssetIds.add(asset.id);
            }
          }
        } else if (publishRequested && !isComplete) {
          successSteps.push({
            key: 'publish_meta',
            label: 'Đăng bản nháp chiến dịch lên Meta',
            status: 'skipped',
            reason: 'Bản nháp chưa đủ dữ liệu',
          });
        }

        // Trạng thái lượt chạy (COMPLETED / nextRunAt / lastRunStatus…) được ghi vào
        // row DraftAutomation ở DraftAutomationEntityScheduler.runOne / runNow (mb-ads).
        // Reason cho lượt tạo/cập nhật nháp DỞ DANG (isComplete=false): nêu rõ đã
        // lấp bao nhiêu/tổng ô, còn thiếu mấy ô, và còn bao nhiêu asset đủ điều kiện
        // chưa dùng — để người vận hành thấy ngay từ lần đầu là pool sắp cạn (nếu
        // "còn lại 0" thì các lượt sau sẽ skip → tiến tới auto-PAUSE).
        const filledContents = filledPlan.length;
        const missingSlots = Math.max(0, totalSlots - filledContents);
        const remainingEligible = Math.max(
          0,
          eligibleAssets.length - filledContents,
        );
        const partialReason =
          (inProgressDraft
            ? 'Đã cập nhật bản nháp chiến dịch tự động'
            : 'Đã tạo bản nháp chiến dịch tự động') +
          ` (mới lấp ${filledContents}/${totalSlots} nội dung, còn thiếu ${missingSlots} ô; ` +
          `còn ${remainingEligible} content đủ điều kiện trong thư mục).`;
        const successReason =
          publishRequested && isComplete
            ? 'Đã tạo bản nháp chiến dịch tự động và đăng lên Meta.'
            : isComplete
              ? 'Bản nháp chiến dịch tự động đã hoàn tất.'
              : partialReason;

        await this.recordAutomationHistory({
          template,
          startedAt,
          status: 'SUCCESS',
          reason: successReason,
          automation,
          creator,
          folderId: automation.folderId,
          publishRequested,
          publishMode,
          draftAutomationId: override?.draftAutomationId,
          publishResult,
          conditionSummary: {
            ...conditionSummary,
            checks: [
              ...conditionSummary.checks,
              {
                key: 'required_assets',
                label: 'Đủ asset đã chọn cho placeholder của mẫu',
                status: isComplete ? 'passed' : 'pending',
                requiredVideos,
                requiredImages,
                selectedVideos: selectedVideos.length,
                selectedImages: selectedImages.length,
              },
            ],
          },
          selectedAssets: {
            videos: selectedVideosForHistory.map(summarizeAsset),
            images: selectedImagesForHistory.map(summarizeAsset),
          },
          generatedCampaignId,
          steps: successSteps,
        });

        lastResult = {
          status: 'SUCCESS',
          reason: successReason,
          isComplete,
          // Chỉ coi là đã publish khi thực sự đăng ở lượt này (không bị khóa chống
          // trùng bỏ qua). Dùng cho quyết định COMPLETED khi publishMode=IMMEDIATELY.
          published: !!(
            publishRequested &&
            isComplete &&
            publishResult &&
            !publishResult.skipped
          ),
          generatedCampaignId,
        };

        this.logger.log(
          publishRequested && isComplete
            ? `Successfully created and published automated campaign for template "${template.name}".`
            : `Successfully created/updated automated draft campaign for template "${template.name}". Complete: ${isComplete}`,
        );
      } catch (err: any) {
        const failureSteps =
          publishRequested && generatedCampaignId
            ? [
                {
                  key: 'create_draft',
                  label: 'Tạo bản nháp chiến dịch',
                  status: 'success',
                  campaignId: generatedCampaignId,
                },
                {
                  key: 'publish_meta',
                  label: 'Đăng bản nháp chiến dịch lên Meta',
                  status: 'failed',
                  error: err?.metaError?.message || err?.message || String(err),
                },
              ]
            : [
                {
                  key: 'process_template',
                  label: 'Xử lý mẫu tự động hóa',
                  status: 'failed',
                  error: err?.message || String(err),
                },
              ];

        // Kèm nguyên nhân thật vào reason để user/dev tự chẩn đoán (trước đây bị
        // nuốt thành câu chung chung → 313 lần FAILED không rõ lý do). Chi tiết đầy
        // đủ vẫn nằm ở cột `error` + steps[]. Parity với mb-ads (reason kèm errMsg).
        const failureCause = String(
          err?.metaError?.message || err?.message || err || '',
        )
          .split('\n')[0]
          .trim()
          .slice(0, 300);
        const failureReason =
          publishRequested && generatedCampaignId
            ? `Đã tạo bản nháp chiến dịch nhưng đăng lên Meta thất bại${
                failureCause ? `: ${failureCause}` : '.'
              }`
            : `Có lỗi khi xử lý mẫu${failureCause ? `: ${failureCause}` : '.'}`;

        await this.recordAutomationHistory({
          template,
          startedAt,
          status: 'FAILED',
          reason: failureReason,
          automation,
          creator,
          folderId: automation?.folderId,
          publishRequested,
          publishMode,
          draftAutomationId: override?.draftAutomationId,
          conditionSummary,
          steps: failureSteps,
          generatedCampaignId,
          publishResult: err?.metaError ? { error: err.metaError } : undefined,
          error: this.formatError(err),
        });
        this.logger.error(
          `Error processing template "${template.name}":`,
          this.formatError(err),
        );
        lastResult = {
          status: 'FAILED',
          reason: failureReason,
          isComplete: false,
          published: false,
          generatedCampaignId,
        };
      }
    }

    return lastResult;
  }

  /**
   * Map một row DraftAutomation → ĐÚNG shape cấu hình mà engine dựng-nháp đang đọc
   * từ template.data.automation. Nguồn: cột của row + JSON `conditions`. Không thêm
   * field lạ ngoài những gì engine dùng (folderId, videoCount, imageCount, nameRule,
   * assetCreatedAfter, slotRules, publishMode, runMode, cronExpression, timezone).
   * normalizeAutomation() sẽ chuẩn hoá tiếp (publishMode/runMode/cron/timezone).
   */
  // Gom tập folderId nguồn: folder gốc + TOÀN BỘ con cháu (khi includeSub=true).
  // GIỮ NGUYÊN VĂN Ở CẢ mb-ads LẪN mb-batch (parity — hai engine phải gom giống hệt).
  // Legacy truyền includeSub=false => chỉ các folder gốc, y hệt hành vi cũ.
  private async resolveAutomationFolderIds(
    roots: string[],
    includeSub: boolean,
  ): Promise<string[]> {
    const clean = Array.from(new Set((roots || []).filter(Boolean)));
    if (!clean.length || !includeSub) return clean;
    const all = await this.prisma.creativeFolder.findMany({
      select: { id: true, parentId: true },
    });
    const childrenOf = new Map<string, string[]>();
    for (const f of all) {
      if (!f.parentId) continue;
      const arr = childrenOf.get(f.parentId);
      if (arr) arr.push(f.id);
      else childrenOf.set(f.parentId, [f.id]);
    }
    const out = new Set<string>();
    const stack = [...clean];
    while (stack.length) {
      const id = stack.pop() as string;
      if (out.has(id)) continue;
      out.add(id);
      for (const c of childrenOf.get(id) || []) stack.push(c);
    }
    return Array.from(out);
  }

  draftAutomationToAutomationConfig(row: {
    folderId: string | null;
    conditions: any;
    publishMode: string;
    runMode: string;
    cronExpression: string | null;
    timezone: string | null;
    inProgressDraftId?: string | null;
  }) {
    const conditions =
      row.conditions && typeof row.conditions === 'object'
        ? (row.conditions as any)
        : {};
    return {
      // enabled=true: engine dùng cờ này để quyết định chạy. Row đã được lọc
      // ACTIVE ở tầng scheduler nên tới đây luôn bật.
      enabled: true,
      folderId: row.folderId ?? conditions.folderId ?? undefined,
      folderIds:
        Array.isArray(conditions.folderIds) && conditions.folderIds.length
          ? conditions.folderIds
          : undefined,
      includeSubfolders:
        conditions.includeSubfolders === true ? true : undefined,
      nameRule: conditions.nameRule ?? undefined,
      // Bộ lọc tên nâng cao (Bao gồm OR + Loại trừ). Non-null ⇒ engine BỎ QUA
      // nameRule legacy; absent ⇒ fold nameRule CSV. Xem normalizeNameFilter.
      nameFilter: conditions.nameFilter ?? undefined,
      videoCount: conditions.videoCount ?? undefined,
      imageCount: conditions.imageCount ?? undefined,
      assetCreatedAfter: conditions.assetCreatedAfter ?? undefined,
      // Khoảng thời gian tạo content (mở 2 đầu). Non-null ⇒ engine BỎ QUA legacy
      // assetCreatedAfter/assetCreationTimeFrom/creationTimeFrom. Xem normalizeCreatedRange.
      assetCreatedRange: conditions.assetCreatedRange ?? undefined,
      // slotRules (nếu có) giữ nguyên khoá VIDEO_n / IMAGE_n mà engine đã hiểu.
      slotRules: conditions.slotRules ?? undefined,
      // Rule per-ô NÂNG CAO (Bao gồm/Loại trừ/khoảng thời gian). Ưu tiên hơn
      // slotRules legacy theo từng khoá khi merge trước planContent.
      slotRulesV2: conditions.slotRulesV2 ?? undefined,
      // Lịch chạy QC của camp dựng ra (Meta start_time/end_time cấp NHÓM). Absent ⇒
      // giữ hành vi cũ (chỉ dọn mốc quá khứ). Xem resolveAdSchedule / AdScheduleConfig.
      adSchedule: conditions.adSchedule ?? undefined,
      cidRequired: conditions.cidRequired ?? undefined,
      assetReuseMode: conditions.assetReuseMode ?? undefined,
      publishMode: row.publishMode,
      runMode: row.runMode,
      cronExpression: row.cronExpression ?? undefined,
      timezone: row.timezone ?? undefined,
      // Nháp "đang gom dở" của CHÍNH row này → engine tìm lại đúng nháp đó thay vì
      // heuristic template+creator+folder (chống latch nhầm nháp automation khác — A7).
      inProgressDraftId: row.inProgressDraftId ?? undefined,
    };
  }

  /**
   * Chạy MỘT lượt dựng-nháp cho một DraftAutomation row (sourceType=TEMPLATE),
   * DÙNG LẠI toàn bộ engine qua processAutomation(_, override). KHÔNG tự chọn asset
   * / lấp slot / thay thế / publish — tất cả nằm trong engine sẵn có. Trả về kết quả
   * để scheduler cập nhật run-tracking + lịch kế tiếp trên row.
   */
  async runDraftAutomationOnce(row: {
    id: string;
    templateId: string | null;
    folderId: string | null;
    conditions: any;
    publishMode: string;
    runMode: string;
    cronExpression: string | null;
    timezone: string | null;
  }): Promise<AutomationRunResult> {
    if (!row.templateId) {
      return {
        status: 'SKIPPED',
        reason: 'DraftAutomation chưa gắn template nguồn.',
        isComplete: false,
        published: false,
      };
    }
    const template = await this.prisma.templateCampaign.findFirst({
      where: { id: row.templateId, deletedAt: null },
    });
    if (!template) {
      return {
        status: 'SKIPPED',
        reason: 'Không tìm thấy template nguồn của DraftAutomation.',
        isComplete: false,
        published: false,
      };
    }
    const rawAutomation = this.draftAutomationToAutomationConfig(row);
    return this.processAutomation({
      template,
      rawAutomation,
      draftAutomationId: row.id,
    });
  }
}
