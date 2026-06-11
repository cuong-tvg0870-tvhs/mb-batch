import { Injectable, Logger } from '@nestjs/common';
import { AssetType, Status } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { DraftAutomationMetaPublisherService } from './draft-automation-meta-publisher.service';

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

function extractMediaIdentifiersFromString(
  jsonString: string,
  keys: Set<string> = new Set(),
  scopedIds: string[] = [],
): Set<string> {
  if (!jsonString) return keys;

  const identifierRegex = /"([a-fA-F0-9]{32}|\d+)"/g;
  let match: RegExpExecArray | null;
  while ((match = identifierRegex.exec(jsonString)) !== null) {
    keys.add(match[1]);
  }

  for (const id of scopedIds) {
    if (id && jsonString.includes(id)) {
      keys.add(id);
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
  if (
    spec.video_data?.video_id ||
    creative.videoId ||
    creative.video_id
  ) {
    return 'VIDEO';
  }

  // Image check
  if (
    spec.link_data?.image_hash ||
    creative.imageHash ||
    creative.image_hash
  ) {
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
      if (idx >= 0 && idx < videos.length) {
        return videos[idx];
      }
    }
    const imageMatch = obj.match(/^IMAGE_(\d+)$/);
    if (imageMatch) {
      const idx = parseInt(imageMatch[1], 10) - 1;
      if (idx >= 0 && idx < images.length) {
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
          if (idx >= 0 && idx < videos.length) {
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
          if (idx >= 0 && idx < images.length) {
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
        videos.find(
          (item) =>
            item.video_id === newObj.videoId ||
            item.video_id === newObj.video_id ||
            item.id === newObj.id,
        );
      if (video) {
        return enrichVideoPlaceholderObject(newObj, video);
      }
    }
    if (matchedImage || (obj.placeholder && finalMediaType === 'IMAGE')) {
      const img =
        matchedImage ||
        images.find(
          (item) =>
            item.imageHash === newObj.imageHash ||
            item.image_hash === newObj.image_hash ||
            item.id === newObj.id,
        );
      if (img) {
        return enrichImagePlaceholderObject(newObj, img);
      }
    }
    return newObj;
  }

  return obj;
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

@Injectable()
export class DraftAutomationScheduler {
  private readonly logger = new Logger(DraftAutomationScheduler.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaPublisher: DraftAutomationMetaPublisherService,
  ) {}

  private formatError(err: any) {
    return err?.stack || err?.message || String(err);
  }

  private async recordAutomationHistory(input: {
    template: any;
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

  private async updateTemplateAutomationState(template: any, patch: any) {
    const currentData = ((template.data || {}) as any) || {};
    const currentAutomation = currentData.automation || {};
    const nextAutomation = {
      ...currentAutomation,
      ...patch,
    };

    template.data = {
      ...currentData,
      automation: nextAutomation,
    };

    await this.prisma.templateCampaign.update({
      where: { id: template.id },
      data: { data: template.data as any },
    });
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
  }) {
    const {
      template,
      creator,
      videos,
      images,
      publishMode,
      requiredVideos,
      requiredImages,
      isComplete,
      automation,
    } = input;
    const templateData = template.data as any;

    // Deep clone the template campaign data to avoid mutating database/in-memory template object
    const clonedTemplateData = JSON.parse(JSON.stringify(templateData));

    // Find all explicitly pre-selected slots in the template
    const usedVideos = new Set<number>();
    const usedImages = new Set<number>();
    findExistingSlots(clonedTemplateData, usedVideos, usedImages);

    // Build lists of available slot indexes within the required range
    const availableVideoIndexes: number[] = [];
    for (let i = 1; i <= requiredVideos; i++) {
      if (!usedVideos.has(i)) {
        availableVideoIndexes.push(i);
      }
    }

    const availableImageIndexes: number[] = [];
    for (let i = 1; i <= requiredImages; i++) {
      if (!usedImages.has(i)) {
        availableImageIndexes.push(i);
      }
    }

    let nextVideoPtr = 0;
    const getNextVideoSlot = (): string => {
      if (requiredVideos <= 0) return '';
      if (availableVideoIndexes.length > 0) {
        const idx = availableVideoIndexes[nextVideoPtr % availableVideoIndexes.length];
        nextVideoPtr++;
        return `VIDEO_${idx}`;
      } else {
        const idx = (nextVideoPtr % requiredVideos) + 1;
        nextVideoPtr++;
        return `VIDEO_${idx}`;
      }
    };

    let nextImagePtr = 0;
    const getNextImageSlot = (): string => {
      if (requiredImages <= 0) return '';
      if (availableImageIndexes.length > 0) {
        const idx = availableImageIndexes[nextImagePtr % availableImageIndexes.length];
        nextImagePtr++;
        return `IMAGE_${idx}`;
      } else {
        const idx = (nextImagePtr % requiredImages) + 1;
        nextImagePtr++;
        return `IMAGE_${idx}`;
      }
    };

    const isSlotPlaceholder = (val: any): boolean => {
      if (typeof val !== 'string') return false;
      return /^VIDEO_\d+$/.test(val) || /^IMAGE_\d+$/.test(val);
    };

    const autoAssignCreativeSlots = (creative: any) => {
      if (!creative) return;

      const mediaType = inferMediaType(creative);
      const spec = creative.object_story_spec || {};

      if (mediaType === 'VIDEO') {
        const hasVideoSlot = isSlotPlaceholder(
          creative.videoId ||
          creative.video_id ||
          spec.video_data?.video_id
        );
        if (!hasVideoSlot) {
          const slot = getNextVideoSlot();
          if (slot) {
            // Clean format
            creative.videoId = slot;
            creative.video_id = slot;
            creative.imageHash = slot;
            creative.image_hash = slot;
            creative.selected_thumbnail_id = slot;

            // Raw Meta spec format support
            if (spec.video_data) {
              spec.video_data.video_id = slot;
              spec.video_data.image_id = slot;
              spec.video_data.image_hash = slot;
            }

            creative.placeholder = true;
          }
        }
      } else if (mediaType === 'IMAGE') {
        const hasImageSlot = isSlotPlaceholder(
          creative.imageHash ||
          creative.image_hash ||
          spec.link_data?.image_hash
        );
        if (!hasImageSlot) {
          const slot = getNextImageSlot();
          if (slot) {
            // Clean format
            creative.imageHash = slot;
            creative.image_hash = slot;

            // Raw Meta spec format support
            if (spec.link_data) {
              spec.link_data.image_hash = slot;
            }

            creative.placeholder = true;
          }
        }
      } else if (mediaType === 'CAROUSEL') {
        // Clean format cards
        if (Array.isArray(creative.carouselCards)) {
          for (const card of creative.carouselCards) {
            const cardType = String(card.mediaType).toUpperCase();
            if (cardType === 'VIDEO') {
              const hasVideoSlot = isSlotPlaceholder(card.videoId);
              if (!hasVideoSlot) {
                const slot = getNextVideoSlot();
                if (slot) {
                  card.videoId = slot;
                  card.imageHash = slot;
                  card.selected_thumbnail_id = slot;
                  card.placeholder = true;
                }
              }
            } else {
              const hasImageSlot = isSlotPlaceholder(card.imageHash);
              if (!hasImageSlot) {
                const slot = getNextImageSlot();
                if (slot) {
                  card.imageHash = slot;
                  card.placeholder = true;
                }
              }
            }
          }
        }

        // Raw Meta format attachments
        if (Array.isArray(spec.link_data?.child_attachments)) {
          for (const attachment of spec.link_data.child_attachments) {
            if (attachment.video_id || attachment.videoId) {
              const hasVideoSlot = isSlotPlaceholder(attachment.video_id || attachment.videoId);
              if (!hasVideoSlot) {
                const slot = getNextVideoSlot();
                if (slot) {
                  attachment.video_id = slot;
                  attachment.videoId = slot;
                  attachment.image_hash = slot;
                  attachment.imageHash = slot;
                  attachment.placeholder = true;
                }
              }
            } else {
              const hasImageSlot = isSlotPlaceholder(attachment.image_hash || attachment.imageHash);
              if (!hasImageSlot) {
                const slot = getNextImageSlot();
                if (slot) {
                  attachment.image_hash = slot;
                  attachment.imageHash = slot;
                  attachment.placeholder = true;
                }
              }
            }
          }
        }
      }

      if (Array.isArray(creative.dynamicAssets)) {
        for (const asset of creative.dynamicAssets) {
          const assetType = String(asset.type).toUpperCase();
          if (assetType === 'VIDEO') {
            const hasVideoSlot = isSlotPlaceholder(
              asset.videoId || asset.video_id,
            );
            if (!hasVideoSlot) {
              const slot = getNextVideoSlot();
              if (slot) {
                asset.videoId = slot;
                asset.video_id = slot;
                asset.imageHash = slot;
                asset.image_hash = slot;
                asset.placeholder = true;
              }
            }
          } else if (assetType === 'IMAGE') {
            const hasImageSlot = isSlotPlaceholder(
              asset.imageHash || asset.image_hash,
            );
            if (!hasImageSlot) {
              const slot = getNextImageSlot();
              if (slot) {
                asset.imageHash = slot;
                asset.image_hash = slot;
                asset.placeholder = true;
              }
            }
          }
        }
      }
    };

    if (Array.isArray(clonedTemplateData.ad_sets)) {
      for (const adset of clonedTemplateData.ad_sets) {
        if (Array.isArray(adset.ads)) {
          for (const ad of adset.ads) {
            if (ad.creative) {
              autoAssignCreativeSlots(ad.creative);
            }
          }
        }
      }
    }

    const substitutedValues = replacePlaceholders(clonedTemplateData, videos, images);

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
        (adset: any) => {
          if (adset.name) {
            adset.name = updateName(adset.name, employeeId, employeeName);
          }
          if (Array.isArray(adset.ads)) {
            adset.ads = adset.ads.map((ad: any) => {
              if (ad.name) {
                ad.name = updateName(ad.name, employeeId, employeeName);
              }
              return ad;
            });
          }
          return adset;
        },
      );
    }

    substitutedValues.automation_used_assets = [
      ...videos.map((v) => v.id),
      ...images.map((i) => i.id),
    ];
    substitutedValues.automation_progress = {
      templateId: template.id,
      templateName: template.name,
      requiredVideos,
      requiredImages,
      currentVideos: videos.length,
      currentImages: images.length,
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
  }) {
    const { existingDraft, substitutedValues, template, creator, publishMode } =
      input;

    return this.prisma.$transaction(async (tx) => {
      const campaignData = {
        accountId: substitutedValues.ad_account_id,
        createdById: creator.id,
        status: Status.DRAFT,
        createdByAutomation: true,
        automationTemplateId: template.id,
        automationTemplateName: template.name,
        automationPublishMode: publishMode,
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
        const adSetIds = existingDraft.ad_sets.map((adset: any) => adset.id);
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

  async processAutomation(templateId?: string) {
    // 1. Fetch all templates with automation configured
    const templates = await this.prisma.templateCampaign.findMany({
      where: {
        deletedAt: null,
        ...(templateId ? { id: templateId } : {}),
      },
    });

    const activeTemplates = templates.filter((template) => {
      const automation = (template.data as any)?.automation;
      return automation?.enabled === true && automation?.folderId;
    });

    if (activeTemplates.length === 0) {
      this.logger.log('No templates configured with active automation rules.');
      return;
    }

    this.logger.log(
      `Found ${activeTemplates.length} templates with active automation rules.`,
    );

    for (const template of activeTemplates) {
      const startedAt = new Date();
      let automation: any;
      let creator: any;
      let conditionSummary: any;
      let generatedCampaignId: string | undefined;
      let publishRequested = false;
      let publishMode: 'DRAFT_ONLY' | 'PUBLISH_IMMEDIATELY' = 'DRAFT_ONLY';
      try {
        automation = this.normalizeAutomation(
          (template.data as any).automation,
        );
        if (
          automation.enabled !== true ||
          automation.status === 'PAUSED' ||
          automation.status === 'DISABLED'
        ) {
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
            steps: [
              {
                key: 'creator',
                label: 'Kiểm tra người tạo mẫu',
                status: 'skipped',
                reason: 'ID người tạo đang trống',
              },
            ],
          });
          continue;
        }

        creator = await this.prisma.user.findUnique({
          where: { id: creatorId },
        });

        if (!creator || !creator.employee_id) {
          this.logger.warn(
            `Creator of template ${template.name} has no employee ID. Skipping.`,
          );
          await this.recordAutomationHistory({
            template,
            startedAt,
            status: 'SKIPPED',
            reason: !creator
              ? 'Không tìm thấy người tạo mẫu.'
              : 'Người tạo template chưa có employee ID.',
            automation,
            creator,
            folderId: automation.folderId,
            publishRequested,
            publishMode,
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
          continue;
        }

        this.logger.log(
          `Processing template "${template.name}" for user ${creator.name} (${creator.employee_id}). folderId: ${automation.folderId}`,
        );

        // 2. Fetch all assets in the target folder
        const allFolderAssets = await this.prisma.creativeAsset.findMany({
          where: {
            folderId: automation.folderId,
          },
          orderBy: {
            createdAtLocal: 'asc', // Oldest first
          },
        });
        const assetCreatedAfter = parseValidDate(
          automation.assetCreatedAfter ||
            automation.assetCreationTimeFrom ||
            automation.creationTimeFrom,
        );
        const folderAssets = assetCreatedAfter
          ? allFolderAssets.filter((asset) => {
              const assetCreationDate = getAssetCreationDate(asset);
              return (
                assetCreationDate &&
                assetCreationDate.getTime() >= assetCreatedAfter.getTime()
              );
            })
          : allFolderAssets;
        const folderAssetIds = folderAssets.map((asset) => asset.id);

        // 3. Query all active drafts of this creator to extract used assets
        const activeDrafts = await this.prisma.systemCampaign.findMany({
          where: {
            createdById: creator.id,
            status: Status.DRAFT,
            deletedAt: null,
          },
          select: {
            data: true,
            ad_sets: {
              select: {
                data: true,
                ads: {
                  select: {
                    data: true,
                  },
                },
              },
            },
          },
        });

        const usedDraftIdentifiers = new Set<string>();
        for (const draft of activeDrafts) {
          extractMediaIdentifiersFromString(
            JSON.stringify(draft),
            usedDraftIdentifiers,
            folderAssetIds,
          );
        }

        const inProgressDraft = await this.getInProgressDraft(
          template,
          creator.id,
          automation,
        );
        const existingAssetIds = Array.isArray(
          (inProgressDraft?.data as any)?.automation_used_assets,
        )
          ? ((inProgressDraft?.data as any).automation_used_assets as string[])
          : [];
        const existingAssets = await this.getAssetsByIds(existingAssetIds);
        const existingAssetsByType = this.splitAssetsByType(existingAssets);

        const creatorSystemCampaignAssetMappings = folderAssetIds.length
          ? await this.prisma.creativeAssetMapping.findMany({
              where: {
                creativeAssetId: { in: folderAssetIds },
                creative: {
                  ads: {
                    some: {
                      campaign: {
                        systemCampaign: {
                          createdById: creator.id,
                        },
                      },
                    },
                  },
                },
              },
              select: { creativeAssetId: true },
            })
          : [];
        const usedByCreatorInSystemCampaignAssetIds = new Set(
          creatorSystemCampaignAssetMappings.map((m) => m.creativeAssetId),
        );

        // 4. Filter assets based on creator system-campaign usage, draft state,
        // and naming rules. This is scoped by system user ID, not Lark metadata.
        const exclusionCounts = {
          alreadyUsedByCreatorInSystemCampaign: 0,
          usedInDraft: 0,
          nameRuleMismatch: 0,
        };

        const eligibleAssets = folderAssets.filter((asset) => {
          const isUsedInDraft =
            usedDraftIdentifiers.has(asset.id) ||
            (asset.video_id && usedDraftIdentifiers.has(asset.video_id)) ||
            (asset.imageHash && usedDraftIdentifiers.has(asset.imageHash));
          const isUsedByCreatorInSystemCampaign =
            usedByCreatorInSystemCampaignAssetIds.has(asset.id);
          const matchesNameRule =
            !automation.nameRule ||
            (asset.name || '')
              .toLowerCase()
              .includes(automation.nameRule.toLowerCase());

          if (isUsedByCreatorInSystemCampaign) {
            exclusionCounts.alreadyUsedByCreatorInSystemCampaign += 1;
          }
          if (isUsedInDraft) exclusionCounts.usedInDraft += 1;
          if (!matchesNameRule) exclusionCounts.nameRuleMismatch += 1;

          return (
            !isUsedByCreatorInSystemCampaign &&
            !isUsedInDraft &&
            matchesNameRule
          );
        });

        const eligibleVideos = eligibleAssets.filter(
          (a) => a.type === AssetType.VIDEO,
        );
        const eligibleImages = eligibleAssets.filter(
          (a) => a.type === AssetType.IMAGE,
        );

        let requiredVideos = Number(automation.videoCount) || 0;
        let requiredImages = Number(automation.imageCount) || 0;

        // Count actual video and image slots or hardcoded media in the template
        let templateVideoCount = 0;
        let templateImageCount = 0;

        const countCreativeAssets = (creative: any) => {
          if (!creative) return;
          const mediaType = inferMediaType(creative);
          const spec = creative.object_story_spec || {};

          if (mediaType === 'VIDEO') {
            templateVideoCount++;
          } else if (mediaType === 'IMAGE') {
            templateImageCount++;
          } else if (mediaType === 'CAROUSEL') {
            if (Array.isArray(creative.carouselCards)) {
              for (const card of creative.carouselCards) {
                const cardType = String(card.mediaType || '').toUpperCase();
                if (cardType === 'VIDEO') {
                  templateVideoCount++;
                } else {
                  templateImageCount++;
                }
              }
            }
            if (Array.isArray(spec.link_data?.child_attachments)) {
              for (const attachment of spec.link_data.child_attachments) {
                if (attachment.video_id || attachment.videoId) {
                  templateVideoCount++;
                } else {
                  templateImageCount++;
                }
              }
            }
          }
          if (Array.isArray(creative.dynamicAssets)) {
            for (const asset of creative.dynamicAssets) {
              const assetType = String(asset.type || asset.mediaType || '').toUpperCase();
              if (assetType === 'VIDEO') {
                templateVideoCount++;
              } else if (assetType === 'IMAGE') {
                templateImageCount++;
              }
            }
          }
        };

        const templateData = template.data as any;
        if (templateData && Array.isArray(templateData.ad_sets)) {
          for (const adset of templateData.ad_sets) {
            if (Array.isArray(adset.ads)) {
              for (const ad of adset.ads) {
                if (ad.creative) {
                  countCreativeAssets(ad.creative);
                }
              }
            }
          }
        }

        if (requiredVideos < templateVideoCount) {
          requiredVideos = templateVideoCount;
        }
        if (requiredImages < templateImageCount) {
          requiredImages = templateImageCount;
        }
        const remainingVideos = Math.max(
          0,
          requiredVideos - existingAssetsByType.videos.length,
        );
        const remainingImages = Math.max(
          0,
          requiredImages - existingAssetsByType.images.length,
        );
        const selectedNewVideos = eligibleVideos.slice(0, remainingVideos);
        const selectedNewImages = eligibleImages.slice(0, remainingImages);
        const selectedVideos = [
          ...existingAssetsByType.videos,
          ...selectedNewVideos,
        ].slice(0, requiredVideos);
        const selectedImages = [
          ...existingAssetsByType.images,
          ...selectedNewImages,
        ].slice(0, requiredImages);
        const selectedNewAssetsWithLark = await this.getAssetsByIds([
          ...selectedNewVideos.map((asset) => asset.id),
          ...selectedNewImages.map((asset) => asset.id),
        ]);
        const selectedNewAssetWithLarkById = new Map(
          selectedNewAssetsWithLark.map((asset) => [asset.id, asset]),
        );
        const selectedNewVideosForHistory = selectedNewVideos.map(
          (asset) => selectedNewAssetWithLarkById.get(asset.id) || asset,
        );
        const selectedNewImagesForHistory = selectedNewImages.map(
          (asset) => selectedNewAssetWithLarkById.get(asset.id) || asset,
        );
        const selectedVideosForHistory = [
          ...existingAssetsByType.videos,
          ...selectedNewVideosForHistory,
        ].slice(0, requiredVideos);
        const selectedImagesForHistory = [
          ...existingAssetsByType.images,
          ...selectedNewImagesForHistory,
        ].slice(0, requiredImages);
        const isComplete =
          selectedVideos.length >= requiredVideos &&
          selectedImages.length >= requiredImages;
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
            publishToMeta: publishRequested,
            publishMode,
            scheduleCheckIntervalMinutes: 30,
            nextRunAt: automation.nextRunAt || null,
            cronExpression: automation.cronExpression,
            timezone: automation.timezone,
            runMode: automation.runMode,
            assetCreatedAfter: assetCreatedAfter?.toISOString() || null,
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
            remainingVideos,
            remainingImages,
            selectedNewVideos: selectedNewVideos.length,
            selectedNewImages: selectedNewImages.length,
            totalSelectedVideos: selectedVideos.length,
            totalSelectedImages: selectedImages.length,
            isComplete,
          },
          counts: {
            folderAssets: allFolderAssets.length,
            folderAssetsAfterCreationTime: folderAssets.length,
            activeDrafts: activeDrafts.length,
            usedDraftIdentifiers: usedDraftIdentifiers.size,
            creatorSystemCampaignUsedAssetsKnown:
              usedByCreatorInSystemCampaignAssetIds.size,
            eligibleAssets: eligibleAssets.length,
            eligibleVideos: eligibleVideos.length,
            eligibleImages: eligibleImages.length,
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
              label:
                'Asset có creation_time nằm trong khoảng thời gian automation',
              status: assetCreatedAfter ? 'passed' : 'not_configured',
              from: assetCreatedAfter?.toISOString() || null,
              excluded: allFolderAssets.length - folderAssets.length,
            },
            {
              key: 'not_used_by_creator_in_system_campaign',
              label:
                'Asset chưa được người tạo này dùng trong campaign tạo từ hệ thống',
              status: 'passed',
              excluded: exclusionCounts.alreadyUsedByCreatorInSystemCampaign,
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
          ],
        };

        this.logger.log(
          `Template "${template.name}": eligible videos: ${eligibleVideos.length}/${requiredVideos}, eligible images: ${eligibleImages.length}/${requiredImages}`,
        );

        if (!hasNewAssets && !isComplete && !shouldCreateEmptyDraft) {
          this.logger.log(
            `No new eligible assets for template "${template.name}". Skipping this run.`,
          );
          await this.recordAutomationHistory({
            template,
            startedAt,
            status: 'SKIPPED',
            reason: inProgressDraft
              ? 'Chưa có asset mới đủ điều kiện cho bản nháp tự động hóa đang xử lý.'
              : 'Chưa có asset đủ điều kiện để bắt đầu bản nháp tự động hóa.',
            automation,
            creator,
            folderId: automation.folderId,
            publishRequested,
            publishMode,
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
          await this.updateTemplateAutomationState(template, {
            ...automation,
            status: 'WAITING_ASSETS',
            lastRunAt: new Date().toISOString(),
          });
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
          videos: selectedVideos,
          images: selectedImages,
          publishMode,
          requiredVideos,
          requiredImages,
          isComplete,
          automation,
        });

        generatedCampaignId = await this.saveAutomationDraft({
          existingDraft: inProgressDraft,
          substitutedValues,
          template,
          creator,
          publishMode,
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

          successSteps[successSteps.length - 1] = {
            key: 'publish_meta',
            label: 'Đăng bản nháp chiến dịch lên Meta',
            status: 'success',
            metaCampaignId: publishResult.campaignId,
            publishHistoryId: publishResult.publishHistoryId,
          };

          for (const asset of [...selectedVideos, ...selectedImages]) {
            usedByCreatorInSystemCampaignAssetIds.add(asset.id);
          }
        } else if (publishRequested && !isComplete) {
          successSteps.push({
            key: 'publish_meta',
            label: 'Đăng bản nháp chiến dịch lên Meta',
            status: 'skipped',
            reason: 'Bản nháp chưa đủ dữ liệu',
          });
        }

        await this.updateTemplateAutomationState(template, {
          ...automation,
          status:
            automation.runMode === 'ONCE' && isComplete
              ? 'COMPLETED'
              : isComplete
                ? 'READY_FOR_NEXT_RUN'
                : 'WAITING_ASSETS',
          enabled:
            automation.runMode === 'ONCE' && isComplete
              ? false
              : automation.enabled,
          lastRunAt: new Date().toISOString(),
          completedAt:
            automation.runMode === 'ONCE' && isComplete
              ? new Date().toISOString()
              : automation.completedAt || null,
          completedDraftId:
            automation.runMode === 'ONCE' && isComplete
              ? generatedCampaignId
              : automation.completedDraftId || null,
          inProgressDraftId: isComplete ? null : generatedCampaignId,
          lastGeneratedDraftId: generatedCampaignId,
        });

        await this.recordAutomationHistory({
          template,
          startedAt,
          status: 'SUCCESS',
          reason:
            publishRequested && isComplete
              ? 'Đã tạo bản nháp chiến dịch tự động và đăng lên Meta.'
              : isComplete
                ? 'Bản nháp chiến dịch tự động đã hoàn tất.'
                : inProgressDraft
                  ? 'Đã cập nhật bản nháp chiến dịch tự động với asset mới đủ điều kiện.'
                  : 'Đã tạo bản nháp chiến dịch tự động với một phần asset đủ điều kiện.',
          automation,
          creator,
          folderId: automation.folderId,
          publishRequested,
          publishMode,
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

        await this.recordAutomationHistory({
          template,
          startedAt,
          status: 'FAILED',
          reason:
            publishRequested && generatedCampaignId
              ? 'Đã tạo bản nháp chiến dịch nhưng đăng lên Meta thất bại.'
              : 'Có lỗi không mong muốn khi xử lý mẫu.',
          automation,
          creator,
          folderId: automation?.folderId,
          publishRequested,
          publishMode,
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
      }
    }
  }
}
