import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { AssetType, Status } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

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

function extractMediaIdentifiers(
  obj: any,
  keys: Set<string> = new Set(),
): Set<string> {
  if (!obj) return keys;
  if (typeof obj === 'string') {
    if (obj.match(/^\d+$/) || obj.match(/^[a-fA-F0-9]{32}$/)) {
      keys.add(obj);
    }
  } else if (Array.isArray(obj)) {
    for (const item of obj) {
      extractMediaIdentifiers(item, keys);
    }
  } else if (typeof obj === 'object') {
    for (const key of Object.keys(obj)) {
      extractMediaIdentifiers(obj[key], keys);
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

function enrichVideoPlaceholderObject(obj: any, video: any) {
  const thumbnailId = getVideoThumbnailId(video);
  const thumbnailUrl = getVideoThumbnailUrl(video);
  const thumbnails = parseJsonValue(video?.video_thumbnails);

  const enriched = {
    ...obj,
    placeholder: undefined,
  };

  if ('videoId' in enriched || !('video_id' in enriched)) {
    enriched.videoId = enriched.videoId || video.video_id;
  }
  if ('video_id' in enriched) {
    enriched.video_id = enriched.video_id || video.video_id;
  }
  if ('source' in enriched) {
    enriched.source = enriched.source || video.video_source;
  }
  if ('video_source' in enriched) {
    enriched.video_source = enriched.video_source || video.video_source;
  }
  if ('previewUrl' in enriched || thumbnailUrl) {
    enriched.previewUrl = enriched.previewUrl || thumbnailUrl;
  }
  if ('thumbnail' in enriched || thumbnailUrl) {
    enriched.thumbnail = enriched.thumbnail || thumbnailUrl;
  }
  if ('image_url' in enriched || thumbnailUrl) {
    enriched.image_url = enriched.image_url || thumbnailUrl;
  }
  if ('selected_thumbnail_id' in enriched || thumbnailId) {
    enriched.selected_thumbnail_id =
      enriched.selected_thumbnail_id || thumbnailId;
  }
  if ('imageHash' in enriched || thumbnailId) {
    enriched.imageHash = enriched.imageHash || thumbnailId;
  }
  if ('image_hash' in enriched || thumbnailId) {
    enriched.image_hash = enriched.image_hash || thumbnailId;
  }
  if ('image_id' in enriched || thumbnailId) {
    enriched.image_id = enriched.image_id || thumbnailId;
  }
  if (thumbnails && !enriched.list_thumbnails) {
    enriched.list_thumbnails = thumbnails;
  }
  if (thumbnails && !enriched.video_thumbnails) {
    enriched.video_thumbnails = thumbnails;
  }

  delete enriched.placeholder;
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
    if (matchedVideo || obj.placeholder) {
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
    if (matchedImage) {
      delete newObj.placeholder;
    }
    return newObj;
  }

  return obj;
}

@Injectable()
export class DraftAutomationScheduler implements OnModuleInit {
  private readonly logger = new Logger(DraftAutomationScheduler.name);

  constructor(private readonly prisma: PrismaService) {}

  onModuleInit() {
    this.logger.log('DraftAutomationScheduler initialized.');
  }

  @Cron('*/5 * * * *')
  async handleDraftAutomationCron() {
    this.logger.log('⏰ Starting draft automation cron...');
    try {
      await this.processAutomation();
    } catch (err: any) {
      this.logger.error(
        'Error in draft automation cron:',
        err.stack || err.message || err,
      );
    }
    this.logger.log('⏰ Draft automation cron finished.');
  }

  async processAutomation() {
    // 1. Fetch all templates with automation configured
    const templates = await this.prisma.templateCampaign.findMany({
      where: {
        deletedAt: null,
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

    // Pre-fetch published asset mappings to minimize DB queries in loops
    const assetMappings = await this.prisma.creativeAssetMapping.findMany({
      select: { creativeAssetId: true },
    });
    const publishedAssetIds = new Set(
      assetMappings.map((m) => m.creativeAssetId),
    );

    for (const template of activeTemplates) {
      try {
        const automation = (template.data as any).automation;
        const creatorId = template.createdById;
        if (!creatorId) {
          this.logger.warn(
            `Template ${template.name} (${template.id}) has no creator ID. Skipping.`,
          );
          continue;
        }

        const creator = await this.prisma.user.findUnique({
          where: { id: creatorId },
        });

        if (!creator || !creator.employee_id) {
          this.logger.warn(
            `Creator of template ${template.name} has no employee ID. Skipping.`,
          );
          continue;
        }

        this.logger.log(
          `Processing template "${template.name}" for user ${creator.name} (${creator.employee_id}). folderId: ${automation.folderId}`,
        );

        // 2. Fetch all assets in the target folder
        const folderAssets = await this.prisma.creativeAsset.findMany({
          where: {
            folderId: automation.folderId,
          },
          include: {
            larkRecord: true,
          },
          orderBy: {
            createdAtLocal: 'asc', // Oldest first
          },
        });

        // 3. Query all active drafts of this creator to extract used assets
        const activeDrafts = await this.prisma.systemCampaign.findMany({
          where: {
            createdById: creator.id,
            status: Status.DRAFT,
            deletedAt: null,
          },
          include: {
            ad_sets: {
              include: {
                ads: true,
              },
            },
          },
        });

        const usedDraftIdentifiers = new Set<string>();
        for (const draft of activeDrafts) {
          // Extract from campaign data
          extractMediaIdentifiers(draft.data, usedDraftIdentifiers);
          for (const adset of draft.ad_sets) {
            extractMediaIdentifiers(adset.data, usedDraftIdentifiers);
            for (const ad of adset.ads) {
              extractMediaIdentifiers(ad.data, usedDraftIdentifiers);
            }
          }
        }

        // 4. Filter assets based on ownership, publish state, draft state, and naming rules
        let eligibleAssets = folderAssets.filter((asset) => {
          return asset.larkRecord?.employee_id === creator.employee_id;
        });

        eligibleAssets = eligibleAssets.filter((asset) => {
          return !publishedAssetIds.has(asset.id);
        });

        eligibleAssets = eligibleAssets.filter((asset) => {
          const isUsedInDraft =
            usedDraftIdentifiers.has(asset.id) ||
            (asset.video_id && usedDraftIdentifiers.has(asset.video_id)) ||
            (asset.imageHash && usedDraftIdentifiers.has(asset.imageHash));
          return !isUsedInDraft;
        });

        if (automation.nameRule) {
          eligibleAssets = eligibleAssets.filter((asset) => {
            return (asset.name || '')
              .toLowerCase()
              .includes(automation.nameRule.toLowerCase());
          });
        }

        const eligibleVideos = eligibleAssets.filter(
          (a) => a.type === AssetType.VIDEO,
        );
        const eligibleImages = eligibleAssets.filter(
          (a) => a.type === AssetType.IMAGE,
        );

        const requiredVideos = Number(automation.videoCount) || 0;
        const requiredImages = Number(automation.imageCount) || 0;

        this.logger.log(
          `Template "${template.name}": eligible videos: ${eligibleVideos.length}/${requiredVideos}, eligible images: ${eligibleImages.length}/${requiredImages}`,
        );

        if (
          eligibleVideos.length < requiredVideos ||
          eligibleImages.length < requiredImages
        ) {
          this.logger.log(
            `Insufficient eligible assets for template "${template.name}". Skipping.`,
          );
          continue;
        }

        // Pick oldest matching assets
        const selectedVideos = eligibleVideos.slice(0, requiredVideos);
        const selectedImages = eligibleImages.slice(0, requiredImages);

        this.logger.log(
          `Generating draft campaign for template "${template.name}" using videos: [${selectedVideos
            .map((v) => v.name)
            .join(
              ', ',
            )}], images: [${selectedImages.map((i) => i.name).join(', ')}]`,
        );

        // Substitute placeholders in template data
        const templateData = template.data as any;
        const substitutedValues = replacePlaceholders(
          templateData,
          selectedVideos,
          selectedImages,
        );

        // Format names
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

        // Store automation metadata
        substitutedValues.automation_used_assets = [
          ...selectedVideos.map((v) => v.id),
          ...selectedImages.map((i) => i.id),
        ];

        // 5. Save draft campaign via a transaction mimicking front-end payload logic
        await this.prisma.$transaction(async (tx) => {
          const campaign = await tx.systemCampaign.create({
            data: {
              accountId: substitutedValues.ad_account_id,
              createdById: creator.id,
              status: Status.DRAFT,
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
            },
          });

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
                  Number(adset.daily_budget || adset.lifetime_budget) ||
                  undefined,
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
        });

        this.logger.log(
          `Successfully created automated draft campaign for template "${template.name}".`,
        );
      } catch (err: any) {
        this.logger.error(
          `Error processing template "${template.name}":`,
          err.stack || err.message || err,
        );
      }
    }
  }
}
