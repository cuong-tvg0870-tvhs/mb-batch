import { Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import {
  AdAccount,
  Campaign,
  FacebookAdsApi,
} from 'facebook-nodejs-business-sdk';
import {
  CleanObjectOrArray,
  metaErrorToFriendly,
  parseMetaError,
  sleep,
} from '../../common/utils';
import {
  AD_FIELDS,
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
} from '../../common/utils/meta-field';
import { PrismaService } from '../prisma/prisma.service';

type PublishStepStatus =
  | 'pending'
  | 'processing'
  | 'success'
  | 'failed'
  | 'partial';

@Injectable()
export class DraftAutomationMetaPublisherService {
  private readonly logger = new Logger(
    DraftAutomationMetaPublisherService.name,
  );
  private initialized = false;

  constructor(private readonly prisma: PrismaService) {}

  private init() {
    if (this.initialized) return;

    const token = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
    if (!token) {
      throw new Error('SDK_FACEBOOK_ACCESS_TOKEN is missing.');
    }

    FacebookAdsApi.init(token);
    this.initialized = true;
  }

  async publishDraftCampaign(systemCampaignId: string) {
    this.init();

    const campaignSystem = await this.prisma.systemCampaign.findUnique({
      where: { id: systemCampaignId },
      include: {
        ad_sets: {
          orderBy: { createdAt: 'asc' },
          include: {
            ads: { orderBy: { createdAt: 'asc' } },
          },
        },
      },
    });

    if (!campaignSystem) {
      throw new Error(`SystemCampaign ${systemCampaignId} not found.`);
    }

    const data = this.clone(
      CleanObjectOrArray(campaignSystem.data || {}) || {},
    );
    const accountId =
      campaignSystem.accountId || data.ad_account_id || data.account_id;
    if (!accountId) {
      throw new Error('Campaign does not have an ad account id.');
    }

    const adAccountId = this.normalizeAdAccountId(accountId);
    const adAccount = new AdAccount(adAccountId);

    // Khóa chống trùng: chỉ MỘT tiến trình được publish một draft tại một thời điểm.
    // updateMany đặt isPublishing=true một cách nguyên tử khi draft đang rảnh và
    // CHƯA có meta_id. Nếu count=0 → đang có tiến trình khác publish hoặc campaign
    // đã được publish rồi → bỏ qua, tránh tạo campaign Meta TRÙNG (tiêu ngân sách
    // hai lần khi hai lần cron chồng nhau). Mirror chốt chặn của publish thủ công.
    const claim = await this.prisma.systemCampaign.updateMany({
      where: { id: campaignSystem.id, isPublishing: false, meta_id: null },
      data: { isPublishing: true, errors: Prisma.DbNull },
    });
    if (claim.count === 0) {
      this.logger.warn(
        `Bỏ qua publish draft ${campaignSystem.id}: đang được publish bởi tiến trình khác hoặc đã có meta_id.`,
      );
      return { skipped: true, reason: 'ALREADY_PUBLISHING_OR_PUBLISHED' };
    }

    const history = await this.createPublishHistory(campaignSystem.id);
    let currentStepKey = 'campaign';
    let campaignMetaId: string | undefined;
    // Kết quả publish per-adset/per-ad ở scope hàm để nhánh catch đọc lại, quyết
    // định có rollback hay không (chỉ rollback khi KHÔNG có ad nào lên live).
    const adSetResults: any[] = [];
    const adResults: any[] = [];

    try {
      currentStepKey = 'campaign';
      await this.updatePublishStep(history.id, 'campaign', {
        status: 'processing',
      });

      const campaignPayload =
        CleanObjectOrArray({
          ...(data.campaign || {}),
          status: this.normalizeMetaStatus(data.campaign?.status, 'ACTIVE'),
        }) || {};

      const campaign = await adAccount.createCampaign(
        CAMPAIGN_FIELDS,
        this.buildCampaignCreatePayload(campaignPayload),
      );
      campaignMetaId = campaign.id || campaign._data?.id;

      await this.prisma.systemCampaign.update({
        where: { id: campaignSystem.id },
        data: {
          meta_id: campaignMetaId,
          status: (campaign._data?.status || campaignPayload.status) as any,
        },
      });

      await this.updatePublishStep(history.id, 'campaign', {
        status: 'success',
        metaId: campaignMetaId,
      });

      await this.updatePublishStep(history.id, 'adsets', {
        status: 'processing',
        total: campaignSystem.ad_sets.length,
        current: 0,
      });

      const totalAds = campaignSystem.ad_sets.reduce(
        (sum, adSet) => sum + adSet.ads.length,
        0,
      );
      await this.updatePublishStep(history.id, 'ads', {
        status: totalAds > 0 ? 'processing' : 'success',
        total: totalAds,
        current: 0,
      });

      let adSetsProcessed = 0;
      let adsProcessed = 0;

      for (const adSetSystem of campaignSystem.ad_sets) {
        currentStepKey = 'adsets';

        const adSetData: any = this.clone(adSetSystem.data || {});
        const catalogProductSetId =
          this.resolveAdSetProductSetId(adSetData) ||
          this.resolveCatalogProductSetIdFromAds(
            adSetSystem.ads.map((ad) => ad.data),
          );

        if (catalogProductSetId) {
          adSetData.promoted_object = {
            ...(adSetData.promoted_object || {}),
            product_set_id: catalogProductSetId,
          };
          adSetData.is_dynamic_creative = false;
        }

        const adSetPayload =
          CleanObjectOrArray({
            ...adSetData,
            status: this.normalizeMetaStatus(adSetData.status, 'PAUSED'),
          }) || {};
        const adSetName = adSetPayload.name || adSetSystem.id;

        let adSetMetaId: string;
        try {
          const adSet = await adAccount.createAdSet(
            ADSET_FIELDS,
            this.buildAdSetCreatePayload(adSetPayload, campaignMetaId),
          );
          adSetMetaId = adSet.id || adSet._data?.id;

          await this.prisma.systemAdSet.update({
            where: { id: adSetSystem.id },
            data: {
              meta_id: adSetMetaId,
              status: (adSet._data?.status || adSetPayload.status) as any,
              data: adSetData as any,
            },
          });
        } catch (err) {
          // Lỗi tạo NHÓM quảng cáo → nhóm + ads của nó coi như lỗi, các nhóm KHÁC
          // vẫn tiếp tục. Ad lỗi giữ meta_id=null nên lần chạy sau tạo lại đúng nó.
          const metaError = parseMetaError(err);
          const friendlyMsg =
            metaErrorToFriendly(metaError) || metaError?.message || String(err);
          this.logger.error(
            `[createAdSet] adset ${adSetSystem.id} lỗi:`,
            metaError,
          );
          adSetResults.push({
            systemAdSetId: adSetSystem.id,
            name: adSetName,
            status: 'failed',
            error: friendlyMsg,
          });
          for (const adSystem of adSetSystem.ads) {
            adResults.push({
              systemAdId: adSystem.id,
              adName: (adSystem.data as any)?.name || adSystem.id,
              adSetName,
              status: 'failed',
              error: `Nhóm quảng cáo lỗi: ${friendlyMsg}`,
            });
          }
          adSetsProcessed += 1;
          adsProcessed += adSetSystem.ads.length;
          await this.updatePublishStep(history.id, 'adsets', {
            status: 'processing',
            current: adSetsProcessed,
          });
          continue;
        }

        adSetsProcessed += 1;
        adSetResults.push({
          systemAdSetId: adSetSystem.id,
          metaId: adSetMetaId,
          name: adSetName,
          status: 'live',
        });
        await this.updatePublishStep(history.id, 'adsets', {
          status: 'processing',
          current: adSetsProcessed,
        });

        currentStepKey = 'ads';
        for (const adSystem of adSetSystem.ads) {
          const adName = (adSystem.data as any)?.name || adSystem.id;
          try {
            const adData: any = this.clone(adSystem.data || {});
            await this.prepareAdDataForPublish(adData, catalogProductSetId);

            const adPayload =
              CleanObjectOrArray({
                ...adData,
                status: this.normalizeMetaStatus(adData.status, 'PAUSED'),
              }) || {};

            const creativeData = this.buildCreativeData(adPayload);
            // Toggle "Hiển thị sản phẩm": OPT_OUT mặc định để tránh lỗi "tạo nội
            // dung động mà không có ID nhóm sản phẩm". Chỉ tác động ở bước tạo
            // creative, không đụng diff/snapshot.
            this.applyProductExtensionsPreference(creativeData, adPayload.creative);
            const creative =
              await this.createAdCreativeWithOptionalDestinationFallback(
                adAccount,
                creativeData,
              );
            const creativeId = creative.id || creative._data?.id;
            await this.waitForCreativePropagation(creativeId);

            const ad = await adAccount.createAd(AD_FIELDS, {
              name: adPayload.name,
              status: adPayload.status ?? 'PAUSED',
              adset_id: adSetMetaId,
              creative: this.buildAdCreativeReference(adPayload, creativeId),
            });
            const adMetaId = ad.id || ad._data?.id;

            await this.prisma.systemAd.update({
              where: { id: adSystem.id },
              data: {
                meta_id: adMetaId,
                status: (ad._data?.status || adPayload.status) as any,
                data: adData as any,
              },
            });

            adsProcessed += 1;
            adResults.push({
              systemAdId: adSystem.id,
              metaId: adMetaId,
              creativeId,
              adName,
              adSetName,
              status: 'live',
            });
            await this.updatePublishStep(history.id, 'ads', {
              status: 'processing',
              current: adsProcessed,
            });
          } catch (err) {
            // 1 quảng cáo lỗi KHÔNG làm hỏng các ad còn lại. Ad lỗi giữ meta_id=null.
            const metaError = parseMetaError(err);
            const friendlyMsg =
              metaErrorToFriendly(metaError) ||
              metaError?.message ||
              String(err);
            this.logger.error(`[createAd] ad ${adSystem.id} lỗi:`, metaError);
            adsProcessed += 1;
            adResults.push({
              systemAdId: adSystem.id,
              adName,
              adSetName,
              status: 'failed',
              error: friendlyMsg,
            });
            await this.updatePublishStep(history.id, 'ads', {
              status: 'processing',
              current: adsProcessed,
            });
          }
        }
      }

      const liveAds = adResults.filter((r) => r.status === 'live').length;
      const failedAds = adResults.filter((r) => r.status === 'failed').length;

      // Tất cả quảng cáo đều lỗi → coi như thất bại → ném để nhánh catch rollback.
      if (adResults.length > 0 && liveAds === 0) {
        throw new Error('Tất cả quảng cáo đều lỗi khi đẩy lên Meta.');
      }

      await this.updatePublishStep(history.id, 'adsets', {
        status: 'success',
        current: adSetsProcessed,
      });
      await this.updatePublishStep(history.id, 'ads', {
        status: failedAds === 0 ? 'success' : 'partial',
        current: adsProcessed,
        results: adResults,
        summary: { total: adResults.length, live: liveAds, failed: failedAds },
      });
      await this.updatePublishStep(
        history.id,
        'sync',
        { status: failedAds > 0 ? 'partial' : 'success' },
        failedAds > 0 ? 'PARTIAL' : 'SUCCESS',
      );

      // PARTIAL: giữ campaign + ad tốt live, KHÔNG rollback; lưu danh sách ad lỗi;
      // hasMetaChanges=true để lần publish/automation sau retry các ad meta_id=null.
      await this.prisma.systemCampaign.update({
        where: { id: campaignSystem.id },
        data: {
          errors:
            failedAds > 0
              ? ({
                  partial: true,
                  message: `${failedAds}/${adResults.length} quảng cáo lỗi — các quảng cáo còn lại đã lên Meta.`,
                  failedAds: adResults.filter((r) => r.status === 'failed'),
                } as any)
              : Prisma.DbNull,
          isPublishing: false,
          hasMetaChanges: failedAds > 0 ? true : false,
        },
      });

      return {
        success: true,
        campaignId: campaignMetaId,
        adSets: adSetResults,
        ads: adResults,
        publishHistoryId: history.id,
      };
    } catch (err: any) {
      const metaError = parseMetaError(err);
      const errorMessage =
        metaErrorToFriendly(metaError) || metaError?.message || String(err);

      this.logger.error(
        `Automation publish failed for system campaign ${campaignSystem.id}:`,
        metaError,
      );

      // E1b: nếu ĐÃ có ad lên live thì KHÔNG rollback (sẽ giết ad tốt) — chỉ báo
      // lỗi PARTIAL, giữ nguyên campaign + ad live. Chỉ rollback khi không có ad nào.
      const hasLiveAds = adResults.some((r) => r.status === 'live');

      await this.updatePublishStep(
        history.id,
        currentStepKey,
        { status: hasLiveAds ? 'partial' : 'failed', error: errorMessage },
        hasLiveAds ? 'PARTIAL' : 'FAILED',
        errorMessage,
      );

      if (hasLiveAds) {
        const failedList = adResults.filter((r) => r.status === 'failed');
        await this.prisma.systemCampaign.update({
          where: { id: campaignSystem.id },
          data: {
            errors: {
              partial: true,
              message: errorMessage,
              failedAds: failedList,
            } as any,
            isPublishing: false,
            hasMetaChanges: true,
          },
        });
        // Đã có ad live → KHÔNG ném lỗi (tránh scheduler đánh dấu cả run thất bại);
        // trả về như partial success.
        return {
          success: true,
          partial: true,
          campaignId: campaignMetaId,
          adSets: adSetResults,
          ads: adResults,
          publishHistoryId: history.id,
        };
      }

      await this.rollbackFailedCreate(
        campaignSystem.id,
        campaignMetaId,
        metaError,
      );
      throw Object.assign(new Error(errorMessage), { metaError });
    }
  }

  private async createPublishHistory(campaignId: string) {
    return this.prisma.publishHistory.create({
      data: {
        campaignId,
        status: 'PUBLISHING',
        steps: [
          {
            key: 'campaign',
            label: 'Đẩy Chiến dịch lên Meta',
            status: 'pending',
            error: null,
            metaId: null,
          },
          {
            key: 'adsets',
            label: 'Đẩy các Nhóm quảng cáo lên Meta',
            status: 'pending',
            error: null,
            total: 0,
            current: 0,
          },
          {
            key: 'ads',
            label: 'Đẩy các Quảng cáo lên Meta',
            status: 'pending',
            error: null,
            total: 0,
            current: 0,
          },
          {
            key: 'sync',
            label: 'Đồng bộ trạng thái publish',
            status: 'pending',
            error: null,
          },
        ] as any,
      },
    });
  }

  private async updatePublishStep(
    historyId: string,
    stepKey: string,
    updates: {
      status: PublishStepStatus;
      error?: string | null;
      metaId?: string | null;
      total?: number;
      current?: number;
      // Kết quả per-ad (E1b) để UI hiển thị checklist "Ad X lỗi: …".
      results?: any[];
      summary?: { total: number; live: number; failed: number };
    },
    overallStatus?: string,
    overallError?: string | null,
  ) {
    const history = await this.prisma.publishHistory.findUnique({
      where: { id: historyId },
    });
    if (!history) return;

    const steps = (history.steps as any[]).map((step) =>
      step.key === stepKey ? { ...step, ...updates } : step,
    );

    await this.prisma.publishHistory.update({
      where: { id: historyId },
      data: {
        steps,
        ...(overallStatus ? { status: overallStatus } : {}),
        ...(overallError !== undefined ? { error: overallError } : {}),
      },
    });
  }

  private async rollbackFailedCreate(
    campaignSystemId: string,
    campaignMetaId: string | undefined,
    metaError: any,
  ) {
    if (campaignMetaId) {
      try {
        const campaign = new Campaign(campaignMetaId);
        await campaign.update(CAMPAIGN_FIELDS, { status: 'ARCHIVED' });
      } catch (archiveError) {
        this.logger.error(
          `Failed to archive failed Meta campaign ${campaignMetaId}:`,
          archiveError,
        );
      }
    }

    await this.prisma.$transaction(async (tx) => {
      const adSets = await tx.systemAdSet.findMany({
        where: { campaignId: campaignSystemId },
        select: { id: true },
      });

      await tx.systemAd.updateMany({
        where: { adSetId: { in: adSets.map((adSet) => adSet.id) } },
        data: { meta_id: null, status: 'DRAFT' },
      });

      await tx.systemAdSet.updateMany({
        where: { campaignId: campaignSystemId },
        data: { meta_id: null, status: 'DRAFT' },
      });

      await tx.systemCampaign.update({
        where: { id: campaignSystemId },
        data: {
          meta_id: null,
          status: 'DRAFT',
          errors: metaError,
          isPublishing: false,
        },
      });
    });
  }

  private normalizeAdAccountId(accountId: string) {
    return accountId.startsWith('act_') ? accountId : `act_${accountId}`;
  }

  private normalizeMetaStatus(status?: string, fallback = 'PAUSED') {
    const allowedStatuses = ['ACTIVE', 'PAUSED', 'ARCHIVED', 'DELETED'];
    return status && allowedStatuses.includes(status) ? status : fallback;
  }

  private buildCampaignCreatePayload(payload: any) {
    const metaPayload = { ...(payload || {}) };
    delete metaPayload.id;
    delete metaPayload.advantage_catalog;
    delete metaPayload.product_catalog_id;
    delete metaPayload.bid_amount;

    return (
      CleanObjectOrArray({
        ...metaPayload,
        targeting: this.cleanTargetingForMeta(payload?.targeting),
        special_ad_categories: payload?.special_ad_categories ?? ['NONE'],
      }) || {}
    );
  }

  private buildAdSetCreatePayload(payload: any, campaignMetaId: string) {
    const metaPayload = { ...(payload || {}) };
    const productSetId = this.resolveAdSetProductSetId(payload);

    if (productSetId) {
      metaPayload.promoted_object = {
        ...(metaPayload.promoted_object || {}),
        product_set_id: productSetId,
      };
      metaPayload.is_dynamic_creative = false;
    }

    delete metaPayload.id;
    delete metaPayload.ads;
    delete metaPayload.timezone_type;

    return (
      CleanObjectOrArray({
        ...metaPayload,
        targeting: this.cleanTargetingForMeta(payload?.targeting),
        campaign_id: campaignMetaId,
      }) || {}
    );
  }

  private resolveAdSetProductSetId(payload: any) {
    if (payload?.promoted_object?.product_set_id) {
      return payload.promoted_object.product_set_id;
    }

    const ads = Array.isArray(payload?.ads) ? payload.ads : [];
    return this.resolveCatalogProductSetIdFromAds(ads);
  }

  private resolveCatalogProductSetIdFromAds(ads: any[]) {
    for (const ad of ads || []) {
      const creative = ad?.creative || {};
      const productSetId = creative.product_set_id || creative.productSetId;
      if (productSetId) return productSetId;
    }
    return undefined;
  }

  private async prepareAdDataForPublish(
    adData: any,
    catalogProductSetId: string | undefined,
  ) {
    const creativeData = adData.creative || {};
    const isCatalogCreative =
      creativeData.productSource === 'CATALOG' ||
      creativeData.useCatalog === true ||
      !!creativeData.productSetId ||
      !!creativeData.product_set_id;

    if (isCatalogCreative) {
      if (catalogProductSetId) {
        creativeData.productSetId =
          creativeData.productSetId ||
          creativeData.product_set_id ||
          catalogProductSetId;
      }
      if (creativeData.asset_feed_spec) {
        delete creativeData.asset_feed_spec.images;
        delete creativeData.asset_feed_spec.videos;
      }
      if (creativeData.object_story_spec) {
        delete creativeData.object_story_spec.link_data;
        delete creativeData.object_story_spec.video_data;
        delete creativeData.object_story_spec.photo_data;
      }
      delete creativeData.imageHash;
      delete creativeData.videoId;
      delete creativeData.dynamicAssets;
      delete creativeData.carouselCards;
      return;
    }

    const resolveRes = await this.resolveCreativeImage(adData);
    const image = resolveRes?.image;
    const isVideo = !!(
      adData.creative?.object_story_spec?.video_data ||
      adData.creative?.asset_feed_spec?.videos?.length
    );

    if (image) {
      if (isVideo && adData.creative?.object_story_spec?.video_data) {
        adData.creative.object_story_spec.video_data.image_url = image;
        delete adData.creative.object_story_spec.video_data.image_hash;
      } else if (adData.creative?.object_story_spec?.link_data) {
        delete adData.creative.object_story_spec.link_data.preview_url;
      }
    }

    const assetVideos = adData.creative?.asset_feed_spec?.videos;
    if (assetVideos && assetVideos.length > 0) {
      await Promise.all(
        assetVideos.map(async (videoAsset: any) => {
          if (!videoAsset?.video_id) return;

          const resolveAssetVideo = await this.resolveCreativeImage({
            creative: {
              asset_feed_spec: {
                videos: [
                  {
                    video_id: videoAsset.video_id,
                    thumbnail_url:
                      videoAsset.thumbnail_url ||
                      videoAsset.image_url ||
                      videoAsset.preview_url ||
                      undefined,
                  },
                ],
              },
            },
          });

          if (resolveAssetVideo?.image) {
            videoAsset.thumbnail_url = resolveAssetVideo.image;
            delete videoAsset.thumbnail_hash;
          }

          delete videoAsset.thumbnail_hash;
          delete videoAsset.image_url;
          delete videoAsset.preview_url;
          delete videoAsset.list_thumbnails;
          delete videoAsset.selected_thumbnail_id;
          delete videoAsset.source;
        }),
      );
    }

    const childAttachments =
      adData.creative?.object_story_spec?.link_data?.child_attachments;
    if (childAttachments && childAttachments.length > 0) {
      await Promise.all(
        childAttachments.map(async (attachment: any) => {
          if (!attachment.video_id) return;

          const resolveAttach = await this.resolveCreativeImage({
            creative: {
              selected_thumbnail_id:
                attachment.selected_thumbnail_id ||
                attachment.image_id ||
                attachment.image_hash ||
                undefined,
              object_story_spec: {
                video_data: {
                  video_id: attachment.video_id,
                  image_hash:
                    attachment.image_hash ||
                    attachment.image_id ||
                    attachment.selected_thumbnail_id ||
                    undefined,
                  image_url:
                    attachment.picture ||
                    attachment.image_url ||
                    attachment.preview_url ||
                    undefined,
                },
              },
            },
          });

          if (resolveAttach?.image) {
            attachment.picture = resolveAttach.image;
            delete attachment.image_hash;
            delete attachment.image_url;
            delete attachment.preview_url;
            delete attachment.list_thumbnails;
            delete attachment.selected_thumbnail_id;
            delete attachment.source;
          }
        }),
      );
    }
  }

  private async resolveCreativeImage(data: any) {
    const creative = data?.creative;
    if (!creative) {
      return { image: null, list_thumbnails: null, source: null };
    }

    const linkData = creative?.object_story_spec?.link_data;
    const assetImage = creative?.asset_feed_spec?.images?.[0];
    const imageHash = linkData?.image_hash || assetImage?.hash;

    if (imageHash) {
      const creativeAsset = await this.prisma.creativeAsset.findFirst({
        where: { imageHash },
        select: { thumbnail: true, imageUrl: true },
      });
      return {
        image: creativeAsset?.thumbnail || creativeAsset?.imageUrl || null,
        list_thumbnails: null,
        source: null,
      };
    }

    const videoData = creative?.object_story_spec?.video_data;
    const assetVideo = creative?.asset_feed_spec?.videos?.[0];
    const videoId =
      videoData?.video_id || creative?.video_id || assetVideo?.video_id;

    if (!videoId) {
      return { image: null, list_thumbnails: null, source: null };
    }

    const creativeAsset = await this.prisma.creativeAsset.findFirst({
      where: { video_id: videoId },
      select: { thumbnail: true, video_thumbnails: true, video_source: true },
    });

    const thumbnails = this.getThumbnailList(creativeAsset?.video_thumbnails);
    const selectedId =
      videoData?.image_id ||
      videoData?.image_hash ||
      assetVideo?.image_id ||
      assetVideo?.thumbnail_hash ||
      assetVideo?.selected_thumbnail_id ||
      creative?.selected_thumbnail_id;
    const selectedUrl = videoData?.image_url || assetVideo?.thumbnail_url;
    const selected =
      thumbnails.find((thumbnail: any) => thumbnail.id === selectedId) ||
      thumbnails.find((thumbnail: any) => thumbnail.uri === selectedUrl) ||
      thumbnails.find((thumbnail: any) => thumbnail?.is_preferred) ||
      thumbnails[0];

    return {
      image:
        selected?.uri ||
        videoData?.image_url ||
        assetVideo?.thumbnail_url ||
        creativeAsset?.thumbnail ||
        null,
      list_thumbnails: thumbnails.length ? thumbnails : null,
      source: creativeAsset?.video_source || null,
    };
  }

  private buildCreativeData(adPayload: any) {
    const sourceCreative = adPayload?.creative || {};
    const isCatalogProductCreative =
      this.isCatalogProductCreative(sourceCreative);
    const catalogProductSetId =
      sourceCreative.product_set_id || sourceCreative.productSetId;
    const creativeData: any = {
      name: `${adPayload.name} - Creative`,
      ...this.clone(sourceCreative || {}),
    };

    const uiOnlyFields = [
      'mediaType',
      'carouselCards',
      'dynamicAssets',
      'list_thumbnails',
      'videoId',
      'imageHash',
      'title',
      'titles',
      'body',
      'bodies',
      'description',
      'descriptions',
      'link',
      'callToAction',
      'previewUrl',
      'source',
      'selected_thumbnail_id',
      'useCatalog',
      'productSource',
      'productSetId',
      'catalogFormat',
      'catalog_format',
      'creativeSource',
      'product_set_id',
      'pageId',
      // Toggle UI "Hiển thị sản phẩm" — không gửi raw lên Meta; dịch sang
      // degrees_of_freedom_spec ở bước tạo creative (applyProductExtensionsPreference).
      'show_products',
    ];
    for (const field of uiOnlyFields) delete creativeData[field];

    const sourcePageId =
      sourceCreative.object_story_spec?.page_id || sourceCreative.pageId;
    if (sourcePageId) {
      creativeData.object_story_spec = {
        ...(creativeData.object_story_spec || {}),
        page_id: creativeData.object_story_spec?.page_id || sourcePageId,
      };
    }

    delete creativeData.instagram_actor_id;
    delete creativeData.instagram_user_id;
    delete creativeData.object_story_spec?.instagram_actor_id;
    delete creativeData.object_story_spec?.instagram_user_id;

    const messageTemplate =
      adPayload?.messageTemplate || sourceCreative?.messageTemplate;
    if (messageTemplate) {
      const formattedTemplate = this.formatMessageTemplate(messageTemplate);
      const spec = creativeData.object_story_spec;
      if (spec?.link_data) {
        spec.link_data.page_welcome_message = formattedTemplate;
      } else if (spec?.video_data) {
        spec.video_data.page_welcome_message = formattedTemplate;
      }
    }

    const personalizedDestinations = creativeData.personalized_destinations;
    delete creativeData.personalized_destinations;
    delete creativeData.messageTemplate;
    if (personalizedDestinations) {
      this.applyPersonalizedDestinations(
        creativeData,
        personalizedDestinations,
      );
    }

    this.applyPromotionalMetadataForMeta(creativeData);
    this.normalizeCreativeMediaForMeta(creativeData);
    if (isCatalogProductCreative) {
      this.normalizeCatalogCreativeForMeta(
        creativeData,
        sourceCreative,
        catalogProductSetId,
      );
    }

    const hasAssetFeed =
      creativeData.asset_feed_spec &&
      Object.keys(creativeData.asset_feed_spec).length > 0;

    if (hasAssetFeed) {
      const isFlexibleFormat =
        creativeData.asset_feed_spec?.optimization_type ===
        'DEGREES_OF_FREEDOM';
      const isCarousel =
        sourceCreative?.mediaType?.toLowerCase() === 'carousel' ||
        (Array.isArray(
          creativeData.object_story_spec?.link_data?.child_attachments,
        ) &&
          creativeData.object_story_spec.link_data.child_attachments.length >
            0) ||
        creativeData.asset_feed_spec?.ad_formats?.some((format: string) =>
          format.includes('CAROUSEL'),
        );

      if (isFlexibleFormat && isCarousel) {
        const allowedKeys = ['optimization_type', 'bodies'];
        for (const key of Object.keys(creativeData.asset_feed_spec)) {
          if (!allowedKeys.includes(key))
            delete creativeData.asset_feed_spec[key];
        }
        if (!creativeData.asset_feed_spec.bodies?.length) {
          delete creativeData.asset_feed_spec;
        }
      }

      if (creativeData.object_story_spec && creativeData.asset_feed_spec) {
        delete creativeData.object_story_spec.link_data;
        delete creativeData.object_story_spec.video_data;
        delete creativeData.object_story_spec.template_data;
      }
    } else {
      delete creativeData.asset_feed_spec;
    }

    return CleanObjectOrArray(creativeData) || {};
  }

  // Parity với mb-ads MetaService: chuẩn hoá promotional_metadata về
  // asset_feed_spec.promotional_metadata (shape flat + propositions), thay vì chỉ
  // strip khi disabled. Đảm bảo ad auto-publish có promo giống hệt publish thủ công.
  private applyPromotionalMetadataForMeta(creativeData: any) {
    const topLevelPromoMeta = this.normalizePromotionalMetadataForMeta(
      creativeData.promotional_metadata,
    );
    if (topLevelPromoMeta) {
      creativeData.asset_feed_spec = creativeData.asset_feed_spec || {};
      creativeData.asset_feed_spec.promotional_metadata =
        creativeData.asset_feed_spec.promotional_metadata || topLevelPromoMeta;
      delete creativeData.promotional_metadata;
    } else {
      delete creativeData.promotional_metadata;
    }

    const assetFeedPromoMeta = this.normalizePromotionalMetadataForMeta(
      creativeData.asset_feed_spec?.promotional_metadata,
    );
    if (assetFeedPromoMeta) {
      creativeData.asset_feed_spec = creativeData.asset_feed_spec || {};
      creativeData.asset_feed_spec.promotional_metadata = assetFeedPromoMeta;
      delete creativeData.promotional_metadata;
    } else if (creativeData.asset_feed_spec) {
      delete creativeData.asset_feed_spec.promotional_metadata;
    }
  }

  // Copy 1:1 mb-ads MetaService.normalizePromotionalMetadataForMeta (kèm hedge
  // propositions). Giữ nguyên xi để 2 writer đẩy payload Meta giống hệt nhau.
  private normalizePromotionalMetadataForMeta(config: any) {
    if (!config) return undefined;
    if (config.enabled === false) return undefined;

    const hasMetaShape =
      'is_auto_update_allowed' in config ||
      'manual_coupon_codes' in config ||
      'allowed_coupon_code_sources' in config ||
      'allowedSources' in config ||
      'excluded_offers' in config ||
      'excludedOffers' in config ||
      Array.isArray(config.propositions);
    if (config.enabled === undefined && !hasMetaShape) return undefined;

    const propositions = Array.isArray(config.propositions)
      ? config.propositions
      : [];
    const propositionManualCodes = propositions
      .filter((item: any) => item?.proposition_type !== 'AUTOMATIC')
      .map((item: any) => item?.coupon_code || item?.promo_code)
      .filter(Boolean);
    const propositionExcludedOffers = propositions
      .filter((item: any) => item?.proposition_type === 'AUTOMATIC')
      .flatMap((item: any) => item?.excluded_coupon_codes || []);
    const isAuto = Boolean(
      config.is_auto_update_allowed ??
        config.isAuto ??
        (propositionExcludedOffers.length > 0 &&
          propositionManualCodes.length === 0),
    );
    const manualCodesSource =
      config.manual_coupon_codes ||
      config.manualCodes ||
      propositionManualCodes;
    const manualCodes = Array.isArray(manualCodesSource)
      ? manualCodesSource
          .map((code: any) => `${code}`.trim())
          .filter((code: string) => code.length > 0)
      : [];
    const excludedOffersSource =
      config.excluded_offers ||
      config.excludedOffers ||
      propositionExcludedOffers;
    const excludedOffers = Array.isArray(excludedOffersSource)
      ? excludedOffersSource
          .map((code: any) => `${code}`.trim())
          .filter((code: string) => code.length > 0)
      : [];
    const allowedSources = Array.isArray(
      config.allowed_coupon_code_sources || config.allowedSources,
    )
      ? config.allowed_coupon_code_sources || config.allowedSources
      : undefined;
    const autoCouponCodeSources = new Set([
      'PROVIDED_BY_MERCHANT',
      'DETECTED_FROM_MERCHANT_ADS',
      'DETECTED_FROM_MERCHANT_WEBSITE',
      'DETECTED_FROM_MERCHANT_WEBSITE_URL',
    ]);
    const manualCouponCodeSources = new Set([
      'AD_CREATIVE_PRIMARY_TEXT',
      'AD_CREATIVE_HEADLINE',
      'AD_CREATIVE_DESCRIPTION',
      'AD_CREATIVE_PRIMARY_TEXT_LLM',
      'AD_CREATIVE_HEADLINE_LLM',
      'AD_CREATIVE_DESCRIPTION_LLM',
      'AD_CREATIVE_MANUAL_COUPON_CODES',
    ]);
    const safeCouponCodeSources = isAuto
      ? autoCouponCodeSources
      : manualCouponCodeSources;
    const safeAllowedSources = allowedSources
      ?.map((source: any) => `${source}`.trim())
      .map((source: string) =>
        source === 'PROVIDED_FROM_MERCHANT'
          ? 'PROVIDED_BY_MERCHANT'
          : source,
      )
      .filter((source: string) => safeCouponCodeSources.has(source));
    const defaultAllowedSources = isAuto
      ? [
          'DETECTED_FROM_MERCHANT_ADS',
          'PROVIDED_BY_MERCHANT',
          'DETECTED_FROM_MERCHANT_WEBSITE',
        ]
      : ['AD_CREATIVE_MANUAL_COUPON_CODES'];

    // Meta's promotional_metadata is an undocumented passthrough dict (not in
    // the SDK field list), so we can't be 100% sure it honours the flat
    // `excluded_offers` / `manual_coupon_codes` shape. Send BOTH the flat fields
    // AND the nested `propositions[]` (same shape used everywhere else in the
    // codebase) so exclusion still lands whichever shape Meta actually reads.
    const metaPropositions = isAuto
      ? [
          {
            proposition_type: 'AUTOMATIC',
            excluded_coupon_codes: excludedOffers,
          },
        ]
      : manualCodes[0]
        ? [
            {
              proposition_type: 'COUPON_CODE',
              coupon_code: manualCodes[0],
            },
          ]
        : undefined;

    return CleanObjectOrArray({
      is_auto_update_allowed: isAuto,
      allowed_coupon_code_sources:
        safeAllowedSources?.length ? safeAllowedSources : defaultAllowedSources,
      excluded_offers:
        isAuto && excludedOffers.length ? excludedOffers : undefined,
      manual_coupon_codes:
        !isAuto && manualCodes.length ? manualCodes : undefined,
      propositions: metaPropositions,
    });
  }

  private async createAdCreativeWithOptionalDestinationFallback(
    adAccount: AdAccount,
    creativeData: any,
  ) {
    try {
      return await adAccount.createAdCreative(['id'], creativeData);
    } catch (error) {
      const fallbackCreativeData = this.clone(creativeData);
      const didStrip =
        this.stripOptionalPersonalizedDestinationFields(fallbackCreativeData);

      if (!didStrip || !this.isOptionalPersonalizedDestinationError(error)) {
        throw error;
      }

      this.logger.warn(
        'Meta rejected optional personalized destination fields. Retrying creative creation without them.',
      );
      return adAccount.createAdCreative(['id'], fallbackCreativeData);
    }
  }

  private async waitForCreativePropagation(creativeId?: string) {
    if (!creativeId) return;

    const delayMs = Number(
      process.env.META_CREATIVE_PROPAGATION_DELAY_MS || 30000,
    );
    if (!Number.isFinite(delayMs) || delayMs <= 0) return;

    this.logger.log(
      `Waiting ${delayMs}ms for creative ${creativeId} propagation`,
    );
    await sleep(delayMs);
  }

  private buildAdCreativeReference(adPayload: any, creativeId?: string) {
    return CleanObjectOrArray({
      creative_id: creativeId,
      page_id: this.extractCreativePageId(adPayload?.creative),
    });
  }

  private extractCreativePageId(creative: any) {
    return (
      creative?.object_story_spec?.page_id ||
      creative?.pageId ||
      creative?.page_id ||
      creative?.actor_id
    );
  }

  private cleanTargetingForMeta(targeting: any) {
    const {
      geo_locations,
      excluded_geo_locations,
      is_manual,
      use_age_min_control,
      ...rest
    } = targeting || {};

    void use_age_min_control;

    // Meta: với Advantage+ Audience (targeting_automation.advantage_audience = 1)
    // KHÔNG được đặt age_max làm "control" thấp hơn 65 — độ tuổi tối đa thấp hơn
    // chỉ được dùng làm GỢI Ý. Nếu gửi age_max < 65 Meta sẽ trả lỗi:
    // "you cannot set the audience control for maximum age lower than 65".
    // → ép age_max về 65 (giữ nguyên giá trị người dùng chọn ở cấp gợi ý/UI).
    // age_min control vẫn hợp lệ nên giữ nguyên.
    const advantageAudienceOn =
      (rest as any)?.targeting_automation?.advantage_audience === 1 ||
      (rest as any)?.targeting_automation?.advantage_audience === true;
    if (
      advantageAudienceOn &&
      typeof (rest as any).age_max === 'number' &&
      (rest as any).age_max < 65
    ) {
      (rest as any).age_max = 65;
    }

    if (!targeting || !geo_locations) return CleanObjectOrArray(rest) || rest;

    const processInclusion = (geo: any) => {
      if (!geo) return geo;
      const subLocationCountryCodes = new Set(
        [
          ...(geo.regions || []).map((region: any) => region.country_code),
          ...(geo.cities || []).map((city: any) => city.country_code),
        ].filter(Boolean),
      );

      const cleanedCountries = (geo.countries || []).filter((cc: string) => {
        if (subLocationCountryCodes.size > 0) {
          return !subLocationCountryCodes.has(cc);
        }
        if ((geo.regions || []).length > 0 || (geo.cities || []).length > 0) {
          if (cc === 'ID') return false;
          if (geo.countries.length === 1) return false;
        }
        return true;
      });

      return {
        location_types: geo.location_types || undefined,
        countries: cleanedCountries.length > 0 ? cleanedCountries : undefined,
        regions: (geo.regions || []).map((region: any) => ({
          key: region.key,
        })),
        cities: (geo.cities || []).map((city: any) => ({ key: city.key })),
      };
    };

    const processExclusion = (geo: any) => {
      if (!geo) return geo;
      return {
        location_types: geo.location_types || ['home'],
        regions: (geo.regions || []).map((region: any) => ({
          key: region.key,
        })),
        cities: (geo.cities || []).map((city: any) => ({ key: city.key })),
      };
    };

    void is_manual;

    return {
      ...rest,
      geo_locations: processInclusion(geo_locations),
      excluded_geo_locations: processExclusion(excluded_geo_locations),
    };
  }

  private isCatalogProductCreative(creative: any) {
    return (
      creative?.productSource === 'CATALOG' ||
      creative?.useCatalog === true ||
      !!creative?.productSetId ||
      !!creative?.product_set_id
    );
  }

  /**
   * "Hiển thị sản phẩm" (Advantage+ product extensions) mặc định BẬT ở cấp tài
   * khoản. Creative KHÔNG gắn catalog mà không chủ động OPT_OUT → Meta báo lỗi
   * "Đã cố tạo nội dung động mà không có ID nhóm sản phẩm" khi TẠO creative.
   * Phải khớp với mb-ads (MetaService.applyProductExtensionsPreference) — hai
   * luồng publish chạy song song nên giữ parity.
   *
   * Chỉ gọi ở bước tạo creative, KHÔNG đưa vào buildCreativeData để tránh lọt vào
   * diff/snapshot khiến creative cũ bị tạo lại oan.
   */
  private applyProductExtensionsPreference(
    creativeData: any,
    sourceCreative: any,
  ) {
    if (this.isCatalogProductCreative(sourceCreative)) return;

    const showProducts = sourceCreative?.show_products === true;

    const dof = creativeData.degrees_of_freedom_spec || {};
    const features = dof.creative_features_spec || {};

    // creative_features_spec yêu cầu KEY theo enum HOA của Meta (lỗi #100 liệt kê
    // tập hợp lệ). Ba feature phụ thuộc catalog/sản phẩm cần product set → opt out
    // cho creative KHÔNG gắn catalog để tránh lỗi "tạo nội dung động mà không có ID
    // nhóm sản phẩm". Phải khớp mb-ads (MetaService.applyProductExtensionsPreference).
    features.PRODUCT_BROWSING = {
      enroll_status: showProducts ? 'OPT_IN' : 'OPT_OUT',
    };
    if (!showProducts) {
      features.STANDARD_ENHANCEMENTS_CATALOG = { enroll_status: 'OPT_OUT' };
      features.PRODUCT_METADATA_AUTOMATION = { enroll_status: 'OPT_OUT' };
    }

    dof.creative_features_spec = features;
    creativeData.degrees_of_freedom_spec = dof;
  }

  private normalizeCatalogCreativeForMeta(
    creativeData: any,
    sourceCreative: any,
    productSetId?: string,
  ) {
    const assetFeed =
      creativeData.asset_feed_spec || sourceCreative?.asset_feed_spec || {};
    const sourceTemplateData =
      sourceCreative?.object_story_spec?.template_data ||
      creativeData?.object_story_spec?.template_data ||
      {};
    const sourceTemplateCta = sourceTemplateData.call_to_action || {};
    const link =
      assetFeed.link_urls?.find((item: any) => item?.website_url)
        ?.website_url ||
      sourceTemplateData.link ||
      sourceTemplateCta.value?.link ||
      sourceCreative?.link ||
      creativeData.link_url;
    const callToActionType =
      assetFeed.call_to_action_types?.[0] ||
      sourceTemplateCta.type ||
      sourceCreative?.callToAction ||
      'LEARN_MORE';
    const pageId =
      creativeData.object_story_spec?.page_id ||
      sourceCreative?.object_story_spec?.page_id ||
      sourceCreative?.pageId;

    creativeData.object_story_spec = CleanObjectOrArray({
      page_id: pageId,
      template_data: {
        message:
          this.extractAssetFeedText(assetFeed.bodies) ||
          sourceTemplateData.message ||
          sourceCreative?.body,
        link,
        name:
          this.extractAssetFeedText(assetFeed.titles) ||
          sourceTemplateData.name ||
          sourceCreative?.title ||
          '{{product.name}}',
        description:
          this.extractAssetFeedText(assetFeed.descriptions) ||
          sourceTemplateData.description ||
          sourceCreative?.description ||
          '{{product.description}}',
        multi_share_end_card:
          ['CAROUSEL', 'COLLECTION'].includes(
            sourceCreative?.catalogFormat || sourceCreative?.catalog_format,
          ) || undefined,
        call_to_action: {
          type: callToActionType,
          value: link ? { link } : undefined,
        },
      },
    });

    if (productSetId) creativeData.product_set_id = productSetId;

    // Giữ lại promotional_metadata khi rút gọn asset_feed_spec cho catalog product
    // creative (song song mb-ads). Trước đây xoá thẳng asset_feed_spec làm mất promo
    // trên path auto-publish — mà "Highlight your promotions" CHỈ áp dụng cho catalog.
    const promotionalMetadata =
      creativeData.asset_feed_spec?.promotional_metadata ||
      creativeData.promotional_metadata;
    delete creativeData.asset_feed_spec;
    if (promotionalMetadata) {
      creativeData.asset_feed_spec = {
        promotional_metadata: promotionalMetadata,
      };
    }
    delete creativeData.promotional_metadata;
    delete creativeData.image_hash;
    delete creativeData.image_url;
    delete creativeData.video_id;
    delete creativeData.thumbnail_url;
    delete creativeData.link_url;
  }

  private normalizeCreativeMediaForMeta(creativeData: any) {
    const linkData = creativeData?.object_story_spec?.link_data;
    const videoData = creativeData?.object_story_spec?.video_data;
    const assetFeed = creativeData?.asset_feed_spec;

    if (assetFeed) {
      const hasImages =
        Array.isArray(assetFeed.images) && assetFeed.images.length > 0;
      const hasVideos =
        Array.isArray(assetFeed.videos) && assetFeed.videos.length > 0;
      if (hasImages && hasVideos) {
        assetFeed.ad_formats = ['AUTOMATIC_FORMAT'];
      } else if (
        Array.isArray(assetFeed.ad_formats) &&
        assetFeed.ad_formats.length > 1
      ) {
        assetFeed.ad_formats = [
          hasVideos && !hasImages ? 'SINGLE_VIDEO' : 'SINGLE_IMAGE',
        ];
      }
    }

    if (videoData) {
      delete videoData.list_thumbnails;
      delete videoData.selected_thumbnail_id;
      delete videoData.preview_url;
      delete videoData.source;
      if (videoData.image_id) {
        delete videoData.image_hash;
        delete videoData.image_id;
      }
    }

    const childAttachments = linkData?.child_attachments;
    if (Array.isArray(childAttachments)) {
      for (const attachment of childAttachments) {
        delete attachment.list_thumbnails;
        delete attachment.selected_thumbnail_id;
        delete attachment.preview_url;
        delete attachment.source;

        if (attachment.video_id) {
          attachment.image_url =
            attachment.image_url || attachment.thumbnail_url || undefined;
          delete attachment.thumbnail_url;
          if (attachment.image_id) delete attachment.image_url;
        }
      }
    }

    const assetVideos = assetFeed?.videos;
    if (!Array.isArray(assetVideos)) return;

    for (const video of assetVideos) {
      video.video_id = video.video_id || video.videoId || video.id;
      video.thumbnail_url =
        video.thumbnail_url ||
        video.image_url ||
        video.preview_url ||
        video.previewUrl ||
        video.thumbnail ||
        undefined;

      const thumbnailHash =
        video.thumbnail_hash ||
        video.image_id ||
        video.imageHash ||
        video.image_hash ||
        video.selected_thumbnail_id;
      if (thumbnailHash) {
        video.thumbnail_hash = thumbnailHash;
      }

      delete video.id;
      delete video.name;
      delete video.url;
      delete video.videoId;
      delete video.imageHash;
      delete video.image_hash;
      delete video.image_id;
      delete video.image_url;
      delete video.thumbnail;
      delete video.preview_url;
      delete video.previewUrl;
      delete video.video_thumbnails;
      delete video.list_thumbnails;
      delete video.selected_thumbnail_id;
      delete video.source;
      delete video.video_source;
    }
  }

  private applyPersonalizedDestinations(creativeData: any, config: any) {
    const websiteUrl = this.resolveCreativeWebsiteUrl(creativeData);
    const shopConfig = config.shop || {
      enabled: false,
      storefrontShopId: config.storefrontShopId,
    };
    const storefrontShopId =
      shopConfig.storefrontShopId ||
      config.storefrontShopId ||
      config.storefront_shop_id ||
      config.onsite_destinations?.[0]?.storefront_shop_id;

    if (shopConfig.enabled && storefrontShopId) {
      creativeData.asset_feed_spec = creativeData.asset_feed_spec || {};
      creativeData.asset_feed_spec.onsite_destinations = [
        { storefront_shop_id: storefrontShopId, auto_optimization: true },
      ];
    }

    if (config.optimize_website && websiteUrl) {
      creativeData.destination_spec = {
        website: {
          optimization: {
            status: 'OPT_IN',
            type: 'website_destination_optimization',
          },
        },
      };
    }
  }

  private stripOptionalPersonalizedDestinationFields(creativeData: any) {
    let stripped = false;

    if (creativeData.destination_spec) {
      delete creativeData.destination_spec;
      stripped = true;
    }

    const assetFeed = creativeData.asset_feed_spec;
    if (assetFeed) {
      for (const field of [
        'call_ads_configuration',
        'message_extensions',
        'onsite_destinations',
        'personalized_destinations',
      ]) {
        if (field in assetFeed) {
          delete assetFeed[field];
          stripped = true;
        }
      }
      if (Object.keys(assetFeed).length === 0)
        delete creativeData.asset_feed_spec;
    }

    return stripped;
  }

  private isOptionalPersonalizedDestinationError(error: any) {
    const response = error?.response || {};
    const rawMessage = [
      response?.message,
      response?.error_user_msg,
      response?.error_user_title,
      error?.message,
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase();

    return [
      'call_ads_configuration',
      'destination_spec',
      'message_extensions',
      'onsite_destinations',
      'personalized_destinations',
      'website_and_shop',
    ].some((field) => rawMessage.includes(field));
  }

  private formatMessageTemplate(template: any) {
    if (!template || !template.greeting) return undefined;

    const actionType = template.actionType || 'ice_breakers';
    const validActions =
      template.actions?.filter((action: any) => action.title?.trim() !== '') ||
      [];
    const mediaType =
      template.mediaType === 'NONE' || !template.mediaType
        ? 'text'
        : template.mediaType.toLowerCase();

    const baseJson: any = {
      type: 'VISUAL_EDITOR',
      version: 2,
      landing_screen_type: 'welcome_message',
      media_type: mediaType,
      user_edit: false,
      surface: 'visual_editor_new',
      text_format: {
        customer_action_type: actionType,
        message: {
          text: template.greeting,
        },
      },
    };

    if (mediaType === 'image' && template.mediaUrl) {
      baseJson.image_url = template.mediaUrl;
    } else if (mediaType === 'video' && template.mediaUrl) {
      baseJson.video_url = template.mediaUrl;
    }

    if (actionType === 'ice_breakers') {
      baseJson.text_format.message.ice_breakers = validActions.map(
        (action: any) => ({
          title: action.title,
          response: action.response || '',
        }),
      );
      baseJson.text_format.message.quick_replies = [];
    } else if (actionType === 'quick_replies') {
      baseJson.text_format.message.quick_replies = validActions.map(
        (action: any) => ({
          content_type: 'text',
          title: action.title,
          payload: action.payload || action.title,
        }),
      );
    } else if (actionType === 'buttons') {
      baseJson.text_format.message.buttons = validActions.map((action: any) => {
        const isUrl = action.payload?.startsWith('http');
        return {
          type: isUrl ? 'web_url' : 'postback',
          title: action.title,
          [isUrl ? 'url' : 'payload']: action.payload || action.title,
        };
      });
    }

    return JSON.stringify(baseJson);
  }

  private resolveCreativeWebsiteUrl(creativeData: any) {
    const assetFeedLink = creativeData.asset_feed_spec?.link_urls?.find(
      (link: any) => link?.website_url,
    )?.website_url;
    const linkData = creativeData.object_story_spec?.link_data;
    const videoData = creativeData.object_story_spec?.video_data;
    const carouselLink = linkData?.child_attachments?.find(
      (item: any) => item?.link,
    )?.link;

    return (
      assetFeedLink ||
      linkData?.link ||
      carouselLink ||
      videoData?.call_to_action?.value?.link ||
      linkData?.call_to_action?.value?.link ||
      creativeData.link_url ||
      undefined
    );
  }

  private extractAssetFeedText(items: any[]) {
    if (!Array.isArray(items)) return undefined;
    const item = items.find(
      (entry) => entry?.text || typeof entry === 'string',
    );
    return typeof item === 'string' ? item : item?.text;
  }

  private getThumbnailList(value: any): any[] {
    const thumbnails =
      typeof value === 'string' ? this.tryParseJson(value) : value;
    if (Array.isArray(thumbnails)) return thumbnails;
    if (Array.isArray(thumbnails?.data)) return thumbnails.data;
    return [];
  }

  private tryParseJson(value: string) {
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }

  private clone<T>(value: T): T {
    return JSON.parse(JSON.stringify(value ?? null));
  }
}
