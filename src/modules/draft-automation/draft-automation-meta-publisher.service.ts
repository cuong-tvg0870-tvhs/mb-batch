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
  MPC_NOT_ELIGIBLE_MESSAGE,
  parseMetaError,
  classifyMetaError,
  pickDominantClassification,
  sleep,
} from '../../common/utils';
import {
  AD_FIELDS,
  ADSET_FIELDS,
  CAMPAIGN_FIELDS,
} from '../../common/utils/meta-field';
import {
  accountCanPromotePage,
  pageIdFromStory,
} from '../../common/utils/promote-pages.util';
import { AppConfigReader } from '../app-config/app-config.reader';
import { PrismaService } from '../prisma/prisma.service';
import { activatePendingAutomationForCampaign } from './pending-automation-activator';

// ── Knob "Tiện ích liên hệ nhanh" (asset_feed_spec.message_extensions) ──────────────
// mb-batch KHÔNG có registry (registry chỉ tồn tại ở mb-ads:
// src/modules/app-config/app-config.registry.ts ~163) nên key + mặc định + NHÃN được
// chép nguyên văn xuống đây để hai repo không lệch chữ. Sửa một bên phải soi bên kia.
//
//   key    = 'quick_contact_capability'   type = string   default = 'UNKNOWN'
//   label  = 'Năng lực "Tiện ích liên hệ nhanh" (Meta cấp cho ứng dụng)'
//
// GẮN THEO ỨNG DỤNG, KHÔNG THEO TÀI KHOẢN QUẢNG CÁO — đã đo chéo 07-08-2026 trên
// act_1016912473495319 và act_1262628191933179: CẢ HAI đều bị Meta từ chối với
// `(#3) Application does not have the capability to make this API call.`
// ⇒ knob TOÀN HỆ THỐNG, đừng biến thành cấu hình per-TKQC.
//
// Ba trạng thái, CỐ Ý không dùng boolean:
//   'UNKNOWN'     – chưa biết (mặc định). KHÔNG gỡ, KHÔNG chặn gì → giữ nguyên 100%
//                   hành vi cũ, deploy knob này không làm ai mất tính năng đang dùng được.
//   'AVAILABLE'   – Meta chấp nhận → gửi bình thường (y như UNKNOWN).
//   'UNAVAILABLE' – gỡ message_extensions TRƯỚC khi gọi Meta (FE khoá công tắc kèm lý do).
// Việc DÒ LẠI (probe) là của mb-ads — do admin bấm; mb-batch tuyệt đối không tự dò.
const QUICK_CONTACT_CAPABILITY_KEY = 'quick_contact_capability';
const QUICK_CONTACT_CAPABILITY_LABEL =
  'Năng lực "Tiện ích liên hệ nhanh" (Meta cấp cho ứng dụng)';
const QUICK_CONTACT_CAPABILITY_DEFAULT = 'UNKNOWN';

/**
 * ⛔ CỜ TẠM 07-08-2026 — bật = KHÔNG gỡ `asset_feed_spec.message_extensions` dù knob đang
 * `UNAVAILABLE`. Chủ hệ thống yêu cầu: thà thấy lỗi Meta thật còn hơn để hệ thống âm thầm
 * bỏ tiện ích của người dùng rồi báo đăng thành công.
 * Đặt `false` để trả lại hành vi gỡ tự động.
 * ⚠️ mb-ads có cờ CÙNG TÊN (`MetaService.TAM_TAT_GO_QUICK_CONTACT`) — phải đổi CẢ HAI
 * cùng lúc, lệch nhau thì cùng một bản nháp đăng tay và cron cho ra payload khác nhau.
 */
const TAM_TAT_GO_QUICK_CONTACT = true;

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

  constructor(
    private readonly prisma: PrismaService,
    // Đọc/ghi knob quick_contact_capability. DraftAutomationModule đã imports
    // AppConfigModule sẵn nên không phải sửa wiring.
    private readonly appConfig: AppConfigReader,
  ) {}

  private init() {
    if (this.initialized) return;

    const token = process.env.SDK_FACEBOOK_ACCESS_TOKEN;
    if (!token) {
      throw new Error('SDK_FACEBOOK_ACCESS_TOKEN is missing.');
    }

    FacebookAdsApi.init(token);
    this.initialized = true;
  }

  // Ngưỡng "tối thiểu N mẫu nội dung/chiến dịch" — cấu hình runtime qua
  // SystemConfig[min_publish_contents] (parity mb-ads DraftCampaignService). value =
  // số (vd 5) hoặc { value: 5 }. FAIL-OPEN về mặc định 5 khi thiếu row/lỗi.
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

  async publishDraftCampaign(systemCampaignId: string) {
    this.init();

    const campaignSystem = await this.prisma.systemCampaign.findUnique({
      where: { id: systemCampaignId },
      include: {
        // orderBy position trước: createdAt cố định theo transaction trong Postgres
        // nên không dùng làm thứ tự tất định được (parity mb-ads).
        ad_sets: {
          orderBy: [{ position: 'asc' }, { createdAt: 'asc' }, { id: 'asc' }],
          include: {
            ads: {
              orderBy: [
                { position: 'asc' },
                { createdAt: 'asc' },
                { id: 'asc' },
              ],
            },
          },
        },
      },
    });

    if (!campaignSystem) {
      throw new Error(`SystemCampaign ${systemCampaignId} not found.`);
    }

    // CHỐT CHẶN Ô-TRỐNG (parity mb-ads validateLaunchReadiness): KHÔNG publish khi còn
    // ô nội dung chưa lấp — creative còn token slot `VIDEO_n`/`IMAGE_n` hoặc cờ
    // `placeholder`. Nếu để lọt, token/creative rỗng lên Meta sẽ bị từ chối. Bản đăng
    // tay chặn được; auto cũng phải chặn (isComplete đếm số lượng nên có thể sai khi
    // template có slot index vượt số lượng required — xem finding [26]). Bỏ qua SỚM,
    // KHÔNG claim, KHÔNG tạo gì trên Meta.
    const hasUnfilledSlot = (campaignSystem.ad_sets || []).some((adSet) =>
      (adSet.ads || []).some((ad) => {
        const cr = (
          (CleanObjectOrArray((ad.data as any) || {}) || {}) as any
        )?.creative;
        return this.creativeHasUnfilledSlot(cr);
      }),
    );
    if (hasUnfilledSlot) {
      this.logger.warn(
        `Bỏ qua publish draft ${campaignSystem.id}: còn ô nội dung trống (slot chưa được lấp bằng ảnh/video).`,
      );
      return { skipped: true, reason: 'EMPTY_SLOT' };
    }

    // LƯỚI AN TOÀN ≥5 NỘI DUNG (parity mb-ads validateLaunchReadiness): cron đăng qua
    // publisher NÀY (không qua pushToMeta mb-ads) nên phải tự kiểm — KHÔNG tạo chiến
    // dịch MỚI lên Meta dưới 5 mẫu nội dung (chuẩn phân phối Meta). Bỏ qua SỚM (không
    // throw → tránh retry-loop/rollback), engine ghi nhận nhẹ nhàng. Chỉ lần tạo mới
    // (chưa meta_id), không phải template, miễn khi tên chiến dịch chứa "TestingContent".
    // Backstop cho row lọt qua gate cấu hình + tự-tạm-dừng (vd nhánh legacy không có row).
    const MIN_PUBLISH_CONTENTS = await this.getMinPublishContents();
    const totalContents = (campaignSystem.ad_sets || []).reduce(
      (sum, adSet) => sum + (adSet.ads?.length || 0),
      0,
    );
    const campaignName = String(
      campaignSystem.campaign_name ||
        (campaignSystem.data as any)?.campaign?.name ||
        '',
    );
    const isTestingContent = campaignName
      .toLowerCase()
      .includes('testingcontent');
    if (
      !campaignSystem.meta_id &&
      !campaignSystem.is_template &&
      !isTestingContent &&
      totalContents < MIN_PUBLISH_CONTENTS
    ) {
      this.logger.warn(
        `Bỏ qua publish draft ${campaignSystem.id}: chỉ ${totalContents}/${MIN_PUBLISH_CONTENTS} mẫu nội dung — chưa đạt chuẩn phân phối Meta.`,
      );
      return { skipped: true, reason: 'MIN_CONTENTS_NOT_MET' };
    }

    // CHỐT CHẶN GIÁ THẦU (parity mb-ads validateLaunchReadiness): Meta bắt buộc chiến
    // lược "Giới hạn giá thầu" (LOWEST_COST_WITH_BID_CAP) / "Chi phí mục tiêu"
    // (COST_CAP, bí danh Meta TARGET_COST) phải kèm bid_amount; "Mục tiêu ROAS"
    // (LOWEST_COST_WITH_MIN_ROAS) phải kèm bid_constraints.roas_average_floor. Thiếu →
    // Meta từ chối cả nhóm với lỗi khó hiểu. Bản đăng tay chặn; cron cũng phải chặn.
    // Bỏ qua SỚM (KHÔNG tự sửa chiến lược để tránh đổi ngầm hành vi chi tiêu), KHÔNG
    // claim, KHÔNG tạo gì trên Meta. Dưới CBO giá thầu khai ở campaign; ABO ở ad set.
    const campaignBidData: any =
      (CleanObjectOrArray(campaignSystem.data || {}) as any)?.campaign ||
      (CleanObjectOrArray(campaignSystem.data || {}) as any) ||
      {};
    const isCboBid = !!(
      campaignBidData.campaign_budget_optimization ||
      campaignBidData.daily_budget ||
      campaignBidData.lifetime_budget
    );
    const CAP_BID_STRATEGIES = [
      'LOWEST_COST_WITH_BID_CAP',
      'COST_CAP',
      'TARGET_COST',
    ];
    const hasBidAmountGap = (campaignSystem.ad_sets || []).some((adSet) => {
      const asData: any = (CleanObjectOrArray((adSet.data as any) || {}) ||
        {}) as any;
      const strat = String(
        (isCboBid ? campaignBidData.bid_strategy : asData.bid_strategy) || '',
      );
      if (CAP_BID_STRATEGIES.includes(strat)) {
        // Meta đòi bid_amount TRÊN TỪNG ad set (kể cả CBO): buildCampaignCreatePayload
        // XOÁ campaign.bid_amount và KHÔNG hạ xuống ad set, nên amount khai ở cấp campaign
        // KHÔNG bao giờ tới Meta → không được coi là "đã có". Chỉ xét bid_amount của ad set.
        return !(Number(asData.bid_amount) > 0);
      }
      if (strat === 'LOWEST_COST_WITH_MIN_ROAS') {
        // ROAS chỉ hợp lệ khi optimization_goal = VALUE; goal ≠ VALUE sẽ được
        // normalizeRoasBidStrategy tự hạ về "Chi phí thấp nhất" → KHÔNG skip ở đây.
        // Chỉ đòi ROAS floor khi thật sự tối ưu theo Giá trị (parity mb-ads gate).
        if (String(asData.optimization_goal || '') !== 'VALUE') return false;
        const floor =
          asData?.bid_constraints?.roas_average_floor ??
          campaignBidData?.bid_constraints?.roas_average_floor;
        return !(Number(floor) > 0);
      }
      return false;
    });
    if (hasBidAmountGap) {
      this.logger.warn(
        `Bỏ qua publish draft ${campaignSystem.id}: có nhóm dùng chiến lược giá thầu cần số tiền (giới hạn giá thầu / chi phí mục tiêu / ROAS) nhưng chưa nhập — Meta sẽ từ chối.`,
      );
      return { skipped: true, reason: 'BID_AMOUNT_MISSING' };
    }

    // Gom mọi story "Dùng bài viết có sẵn" để gate quyền Trang bên dưới (promotePages).
    // (Đã BỎ chặn "dark post": dark post là bài hợp lệ thuộc Trang, đăng lại được như bài
    // thường nếu TKQC có quyền page — do gate promotePages lo; bài thật sự không đăng lại
    // được thì để Meta tự báo.)
    const postStories = [
      ...new Set(
        (campaignSystem.ad_sets || []).flatMap((adSet) =>
          (adSet.ads || []).map((ad) => {
            const cr = (
              (CleanObjectOrArray((ad.data as any) || {}) || {}) as any
            )?.creative;
            return String(
              cr?.object_story_id ||
                cr?.effective_object_story_id ||
                cr?.datasource?.object_story_id ||
                cr?.datasource?.effective_object_story_id ||
                '',
            );
          }),
        ),
      ),
    ].filter(Boolean);

    const data = this.clone(
      CleanObjectOrArray(campaignSystem.data || {}) || {},
    );

    // Hạ "Mục tiêu ROAS" (LOWEST_COST_WITH_MIN_ROAS) về "Chi phí thấp nhất" khi mục tiêu
    // tối ưu KHÔNG phải VALUE — Meta cấm cặp này, luôn nổ khi publish. Nhiều template cố
    // định (combo scale) để ROAS nhưng goal là OFFSITE/MESSAGING_* → sửa TẠI CHỖ trên
    // data.campaign + ad_sets[].data trước khi build payload (loop dưới re-clone adSet.data
    // nên bắt được thay đổi). Idempotent, không ghi DB. Parity mb-ads pushToMetaCore.
    this.normalizeRoasBidStrategy(data.campaign, campaignSystem.ad_sets || []);

    const accountId =
      campaignSystem.accountId || data.ad_account_id || data.account_id;
    if (!accountId) {
      throw new Error('Campaign does not have an ad account id.');
    }

    const adAccountId = this.normalizeAdAccountId(accountId);
    const adAccount = new AdAccount(adAccountId);

    // CHỐT CHẶN "TKQC thiếu quyền Trang" (parity mb-ads validateLaunchReadiness):
    // story organic hợp lệ vẫn bị Meta từ chối (1815017) nếu TKQC ĐÍCH không được
    // cấp quyền quảng bá Trang của bài (không nằm trong promote_pages). Chỉ chặn khi
    // promotePages ĐÃ sync (có dữ liệu); null → bỏ qua (fallback). Chặn TRƯỚC khi
    // claim để không tạo gì trên Meta.
    if (postStories.length) {
      const acc = await this.prisma.account.findUnique({
        where: { id: adAccountId },
        select: { promotePages: true },
      });
      const blockedStory = postStories.find((s) => {
        const pageId = pageIdFromStory(s);
        return pageId && !accountCanPromotePage(acc?.promotePages, pageId);
      });
      if (blockedStory) {
        this.logger.warn(
          `Bỏ qua publish draft ${campaignSystem.id}: TKQC ${adAccountId} chưa được cấp quyền quảng bá Trang ${pageIdFromStory(blockedStory)} của bài "${blockedStory}" — Meta sẽ từ chối (1815017).`,
        );
        return { skipped: true, reason: 'PAGE_NOT_PROMOTABLE' };
      }
    }

    // Khóa chống trùng: chỉ MỘT tiến trình được publish một draft tại một thời điểm.
    // updateMany đặt isPublishing=true một cách nguyên tử khi draft đang rảnh và
    // CHƯA có meta_id. Nếu count=0 → đang có tiến trình khác publish hoặc campaign
    // đã được publish rồi → bỏ qua, tránh tạo campaign Meta TRÙNG (tiêu ngân sách
    // hai lần khi hai lần cron chồng nhau). Mirror chốt chặn của publish thủ công.
    //
    // GỠ KHÓA KẸT: pod crash sau khi chiếm cờ nhưng trước khi tạo campaign →
    // isPublishing=true vĩnh viễn, mọi lần cron sau đều claim.count=0 và cleanup
    // 7 ngày cũng né (yêu cầu isPublishing=false) → draft bị treo mãi mãi. Cho
    // phép chiếm lại khi claim đã quá 20 phút (mirror STUCK_PUBLISH_MS của
    // mb-ads). An toàn vì cửa sổ cần bảo vệ rất ngắn: campaign create chạy ngay
    // sau claim và khi meta_id đã được ghi thì điều kiện meta_id=null tự chặn.
    const STUCK_PUBLISH_MS = 20 * 60 * 1000; // 20 phút
    const stuckBefore = new Date(Date.now() - STUCK_PUBLISH_MS);
    const claim = await this.prisma.systemCampaign.updateMany({
      where: {
        id: campaignSystem.id,
        meta_id: null,
        OR: [
          { isPublishing: false },
          { publishClaimedAt: { lt: stuckBefore } },
          // Khóa kẹt từ trước khi có cột publishClaimedAt (hoặc do writer cũ
          // chưa ghi cột này): chỉ coi là kẹt khi bản ghi đã lâu không có ai
          // đụng tới — mọi write hợp lệ trong lúc publish đều làm mới updatedAt.
          { publishClaimedAt: null, updatedAt: { lt: stuckBefore } },
        ],
      },
      data: {
        isPublishing: true,
        publishClaimedAt: new Date(),
        errors: Prisma.DbNull,
      },
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

      // Trang có Instagram liên kết → Meta yêu cầu đích MESSAGING_INSTAGRAM_DIRECT_MESSENGER
      // cho "Mua hàng qua tin nhắn"; plain MESSENGER bị từ chối (subcode 2490408). Trang
      // không IG vẫn dùng MESSENGER được. Nạp 1 lần. Đồng bộ với mb-ads.
      const igLinkedPageIds = await this.loadIgLinkedPageIds(accountId);

      for (const adSetSystem of campaignSystem.ad_sets) {
        currentStepKey = 'adsets';

        const adSetData: any = this.clone(adSetSystem.data || {});

        // Đích nhắn tin phụ thuộc Trang có Instagram liên kết hay không (xem ghi chú ở
        // igLinkedPageIds). Chốt cả 2 chiều cho goal nhắn tin (MPC + CONVERSATIONS).
        // Đồng bộ với mb-ads draft-campaign.service.ts:
        //  - Trang CÓ IG + MPC + Messenger/trống → nâng lên combo (Meta bắt buộc).
        //  - Chọn đích Instagram/combo NHƯNG Trang KHÔNG có IG → hạ về Messenger để
        //    tránh Meta từ chối cả nhóm (subcode 2490408).
        const mpcPageId = adSetData.promoted_object?.page_id;
        const isMessagingGoal =
          adSetData.optimization_goal === 'MESSAGING_PURCHASE_CONVERSION' ||
          adSetData.optimization_goal === 'CONVERSATIONS';
        const isIgDestination =
          adSetData.destination_type === 'INSTAGRAM_DIRECT' ||
          adSetData.destination_type === 'MESSAGING_INSTAGRAM_DIRECT_MESSENGER';
        if (isMessagingGoal && mpcPageId) {
          const pageHasIg = igLinkedPageIds.has(String(mpcPageId));
          if (
            pageHasIg &&
            adSetData.optimization_goal === 'MESSAGING_PURCHASE_CONVERSION' &&
            (adSetData.destination_type === 'MESSENGER' ||
              !adSetData.destination_type)
          ) {
            adSetData.destination_type = 'MESSAGING_INSTAGRAM_DIRECT_MESSENGER';
          } else if (!pageHasIg && isIgDestination) {
            console.warn(
              `[draft-automation-publish] adSet ${adSetSystem.id}: Trang ${mpcPageId} chưa liên kết Instagram — hạ đích ${adSetData.destination_type} → MESSENGER để tránh Meta từ chối.`,
            );
            adSetData.destination_type = 'MESSENGER';
          }
        }
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
          const classification = classifyMetaError(metaError);
          let friendlyMsg =
            metaErrorToFriendly(metaError) || metaError?.message || String(err);
          // Meta subcode 2490408 cho MPC = Trang chưa đủ điều kiện mua-hàng-qua-tin-nhắn
          // (không phải sai objective). Dịch thành thông báo hành động được. Đồng bộ
          // với mb-ads draft-campaign.service.ts. Automation không có UI hỏi trước nên
          // đây là lưới chính để log/hiện lỗi rõ ràng.
          if (
            Number(metaError?.subcode) === 2490408 &&
            adSetData.optimization_goal === 'MESSAGING_PURCHASE_CONVERSION'
          ) {
            friendlyMsg = MPC_NOT_ELIGIBLE_MESSAGE;
            // Chỉ ca MPC mới là META_LIMITATION (Trang chưa đủ điều kiện). Ngoài MPC,
            // 2490408 = chọn sai goal↔objective → giữ DRAFT_CONFIG (user tự sửa).
            classification.category = 'META_LIMITATION';
            classification.fixableInDraft = false;
            classification.userMessage = MPC_NOT_ELIGIBLE_MESSAGE;
            classification.howToFix = MPC_NOT_ELIGIBLE_MESSAGE;
          }
          this.logger.error(
            `[createAdSet] adset ${adSetSystem.id} lỗi:`,
            metaError,
          );
          adSetResults.push({
            systemAdSetId: adSetSystem.id,
            name: adSetName,
            status: 'failed',
            error: friendlyMsg,
            classification,
          });
          for (const adSystem of adSetSystem.ads) {
            adResults.push({
              // adSystemId: khoá CHUẨN mb-ads/frontend đọc để thắp cảnh báo đúng ad.
              // systemAdId: giữ lại chỉ để tương thích ngược, sẽ bỏ sau.
              adSystemId: adSystem.id,
              systemAdId: adSystem.id,
              adName: (adSystem.data as any)?.name || adSystem.id,
              adSetName,
              status: 'failed',
              error: `Nhóm quảng cáo lỗi: ${friendlyMsg}`,
              classification,
              // scope='adset': lỗi nằm ở NHÓM quảng cáo, không phải từng mẫu. UI gom
              // theo adSetId để hiện 1 lần ở cấp nhóm (parity mb-ads).
              scope: 'adset',
              adSetId: adSetSystem.id,
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

            // Post-ID scaling: nội dung có bài viết sẵn cùng Trang + CTA → tham chiếu bài
            // viết (tránh lỗi "Ảnh không hợp lệ hoặc đã hết hạn"). Mutate tại chỗ.
            await this.applyScalePostId(adPayload.creative, adAccountId);

            const creativeData = this.buildCreativeData(adPayload);
            // Toggle "Hiển thị sản phẩm": OPT_OUT mặc định để tránh lỗi "tạo nội
            // dung động mà không có ID nhóm sản phẩm". Hàm này cũng ghi toggle "Duyệt
            // sản phẩm" (product_browsing) vì nó là nơi duy nhất được chạm
            // degrees_of_freedom_spec. Chỉ tác động ở bước tạo creative, không đụng
            // diff/snapshot. KHÔNG áp cho POST_ID (bài viết có sẵn).
            // ⚠️ LỆCH PARITY CÓ SẴN (không do thay đổi này tạo ra): mb-ads gọi hàm tương
            // ứng KHÔNG có guard object_story_id (meta.service.ts ~3462) ⇒ với creative
            // tham chiếu bài viết, đăng tay qua mb-ads có degrees_of_freedom_spec (kể cả
            // product_browsing) còn cron này thì không. Ai gỡ/đổi guard phải sửa CẢ HAI.
            if (!creativeData.object_story_id)
              this.applyProductExtensionsPreference(
                creativeData,
                adPayload.creative,
              );
            // 🔴 GỌI RIÊNG, NGOÀI cả guard `object_story_id` lẫn guard catalog bên trong
            // applyProductExtensionsPreference — parity với mb-ads (meta.service.ts, ngay
            // sau applyProductExtensionsPreference). Hai guard đó là của "Hiển thị sản
            // phẩm"; để "Duyệt sản phẩm" thừa hưởng chúng thì toggle bị NUỐT IM LẶNG với
            // catalog ad và với quảng cáo dùng bài viết có sẵn: người dùng bật được, nháp
            // lưu đúng, publish không lỗi, mà Meta không nhận gì.
            this.applyProductBrowsingPreference(creativeData, adPayload.creative);
            // Catalog ad BẮT BUỘC instagram_user_id, nếu không Meta từ chối ở bước tạo AD
            // (#100 subcode 1772103). Parity mb-ads
            // MetaService.attachInstagramActorForCatalogCreative.
            await this.attachInstagramActorForCatalogCreative(creativeData);
            // Đích nhắn tin combo (Messenger + Instagram…) → creative BẮT BUỘC khai
            // asset_feed_spec DOF_MESSAGING_DESTINATION, nếu không Meta từ chối tạo Ad
            // (subcode 2446493). destination_type ĐÃ CHỐT nằm ở adSetData. Parity mb-ads.
            this.applyMultiDestinationMessaging(
              creativeData,
              adSetData.destination_type,
            );
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
              // adSystemId: khoá CHUẨN mb-ads/frontend đọc; systemAdId giữ lại để
              // tương thích ngược, sẽ bỏ sau.
              adSystemId: adSystem.id,
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
            const classification = classifyMetaError(metaError);
            const friendlyMsg =
              metaErrorToFriendly(metaError) ||
              metaError?.message ||
              String(err);
            this.logger.error(`[createAd] ad ${adSystem.id} lỗi:`, metaError);
            adsProcessed += 1;
            adResults.push({
              // adSystemId: khoá CHUẨN mb-ads/frontend đọc; systemAdId giữ lại để
              // tương thích ngược, sẽ bỏ sau.
              adSystemId: adSystem.id,
              systemAdId: adSystem.id,
              adName,
              adSetName,
              status: 'failed',
              error: friendlyMsg,
              classification,
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

      // TỰ ĐỘNG HOÁ SAU KHI LÊN CAMP (đường cron DraftAutomation): vật chất hoá config
      // pendingAutomation (đã nhân từ mẫu) thành CampaignRule thật — khung giờ đẩy Meta,
      // điều kiện bật ACTIVE cho runner. CHẠY TRƯỚC khi chốt publish (history SUCCESS +
      // isPublishing=false) để warning được ghi CÙNG NHỊP — không có race "chốt xong mới
      // ghi warning". Best-effort: lỗi automation KHÔNG fail publish (chỉ warning). CHỈ khi
      // publish ĐỦ (failedAds===0) — không scale/đẩy lịch cho camp thiếu QC.
      let autoSummary: { failed: number; errors: string[] } = {
        failed: 0,
        errors: [],
      };
      if (campaignMetaId && failedAds === 0) {
        try {
          autoSummary = await activatePendingAutomationForCampaign({
            prisma: this.prisma,
            systemCampaignId: campaignSystem.id,
            userId: campaignSystem.createdById,
            accountId: campaignSystem.accountId as string,
            campaignMetaId,
            // Suy CBO/ABO từ trạng thái THẬT của campaign, không tin entry.level. CBO đọc
            // config cấp campaign; ABO tự re-query SystemAdSet.data.pendingAutomation từng nhóm.
            isCbo: !!campaignSystem.campaign_CBO,
            campaignPendingAutomation: (campaignSystem as any).pendingAutomation,
          });
        } catch (e: any) {
          // activatePendingAutomationForCampaign vốn NUỐT lỗi & trả {failed,errors}; throw ở
          // đây là ngoài dự kiến → vẫn coi là 1 lỗi automation để SINH WARNING (không nuốt
          // thành log câm).
          this.logger.warn(
            `activatePendingAutomation (camp ${campaignMetaId}) lỗi: ${e?.message ?? e}`,
          );
          autoSummary = { failed: 1, errors: [String(e?.message ?? e)] };
        }
      }

      // errors: PARTIAL (ad lỗi) ƯU TIÊN; nếu publish đủ mà automation lỗi → automationWarning
      // (banner + nút Thử lại ở màn chi tiết, parity mb-ads); còn lại DbNull.
      const errorsValue =
        failedAds > 0
          ? ({
              partial: true,
              message: `${failedAds}/${adResults.length} quảng cáo lỗi — các quảng cáo còn lại đã lên Meta.`,
              failedAds: adResults.filter((r) => r.status === 'failed'),
            } as any)
          : autoSummary.failed > 0
            ? ({
                automationWarning: {
                  failed: autoSummary.failed,
                  messages: autoSummary.errors.slice(0, 5),
                },
              } as any)
            : Prisma.DbNull;

      await this.updatePublishStep(
        history.id,
        'sync',
        { status: failedAds > 0 ? 'partial' : 'success' },
        failedAds > 0 ? 'PARTIAL' : 'SUCCESS',
      );

      // PARTIAL: giữ campaign + ad tốt live, KHÔNG rollback; lưu danh sách ad lỗi / warning;
      // hasMetaChanges=true để lần publish/automation sau retry các ad meta_id=null.
      // isPublishing=false đặt CUỐI CÙNG (sau history SUCCESS) → FE dừng poll khi mọi thứ đã
      // chốt (warning + success cùng nhịp).
      await this.prisma.systemCampaign.update({
        where: { id: campaignSystem.id },
        data: {
          errors: errorsValue,
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
      const classification = classifyMetaError(metaError);
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
        // Lỗi ném lên có thể là wrapper không mang code Meta → lấy phân loại thực từ
        // các ad lỗi để badge tổng đúng (khớp mb-ads draft-campaign.service.ts).
        const topClassification =
          classification.code != null
            ? classification
            : (pickDominantClassification(failedList) ?? classification);
        await this.prisma.systemCampaign.update({
          where: { id: campaignSystem.id },
          data: {
            errors: {
              partial: true,
              message: errorMessage,
              classification: topClassification,
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

  // Tập page_id có Instagram liên kết (từ Account.pages đã sync). Quyết định
  // destination_type cho ad set "Mua hàng qua tin nhắn". Lỗi DB → Set rỗng (giữ
  // MESSENGER như cũ). Đồng bộ với mb-ads draft-campaign.service.loadIgLinkedPageIds.
  private async loadIgLinkedPageIds(accountId: string): Promise<Set<string>> {
    const ids = new Set<string>();
    try {
      const account = await this.prisma.account.findUnique({
        where: { id: this.normalizeAdAccountId(accountId) },
        select: { pages: true },
      });
      const pages = Array.isArray(account?.pages)
        ? (account?.pages as any[])
        : [];
      for (const p of pages) {
        const hasIg =
          !!p?.instagram_business_account ||
          !!p?.connected_instagram_account ||
          (Array.isArray(p?.instagram_accounts?.data) &&
            p.instagram_accounts.data.length > 0);
        if (hasIg && p?.id) ids.add(String(p.id));
      }
    } catch (e) {
      this.logger.error('[loadIgLinkedPageIds] đọc Account.pages lỗi, bỏ qua:', e);
    }
    return ids;
  }

  private normalizeMetaStatus(status?: string, fallback = 'PAUSED') {
    const allowedStatuses = ['ACTIVE', 'PAUSED', 'ARCHIVED', 'DELETED'];
    return status && allowedStatuses.includes(status) ? status : fallback;
  }

  // Hạ "Mục tiêu ROAS" về "Chi phí thấp nhất" khi optimization_goal ≠ VALUE (Meta cấm).
  // Dưới CBO giá thầu ở campaign (hợp lệ chỉ khi MỌI ad set = VALUE); ABO ở từng ad set.
  // Sửa tại chỗ trên object in-memory; idempotent. Parity mb-ads normalizeRoasBidStrategy.
  private normalizeRoasBidStrategy(
    campaignData: any,
    adSets: Array<{ data: any }>,
  ): void {
    if (!campaignData) return;
    const ROAS = 'LOWEST_COST_WITH_MIN_ROAS';
    const SAFE = 'LOWEST_COST_WITHOUT_CAP';
    const isValueGoal = (g: any) => String(g || '') === 'VALUE';
    const isCbo = !!(
      campaignData.campaign_budget_optimization ||
      campaignData.daily_budget ||
      campaignData.lifetime_budget
    );

    if (isCbo) {
      if (String(campaignData.bid_strategy || '') !== ROAS) return;
      const allValue =
        adSets.length > 0 &&
        adSets.every((as) => isValueGoal((as?.data as any)?.optimization_goal));
      if (!allValue) {
        campaignData.bid_strategy = SAFE;
        delete campaignData.bid_constraints;
      }
      return;
    }

    for (const as of adSets) {
      const d: any = as?.data;
      if (!d) continue;
      if (
        String(d.bid_strategy || '') === ROAS &&
        !isValueGoal(d.optimization_goal)
      ) {
        d.bid_strategy = SAFE;
        delete d.bid_constraints;
      }
    }
  }

  private buildCampaignCreatePayload(payload: any) {
    const metaPayload = { ...(payload || {}) };
    delete metaPayload.id;
    delete metaPayload.advantage_catalog;
    // Chỉ xoá field PHẲNG (metadata nháp); `promoted_object` lồng bên trong được spread
    // giữ nguyên — đó mới là chỗ Meta nhận catalog id cho catalog ad.
    delete metaPayload.product_catalog_id;
    delete metaPayload.bid_amount;

    // LƯỚI AN TOÀN (parity mb-ads MetaService.buildCampaignCreatePayload): Meta BẮT BUỘC
    // `is_adset_budget_sharing_enabled` khi campaign KHÔNG có ngân sách cấp chiến dịch —
    // thiếu thì từ chối tạo, lỗi #100 subcode 4834011. Đã tái hiện thật 07-08-2026.
    // Nháp do automation sinh ra không đi qua FE nên KHÔNG chắc có field này ⇒ mặc định
    // false. Có CBO thì để nguyên, không gửi thừa.
    const hasCampaignBudget =
      metaPayload.daily_budget !== undefined ||
      metaPayload.lifetime_budget !== undefined;
    if (
      !hasCampaignBudget &&
      metaPayload.is_adset_budget_sharing_enabled === undefined
    ) {
      metaPayload.is_adset_budget_sharing_enabled = false;
    }

    // BACKSTOP lịch chạy quá khứ cấp CHIẾN DỊCH (CBO lifetime). Meta dùng start_time/
    // stop_time ở campaign khi ngân sách trọn đời nằm ở đây; mốc bê từ mẫu có thể đã QUA →
    // Meta từ chối. Song song với ép mốc ở saveAutomationDraft (C6). Parity với backstop ad
    // set (fix a65bd18): parse epoch/ISO an toàn, lifetime bắt buộc có stop_time tương lai.
    {
      const nowUnix = Math.floor(Date.now() / 1000);
      const toUnix = (v: any): number | null => {
        if (v === null || v === undefined || v === '') return null;
        if (typeof v === 'number' && Number.isFinite(v)) return Math.floor(v);
        const s = String(v).trim();
        if (/^\d+$/.test(s)) return parseInt(s, 10);
        const ms = Date.parse(s);
        return Number.isNaN(ms) ? null : Math.floor(ms / 1000);
      };
      const startUnix = toUnix(metaPayload.start_time);
      if (startUnix !== null && startUnix <= nowUnix) {
        // start_time quá khứ → bỏ trống để Meta bắt đầu ngay lúc publish.
        delete metaPayload.start_time;
      }
      const stopUnix = toUnix(metaPayload.stop_time);
      if (stopUnix !== null && stopUnix <= nowUnix) {
        if (!metaPayload.lifetime_budget) {
          // Không phải CBO lifetime → stop_time thừa/quá khứ, bỏ để camp chạy liên tục.
          delete metaPayload.stop_time;
        } else {
          // CBO lifetime BẮT BUỘC có stop_time tương lai → dời +30 ngày kể từ bây giờ.
          metaPayload.stop_time = String(nowUnix + 30 * 24 * 60 * 60);
        }
      }
    }

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
    // Cấu hình tự-động-hoá RIÊNG cho nhóm — chỉ lưu ở SystemAdSet.data, KHÔNG gửi lên Meta.
    delete metaPayload.pendingAutomation;

    // ABO (ngân sách nằm ở ad set) BẮT BUỘC có bid_strategy ở cấp ad set — nếu
    // thiếu, Meta từ chối bằng thông báo khó hiểu ("Cần có giá thầu… đặt GIÁ TRỊ
    // là mục tiêu tối ưu"). Mặc định "Chi phí thấp nhất, không giới hạn giá thầu".
    // CHỈ áp khi ad set tự mang ngân sách (=ABO); dưới CBO ad set KHÔNG có ngân
    // sách và Meta CẤM khai bid_strategy ở đây. Giữ đồng bộ với mb-ads.
    const adSetHasOwnBudget =
      metaPayload.daily_budget != null || metaPayload.lifetime_budget != null;
    if (adSetHasOwnBudget && !metaPayload.bid_strategy) {
      metaPayload.bid_strategy = 'LOWEST_COST_WITHOUT_CAP';
    } else if (!adSetHasOwnBudget && metaPayload.bid_strategy) {
      // CBO: Meta CẤM khai bid_strategy ở ad set → xoá giá trị thừa (parity mb-ads).
      delete metaPayload.bid_strategy;
    }

    // MESSAGING_PURCHASE_CONVERSION ("Mua hàng qua tin nhắn" — CTM/CTWA): Meta đo sự
    // kiện Purchase qua Conversions API for Business Messaging (page_id + PSID), KHÔNG
    // dùng pixel trình duyệt. promoted_object PHẢI khai `smart_pse_enabled` để Meta biết
    // nguồn tín hiệu mua hàng; thiếu nó Meta từ chối cả cụm với thông báo khó hiểu "không
    // thể dùng mục tiêu hiệu quả cho mục tiêu chiến dịch". Bằng chứng THẬT: 2014/2014 ad
    // set MESSAGING_PURCHASE_CONVERSION đang chạy đều có smart_pse_enabled=false. Mặc định
    // false, không đè nếu đã set. Áp cho cả MESSENGER lẫn WHATSAPP. Giữ đồng bộ với mb-ads.
    if (
      metaPayload.optimization_goal === 'MESSAGING_PURCHASE_CONVERSION' &&
      metaPayload.promoted_object?.smart_pse_enabled === undefined
    ) {
      metaPayload.promoted_object = {
        ...(metaPayload.promoted_object || {}),
        smart_pse_enabled: false,
      };
    }

    // Loại audience_controls.min_age khi Advantage+ bật — Meta từ chối. Độ tuổi
    // tối thiểu đi qua targeting.age_min. Giữ đồng bộ với mb-ads.
    metaPayload.audience_controls = this.sanitizeAudienceControls(metaPayload);

    // PORT từ mb-ads meta.service.ts buildAdSetCreatePayload (fix a65bd18) — GIỮ PARITY.
    // BACKSTOP lịch chạy quá khứ: nháp clone từ mẫu (Test content tự động) có thể bê
    // start_time/end_time đã QUA từ lúc tạo mẫu → Meta từ chối cả cụm ad set ("Ngày kết
    // thúc… phải ở trong tương lai"). end_time/start_time có thể là epoch (string/number,
    // GIÂY) hoặc ISO string → parse an toàn cả hai.
    {
      const nowUnix = Math.floor(Date.now() / 1000);
      const toUnix = (v: any): number | null => {
        if (v === null || v === undefined || v === '') return null;
        if (typeof v === 'number' && Number.isFinite(v)) return Math.floor(v);
        const s = String(v).trim();
        if (/^\d+$/.test(s)) return parseInt(s, 10);
        const ms = Date.parse(s);
        return Number.isNaN(ms) ? null : Math.floor(ms / 1000);
      };
      const startUnix = toUnix(metaPayload.start_time);
      if (startUnix !== null && startUnix <= nowUnix) {
        // start_time quá khứ → bỏ trống để Meta hiểu "chạy ngay".
        delete metaPayload.start_time;
      }
      const endUnix = toUnix(metaPayload.end_time);
      if (endUnix !== null && endUnix <= nowUnix) {
        if (!metaPayload.lifetime_budget) {
          delete metaPayload.end_time;
          if ('has_end_date' in metaPayload) delete metaPayload.has_end_date;
        } else {
          // lifetime_budget BẮT BUỘC có end_time trong tương lai → không thể xoá (Meta từ
          // chối "end_time is required for lifetime budget"). Dời +30 ngày kể từ bây giờ,
          // khớp helper sanitizeAdSetScheduleFromTemplate.
          metaPayload.end_time = String(nowUnix + 30 * 24 * 60 * 60);
          metaPayload.has_end_date = true;
        }
      }
    }

    return (
      CleanObjectOrArray({
        ...metaPayload,
        targeting: this.cleanTargetingForMeta(payload?.targeting),
        campaign_id: campaignMetaId,
      }) || {}
    );
  }

  // Meta TỪ CHỐI audience_controls.min_age khi Advantage+ Audience bật ("thêm độ
  // tuổi tối thiểu cao hơn làm gợi ý"). Độ tuổi tối thiểu phải đi qua
  // targeting.age_min. Loại min_age khi advantage on; giữ control khác. Đồng bộ mb-ads.
  private sanitizeAudienceControls(payload: any) {
    const advOn =
      payload?.targeting?.targeting_automation?.advantage_audience === 1 ||
      payload?.targeting?.targeting_automation?.advantage_audience === true;
    const ac = payload?.audience_controls;
    if (!advOn || !ac || typeof ac !== 'object') return ac;
    const { min_age, ...restControls } = ac as Record<string, any>;
    void min_age;
    return Object.keys(restControls).length > 0 ? restControls : undefined;
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

  // ===== POST-ID SCALING (an toàn cross-account) — parity với mb-ads =====
  // Bài viết nhắn tin (MESSAGE_PAGE/WhatsApp/IG) KHÔNG dùng được cho ad-set website và
  // ngược lại → chỉ ghép post cùng LỚP CTA.
  private static readonly MESSAGING_CTAS = new Set([
    'MESSAGE_PAGE',
    'MESSENGER',
    'WHATSAPP_MESSAGE',
    'INSTAGRAM_MESSAGE',
    'CONTACT_US',
  ]);

  // CTA nhắn tin THUẦN: đích đến do ad set (destination_type + promoted_object.page_id) quyết định;
  // KHÔNG gửi kèm value.link website. (CONTACT_US KHÔNG thuộc nhóm này vì vẫn cần link.)
  private static readonly MESSAGING_CTA_NO_LINK = new Set([
    'MESSAGE_PAGE',
    'MESSENGER',
    'WHATSAPP_MESSAGE',
    'INSTAGRAM_MESSAGE',
  ]);

  private extractScaleMedia(creative: any): {
    videoId?: string;
    imageHash?: string;
    pageId?: string;
    ctaType?: string;
    isCustom: boolean;
  } {
    if (!creative || typeof creative !== 'object') return { isCustom: false };
    if (creative.object_story_id || creative.effective_object_story_id)
      return { isCustom: false };
    if (
      creative.useCatalog === true ||
      creative.productSetId ||
      creative.product_set_id
    )
      return { isCustom: false };
    const oss = creative.object_story_spec || {};
    if (
      oss.link_data?.child_attachments?.length ||
      String(creative.mediaType || '').toLowerCase() === 'carousel'
    )
      return { isCustom: false };
    const vd = oss.video_data || {};
    const ld = oss.link_data || {};
    const af = creative.asset_feed_spec || {};
    const videoId =
      vd.video_id ||
      creative.videoId ||
      creative.video_id ||
      af.videos?.[0]?.video_id ||
      undefined;
    const imageHash =
      ld.image_hash ||
      creative.imageHash ||
      creative.image_hash ||
      af.images?.[0]?.hash ||
      undefined;
    const pageId = oss.page_id || creative.pageId || undefined;
    const ctaType =
      vd.call_to_action?.type ||
      ld.call_to_action?.type ||
      af.call_to_action_types?.[0] ||
      creative.callToAction ||
      undefined;
    return {
      videoId,
      imageHash,
      pageId,
      ctaType,
      isCustom: !!(videoId || imageHash),
    };
  }

  /**
   * Nếu creative dùng media thô mà nội dung đó có BÀI VIẾT có sẵn cùng Trang + cùng lớp
   * CTA → chuyển sang tham chiếu bài viết (object_story_id), xoá object_story_spec/
   * asset_feed_spec. Post gắn theo Trang nên hợp lệ cross-account và mang media gốc còn
   * hạn (fix cả lỗi ảnh/video hết hạn). Không có post phù hợp thì giữ nguyên media thô.
   * Nuốt lỗi để không chặn auto-publish. (Tham số adAccountId giữ để log/def tương thích.)
   */
  private async applyScalePostId(
    creative: any,
    _adAccountId?: string,
  ): Promise<void> {
    try {
      if (!creative) return;
      const m = this.extractScaleMedia(creative);
      if (!m.isCustom || !m.pageId) return;

      // Video: khớp theo videoId. Ảnh (không có videoId): khớp theo imageHash. KHÔNG
      // khớp imageHash cho creative video — imageHash đó là THUMBNAIL, dễ đụng bài viết
      // khác cùng thumbnail → chọn nhầm nội dung.
      const orMatch = m.videoId
        ? [{ videoId: m.videoId }]
        : m.imageHash
          ? [{ imageHash: m.imageHash }]
          : [];
      if (!orMatch.length) return;

      const candidates = await this.prisma.creative.findMany({
        where: {
          AND: [
            { OR: orMatch },
            { effectObjectStoryId: { startsWith: `${m.pageId}_` } },
          ],
        },
        select: {
          effectObjectStoryId: true,
          objectStoryId: true,
          performanceStatus: true,
          roas: true,
          results: true,
          spend: true,
          remoteUpdatedAt: true,
          rawPayload: true,
        },
      });
      if (!candidates.length) return;

      const Pub = DraftAutomationMetaPublisherService;
      const wantMessaging = m.ctaType
        ? Pub.MESSAGING_CTAS.has(m.ctaType)
        : undefined;
      const ctaOf = (c: any): string | undefined => {
        const s = (c.rawPayload as any)?.object_story_spec || {};
        return (
          s.video_data?.call_to_action?.type ||
          s.link_data?.call_to_action?.type ||
          (c.rawPayload as any)?.asset_feed_spec?.call_to_action_types?.[0] ||
          undefined
        );
      };
      const ok = candidates.filter((c) => {
        const story = c.effectObjectStoryId || c.objectStoryId;
        if (!story) return false;
        // Không rõ lớp CTA đích → KHÔNG scale (tránh ghép nhầm bài viết nhắn tin vào
        // ad-set website và ngược lại → Meta từ chối / sai mục tiêu).
        if (wantMessaging === undefined) return false;
        const t = ctaOf(c);
        if (!t) return false;
        return Pub.MESSAGING_CTAS.has(t) === wantMessaging;
      });
      if (!ok.length) return;

      const scaleRank = (s?: string | null): number =>
        s === 'SCALE_P2' ? 3 : s === 'SCALE_P1' ? 2 : s && s !== 'OFF' ? 1 : 0;
      const rank = (c: any): number[] => [
        scaleRank(c.performanceStatus),
        c.roas || 0,
        c.results || 0,
        c.spend || 0,
        c.remoteUpdatedAt ? new Date(c.remoteUpdatedAt).getTime() : 0,
      ];
      ok.sort((a, b) => {
        const ra = rank(a);
        const rb = rank(b);
        for (let i = 0; i < ra.length; i++) {
          if (ra[i] !== rb[i]) return rb[i] - ra[i];
        }
        return 0;
      });
      const story = ok[0].effectObjectStoryId || ok[0].objectStoryId;
      if (!story) return;

      creative.object_story_id = story;
      creative.effective_object_story_id = story;
      creative.pageId = story.split('_')[0] || creative.pageId;
      delete creative.object_story_spec;
      delete creative.asset_feed_spec;
      delete creative.videoId;
      delete creative.video_id;
      delete creative.imageHash;
      delete creative.image_hash;
    } catch (e) {
      this.logger.warn(`applyScalePostId skipped: ${(e as Error)?.message}`);
    }
  }

  // Đích nhắn tin chuẩn cho từng kênh (link tài liệu + app_destination). Meta cần
  // value.app_destination để nhận diện quảng cáo click-to-message; thiếu nó thì
  // optimization MESSAGING_PURCHASE_CONVERSION bị từ chối lúc tạo Ad. Đồng bộ mb-ads.
  private static readonly MESSAGING_CTA_DESTINATION: Record<
    string,
    { link: string; app_destination: string }
  > = {
    MESSAGE_PAGE: {
      link: 'https://fb.com/messenger_doc/',
      app_destination: 'MESSENGER',
    },
    MESSENGER: {
      link: 'https://fb.com/messenger_doc/',
      app_destination: 'MESSENGER',
    },
    WHATSAPP_MESSAGE: {
      link: 'https://api.whatsapp.com/send',
      app_destination: 'WHATSAPP',
    },
    INSTAGRAM_MESSAGE: {
      link: 'https://www.instagram.com/',
      app_destination: 'INSTAGRAM_DIRECT',
    },
  };

  // Rào an toàn: nháp/mẫu cũ có thể lưu call_to_action nhắn tin dạng trơ {type} —
  // thiếu value.app_destination → Meta từ chối MPC. Bơm đích nhắn tin chuẩn nếu thiếu.
  // KHÔNG đụng CTA website. Đồng bộ với mb-ads meta.service.normalizeMessagingCtaForMeta.
  private normalizeMessagingCtaForMeta(creativeData: any) {
    const spec = creativeData?.object_story_spec;
    if (!spec) return;
    const fix = (cta: any) => {
      if (!cta || typeof cta !== 'object') return;
      const dest =
        DraftAutomationMetaPublisherService.MESSAGING_CTA_DESTINATION[cta.type];
      if (!dest) return;
      if (!cta.value?.app_destination) {
        cta.value = { ...dest, ...(cta.value || {}) };
        cta.value.app_destination = dest.app_destination;
        if (!cta.value.link) cta.value.link = dest.link;
      }
    };
    fix(spec.video_data?.call_to_action);
    fix(spec.link_data?.call_to_action);
    if (Array.isArray(spec.link_data?.child_attachments)) {
      for (const att of spec.link_data.child_attachments) {
        fix(att?.call_to_action);
      }
    }
  }

  private buildCreativeData(adPayload: any) {
    const sourceCreative = adPayload?.creative || {};

    // POST-ID: tham chiếu BÀI VIẾT có sẵn → chỉ gửi object_story_id, bỏ object_story_spec/
    // asset_feed_spec. Bài viết gắn theo Trang nên hợp lệ cả khi khác tài khoản (parity
    // với mb-ads buildNormalizedCreativePayload / meta.service publish).
    const storyId =
      sourceCreative.object_story_id || sourceCreative.effective_object_story_id;
    const oss = sourceCreative.object_story_spec || {};
    const hasCustomContent =
      Object.keys(oss).length > 0 || !!sourceCreative.asset_feed_spec;
    // "Ghim nội dung" (PINNED_POST, parity với mb-ads buildDraftAdCreative): dùng LẠI đúng
    // bài viết gốc để giữ engagement — ép nhánh POST_ID kể cả khi còn object_story_spec.
    //
    // 🔒 VỊ NGỮ GỐC — mb-ads COPY NGUYÊN VĂN biểu thức này vào meta.service.ts
    // #buildCreativeData (biến `isPostReferenceCreative`) để bỏ qua khối "đích đến cá
    // nhân hoá". Sửa vị ngữ ở đây BẮT BUỘC sửa cùng lúc bên kia, nếu không cùng một bản
    // nháp lại rẽ nhánh khác nhau tuỳ ai bấm publish.
    // Lý do phải chặn: creative tham chiếu bài viết chỉ được mang object_story_id — kèm
    // asset_feed_spec là Meta từ chối 1815809 ("dữ liệu nội dung bị lặp"). Repo này an
    // toàn sẵn nhờ return sớm; mb-ads trước đây KHÔNG có → khối personalized_destinations
    // vẫn gắn asset_feed_spec.{message_extensions, call_ads_configuration,
    // onsite_destinations} cạnh object_story_id.
    // ⚠️ Bất đối xứng 23% vs 336 (đo production 2026-08-06): 12.799/55.227 creative có
    // asset_feed_spec.message_extensions nhưng toggle "Tiện ích trình duyệt" của hệ thống
    // mới dùng ~336 lần ⇒ field đó phần lớn phản ánh kênh nhắn tin Trang đang có, KHÔNG
    // phải ý định người dùng. Luồng ĐỌC của mb-ads vẫn CỐ Ý suy `browser_addons.enabled`
    // từ field này (để UI soi đúng Meta) — chỉ luồng GHI bị bịt. Nháp do CHÍNH cron này
    // tạo là nhóm rủi ro nhất vì không bao giờ lưu khoá browser_addons.
    if (storyId && (sourceCreative.pinnedPost === true || !hasCustomContent)) {
      return (
        CleanObjectOrArray({
          name: `${adPayload.name} - Creative`,
          object_story_id: storyId,
          url_tags:
            sourceCreative.url_tags || sourceCreative.urlTags || undefined,
        }) || {}
      );
    }

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
      // Product set của Tiện ích sản phẩm — cũng chỉ là field nháp; giá trị thật đi lên
      // qua creative_sourcing_spec.associated_product_set_id do
      // applyProductExtensionsPreference đặt. An toàn khi strip ở đây vì hàm đó đọc từ
      // `adPayload.creative` (bản gốc), không đọc từ creativeData đã dọn.
      'productExtensionsSetId',
      'product_extensions_set_id',
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
    this.normalizeMessagingCtaForMeta(creativeData);
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

      // PARITY mb-ads (meta.service buildCreativeData): asset_feed_spec CHỈ được coi là
      // "mang creative" khi có trường media/nội dung thật. Nếu nó sinh ra chỉ để chở
      // promotional_metadata / onsite_destinations (không images/videos) thì KHÔNG được
      // xoá media trong object_story_spec — nếu không ad sẽ mất ảnh/video → Meta từ chối.
      const assetFeedCreativeKeys = [
        'ad_formats',
        'images',
        'videos',
        'bodies',
        'titles',
        'descriptions',
        'link_urls',
        'call_to_action_types',
        'optimization_type',
      ];
      const hasCreativeAssetFeedFields = assetFeedCreativeKeys.some((key) => {
        const value = creativeData.asset_feed_spec?.[key];
        return Array.isArray(value) ? value.length > 0 : value != null;
      });

      const isCarousel =
        sourceCreative?.mediaType?.toLowerCase() === 'carousel' ||
        creativeData?.mediaType?.toLowerCase() === 'carousel' ||
        (Array.isArray(
          creativeData.object_story_spec?.link_data?.child_attachments,
        ) &&
          creativeData.object_story_spec.link_data.child_attachments.length >
            0) ||
        creativeData.asset_feed_spec?.ad_formats?.some((format: string) =>
          format.includes('CAROUSEL'),
        ) ||
        (Array.isArray(sourceCreative?.carouselCards) &&
          sourceCreative.carouselCards.length > 0) ||
        (Array.isArray(creativeData.carouselCards) &&
          creativeData.carouselCards.length > 0);

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

      // Flexible nhưng thực chất chỉ 1 asset (không trường DOF nào length>1) → gỡ hẳn
      // asset_feed_spec, đăng như ad 1-asset thường (parity mb-ads — tránh Meta dựng ad
      // Flexible lệch cấu trúc so với bản đăng tay).
      if (isFlexibleFormat && creativeData.asset_feed_spec) {
        const dofFields = [
          'bodies',
          'titles',
          'descriptions',
          'images',
          'videos',
          'link_urls',
          'call_to_action_types',
        ];
        const hasMultiAssetDofField = dofFields.some((key) => {
          const value = creativeData.asset_feed_spec?.[key];
          return Array.isArray(value) && value.length > 1;
        });
        const hasOnlyOptimizationType = Object.keys(
          creativeData.asset_feed_spec,
        ).every((key) => key === 'optimization_type');
        if (hasOnlyOptimizationType || !hasMultiAssetDofField) {
          delete creativeData.asset_feed_spec;
        }
      }

      if (
        creativeData.object_story_spec &&
        creativeData.asset_feed_spec &&
        hasCreativeAssetFeedFields &&
        !isFlexibleFormat
      ) {
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
    // CỔNG "Tiện ích liên hệ nhanh" đặt Ở ĐÂY chứ không nằm trong
    // applyPersonalizedDestinations (2673): khối đó SYNC và PARITY-LOCKED nguyên văn với
    // mb-ads, còn đọc knob thì phải await. Đây lại là điểm gọi Meta tạo creative DUY NHẤT
    // của toàn mb-batch nên gỡ ở đây phủ 100% đường publish tự động — và quan trọng hơn:
    // payload sau khi gỡ GIỐNG HỆT payload mb-ads gửi, tức parity KẾT QUẢ vẫn nguyên vẹn
    // dù chỗ đặt gate hai bên khác nhau.
    // ⛔ Cờ tạm đặt Ở ĐÂY (nơi gọi), CỐ Ý không đặt bên trong hàm gate — parity với
    // mb-ads. Đặt trong hàm sẽ biến thân hàm thành code chết và làm đỏ test khoá hành vi,
    // dẫn người sửa sau tới chỗ "sửa cho test xanh" bằng cách gỡ cờ.
    if (!TAM_TAT_GO_QUICK_CONTACT) {
      await this.applyQuickContactCapabilityGate(creativeData);
    }

    try {
      return await adAccount.createAdCreative(['id'], creativeData);
    } catch (error) {
      // TỰ HỌC: mã 3 + payload vừa gửi CÓ message_extensions ⇒ Meta chưa cấp năng lực
      // cho ứng dụng. Ghi knob trước khi ném lỗi (nuốt lỗi ghi, không đổi lỗi user thấy).
      await this.learnQuickContactUnavailable(error, creativeData);

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

  /** Payload sắp gửi Meta có mang "Tiện ích liên hệ nhanh" hay không. */
  private hasQuickContactMessageExtensions(creativeData: any) {
    const assetFeed = creativeData?.asset_feed_spec;
    return !!(
      assetFeed &&
      typeof assetFeed === 'object' &&
      !Array.isArray(assetFeed) &&
      'message_extensions' in assetFeed
    );
  }

  /**
   * (A) GỠ KHI UNAVAILABLE — xoá `asset_feed_spec.message_extensions` TRƯỚC khi gọi Meta.
   *
   * VÌ SAO GỠ Ở BACKEND CHỨ KHÔNG CHỈ KHOÁ CÔNG TẮC Ở FE: production có 122 SystemAd ĐÃ
   * mang sẵn cờ bật (nhóm CUSTOM 57 nháp từ 31-07 publish 0 cái). Khoá công tắc chỉ chặn
   * người dùng BẬT MỚI; mọi lượt cron chạy lại nháp cũ vẫn gãy mã 3 như cũ. Gỡ ở đây làm
   * chúng đăng được, chỉ mất phần tiện ích — đó mới là mục đích chính.
   *
   * UNKNOWN/AVAILABLE → KHÔNG đụng gì (giữ nguyên hành vi cũ). Chỉ 'UNAVAILABLE' mới gỡ.
   * Đọc knob CHỈ KHI payload thật sự có field (đường nóng: đại đa số creative không có
   * → không tốn lượt đọc DB nào; reader còn cache 30s).
   * Lỗi đọc knob = fail-open (getString đã nuốt lỗi DB về default 'UNKNOWN') → hỏng DB
   * cấu hình không bao giờ tự ý cắt tính năng của người dùng.
   */
  private async applyQuickContactCapabilityGate(creativeData: any) {
    if (!this.hasQuickContactMessageExtensions(creativeData)) return;

    const capability = (
      await this.appConfig.getString(
        QUICK_CONTACT_CAPABILITY_KEY,
        QUICK_CONTACT_CAPABILITY_DEFAULT,
      )
    )
      .trim()
      .toUpperCase();
    if (capability !== 'UNAVAILABLE') return;

    const assetFeed = creativeData.asset_feed_spec;
    delete assetFeed.message_extensions;
    // asset_feed_spec rỗng thì XOÁ LUÔN: gửi `asset_feed_spec: {}` là gửi thừa một khoá
    // vô nghĩa, và với creative tham chiếu bài viết (object_story_id) Meta còn từ chối
    // hẳn (1815809). Chỉ xoá khi KHÔNG CÒN KHOÁ NÀO — các khoá anh em
    // (optimization_type/call_to_actions của đích nhắn tin combo, onsite_destinations,
    // call_ads_configuration…) phải sống nguyên.
    if (Object.keys(assetFeed).length === 0)
      delete creativeData.asset_feed_spec;

    this.logger.warn(
      `[quick-contact] ${QUICK_CONTACT_CAPABILITY_KEY}=UNAVAILABLE → đã gỡ asset_feed_spec.message_extensions khỏi payload trước khi gọi Meta (quảng cáo vẫn lên, chỉ mất "Tiện ích liên hệ nhanh").`,
    );
  }

  /**
   * (B) TỰ HỌC — publish thất bại mã 3 TRONG KHI payload có message_extensions ⇒ đặt knob
   * 'UNAVAILABLE'. Từ lượt sau (cả mb-ads lẫn mb-batch) field này bị gỡ tự động, không
   * tốn thêm lời gọi Meta nào để dò.
   *
   * ĐIỀU KIỆN "CÓ message_extensions" LÀ BẮT BUỘC — chống KHOÁ OAN: mã 3 là mã CHUNG
   * ("Application does not have the capability to make this API call"), Meta còn trả nó
   * cho những năng lực khác chẳng liên quan gì tới tiện ích liên hệ nhanh. Thiếu vế này
   * thì một lỗi mã 3 bất kỳ sẽ khoá tính năng của cả hệ thống.
   *
   * Kiểm trên creativeData SAU khi gate (A) đã chạy — tức đúng payload ĐÃ GỬI. Nếu knob
   * đã UNAVAILABLE thì field đã bị gỡ ⇒ không tự học lại (ghi lại cùng giá trị cũng vô hại
   * nhưng thừa một lượt ghi DB).
   *
   * NUỐT LỖI TUYỆT ĐỐI: đây là việc phụ. Ghi knob hỏng (mất DB, race…) KHÔNG được đổi lỗi
   * người dùng nhìn thấy, càng không được nuốt mất lỗi gốc — caller vẫn ném `error` như cũ.
   */
  private async learnQuickContactUnavailable(error: any, creativeData: any) {
    try {
      if (!this.hasQuickContactMessageExtensions(creativeData)) return;
      if (Number(parseMetaError(error)?.code) !== 3) return;

      // 🔴 CHỈ tự học khi knob còn 'UNKNOWN' — parity với mb-ads
      // (`rememberQuickContactUnavailable`). Hai giá trị kia đều là QUYẾT ĐỊNH CỦA NGƯỜI
      // (admin đặt tay, hoặc "Dò lại năng lực"), máy không được đè lên:
      //   • 'UNAVAILABLE' → đã khoá rồi, ghi lại là thừa một lượt ghi DB.
      //   • 'AVAILABLE'   → admin CỐ Ý muốn vẫn gửi message_extensions để nhìn thấy lỗi
      //     thật thay vì để hệ thống âm thầm gỡ field. Thiếu chốt này thì cron chỉ cần
      //     gãy một lần là đè knob về UNAVAILABLE — admin mở bao nhiêu lần cũng vô ích,
      //     mà lại rất khó lần ra vì thủ phạm nằm ở repo KHÁC với nơi họ vừa bấm mở.
      const capability = (
        await this.appConfig.getString(
          QUICK_CONTACT_CAPABILITY_KEY,
          QUICK_CONTACT_CAPABILITY_DEFAULT,
        )
      )
        .trim()
        .toUpperCase();
      if (capability !== 'UNKNOWN') return;

      await this.appConfig.setString(
        QUICK_CONTACT_CAPABILITY_KEY,
        'UNAVAILABLE',
        QUICK_CONTACT_CAPABILITY_LABEL,
      );
      this.logger.warn(
        `[quick-contact] Meta trả mã 3 trong khi payload có asset_feed_spec.message_extensions → đã đặt ${QUICK_CONTACT_CAPABILITY_KEY}='UNAVAILABLE'. Các quảng cáo sau sẽ tự gỡ field này; muốn mở lại thì admin bấm "Dò lại năng lực" bên mb-ads.`,
      );
    } catch (learnErr) {
      this.logger.warn(
        `[quick-contact] Không ghi được ${QUICK_CONTACT_CAPABILITY_KEY}: ${
          (learnErr as any)?.message || learnErr
        }`,
      );
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
      minimum_age,
      min_age,
      ...rest
    } = targeting || {};

    void use_age_min_control;
    void minimum_age;
    void min_age;

    // Meta: với Advantage+ Audience (targeting_automation.advantage_audience = 1)
    // age_min/age_max KHÔNG phải "độ tuổi user chọn" mà là AUDIENCE CONTROL (giới
    // hạn CỨNG). Meta chỉ cho nâng min-age control tối đa 25 và KHÔNG có max-age
    // control (luôn = 65). Độ tuổi user chọn (vd 25-54) phải gửi qua `age_range` =
    // GỢI Ý để Advantage+ dùng làm hạt giống audience. Nếu chỉ gửi age_min/age_max
    // (ép về [25,65]) mà BỎ age_range → Meta suy age_range = [age_min, 65] → ad
    // nhắm/hiển thị 25-65 (đúng lỗi báo: set 25-54 lên thành 25-65). Ads Manager
    // luôn gửi SONG SONG control [≤25, 65] + age_range [25,54]. Bằng chứng prod:
    // 288 ad set A++ có age_range=[25,54] kèm age_max=65 (control mở, gợi ý hẹp).
    // Docs Advantage+ Audience v25: {"age_min":18,"age_max":65,"age_range":[25,35]}.
    const advantageAudienceOn =
      (rest as any)?.targeting_automation?.advantage_audience === 1 ||
      (rest as any)?.targeting_automation?.advantage_audience === true;
    if (advantageAudienceOn) {
      const origMin =
        typeof (rest as any).age_min === 'number'
          ? (rest as any).age_min
          : undefined;
      const origMax =
        typeof (rest as any).age_max === 'number'
          ? (rest as any).age_max
          : undefined;

      // User thu hẹp so với biên control mà Meta cho phép ([≤25, 65])
      // → chuyển nguyên khoảng đã chọn thành GỢI Ý age_range để không mất ý định.
      const userNarrowed =
        (origMax !== undefined && origMax < 65) ||
        (origMin !== undefined && origMin > 25);
      if (
        userNarrowed &&
        origMin !== undefined &&
        origMax !== undefined &&
        !Array.isArray((rest as any).age_range)
      ) {
        (rest as any).age_range = [origMin, origMax];
      }

      // Kẹp CONTROL về đúng biên Meta chấp nhận (tránh lỗi "cannot set the audience
      // control for maximum age lower than 65" và "age_min tối đa 25").
      if (origMax !== undefined && origMax < 65) (rest as any).age_max = 65;
      if (origMin !== undefined && origMin > 25) (rest as any).age_min = 25;
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
   *
   * Hàm này cũng là nơi ghi toggle "Duyệt sản phẩm"
   * (`personalized_destinations.product_browsing`) — vì đây là NƠI DUY NHẤT được phép
   * chạm degrees_of_freedom_spec, xem khối chú thích ngay tại chỗ ghi bên dưới.
   */
  /**
   * Toggle "Duyệt sản phẩm" (`product_browsing`) — mục thứ 3 của hộp "Điểm đến cá nhân
   * hoá" bên Meta Business. Bản song sinh của `MetaService.applyProductBrowsingPreference`
   * ở mb-ads; sửa một bên nhớ sửa bên kia.
   *
   * TÁCH RIÊNG khỏi `applyProductExtensionsPreference` CÓ CHỦ Ý, đừng gộp lại cho gọn:
   * hàm kia có HAI cái chặn (`isCatalogProductCreative` bên trong, và guard
   * `!object_story_id` ở nơi gọi) — cả hai đều là chặn của "Hiển thị sản phẩm". Gộp chung
   * thì toggle này bị NUỐT IM LẶNG với catalog ad và với quảng cáo dùng bài viết có sẵn:
   * người dùng bật được, nháp lưu đúng, publish báo thành công, mà Meta không nhận gì.
   *
   * Chỉ ghi khi người dùng BẬT. Tắt/chưa đặt ⇒ KHÔNG đụng gì, để nhánh OPT_OUT mặc định
   * của `applyProductExtensionsPreference` lo (giữ nguyên hành vi cũ cho nháp chưa chạm).
   *
   * Đo thật 07-08-2026 (act_1262628191933179): `product_browsing` OPT_IN gửi MỘT MÌNH
   * được Meta nhận và echo đúng, KHÔNG cần nguồn sản phẩm; bật cùng `product_extensions`
   * cũng được ⇒ hai tính năng độc lập.
   */
  private applyProductBrowsingPreference(
    creativeData: any,
    sourceCreative: any,
  ) {
    const enabled =
      sourceCreative?.personalized_destinations?.product_browsing?.enabled ===
      true;
    if (!enabled) return;

    // 🔴 ĐÚNG FIELD: `destination_spec.native_commerce_experience.product_browsing` —
    // parity với mb-ads `applyProductBrowsingPreference`. Đo 07-08-2026 bằng cách để
    // giao diện Meta tự ghi rồi đọc lại (creative `2568103560315023`).
    // ⛔ TỪNG GHI NHẦM vào `degrees_of_freedom_spec.creative_features_spec.product_browsing`
    // — Meta NHẬN và echo lại nhưng Ads Manager vẫn hiện Off ⇒ không phải công tắc thật.
    // ⚠️ Sẽ lỗi mã 3 tới khi Meta cấp năng lực cho ứng dụng (cùng bức tường với
    // asset_feed_spec.message_extensions).
    const dest = creativeData.destination_spec || {};
    creativeData.destination_spec = {
      ...dest,
      native_commerce_experience: {
        ...(dest.native_commerce_experience || {}),
        product_browsing: {
          enroll_status: 'OPT_IN',
          action_metadata: { type: 'MANUAL' },
        },
      },
    };
  }

  private applyProductExtensionsPreference(
    creativeData: any,
    sourceCreative: any,
  ) {
    if (this.isCatalogProductCreative(sourceCreative)) return;

    const productSetId = this.resolveProductExtensionsSetId(sourceCreative);
    // Chỉ bật khi có toggle VÀ có product set: bật mà không có nguồn sản phẩm thì Meta
    // tự chọn, kết quả không tất định. Parity mb-ads.
    const showProducts =
      sourceCreative?.show_products === true && !!productSetId;

    // Toggle "Duyệt sản phẩm" — nằm trong `personalized_destinations`, hình dạng bắt
    // chước Y HỆT `browser_addons: { enabled: boolean }` (chưa từng lưu = undefined,
    // đã lưu-mà-tắt = false). Ở luồng GHI này hai trạng thái đó cho ra CÙNG kết quả (giữ
    // nguyên hành vi cũ), CHỈ `=== true` mới đổi payload — nên đọc bằng so sánh chặt,
    // đừng đổi thành truthy. Đọc từ `sourceCreative` (adPayload.creative bản gốc) vì
    // buildCreativeData đã bóc `personalized_destinations` khỏi creativeData. Parity mb-ads.
    const productBrowsingEnabled =
      sourceCreative?.personalized_destinations?.product_browsing?.enabled ===
      true;

    const dof = creativeData.degrees_of_freedom_spec || {};
    const features = dof.creative_features_spec || {};

    // ĐO THẬT 07-08-2026 (POST creative lên TKQC test rồi GET lại rồi xoá): key phải là
    // `product_extensions` CHỮ THƯỜNG. Bản cũ ghi `PRODUCT_BROWSING` CHỮ HOA — Meta VẪN
    // NHẬN không báo lỗi nhưng chuẩn hoá thành `product_browsing`, tức bật NHẦM tính
    // năng (duyệt sản phẩm trong Shop) trong khi `product_extensions` vẫn OPT_OUT.
    // Ba feature phụ thuộc catalog vẫn OPT_OUT khi tắt để tránh lỗi "tạo nội dung động
    // mà không có ID nhóm sản phẩm" — chưa rõ key nào chặn lỗi đó nên giữ đủ cả ba.
    // (NGOẠI LỆ DUY NHẤT: product_browsing khi người dùng CHỦ ĐỘNG bật toggle "Duyệt sản
    // phẩm" — xem hai khối chú thích ngay dưới; đo trên TKQC test thì OPT_IN một mình
    // không sinh lỗi, nhưng nếu lỗi đó quay lại thì đây là nghi phạm số một.)
    // Phải khớp mb-ads (MetaService.applyProductExtensionsPreference).
    features.product_extensions = {
      enroll_status: showProducts ? 'OPT_IN' : 'OPT_OUT',
    };
    if (!showProducts) {
      // 🔴 product_browsing ở ĐÂY KHÔNG phải ý định người dùng — nó là mặc định TẮT đi
      // kèm hai feature phụ thuộc catalog để chặn lỗi "tạo nội dung động mà không có ID
      // nhóm sản phẩm" (chưa rõ key nào chặn nên giữ đủ cả ba, đừng tỉa). Chỉ ghi khi
      // toggle "Duyệt sản phẩm" KHÔNG bật; toggle bật thì nhánh OPT_IN bên dưới lo.
      // Bỏ điều kiện này = mặc định cũ đè chết toggle mới.
      if (!productBrowsingEnabled) {
        features.product_browsing = { enroll_status: 'OPT_OUT' };
      }
      features.standard_enhancements_catalog = { enroll_status: 'OPT_OUT' };
      features.product_metadata_automation = { enroll_status: 'OPT_OUT' };
    }

    // ── Toggle "Duyệt sản phẩm" BẬT → OPT_IN, BẤT KỂ showProducts ────────────────────
    // Ý nghĩa (theo mô tả của Meta trên giao diện): người dùng bấm nút kêu gọi hành động
    // thì sang trải nghiệm DUYỆT SẢN PHẨM trên Facebook/Instagram thay vì mở website.
    // KHÁC HẲN "Hiển thị sản phẩm" (product_extensions — danh sách sản phẩm hiện DƯỚI
    // quảng cáo) ở ngay phía trên.
    // ĐO THẬT 07-08-2026 (POST → GET → DELETE trên act_1262628191933179):
    //   • product_browsing:{enroll_status:'OPT_IN'} gửi MỘT MÌNH → Meta NHẬN, GET lại
    //     echo đúng 'OPT_IN'. KHÔNG cần nguồn sản phẩm (khác hẳn product_extensions,
    //     thứ bắt buộc phải có product set mới tất định) ⇒ không gate theo showProducts.
    //   • product_browsing OPT_IN và product_extensions OPT_IN BẬT CÙNG LÚC ĐƯỢC, Meta
    //     echo cả hai ⇒ hai tính năng ĐỘC LẬP, không loại trừ nhau.
    //   • Meta còn TỰ THÊM creative_sourcing_spec.destination_screenshot_spec và echo về
    //     ~82 feature (đa số OPT_OUT) ⇒ TUYỆT ĐỐI không diff nguyên khối
    //     degrees_of_freedom_spec giữa nháp và live.
    // Production hiện có 5.650/56.283 creative live mang product_browsing OPT_IN (10%).
    // Đặt SAU khối trên để bảng điều kiện đọc thẳng từ trên xuống; hai nhánh đã loại trừ
    // nhau bằng `!productBrowsingEnabled` nên không có chuyện ghi đè ngầm.
    // Parity mb-ads (MetaService.applyProductExtensionsPreference) — sửa bên này BẮT BUỘC
    // soát bên kia, nếu không cùng một bản nháp cho payload khác nhau tuỳ ai bấm publish.
    if (productBrowsingEnabled) {
      features.product_browsing = { enroll_status: 'OPT_IN' };
    }

    dof.creative_features_spec = features;
    creativeData.degrees_of_freedom_spec = dof;

    if (showProducts) {
      // SPREAD-MERGE, KHÔNG gán đè: creative_sourcing_spec còn chứa site_links_spec /
      // destination_screenshot_spec… của quảng cáo đang chạy (Meta còn tự thêm
      // destination_screenshot_spec). Gán đè = xoá tiện ích đang chạy → lệch
      // publishedSnapshot → tạo lại creative → reset learning + mất social proof.
      creativeData.creative_sourcing_spec = {
        ...(creativeData.creative_sourcing_spec ||
          sourceCreative?.creative_sourcing_spec ||
          {}),
        associated_product_set_id: String(productSetId),
      };
    }
  }

  /**
   * Product set cho Tiện ích sản phẩm. KHÔNG đọc productSetId/product_set_id — hai tên
   * đó thuộc catalog ad (DPA) và sẽ khiến isCatalogProductCreative trả true. Parity mb-ads.
   */
  private resolveProductExtensionsSetId(
    sourceCreative: any,
  ): string | undefined {
    const raw =
      sourceCreative?.productExtensionsSetId ||
      sourceCreative?.product_extensions_set_id ||
      sourceCreative?.creative_sourcing_spec?.associated_product_set_id;
    const value = raw === undefined || raw === null ? '' : String(raw).trim();
    return value === '' ? undefined : value;
  }

  // Đích nhắn tin "combo" (nhiều kênh trong 1 nhóm) → tập app_destination cần khai
  // trong creative. Hiện chỉ 1 combo: Messenger + Instagram. Parity mb-ads.
  private static readonly MULTI_MESSAGING_DESTINATION_CHANNELS: Record<
    string,
    string[]
  > = {
    MESSAGING_INSTAGRAM_DIRECT_MESSENGER: ['MESSENGER', 'INSTAGRAM_DIRECT'],
  };

  // app_destination → {cta type, link tài liệu} để dựng call_to_actions creative
  // nhiều đích. Khoá theo app_destination (khác MESSAGING_CTA_DESTINATION keyed cta.type).
  private static readonly MESSAGING_DESTINATION_CTA: Record<
    string,
    { type: string; link: string }
  > = {
    MESSENGER: { type: 'MESSAGE_PAGE', link: 'https://fb.com/messenger_doc/' },
    INSTAGRAM_DIRECT: {
      type: 'INSTAGRAM_MESSAGE',
      link: 'https://www.instagram.com',
    },
    WHATSAPP: {
      type: 'WHATSAPP_MESSAGE',
      link: 'https://api.whatsapp.com/send',
    },
  };

  /**
   * Quảng cáo click-to-message NHIỀU ĐÍCH (destination_type combo, vd
   * MESSAGING_INSTAGRAM_DIRECT_MESSENGER): Meta yêu cầu creative khai
   * `asset_feed_spec.optimization_type = 'DOF_MESSAGING_DESTINATION'` +
   * `call_to_actions` cho TỪNG kênh. THIẾU → Meta từ chối tạo Ad với subcode 2446493.
   * KHÔNG kèm standard_enhancements (đã ngừng — subcode 3858504). Giữ object_story_spec làm CTA chính
   * (Meta cho gửi kèm khi asset_feed_spec chỉ có optimization_type + call_to_actions).
   * Chỉ gọi ở bước TẠO creative (không đưa vào diff/snapshot). Parity mb-ads
   * (MetaService.applyMultiDestinationMessaging).
   */
  private applyMultiDestinationMessaging(
    creativeData: any,
    destinationType?: string,
  ) {
    const channels = destinationType
      ? DraftAutomationMetaPublisherService
          .MULTI_MESSAGING_DESTINATION_CHANNELS[destinationType]
      : undefined;
    if (!channels || channels.length < 2) return;

    if (
      creativeData.object_story_id ||
      creativeData.object_story_spec?.object_story_id ||
      creativeData.asset_feed_spec?.onsite_destinations
    ) {
      return;
    }

    const callToActions: any[] = [];
    for (const dest of channels) {
      const cfg =
        DraftAutomationMetaPublisherService.MESSAGING_DESTINATION_CTA[dest];
      if (cfg) {
        callToActions.push({
          type: cfg.type,
          value: { app_destination: dest, link: cfg.link },
        });
      }
    }
    if (callToActions.length < 2) return;

    const assetFeed = creativeData.asset_feed_spec || {};
    if (
      assetFeed.optimization_type &&
      assetFeed.optimization_type !== 'DOF_MESSAGING_DESTINATION'
    ) {
      this.logger.warn(
        `[multi-destination] asset_feed_spec.optimization_type='${assetFeed.optimization_type}' đã đặt — bỏ qua DOF_MESSAGING_DESTINATION cho ${destinationType}.`,
      );
      return;
    }
    assetFeed.optimization_type = 'DOF_MESSAGING_DESTINATION';
    assetFeed.call_to_actions = callToActions;
    creativeData.asset_feed_spec = assetFeed;

    // KHÔNG thêm standard_enhancements: Meta đã NGỪNG field này (subcode 3858504),
    // phải chọn từng tính năng — đã do applyProductExtensionsPreference đặt. Parity mb-ads.
  }

  /**
   * Gắn `object_story_spec.instagram_user_id` cho creative CATALOG.
   * BẢN SAO 1:1 của mb-ads MetaService.attachInstagramActorForCatalogCreative — sửa một
   * bên PHẢI sửa bên kia.
   *
   * buildCreativeData cố ý xoá field Instagram (đúng cho QC thường, Meta tự suy từ Trang),
   * nhưng catalog ad thì thiếu là Meta TỪ CHỐI Ở BƯỚC TẠO AD — tức lỗi rơi vào lúc muộn
   * nhất, sau khi đã tạo campaign + ad set + creative. Lỗi #100 subcode 1772103.
   * Đo thật 07-08-2026: cùng creative catalog, không có field → tạo ad LỖI; có → OK.
   * Nguồn id: Fanpage.rawPayload.instagram_business_account.id (entity-sync đã fetch sẵn).
   */
  private async attachInstagramActorForCatalogCreative(creativeData: any) {
    if (!creativeData?.product_set_id) return;
    const oss = creativeData.object_story_spec;
    if (!oss || oss.instagram_user_id) return;
    const pageId = oss.page_id;
    if (!pageId) return;

    try {
      const fanpage = await this.prisma.fanpage.findUnique({
        where: { id: String(pageId) },
        select: { rawPayload: true },
      });
      const igId = (fanpage?.rawPayload as any)?.instagram_business_account?.id;
      if (igId) {
        oss.instagram_user_id = String(igId);
      } else {
        this.logger.warn(
          `[catalog-creative] Trang ${pageId} chưa có instagram_business_account trong DB — quảng cáo danh mục có thể bị Meta từ chối (#100/1772103).`,
        );
      }
    } catch (e) {
      // Tra cứu hỏng KHÔNG được làm hỏng publish — để Meta phán quyết.
      this.logger.error('[catalog-creative] tra Instagram của Trang lỗi', e as any);
    }
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

    // Map catalogFormat (UI: SINGLE/CAROUSEL/COLLECTION) -> Meta format_option.
    // Thiếu field này, catalog ad mặc định về Carousel (parity với mb-ads).
    const catalogFormat =
      sourceCreative?.catalogFormat || sourceCreative?.catalog_format;
    const formatOption =
      catalogFormat === 'COLLECTION'
        ? 'collection_video'
        : catalogFormat === 'CAROUSEL'
          ? 'carousel_images_multi_items'
          : 'single_image';

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
        format_option: formatOption,
        multi_share_end_card:
          ['CAROUSEL', 'COLLECTION'].includes(catalogFormat) || undefined,
        call_to_action: {
          type: callToActionType,
          value:
            link &&
            !DraftAutomationMetaPublisherService.MESSAGING_CTA_NO_LINK.has(
              callToActionType,
            )
              ? { link }
              : undefined,
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
      // Draft cũ nhét cả camelCase lẫn snake_case vào cùng object video_data.
      // Coalesce về snake_case canonical trước khi xoá để không mất media.
      // Đồng bộ mb-ads (meta.service.ts normalizeCreativeMediaForMeta).
      if (!videoData.video_id && videoData.videoId)
        videoData.video_id = videoData.videoId;
      if (!videoData.image_hash && videoData.imageHash)
        videoData.image_hash = videoData.imageHash;

      // Xoá field UI-only / camelCase / non-Meta lọt vào video_data.
      // LƯU Ý: image_url HỢP LỆ trong video_data (custom thumbnail URL) → GIỮ.
      for (const key of [
        'videoId',
        'imageHash',
        'thumbnail',
        'previewUrl',
        'preview_url',
        'source',
        'video_source',
        'video_thumbnails',
        'list_thumbnails',
        'selected_thumbnail_id',
      ]) {
        delete videoData[key];
      }

      if (videoData.image_id) {
        delete videoData.image_hash;
        delete videoData.image_id;
      }
    }

    if (linkData) {
      // Coalesce camelCase → snake_case canonical.
      if (!linkData.image_hash && linkData.imageHash)
        linkData.image_hash = linkData.imageHash;

      // Xoá field UI-only / camelCase / non-Meta lọt vào link_data.
      // LƯU Ý: image_url KHÔNG hợp lệ trong link_data (khác video_data) → XOÁ.
      // GIỮ: child_attachments, image_hash, link, name, description, caption,
      //      message, call_to_action, picture, page_welcome_message.
      for (const key of [
        'imageHash',
        'thumbnail',
        'thumbnail_url',
        'previewUrl',
        'preview_url',
        'image_url',
        'videoId',
        'mediaType',
        'source',
        'video_source',
        'video_thumbnails',
        'list_thumbnails',
        'selected_thumbnail_id',
      ]) {
        delete linkData[key];
      }
    }

    const childAttachments = linkData?.child_attachments;
    if (Array.isArray(childAttachments)) {
      for (const attachment of childAttachments) {
        // Coalesce camelCase → snake_case canonical trước khi xoá.
        if (!attachment.video_id && attachment.videoId)
          attachment.video_id = attachment.videoId;
        if (!attachment.image_hash && attachment.imageHash)
          attachment.image_hash = attachment.imageHash;

        // Xoá field UI-only / camelCase / non-Meta lọt vào child_attachments.
        for (const key of [
          'imageHash',
          'videoId',
          'thumbnail',
          'previewUrl',
          'preview_url',
          'mediaType',
          'source',
          'video_source',
          'video_thumbnails',
          'list_thumbnails',
          'selected_thumbnail_id',
        ]) {
          delete attachment[key];
        }

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

  // PARITY-LOCKED với mb-ads: src/modules/meta/meta.service.ts, khối xử lý
  // `personalized_destinations` bên trong buildCreativeData (3 nhánh Shop/Browser
  // add-ons/Tối ưu website, hiện ~dòng 3637-3807) + helper resolveBrowserAddOnPhoneNumber.
  // (Bên mb-ads còn MỘT phần KHÔNG có ở repo này và KHÔNG cần parity: phép so sánh
  // không phân biệt hoa/thường cho message_extensions[].type trong isSubset ~3045 —
  // repo này không có luồng diff/snapshot creative, luôn TẠO MỚI.)
  // Đây là 2 ĐƯỜNG PUBLISH khác nhau cho CÙNG 1 bản nháp (đăng tay qua mb-ads vs cron
  // draft-automation qua mb-batch) — lệch nhánh nào ở đây nghĩa là cùng cấu hình
  // personalized_destinations cho ra kết quả KHÁC NHAU trên Meta tuỳ ai bấm publish.
  // Sửa ở repo này BẮT BUỘC soát lại bên mb-ads (và ngược lại).
  private applyPersonalizedDestinations(creativeData: any, config: any) {
    if (!config) return;

    // ⛔ ĐÃ THỬ VÀ BÁC BỎ 07-08-2026 — ĐỪNG LÀM LẠI: từng cho "Duyệt sản phẩm" kéo theo
    // "Tối ưu đích đến website". SAI — ảnh Ads Manager cho thấy Product browsing = On
    // trong khi Optimize website destination = Off ⇒ hai thứ ĐỘC LẬP.
    const optimizeWebsiteEnabled = !!config.optimize_website;
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

    if (shopConfig.enabled) {
      if (storefrontShopId) {
        creativeData.asset_feed_spec = creativeData.asset_feed_spec || {};
        creativeData.asset_feed_spec.onsite_destinations = [
          { storefront_shop_id: storefrontShopId, auto_optimization: true },
        ];
      } else {
        this.logger.warn(
          '[personalized-destinations] "Shop" đang bật nhưng thiếu storefront_shop_id — bỏ qua onsite_destinations.',
        );
      }
    } else if (creativeData.asset_feed_spec) {
      delete creativeData.asset_feed_spec.onsite_destinations;
    }

    // Browser add-ons: CALL → asset_feed_spec.call_ads_configuration (yêu cầu số điện
    // thoại hợp lệ, có guard riêng); các loại nhắn tin (WHATSAPP/MESSENGER/…) →
    // asset_feed_spec.message_extensions. Mỗi nhánh phải xoá key của nhánh kia — thiếu
    // nhánh này (bug gốc) làm cấu hình Browser add-ons của người dùng ÂM THẦM KHÔNG lên
    // Meta khi publish qua cron tự động.
    //
    // ⚠️ SỬA LẠI CHÚ THÍCH CŨ: bản trước khẳng định hai key "LOẠI TRỪ NHAU trên Meta"
    // như một quy tắc của Meta — đó là SUY ĐOÁN. Số đo production 2026-08-06 xác nhận
    // đúng, nhưng là quan sát chứ không phải luật: 12.799 creative có message_extensions,
    // 336 có call_ads_configuration, ĐÚNG 0 creative có CẢ HAI.
    //
    // ✅ ĐÃ ĐÓNG câu hỏi mở "xoá message_extensions có làm mất đích nhắn tin của quảng
    // cáo click-to-WhatsApp/Messenger không?" → KHÔNG. Join Ad→AdSet→Creative toàn
    // production, đếm theo lượt ad: 15.441 quảng cáo CTWA/CTM (destinationType MESSENGER
    // 7.617 / WHATSAPP 4.354 / MESSAGING_INSTAGRAM_DIRECT_MESSENGER 3.243 /
    // MESSAGING_MESSENGER_WHATSAPP 116 / INSTAGRAM_DIRECT 86 /
    // MESSAGING_INSTAGRAM_DIRECT_WHATSAPP 17 / MESSAGING_INSTAGRAM_DIRECT_MESSENGER_
    // WHATSAPP 8) có ĐÚNG 0 quảng cáo mang message_extensions; ngược lại toàn bộ 15.595
    // lượt ad CÓ message_extensions đều nằm ngoài nhóm nhắn tin (UNDEFINED 15.151 /
    // destinationType rỗng 366 / WEBSITE 66 / ON_POST 12). Đích nhắn tin CTWA mã hoá
    // bằng destination_type của NHÓM + call_to_action.value.app_destination
    // (normalizeMessagingCtaForMeta), và với combo nhiều kênh là
    // asset_feed_spec.optimization_type='DOF_MESSAGING_DESTINATION' + call_to_actions —
    // KHÔNG hàm nào trong hai đường đó chạm message_extensions. Vì vậy `delete` ở nhánh
    // TẮT là ĐÚNG và không thể cắt mất đích nhắn tin.
    if (config.browser_addons?.enabled) {
      const browserAddOn = config.browser_addons;
      const validMessageExtensionTypes = [
        'WHATSAPP',
        'MESSENGER',
        'MESSENGER_FACEBOOK_PAGE',
        'INSTAGRAM_MESSAGE',
        'WHATSAPP_FACEBOOK_PAGE',
        'WHATSAPP_PROMO',
        'NONE',
      ];
      const normalizedType = `${browserAddOn.type || 'NONE'}`.toUpperCase();

      if (normalizedType === 'CALL') {
        const phoneNumber = this.resolveBrowserAddOnPhoneNumber(browserAddOn);

        if (phoneNumber) {
          creativeData.asset_feed_spec = creativeData.asset_feed_spec || {};
          creativeData.asset_feed_spec.call_ads_configuration = {
            call_destination_type: 'WEBSITE_AND_CALL',
            phone_number: phoneNumber,
          };
          delete creativeData.asset_feed_spec.message_extensions;
        } else {
          this.logger.warn(
            '[personalized-destinations] Browser add-on loại "CALL" đang bật nhưng thiếu số điện thoại — bỏ qua call_ads_configuration.',
          );
          if (creativeData.asset_feed_spec) {
            delete creativeData.asset_feed_spec.call_ads_configuration;
            // Người dùng đã CHỌN loại CALL → mọi loại NHẮN TIN cũ phải biến mất kể cả
            // khi nhánh CALL bị bỏ qua vì thiếu số điện thoại. Thiếu dòng này thì
            // message_extensions của lần lưu trước sống sót → creative lên Meta mang
            // add-on loại KHÁC thứ người dùng thấy trên UI (và với creative POST_ID còn
            // kéo lỗi 1815809 vì asset_feed_spec đi kèm object_story_id).
            delete creativeData.asset_feed_spec.message_extensions;
          }
        }
      } else if (
        validMessageExtensionTypes.includes(normalizedType) &&
        normalizedType !== 'NONE'
      ) {
        creativeData.asset_feed_spec = creativeData.asset_feed_spec || {};
        creativeData.asset_feed_spec.message_extensions = [
          { type: normalizedType },
        ];
        delete creativeData.asset_feed_spec.call_ads_configuration;
      } else {
        this.logger.warn(
          `[personalized-destinations] Browser add-on loại "${browserAddOn.type}" không được path Meta API này hỗ trợ — bỏ qua browser add-ons.`,
        );
        if (creativeData.asset_feed_spec) {
          delete creativeData.asset_feed_spec.call_ads_configuration;
          delete creativeData.asset_feed_spec.message_extensions;
        }
      }
    } else if (creativeData.asset_feed_spec) {
      delete creativeData.asset_feed_spec.call_ads_configuration;
      delete creativeData.asset_feed_spec.message_extensions;
    }

    // ── "Tối ưu đích đến website" → destination_spec.website.optimization ─────────
    // PARITY mb-ads meta.service.ts (khối cùng tên trong buildCreativeData) — đọc chú
    // thích đầy đủ ở đó. Tóm tắt bằng chứng: thí nghiệm có kiểm soát trên TKQC
    // 281916477943407, diff 3 đời creative của ad 120255517228590196 (828765756870210
    // khoá VẮNG MẶT → 1349235943997738 'OPT_IN' → 1566203988472853 'OPT_OUT') ⇒ Meta ghi
    // hẳn OPT_OUT, không xoá khoá.
    //
    // 🔴 SỬA LỖI: nhánh TẮT trước đây xoá CẢ object destination_spec. Đo 24.040 creative
    // production có destination_spec: 3 khoá con ĐỘC LẬP — native_commerce_experience
    // 15.764 / website 15.155 / destination_type 569 (8.484 có native_commerce_experience
    // mà KHÔNG có website; 497 có destination_type mà không có website).
    // native_commerce_experience.shop = đích Shops ta KHÔNG quản, destination_type
    // (SHOPS_MESSAGING / WEBSITE_AND_SHOP / SHOPS_MESSAGING_OPT_OUT) là đích quảng cáo
    // Shops ⇒ xoá cả object = âm thầm cắt đích của quảng cáo Shops. Nay chỉ ghi/đè nhánh
    // `website`, giữ nguyên mọi khoá anh em.
    //
    // ⚠️ Nhánh TẮT chỉ ghi OPT_OUT KHI creative ĐÃ CÓ destination_spec: bên mb-ads đầu ra
    // buildCreativeData là publishedSnapshot + vế so sánh hai chiều của
    // hasCreativeChangedVsSnapshot, thêm khoá mới vô điều kiện sẽ làm 2.063 quảng cáo
    // đang chạy lệch snapshot → tạo lại creative hàng loạt (reset learning + mất social
    // proof). Không ghi gì cũng đã đúng: 2.043/2.063 quảng cáo publish với
    // optimize_website=false được Meta trả về "khoá website vắng mặt", 20 còn lại
    // 'OPT_OUT' — cả hai đều là TẮT. Repo này BẮT BUỘC giữ y hệt điều kiện đó, nếu không
    // cùng một bản nháp sẽ cho payload khác nhau tuỳ ai bấm publish.
    // asObject: nháp là JSON tự do → chặn trường hợp destination_spec/website bị lưu
    // sai kiểu (chuỗi/mảng) làm phép spread đẻ ra khoá rác "0","1"… gửi lên Meta.
    const asObject = (value: any) =>
      value && typeof value === 'object' && !Array.isArray(value)
        ? value
        : undefined;
    const currentDestinationSpec = asObject(creativeData.destination_spec);
    const withWebsiteOptimization = (status: 'OPT_IN' | 'OPT_OUT') => ({
      ...(currentDestinationSpec || {}),
      website: {
        ...(asObject(currentDestinationSpec?.website) || {}),
        optimization: {
          status,
          type: 'website_destination_optimization',
        },
      },
    });

    if (optimizeWebsiteEnabled) {
      if (websiteUrl) {
        creativeData.destination_spec = withWebsiteOptimization('OPT_IN');
      } else {
        this.logger.warn(
          '[personalized-destinations] "Tối ưu website" đang bật nhưng không tìm thấy website URL — bỏ qua destination_spec.',
        );
      }
    } else if (currentDestinationSpec) {
      creativeData.destination_spec = withWebsiteOptimization('OPT_OUT');
    }

    if (creativeData.asset_feed_spec) {
      delete creativeData.asset_feed_spec.personalized_destinations;
    }
  }

  // PARITY-LOCKED với mb-ads meta.service.ts#resolveBrowserAddOnPhoneNumber — chuẩn
  // hoá số điện thoại Browser add-on "CALL" về dạng `tel:+<mã vùng><số>` Meta yêu cầu.
  private resolveBrowserAddOnPhoneNumber(browserAddOn: any) {
    const rawPhoneNumber = `${browserAddOn?.phoneNumber || browserAddOn?.phone_number || ''}`;
    const rawCountryCode = `${browserAddOn?.countryCode || browserAddOn?.country_code || ''}`;
    const phoneNumber = rawPhoneNumber.replace(/[^\d+]/g, '');
    const countryCode = rawCountryCode.replace(/[^\d+]/g, '');

    if (!phoneNumber) return undefined;
    if (phoneNumber.startsWith('+')) return `tel:${phoneNumber}`;

    if (countryCode) {
      const normalizedCountryCode = countryCode.startsWith('+')
        ? countryCode
        : `+${countryCode}`;
      return `tel:${normalizedCountryCode}${phoneNumber.replace(/^0+/, '')}`;
    }

    return `tel:+${phoneNumber.replace(/^0+/, '')}`;
  }

  // PARITY-LOCKED với mb-ads meta.service.ts#stripOptionalPersonalizedDestinationFields.
  private stripOptionalPersonalizedDestinationFields(creativeData: any) {
    let stripped = false;

    // 🔴 SỬA LỖI (cùng lớp lỗi vừa vá ở applyPersonalizedDestinations ~2653): bản trước
    // `delete creativeData.destination_spec` — XOÁ CẢ OBJECT. destination_spec có 3 khoá
    // con ĐỘC LẬP (đo production: native_commerce_experience 15.764 / website 15.155 /
    // destination_type 569); fallback này chạy khi Meta từ chối creative, tức đúng lúc
    // payload đang mang dữ liệu thật → xoá cả object là âm thầm cắt đích Shops rồi TẠO
    // creative bằng payload đã bị cắt. Khoá TUỲ CHỌN duy nhất do khối
    // personalized_destinations sinh ra là `website.optimization` → chỉ bóc đúng nó, và
    // chỉ xoá cả destination_spec khi bóc xong KHÔNG CÒN GÌ.
    const destinationSpec = creativeData.destination_spec;
    if (
      destinationSpec &&
      typeof destinationSpec === 'object' &&
      !Array.isArray(destinationSpec)
    ) {
      const website = destinationSpec.website;
      if (website && typeof website === 'object' && !Array.isArray(website)) {
        if ('optimization' in website) {
          delete website.optimization;
          stripped = true;
        }
        if (Object.keys(website).length === 0) {
          delete destinationSpec.website;
        }
      }

      if (Object.keys(destinationSpec).length === 0) {
        delete creativeData.destination_spec;
      }
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

    // 🔴 PARITY-LOCKED với mb-ads meta.service.ts#isOptionalPersonalizedDestinationError.
    // Điều kiện kích hoạt PHẢI khớp đúng thứ stripOptionalPersonalizedDestinationFields
    // bóc đi (4 khoá asset_feed_spec + destination_spec.website.optimization), nếu không
    // lần thử lại gửi ĐÚNG payload cũ → hỏng lại → phí 1 lượt gọi Meta.
    //  • BỎ 'destination_spec' trần: các khoá anh em (native_commerce_experience /
    //    destination_type) nay KHÔNG còn bị bóc ⇒ chỉ nhận khi thông điệp có dấu hiệu
    //    đúng nhánh website/optimization.
    //  • BỎ 'website_and_shop': là GIÁ TRỊ của destination_spec.destination_type, không
    //    đường nào bóc ⇒ retry phí 100%.
    const mentionsStrippedField = [
      'call_ads_configuration',
      'message_extensions',
      'onsite_destinations',
      'personalized_destinations',
      'website_destination_optimization',
    ].some((field) => rawMessage.includes(field));

    return (
      mentionsStrippedField ||
      (rawMessage.includes('destination_spec') &&
        (rawMessage.includes('website') || rawMessage.includes('optimization')))
    );
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

  // Quét đệ quy 1 creative: còn ô slot chưa lấp không? Tín hiệu chắc chắn là chuỗi
  // đúng dạng token `VIDEO_n`/`IMAGE_n` (media đã lấp là id THẬT dạng số, không khớp);
  // kèm cờ `placeholder` (đã bị xoá khi lấp nên chỉ còn ở creative chưa lấp). Ad "ghim
  // bài" giữ object_story_id, không có token/placeholder nên KHÔNG bị chặn nhầm.
  private creativeHasUnfilledSlot(node: any): boolean {
    if (!node) return false;
    if (typeof node === 'string') {
      return /^VIDEO_\d+$/.test(node) || /^IMAGE_\d+$/.test(node);
    }
    if (Array.isArray(node)) {
      return node.some((n) => this.creativeHasUnfilledSlot(n));
    }
    if (typeof node === 'object') {
      if (node.placeholder === true) return true;
      return Object.values(node).some((v) =>
        this.creativeHasUnfilledSlot(v),
      );
    }
    return false;
  }
}
