// =============================================================================
// computeTemplateSlots / forEachMediaSlot / coerceCardToType — NGUỒN DUY NHẤT để
// ĐẾM & DUYỆT các Ô MEDIA của một mẫu (template.data) cho engine "Tự lên Camp với
// content mới".
//
// Trước đây logic đếm ô bị COPY 3 nơi (DraftCampaignService.computeAutomationRequiredCounts
// + 2 khối `countCreativeAssets` inline trong planner mb-ads/mb-batch) và mỗi bản chỉ
// ĐẾM chứ không DUYỆT được từng ô → planner (chọn asset) và builder (lấp token) dễ lệch
// ô 1-1, gây "đốt content" (chọn dư rồi ghi vào automation_used_assets nhưng không đặt
// vào ad nào → khoá vĩnh viễn). Gom về một chỗ THUẦN (pure) + test được.
//
// Ô = một vị trí media cần lấp trong mẫu, phủ:
//   - single      : creative đơn-media (VIDEO/IMAGE), bỏ qua creative "ghim bài".
//   - card        : carouselCards[i] (định dạng clean).
//   - attachment  : object_story_spec.link_data.child_attachments[i] (raw Meta).
//   - dynamic     : dynamicAssets[i] (DOF).
// Thứ tự duyệt CỐ ĐỊNH (builder đóng token theo đúng thứ tự này): ad_sets[].ads[].creative →
// single → carouselCards theo index → child_attachments → dynamicAssets. Nhờ thứ tự
// bất biến này mà planner và builder luôn khớp ô 1-1.
//
// crossTypeAllowed = true cho single/card/attachment (được phép morph ảnh↔video khi
// pool thiếu đúng loại); = false cho dynamic — morph DOF sẽ đổi ad_formats Meta ngoài
// ý muốn (đã quyết LOẠI TRỪ).
//
// PARITY: file này GIỮ GIỐNG NHAU TỪNG DÒNG giữa 2 repo:
//   - mb-ads   src/modules/draft-campaign/draft-slot-count.util.ts
//   - mb-batch src/modules/draft-automation/draft-slot-count.util.ts
// (bản sao thuần, không phụ thuộc service — nên copy nhỏ vài helper suy loại media).
// =============================================================================

import {
  getAssetCreatedDate,
  inCreatedRange,
  matchesNameFilter,
  normalizeCreatedRange,
  normalizeNameFilter,
} from './name-term-match';

export type SlotKind = 'single' | 'card' | 'attachment' | 'dynamic';
export type SlotMediaType = 'VIDEO' | 'IMAGE';

// Rule per-ô dạng NÂNG CAO: Bao gồm (OR) + Loại trừ theo tên + khoảng thời gian
// tạo (createdFrom/To ISO, inclusive). Song song với string legacy (1 substring).
export type SlotRuleV2 = {
  include?: string[] | null;
  exclude?: string[] | null;
  createdFrom?: string | null;
  createdTo?: string | null;
};

export interface MediaSlot {
  kind: SlotKind;
  currentType: SlotMediaType;
  // Ô được phép nhận content KHÁC loại (morph ảnh↔video). false cho dynamic.
  crossTypeAllowed: boolean;
  // Tham chiếu tới object gốc trong template.data (creative / card / attachment /
  // dynamicAsset) để builder morph + đọc token tại đúng ô. undefined khi CHỈ đếm.
  ref?: any;
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
    thumbnails.find((thumbnail: any) => thumbnail?.is_preferred) || thumbnails[0]
  );
}

// Suy id thumbnail của một video asset (copy từ service để util tự chứa).
export function getVideoThumbnailId(video: any) {
  return (
    video?.selected_thumbnail_id ||
    video?.imageHash ||
    video?.thumbnailHash ||
    video?.thumbnail_hash ||
    getPreferredThumbnail(video)?.id ||
    undefined
  );
}

// Suy url thumbnail của một video asset (copy từ service để util tự chứa).
export function getVideoThumbnailUrl(video: any) {
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

// Loại media của một creative trong mẫu (copy rút gọn từ service.inferMediaType).
function inferCreativeMediaType(creative: any): string {
  if (!creative) return 'NONE';
  if (creative.mediaType) {
    return String(creative.mediaType).toUpperCase();
  }
  const spec = creative.object_story_spec || {};
  if (
    spec.link_data?.child_attachments?.length > 0 ||
    creative.carouselCards?.length > 0
  ) {
    return 'CAROUSEL';
  }
  if (spec.video_data?.video_id || creative.videoId || creative.video_id) {
    return 'VIDEO';
  }
  if (spec.link_data?.image_hash || creative.imageHash || creative.image_hash) {
    return 'IMAGE';
  }
  return 'NONE';
}

// Loại media của một creativeAsset row (từ folder): 'VIDEO' | 'IMAGE'.
export function assetMediaType(asset: any): SlotMediaType {
  const t = String(asset?.type || asset?.mediaType || '').toUpperCase();
  if (t === 'VIDEO') return 'VIDEO';
  if (t === 'IMAGE') return 'IMAGE';
  return asset?.video_id || asset?.videoId ? 'VIDEO' : 'IMAGE';
}

// Duyệt PHẲNG mọi ô media của mẫu theo thứ tự CỐ ĐỊNH (xem đầu file). Trả về refs
// trỏ vào chính object trong `templateData` (dùng cho builder morph/đọc token).
export function forEachMediaSlot(templateData: any): MediaSlot[] {
  const slots: MediaSlot[] = [];
  const adSets = Array.isArray(templateData?.ad_sets)
    ? templateData.ad_sets
    : [];
  for (const adset of adSets) {
    const ads = Array.isArray(adset?.ads) ? adset.ads : [];
    for (const ad of ads) {
      const c = ad?.creative;
      // Ad "ghim bài" (pinnedPost) giữ media gốc, KHÔNG lấp bằng slot → bỏ qua cả
      // creative (parity slotifyTemplateCreatives; builder không đóng token ô pinned).
      if (!c || c.pinnedPost === true) continue;
      const mt = inferCreativeMediaType(c);
      const spec = c.object_story_spec || {};

      // 1) Ô ĐƠN-MEDIA (creative đơn video/ảnh).
      if (mt === 'VIDEO' || mt === 'IMAGE') {
        slots.push({
          kind: 'single',
          currentType: mt,
          crossTypeAllowed: true,
          ref: c,
        });
      }

      // 2) carouselCards[i] (định dạng clean).
      if (Array.isArray(c.carouselCards)) {
        for (const card of c.carouselCards) {
          const currentType: SlotMediaType =
            String(card?.mediaType || '').toUpperCase() === 'VIDEO'
              ? 'VIDEO'
              : 'IMAGE';
          slots.push({
            kind: 'card',
            currentType,
            crossTypeAllowed: true,
            ref: card,
          });
        }
      }

      // 3) child_attachments[i] (raw Meta).
      if (Array.isArray(spec.link_data?.child_attachments)) {
        for (const att of spec.link_data.child_attachments) {
          const currentType: SlotMediaType =
            att?.video_id || att?.videoId ? 'VIDEO' : 'IMAGE';
          slots.push({
            kind: 'attachment',
            currentType,
            crossTypeAllowed: true,
            ref: att,
          });
        }
      }

      // 4) dynamicAssets[i] (DOF) — KHÔNG cho morph.
      if (Array.isArray(c.dynamicAssets)) {
        for (const asset of c.dynamicAssets) {
          const currentType: SlotMediaType =
            String(asset?.type || asset?.mediaType || '').toUpperCase() ===
            'VIDEO'
              ? 'VIDEO'
              : 'IMAGE';
          slots.push({
            kind: 'dynamic',
            currentType,
            crossTypeAllowed: false,
            ref: asset,
          });
        }
      }
    }
  }
  return slots;
}

// Đếm ô của mẫu (không kèm ref — chỉ để hiển thị/ngưỡng). Dùng CHUNG forEachMediaSlot.
export function computeTemplateSlots(templateData: any): {
  slots: Array<{
    kind: SlotKind;
    currentType: SlotMediaType;
    crossTypeAllowed: boolean;
  }>;
  totalSlots: number;
  videoSlots: number;
  imageSlots: number;
} {
  const slots = forEachMediaSlot(templateData).map((s) => ({
    kind: s.kind,
    currentType: s.currentType,
    crossTypeAllowed: s.crossTypeAllowed,
  }));
  let videoSlots = 0;
  let imageSlots = 0;
  for (const s of slots) {
    if (s.currentType === 'VIDEO') videoSlots += 1;
    else imageSlots += 1;
  }
  return { slots, totalSlots: slots.length, videoSlots, imageSlots };
}

// Ô lấy content khớp slotRule. HAI dạng rule:
//   - string (legacy): 1 "dấu hiệu" substring DUY NHẤT, không split phẩy; rỗng →
//     khớp mọi asset. GIỮ NGUYÊN semantics cũ.
//   - SlotRuleV2: Bao gồm (OR) + Loại trừ theo tên + khoảng thời gian tạo, dùng
//     chung helper parity trong ./name-term-match. Field nào trống → không ràng
//     buộc phần đó (include rỗng ⇒ pass, range không bound ⇒ không lọc).
// Dùng chung planContent.
export function matchesSlotRule(
  asset: any,
  rule?: string | SlotRuleV2 | null,
): boolean {
  if (rule == null) return true;
  if (typeof rule === 'string') {
    const needle = rule.trim().toLowerCase();
    if (!needle) return true;
    return (asset?.name || '').toLowerCase().includes(needle);
  }
  const f = normalizeNameFilter(
    { include: rule.include, exclude: rule.exclude },
    null,
  );
  if (!matchesNameFilter(asset?.name, f)) return false;
  const range = normalizeCreatedRange({
    from: rule.createdFrom,
    to: rule.createdTo,
  });
  return inCreatedRange(getAssetCreatedDate(asset), range);
}

// PLANNER THỐNG NHẤT (thuần, test được) — gán content cho TỪNG Ô theo thứ tự
// forEachMediaSlot. Mỗi ô: lấy ĐÚNG loại trước (same-type-first, không hồi quy mẫu
// đang chạy tốt); thiếu VÀ ô cho phép cross-type (single/card/attachment) thì lấy
// CHÉO (ảnh↔video). Ô dynamic (DOF) crossTypeAllowed=false → chỉ nhận đúng loại.
// Pool truyền vào GIẢ ĐỊNH đã sắp xếp FIFO (createdAtLocal ASC) → giữ nguyên FIFO.
// Trả về mảng cùng độ dài slots (null tại ô không lấp được → giữ hợp đồng token
// leakage + isComplete=false). KHÔNG bơm theo videoCount/imageCount legacy → chống
// "đốt content" (contentPlan.length === totalSlots).
export function planContent(
  slots: Array<{ currentType: SlotMediaType; crossTypeAllowed: boolean }>,
  eligibleVideos: any[],
  eligibleImages: any[],
  slotRules: Record<string, string | SlotRuleV2> | null = {},
): any[] {
  const rules = slotRules ?? {};
  const usedPlan = new Set<string>();
  const perTypeOrd: Record<string, number> = { VIDEO: 0, IMAGE: 0 };
  const pickFrom = (
    pool: any[],
    rule?: string | SlotRuleV2,
    extra?: (a: any) => boolean,
  ) =>
    pool.find(
      (a) =>
        !usedPlan.has(a.id) && matchesSlotRule(a, rule) && (!extra || extra(a)),
    );
  // Video KHÔNG có ảnh đại diện (thumbnail id lẫn url đều rỗng) thì KHÔNG được lấp
  // CHÉO vào ô ảnh: ô sẽ morph sang video, mà card/attachment video khi publish cần
  // image_url — thiếu thumbnail sẽ đẩy lên Meta card không ảnh đại diện, và
  // creativeHasUnfilledSlot không bắt được (giá trị thật, không còn token/placeholder).
  // Bỏ qua candidate đó, thử video kế tiếp; hết candidate hợp lệ → ô để null (giữ
  // token + placeholder, isComplete=false như hợp đồng).
  const hasVideoThumbnail = (a: any) =>
    Boolean(getVideoThumbnailId(a) || getVideoThumbnailUrl(a));
  return slots.map((slot) => {
    perTypeOrd[slot.currentType] += 1;
    const rule = rules[`${slot.currentType}_${perTypeOrd[slot.currentType]}`];
    const sameType =
      slot.currentType === 'VIDEO' ? eligibleVideos : eligibleImages;
    const crossType =
      slot.currentType === 'VIDEO' ? eligibleImages : eligibleVideos;
    let pick = pickFrom(sameType, rule);
    if (!pick && slot.crossTypeAllowed)
      pick = pickFrom(
        crossType,
        rule,
        slot.currentType === 'IMAGE' ? hasVideoThumbnail : undefined,
      );
    if (pick) usedPlan.add(pick.id);
    return pick || null;
  });
}

// Morph (đổi loại) một ô carousel card / child_attachment sang loại đích khi content
// được gán KHÁC loại với ô. Chỉ morph khi currentType !== loại asset. Giữ text/CTA/
// link/title/description của ô. Trả về true nếu có đổi loại.
//
// LƯU Ý: stampSlotToken (bên builder) chạy SAU và sẽ GHI ĐÈ videoId/imageHash
// của ô đã morph bằng token VIDEO_n/IMAGE_n (vì giá trị thật không phải placeholder),
// rồi replacePlaceholders lấp lại giá trị thật. Việc set giá trị asset ở đây là để ô
// tự nhất quán (test được đứng một mình) + phòng khi cơ chế token không áp dụng.
//
// `kind` phân biệt shape: 'card' (clean: videoId/imageHash/mediaType/previewUrl) vs
// 'attachment' (raw: video_id/image_hash/image_url/picture). (Tham số kind là bổ sung
// so với chữ ký gợi ý trong thiết kế — cần để xử lý đúng 2 shape khác nhau.)
export function coerceCardToType(
  card: any,
  targetType: SlotMediaType,
  asset: any,
  kind: 'card' | 'attachment',
): boolean {
  const current: SlotMediaType =
    kind === 'attachment'
      ? card?.video_id || card?.videoId
        ? 'VIDEO'
        : 'IMAGE'
      : String(card?.mediaType || '').toUpperCase() === 'VIDEO'
        ? 'VIDEO'
        : 'IMAGE';
  if (current === targetType) return false;

  const thumbUrl = getVideoThumbnailUrl(asset);
  const thumbId = getVideoThumbnailId(asset);
  const imageUrl = asset?.imageUrl || asset?.thumbnail;

  if (kind === 'attachment') {
    if (targetType === 'VIDEO') {
      // ảnh → video: set video_id + thumbnail; xoá image_hash ảnh gốc.
      card.video_id = asset?.video_id;
      card.videoId = asset?.video_id;
      card.image_url = thumbUrl;
      card.picture = thumbUrl;
      card.selected_thumbnail_id = thumbId;
      delete card.image_hash;
      delete card.imageHash;
    } else {
      // video → ảnh: set image_hash; xoá video_id + field thumbnail của video.
      card.image_hash = asset?.imageHash;
      card.imageHash = asset?.imageHash;
      card.image_url = imageUrl ?? card.image_url;
      card.picture = imageUrl ?? card.picture;
      delete card.video_id;
      delete card.videoId;
      delete card.selected_thumbnail_id;
    }
    return true;
  }

  // kind === 'card'
  if (targetType === 'VIDEO') {
    // ảnh → video.
    card.videoId = asset?.video_id;
    card.previewUrl = thumbUrl;
    card.image = thumbUrl;
    card.selected_thumbnail_id = thumbId;
    card.mediaType = 'video';
    delete card.imageHash;
  } else {
    // video → ảnh.
    card.imageHash = asset?.imageHash;
    card.previewUrl = imageUrl ?? card.previewUrl;
    card.image = imageUrl ?? card.image;
    card.mediaType = 'image';
    delete card.videoId;
    delete card.selected_thumbnail_id;
  }
  return true;
}

// Đóng token placeholder lên MỘT Ô theo (kind, loại) — ghi đúng bộ field mà engine
// dựng-nháp trước đây ghi trong autoAssignCreativeSlots, kèm placeholder=true. Là
// bước "hệ thống TỰ đánh số vị trí": engine gọi với token tuần tự theo loại và GHI ĐÈ
// mọi token/ID sẵn có trên ô (token lưu trong mẫu không còn là đầu vào gán content).
// Ô chưa có content cũng được đóng token+placeholder làm DẤU Ô TRỐNG — kể cả khi mẫu
// chưa slotify (trước đây ô như vậy giữ media cũ của mẫu và publish nhầm media rác).
// XOÁ HẲN các Ô TRỐNG khỏi mẫu đã dựng — dùng cho "CHẠY NGAY" (allowPartial): người
// dùng không chờ gom đủ ô, nháp chỉ giữ ĐÚNG số content đang có (2 content / mẫu 5 ô →
// nháp 2 quảng cáo, 3 ô trống biến mất thay vì để token VIDEO_n + placeholder).
//
// `emptyRefs` = tập REF (so sánh theo object identity) của các ô KHÔNG lấp được, thu
// thập ở builder theo đúng thứ tự forEachMediaSlot (contentPlan[i] == null).
//
// Quy tắc: (1) card/attachment/dynamic trống → xoá khỏi mảng; (2) ad CÓ ô nhưng sau khi
// xoá KHÔNG còn ô nào → xoá cả ad; (3) ad set CÓ ad nhưng không còn ad nào → xoá ad set.
// Ad "ghim bài" (pinnedPost) và ad không có ô media nào GIỮ NGUYÊN (không phải ô để lấp)
// — chúng không tham gia forEachMediaSlot nên cũng không bao giờ nằm trong emptyRefs.
//
// Mutate templateData TẠI CHỖ (bản clone của builder), gọi TRƯỚC replacePlaceholders để
// mọi chỉ số ad_sets[ai].ads[di] mà builder dùng sau đó (đặt tên/CID) khớp cấu trúc mới.
// KHÔNG đánh số lại token: ô trống bị xoá chỉ để LẠI KHOẢNG TRỐNG trong dãy ordinal, mà
// replacePlaceholders tra theo chỉ số nên token còn lại vẫn trỏ đúng asset.
export function pruneEmptySlots(
  templateData: any,
  emptyRefs: Set<any>,
): {
  prunedSlots: number;
  prunedAds: number;
  prunedAdSets: number;
  remainingSlots: number;
  // Carousel còn <2 ô sau khi xoá — Meta yêu cầu ≥2 card. Nháp vẫn tạo (người dùng bổ
  // sung tay trong màn nháp), nhưng caller nên cảnh báo.
  thinCarousels: number;
} {
  let prunedSlots = 0;
  let prunedAds = 0;
  let prunedAdSets = 0;
  let remainingSlots = 0;
  let thinCarousels = 0;

  const dropEmpty = (list: any[]): any[] => {
    const next = list.filter((item) => !emptyRefs.has(item));
    prunedSlots += list.length - next.length;
    return next;
  };

  const adSets = Array.isArray(templateData?.ad_sets)
    ? templateData.ad_sets
    : [];
  const keptAdSets: any[] = [];

  for (const adset of adSets) {
    const ads = Array.isArray(adset?.ads) ? adset.ads : [];
    const keptAds: any[] = [];

    for (const ad of ads) {
      const c = ad?.creative;
      if (!c || c.pinnedPost === true) {
        keptAds.push(ad);
        continue;
      }

      let hadSlots = 0;
      let keptSlots = 0;
      let carouselLike = false;

      const mt = inferCreativeMediaType(c);
      if (mt === 'VIDEO' || mt === 'IMAGE') {
        hadSlots += 1;
        if (emptyRefs.has(c)) prunedSlots += 1;
        else keptSlots += 1;
      }

      if (Array.isArray(c.carouselCards)) {
        carouselLike = true;
        hadSlots += c.carouselCards.length;
        c.carouselCards = dropEmpty(c.carouselCards);
        keptSlots += c.carouselCards.length;
      }

      const linkData = c.object_story_spec?.link_data;
      if (Array.isArray(linkData?.child_attachments)) {
        carouselLike = true;
        hadSlots += linkData.child_attachments.length;
        linkData.child_attachments = dropEmpty(linkData.child_attachments);
        keptSlots += linkData.child_attachments.length;
      }

      if (Array.isArray(c.dynamicAssets)) {
        hadSlots += c.dynamicAssets.length;
        c.dynamicAssets = dropEmpty(c.dynamicAssets);
        keptSlots += c.dynamicAssets.length;
      }

      // Ad từng có ô mà giờ trắng ô → không còn media để chạy, xoá cả ad.
      if (hadSlots > 0 && keptSlots === 0) {
        prunedAds += 1;
        continue;
      }
      if (carouselLike && keptSlots > 0 && keptSlots < 2) thinCarousels += 1;
      remainingSlots += keptSlots;
      keptAds.push(ad);
    }

    adset.ads = keptAds;
    // Ad set từng có ad mà giờ rỗng → xoá; ad set vốn không có ad thì giữ nguyên.
    if (ads.length > 0 && keptAds.length === 0) {
      prunedAdSets += 1;
      continue;
    }
    keptAdSets.push(adset);
  }

  if (Array.isArray(templateData?.ad_sets)) {
    templateData.ad_sets = keptAdSets;
  }

  return { prunedSlots, prunedAds, prunedAdSets, remainingSlots, thinCarousels };
}

export function stampSlotToken(
  ref: any,
  kind: SlotKind,
  mediaType: SlotMediaType,
  token: string,
): void {
  if (!ref || !token) return;
  if (kind === 'single') {
    if (mediaType === 'VIDEO') {
      ref.videoId = token;
      ref.video_id = token;
      ref.imageHash = token;
      ref.image_hash = token;
      ref.selected_thumbnail_id = token;
      if (ref.object_story_spec?.video_data) {
        ref.object_story_spec.video_data.video_id = token;
        ref.object_story_spec.video_data.image_id = token;
        ref.object_story_spec.video_data.image_hash = token;
      }
    } else {
      ref.imageHash = token;
      ref.image_hash = token;
      if (ref.object_story_spec?.link_data) {
        ref.object_story_spec.link_data.image_hash = token;
      }
    }
  } else if (kind === 'card') {
    if (mediaType === 'VIDEO') {
      ref.videoId = token;
      ref.imageHash = token;
      ref.selected_thumbnail_id = token;
    } else {
      ref.imageHash = token;
    }
  } else if (kind === 'attachment') {
    if (mediaType === 'VIDEO') {
      ref.video_id = token;
      ref.videoId = token;
      ref.image_hash = token;
      ref.imageHash = token;
    } else {
      ref.image_hash = token;
      ref.imageHash = token;
    }
  } else {
    // dynamic
    if (mediaType === 'VIDEO') {
      ref.videoId = token;
      ref.video_id = token;
      ref.imageHash = token;
      ref.image_hash = token;
    } else {
      ref.imageHash = token;
      ref.image_hash = token;
    }
  }
  ref.placeholder = true;
}
