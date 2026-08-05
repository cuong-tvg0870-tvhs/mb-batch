import {
  forEachMediaSlot,
  computeTemplateSlots,
  coerceCardToType,
  planContent,
  pruneEmptySlots,
  matchesSlotRule,
  type SlotRuleV2,
} from './draft-slot-count.util';

// ---------------------------------------------------------------------------
// Factory helper dựng template.data tối giản.
// ---------------------------------------------------------------------------
const tpl = (...creatives: any[]) => ({
  ad_sets: [{ ads: creatives.map((c) => ({ creative: c })) }],
});

// Asset (CreativeAsset row) giả lập.
const vid = (id: string, name = id) => ({
  id,
  name,
  type: 'VIDEO',
  video_id: `vsrc-${id}`,
  thumbnail: `thumb-${id}`,
  selected_thumbnail_id: `tid-${id}`,
});
const img = (id: string, name = id) => ({
  id,
  name,
  type: 'IMAGE',
  imageHash: `hash-${id}`,
  imageUrl: `url-${id}`,
});

describe('forEachMediaSlot / computeTemplateSlots — đếm & duyệt ô', () => {
  it('creative đơn video/ảnh → 1 ô single, crossTypeAllowed=true', () => {
    const slots = forEachMediaSlot(tpl({ videoId: 'VIDEO_1' }, { imageHash: 'IMAGE_1' }));
    expect(slots.map((s) => s.kind)).toEqual(['single', 'single']);
    expect(slots.map((s) => s.currentType)).toEqual(['VIDEO', 'IMAGE']);
    expect(slots.every((s) => s.crossTypeAllowed)).toBe(true);
  });

  it('carousel carouselCards → mỗi card 1 ô card theo mediaType', () => {
    const slots = forEachMediaSlot(
      tpl({
        carouselCards: [
          { mediaType: 'image', imageHash: 'a' },
          { mediaType: 'video', videoId: 'b' },
        ],
      }),
    );
    expect(slots.map((s) => s.kind)).toEqual(['card', 'card']);
    expect(slots.map((s) => s.currentType)).toEqual(['IMAGE', 'VIDEO']);
    expect(slots.every((s) => s.crossTypeAllowed)).toBe(true);
  });

  it('carousel child_attachments (raw) → mỗi attachment 1 ô attachment', () => {
    const slots = forEachMediaSlot(
      tpl({
        object_story_spec: {
          link_data: {
            child_attachments: [{ image_hash: 'a' }, { video_id: 'b' }],
          },
        },
      }),
    );
    expect(slots.map((s) => s.kind)).toEqual(['attachment', 'attachment']);
    expect(slots.map((s) => s.currentType)).toEqual(['IMAGE', 'VIDEO']);
    expect(slots.every((s) => s.crossTypeAllowed)).toBe(true);
  });

  it('dynamicAssets → ô dynamic, crossTypeAllowed=FALSE', () => {
    const slots = forEachMediaSlot(
      tpl({ dynamicAssets: [{ type: 'VIDEO' }, { type: 'IMAGE' }] }),
    );
    expect(slots.map((s) => s.kind)).toEqual(['dynamic', 'dynamic']);
    expect(slots.map((s) => s.currentType)).toEqual(['VIDEO', 'IMAGE']);
    expect(slots.every((s) => s.crossTypeAllowed)).toBe(false);
  });

  it('creative "ghim bài" (pinnedPost) → BỎ QUA toàn bộ', () => {
    const slots = forEachMediaSlot(
      tpl(
        { videoId: 'VIDEO_1', pinnedPost: true },
        { imageHash: 'IMAGE_1' },
      ),
    );
    expect(slots).toHaveLength(1);
    expect(slots[0].currentType).toBe('IMAGE');
  });

  it('mẫu hỗn hợp: thứ tự CỐ ĐỊNH single → card → attachment → dynamic', () => {
    const slots = forEachMediaSlot(
      tpl({
        // creative có carouselCards → là CAROUSEL (không có ô single) nhưng vẫn kèm
        // child_attachments + dynamicAssets → phủ đủ 3 loại ô sau.
        carouselCards: [{ mediaType: 'video', videoId: 'c1' }],
        object_story_spec: {
          link_data: { child_attachments: [{ image_hash: 'a1' }] },
        },
        dynamicAssets: [{ type: 'IMAGE' }],
      }),
    );
    expect(slots.map((s) => s.kind)).toEqual(['card', 'attachment', 'dynamic']);
  });

  it('computeTemplateSlots đếm total/video/image, slots KHÔNG kèm ref', () => {
    const r = computeTemplateSlots(
      tpl(
        { videoId: 'VIDEO_1' },
        {
          carouselCards: [
            { mediaType: 'image', imageHash: 'a' },
            { mediaType: 'video', videoId: 'b' },
          ],
        },
        { dynamicAssets: [{ type: 'IMAGE' }] },
      ),
    );
    expect(r.totalSlots).toBe(4);
    expect(r.videoSlots).toBe(2);
    expect(r.imageSlots).toBe(2);
    expect((r.slots[0] as any).ref).toBeUndefined();
  });

  it('mẫu 0 ô media (toàn pinnedPost) → totalSlots=0', () => {
    const r = computeTemplateSlots(tpl({ videoId: 'VIDEO_1', pinnedPost: true }));
    expect(r.totalSlots).toBe(0);
  });
});

describe('coerceCardToType — morph card/attachment 2 chiều', () => {
  it('card ảnh → video: set videoId/previewUrl/selected_thumbnail_id, mediaType, xoá imageHash', () => {
    const card: any = {
      mediaType: 'image',
      imageHash: 'old-hash',
      title: 'T',
      description: 'D',
      link: 'L',
      callToAction: 'SHOP_NOW',
    };
    const changed = coerceCardToType(card, 'VIDEO', vid('v1'), 'card');
    expect(changed).toBe(true);
    expect(card.mediaType).toBe('video');
    expect(card.videoId).toBe('vsrc-v1');
    expect(card.previewUrl).toBe('thumb-v1');
    expect(card.selected_thumbnail_id).toBe('tid-v1');
    expect(card.imageHash).toBeUndefined();
    // Giữ text/CTA/link.
    expect(card.title).toBe('T');
    expect(card.description).toBe('D');
    expect(card.link).toBe('L');
    expect(card.callToAction).toBe('SHOP_NOW');
  });

  it('card video → ảnh: set imageHash, mediaType, xoá videoId/selected_thumbnail_id', () => {
    const card: any = {
      mediaType: 'video',
      videoId: 'old-v',
      selected_thumbnail_id: 'old-tid',
      title: 'T',
    };
    const changed = coerceCardToType(card, 'IMAGE', img('i1'), 'card');
    expect(changed).toBe(true);
    expect(card.mediaType).toBe('image');
    expect(card.imageHash).toBe('hash-i1');
    expect(card.videoId).toBeUndefined();
    expect(card.selected_thumbnail_id).toBeUndefined();
    expect(card.title).toBe('T');
  });

  it('attachment ảnh → video: set video_id + thumbnail, xoá image_hash', () => {
    const att: any = {
      image_hash: 'old-hash',
      link: 'L',
      name: 'N',
      description: 'D',
      call_to_action: { type: 'SHOP_NOW' },
    };
    const changed = coerceCardToType(att, 'VIDEO', vid('v2'), 'attachment');
    expect(changed).toBe(true);
    expect(att.video_id).toBe('vsrc-v2');
    expect(att.image_url).toBe('thumb-v2');
    expect(att.selected_thumbnail_id).toBe('tid-v2');
    expect(att.image_hash).toBeUndefined();
    expect(att.link).toBe('L');
    expect(att.name).toBe('N');
    expect(att.call_to_action).toEqual({ type: 'SHOP_NOW' });
  });

  it('attachment video → ảnh: set image_hash, xoá video_id + thumbnail', () => {
    const att: any = {
      video_id: 'old-v',
      selected_thumbnail_id: 'old-tid',
      name: 'N',
    };
    const changed = coerceCardToType(att, 'IMAGE', img('i2'), 'attachment');
    expect(changed).toBe(true);
    expect(att.image_hash).toBe('hash-i2');
    expect(att.video_id).toBeUndefined();
    expect(att.selected_thumbnail_id).toBeUndefined();
    expect(att.name).toBe('N');
  });

  it('cùng loại → KHÔNG morph (trả false)', () => {
    const card: any = { mediaType: 'video', videoId: 'v' };
    expect(coerceCardToType(card, 'VIDEO', vid('v3'), 'card')).toBe(false);
  });
});

describe('planContent — gán content cho từng ô', () => {
  const V = (currentType: 'VIDEO' | 'IMAGE', crossTypeAllowed = true) => ({
    currentType,
    crossTypeAllowed,
  });

  it('same-type-first: pool đủ 2 loại → lấy ĐÚNG loại, không morph', () => {
    const plan = planContent([V('VIDEO')], [vid('v1')], [img('i1')]);
    expect(plan).toEqual([vid('v1')]);
  });

  it('cross-type khi thiếu: 2 ô ảnh + pool TOÀN video → 2 pick đều video', () => {
    const plan = planContent(
      [V('IMAGE'), V('IMAGE')],
      [vid('v1'), vid('v2')],
      [],
    );
    expect(plan.map((a: any) => a?.id)).toEqual(['v1', 'v2']);
  });

  it('ô dynamic (crossTypeAllowed=false) KHÔNG lấy chéo → null', () => {
    const plan = planContent([V('IMAGE', false)], [vid('v1')], []);
    expect(plan).toEqual([null]);
  });

  it('FIFO: lấy theo thứ tự pool truyền vào', () => {
    const plan = planContent(
      [V('VIDEO'), V('VIDEO')],
      [vid('v1'), vid('v2'), vid('v3')],
      [],
    );
    expect(plan.map((a: any) => a.id)).toEqual(['v1', 'v2']);
  });

  it('CHỐNG ĐỐT: 2 ô + pool 15 → đúng 2 pick (contentPlan.length=totalSlots)', () => {
    const pool = Array.from({ length: 15 }, (_, i) => vid(`v${i}`));
    const plan = planContent([V('VIDEO'), V('VIDEO')], pool, []);
    expect(plan).toHaveLength(2);
    const usedIds = plan.filter(Boolean).map((a: any) => a.id);
    expect(usedIds).toEqual(['v0', 'v1']);
  });

  it('ô không lấp được → null (isComplete=false)', () => {
    const plan = planContent([V('VIDEO')], [], []);
    expect(plan).toEqual([null]);
    const isComplete = plan.every(Boolean) && (plan.length > 0 || false);
    expect(isComplete).toBe(false);
  });

  it('slotRule: ô ưu tiên asset có tên chứa dấu hiệu', () => {
    const plan = planContent(
      [V('VIDEO')],
      [vid('v1', 'aaa'), vid('v2', 'hook-xyz')],
      [],
      { VIDEO_1: 'hook' },
    );
    expect((plan[0] as any).id).toBe('v2');
  });

  it('video KHÔNG thumbnail: không lấp CHÉO vào ô ảnh (bỏ qua → video kế; hết → null); ô VIDEO cùng loại vẫn nhận', () => {
    // Video "trần": không thumbnail/selected_thumbnail_id/imageUrl — morph vào ô ảnh
    // sẽ tạo card video thiếu image_url mà publisher không chặn được.
    const bare = {
      id: 'v-bare',
      name: 'v-bare',
      type: 'VIDEO',
      video_id: 'vsrc-bare',
    };
    // Bỏ qua video trần, lấy video kế tiếp có thumbnail.
    const plan = planContent([V('IMAGE')], [bare, vid('v2')], []);
    expect((plan[0] as any).id).toBe('v2');
    // Pool chỉ còn video trần → ô để null (giữ token + placeholder, isComplete=false).
    expect(planContent([V('IMAGE')], [bare], [])).toEqual([null]);
    // Ô VIDEO cùng loại KHÔNG siết: video trần vẫn dùng được như trước.
    const plan3 = planContent([V('VIDEO')], [bare], []);
    expect((plan3[0] as any).id).toBe('v-bare');
  });
});

describe('matchesSlotRule — string legacy + SlotRuleV2', () => {
  it('string legacy: 1 substring đơn, rỗng/absent/null = pass, KHÔNG split phẩy', () => {
    expect(matchesSlotRule({ name: 'hook-xyz' }, 'hook')).toBe(true);
    expect(matchesSlotRule({ name: 'aaa' }, 'hook')).toBe(false);
    expect(matchesSlotRule({ name: 'HOOK-abc' }, 'hook')).toBe(true); // case-insensitive
    // rỗng / absent / null ⇒ khớp mọi asset (không biến ô trống thành bộ chặn).
    expect(matchesSlotRule({ name: 'x' }, '')).toBe(true);
    expect(matchesSlotRule({ name: 'x' }, undefined)).toBe(true);
    expect(matchesSlotRule({ name: 'x' }, null)).toBe(true);
    // Dấu phẩy KHÔNG được split (giữ nguyên semantics cũ: 1 substring nguyên khối).
    expect(matchesSlotRule({ name: 'sp1 video' }, 'sp1,sp2')).toBe(false);
  });

  it('SlotRuleV2 include OR (khớp bất kỳ) + object rỗng = pass', () => {
    const rule: SlotRuleV2 = { include: ['sp00019', 'sp00020'] };
    expect(matchesSlotRule({ name: 'SP00019 - v' }, rule)).toBe(true);
    expect(matchesSlotRule({ name: 'SP00020 - v' }, rule)).toBe(true);
    expect(matchesSlotRule({ name: 'SP00021 - v' }, rule)).toBe(false);
    // Object rỗng ⇒ không ràng buộc gì ⇒ pass.
    expect(matchesSlotRule({ name: 'bất kỳ' }, {})).toBe(true);
  });

  it('SlotRuleV2 exclude (áp dụng độc lập) + exclude THẮNG include', () => {
    expect(matchesSlotRule({ name: 'bài test' }, { exclude: ['test'] })).toBe(
      false,
    );
    expect(matchesSlotRule({ name: 'bài chính' }, { exclude: ['test'] })).toBe(
      true,
    );
    // Dính exclude thì loại dù include đạt.
    expect(
      matchesSlotRule(
        { name: 'sp00019 test' },
        { include: ['sp00019'], exclude: ['test'] },
      ),
    ).toBe(false);
  });

  it('SlotRuleV2 khoảng thời gian tạo (createdFrom/To, inclusive, hỗ trợ epoch-seconds)', () => {
    const rule: SlotRuleV2 = {
      createdFrom: '2024-01-01T00:00:00.000Z',
      createdTo: '2024-12-31T23:59:59.000Z',
    };
    // creation_time ISO trong khoảng ⇒ đạt.
    expect(
      matchesSlotRule({ name: 'x', creation_time: '2024-06-15T00:00:00.000Z' }, rule),
    ).toBe(true);
    // creation_time epoch-seconds (=2024-06-15Z) ⇒ đạt.
    expect(matchesSlotRule({ name: 'x', creation_time: '1718409600' }, rule)).toBe(
      true,
    );
    // Trước khoảng ⇒ loại.
    expect(
      matchesSlotRule({ name: 'x', creation_time: '2023-12-31T00:00:00.000Z' }, rule),
    ).toBe(false);
    // Chỉ createdFrom: mở đầu "đến"; asset trước from ⇒ loại.
    expect(
      matchesSlotRule(
        { name: 'x', creation_time: '2023-01-01T00:00:00.000Z' },
        { createdFrom: '2024-01-01T00:00:00.000Z' },
      ),
    ).toBe(false);
  });

  it('SlotRuleV2 kết hợp tên + ngày: tên đạt nhưng ngày ngoài khoảng ⇒ loại', () => {
    expect(
      matchesSlotRule(
        { name: 'sp00019 - v', creation_time: '2023-01-01T00:00:00.000Z' },
        { include: ['sp00019'], createdFrom: '2024-01-01T00:00:00.000Z' },
      ),
    ).toBe(false);
  });
});

describe('planContent — slotRules trộn string | SlotRuleV2', () => {
  const V = (currentType: 'VIDEO' | 'IMAGE', crossTypeAllowed = true) => ({
    currentType,
    crossTypeAllowed,
  });
  // Asset ảnh có creation_time (cho test lọc ngày per-ô).
  const imgAt = (id: string, name: string, creation_time: string) => ({
    id,
    name,
    type: 'IMAGE',
    imageHash: `hash-${id}`,
    imageUrl: `url-${id}`,
    creation_time,
  });

  it('mỗi ô đọc đúng key theo loại: VIDEO_1 string legacy + IMAGE_1 SlotRuleV2 include', () => {
    const plan = planContent(
      [V('VIDEO'), V('IMAGE')],
      [vid('v1', 'aaa'), vid('v2', 'hook clip')],
      [img('i1', 'banner one'), img('i2', 'plain')],
      { VIDEO_1: 'hook', IMAGE_1: { include: ['banner'] } },
    );
    expect((plan[0] as any).id).toBe('v2'); // string 'hook' → v2
    expect((plan[1] as any).id).toBe('i1'); // include 'banner' → i1
  });

  it('SlotRuleV2 exclude trong 1 ô: bỏ qua asset dính từ khoá loại trừ', () => {
    const plan = planContent(
      [V('IMAGE')],
      [],
      [img('i1', 'old-banner'), img('i2', 'new-banner')],
      { IMAGE_1: { exclude: ['old'] } },
    );
    expect((plan[0] as any).id).toBe('i2');
  });

  it('SlotRuleV2 lọc theo khoảng thời gian tạo per-ô', () => {
    const plan = planContent(
      [V('IMAGE')],
      [],
      [
        imgAt('i1', 'banner', '2024-01-01T00:00:00.000Z'),
        imgAt('i2', 'banner', '2024-07-01T00:00:00.000Z'),
      ],
      { IMAGE_1: { createdFrom: '2024-06-01T00:00:00.000Z' } },
    );
    // i1 tạo trước 06-01 ⇒ bị bỏ; i2 sau ⇒ được chọn.
    expect((plan[0] as any).id).toBe('i2');
  });

  it('slotRules = null ⇒ không lọc, hành vi như không có rule', () => {
    const plan = planContent([V('VIDEO')], [vid('v1')], [], null);
    expect((plan[0] as any).id).toBe('v1');
  });
});

// ---------------------------------------------------------------------------
// pruneEmptySlots — "CHẠY NGAY": cắt ô không lấp được khỏi nháp.
// ---------------------------------------------------------------------------
describe('pruneEmptySlots — cắt ô trống cho lượt chạy ngay', () => {
  // Mô phỏng đúng cách builder gom emptyRefs: duyệt ô theo forEachMediaSlot rồi lấy
  // ref của những ô mà contentPlan[i] == null.
  const emptyRefsOf = (data: any, plan: any[]) => {
    const refs = new Set<any>();
    forEachMediaSlot(data).forEach((slot, i) => {
      if (!plan[i] && slot.ref) refs.add(slot.ref);
    });
    return refs;
  };

  it('mẫu 5 ô đơn, chỉ 2 ô có content ⇒ nháp còn đúng 2 ad', () => {
    const data = tpl(
      { videoId: 'a' },
      { imageHash: 'b' },
      { videoId: 'c' },
      { imageHash: 'd' },
      { videoId: 'e' },
    );
    const plan = [vid('v1'), img('i1'), null, null, null];
    const stats = pruneEmptySlots(data, emptyRefsOf(data, plan));

    expect(data.ad_sets[0].ads).toHaveLength(2);
    expect(stats).toMatchObject({
      prunedSlots: 3,
      prunedAds: 3,
      prunedAdSets: 0,
      remainingSlots: 2,
      thinCarousels: 0,
    });
  });

  it('carousel 3 card lấp 2 ⇒ giữ ad, chỉ xoá card trống', () => {
    const data = tpl({
      carouselCards: [
        { mediaType: 'image', imageHash: 'a' },
        { mediaType: 'image', imageHash: 'b' },
        { mediaType: 'video', videoId: 'c' },
      ],
    });
    const plan = [img('i1'), img('i2'), null];
    const stats = pruneEmptySlots(data, emptyRefsOf(data, plan));

    expect(data.ad_sets[0].ads).toHaveLength(1);
    expect(data.ad_sets[0].ads[0].creative.carouselCards).toHaveLength(2);
    expect(stats).toMatchObject({ prunedSlots: 1, prunedAds: 0, thinCarousels: 0 });
  });

  it('carousel còn đúng 1 card ⇒ vẫn giữ nhưng báo thinCarousels (Meta cần ≥2)', () => {
    const data = tpl({
      carouselCards: [
        { mediaType: 'image', imageHash: 'a' },
        { mediaType: 'image', imageHash: 'b' },
      ],
    });
    const stats = pruneEmptySlots(data, emptyRefsOf(data, [img('i1'), null]));

    expect(data.ad_sets[0].ads[0].creative.carouselCards).toHaveLength(1);
    expect(stats.thinCarousels).toBe(1);
  });

  it('child_attachments raw trống hết ⇒ xoá ad, ad set rỗng ⇒ xoá ad set', () => {
    const data = tpl({
      object_story_spec: {
        link_data: { child_attachments: [{ image_hash: 'a' }, { video_id: 'b' }] },
      },
    });
    const stats = pruneEmptySlots(data, emptyRefsOf(data, [null, null]));

    expect(data.ad_sets).toHaveLength(0);
    expect(stats).toMatchObject({
      prunedSlots: 2,
      prunedAds: 1,
      prunedAdSets: 1,
      remainingSlots: 0,
    });
  });

  it('ad ghim bài (pinnedPost) không phải ô ⇒ luôn giữ nguyên', () => {
    const data = tpl({ pinnedPost: true, videoId: 'keep' }, { videoId: 'a' });
    // pinnedPost bị forEachMediaSlot bỏ qua ⇒ ô duy nhất là creative thứ 2.
    const stats = pruneEmptySlots(data, emptyRefsOf(data, [null]));

    expect(data.ad_sets[0].ads).toHaveLength(1);
    expect(data.ad_sets[0].ads[0].creative.pinnedPost).toBe(true);
    expect(stats).toMatchObject({ prunedAds: 1, prunedAdSets: 0 });
  });

  it('không có ô nào trống ⇒ giữ nguyên toàn bộ cấu trúc', () => {
    const data = tpl({ videoId: 'a' }, { imageHash: 'b' });
    const stats = pruneEmptySlots(data, emptyRefsOf(data, [vid('v1'), img('i1')]));

    expect(data.ad_sets[0].ads).toHaveLength(2);
    expect(stats).toMatchObject({
      prunedSlots: 0,
      prunedAds: 0,
      prunedAdSets: 0,
      remainingSlots: 2,
    });
  });
});
