import { DraftAutomationScheduler } from './draft-automation.scheduler';

// Regression cho sự cố prod 2026-07-27 (draft cms2ne94c000408jsrst9p835): template
// "Testing Content" nhân bản ad đã slotify nên 5 ad đều mang CÙNG token VIDEO_1 →
// engine cũ (gán theo token của mẫu) dồn video CUỐI của selectedAssets vào CẢ 3 ad
// video. Engine mới KHÔNG đọc token mẫu nữa: định danh ô = VỊ TRÍ (forEachMediaSlot),
// content per ô = contentPlan, token do engine tự đánh tuần tự. Gọi thẳng
// buildSubstitutedValues (method thuần, không đụng this.*) qua prototype để khỏi
// dựng DI.
const build = (input: any): any =>
  (DraftAutomationScheduler.prototype as any).buildSubstitutedValues.call(
    Object.create(DraftAutomationScheduler.prototype),
    input,
  );

const video = (n: number) => ({
  id: `asset-v${n}`,
  type: 'VIDEO',
  name: `TVG0189_SP00555_${n}_CID0000010${n}.mp4`,
  video_id: `meta-video-${n}`,
  video_source: `https://video/${n}`,
});

const image = (n: number) => ({
  id: `asset-i${n}`,
  type: 'IMAGE',
  name: `TVG0189_SP00555_${n}_CID0000020${n}.jpg`,
  imageHash: `hash-${n}`,
  imageUrl: `https://img/${n}`,
});

// Đúng shape prod: token nằm trong object_story_spec.video_data (cả video_id lẫn
// image_id), KHÔNG có creative.mediaType.
const dupTokenTemplate = (adCount: number, token = 'VIDEO_1') => ({
  id: 'tpl-1',
  name: 'OUTCOME_SALES|MESSAGING_PURCHASE_CONVERSION|5|5|Testing Content',
  data: {
    campaign: { name: 'SP00555|TVG0189|20260101|NewCustomer' },
    ad_sets: Array.from({ length: adCount }, (_, i) => ({
      name: 'SP00555|TVG0189|20260101|NewCustomer',
      ads: [
        {
          name: 'Justshine|TVG0189|SP00555|Purchase|CID00000000',
          creative: {
            object_story_spec: {
              page_id: '111',
              video_data: {
                video_id: token,
                image_id: token,
                message: `msg ${i}`,
              },
            },
          },
        },
      ],
    })),
  },
});

// Token nằm trong object_story_spec nên replacePlaceholders enrich media vào CHÍNH
// object đó (không phải creative top-level) — đọc theo cả 2 chỗ.
const videoIdOf = (creative: any) =>
  creative.video_id || creative.object_story_spec?.video_data?.video_id;
const imageHashOf = (creative: any) =>
  creative.image_hash || creative.object_story_spec?.link_data?.image_hash;

const baseInput = (overrides: any) => ({
  creator: { id: 'u1', name: 'Tester', employee_id: 'TVG0189' },
  publishMode: 'DRAFT_ONLY',
  isComplete: true,
  automation: { runMode: 'LOOP', folderId: 'folder-1' },
  ...overrides,
});

describe('buildSubstitutedValues — gán content theo VỊ TRÍ ô, token mẫu vô hiệu', () => {
  it('tái hiện sự cố: 5 ô đều VIDEO_1, kế hoạch 3 video + 2 ảnh → mỗi ad một asset RIÊNG', () => {
    const vids = [video(1), video(2), video(3)];
    const imgs = [image(1), image(2)];
    const result = build(
      baseInput({
        template: dupTokenTemplate(5),
        videos: vids,
        images: imgs,
        requiredVideos: 3,
        requiredImages: 2,
        contentPlan: [vids[0], vids[1], vids[2], imgs[0], imgs[1]],
        totalSlots: 5,
      }),
    );

    const creatives = result.ad_sets.map((as: any) => as.ads[0].creative);
    // 3 ad đầu: 3 video KHÁC nhau, đúng thứ tự kế hoạch (bug cũ: cả 3 = video CUỐI).
    expect(videoIdOf(creatives[0])).toBe('meta-video-1');
    expect(videoIdOf(creatives[1])).toBe('meta-video-2');
    expect(videoIdOf(creatives[2])).toBe('meta-video-3');
    // 2 ad sau morph sang ảnh, mỗi ad một ảnh riêng.
    expect(imageHashOf(creatives[3])).toBe('hash-1');
    expect(imageHashOf(creatives[4])).toBe('hash-2');

    // Tên ad mang CID của CHÍNH asset lấp ô đó (bug cũ: cả 3 dính CID video cuối).
    const names = result.ad_sets.map((as: any) => as.ads[0].name);
    expect(names[0]).toContain('CID00000101');
    expect(names[1]).toContain('CID00000102');
    expect(names[2]).toContain('CID00000103');
    expect(names[3]).toContain('CID00000201');
    expect(names[4]).toContain('CID00000202');

    // Không đốt content: cả 5 asset đều được ghi nhận đã dùng.
    expect(new Set(result.automation_used_assets)).toEqual(
      new Set(['asset-v1', 'asset-v2', 'asset-v3', 'asset-i1', 'asset-i2']),
    );
  });

  it('token rác/lệch dải trong mẫu bị bỏ qua hoàn toàn — gán thuần theo vị trí', () => {
    const template = dupTokenTemplate(3);
    // Mẫu hỏng kiểu khác: token lệch dải + trùng lung tung.
    const tokens = ['VIDEO_9', 'VIDEO_9', 'VIDEO_2'];
    template.data.ad_sets.forEach((as: any, i: number) => {
      const vd = as.ads[0].creative.object_story_spec.video_data;
      vd.video_id = tokens[i];
      vd.image_id = tokens[i];
    });
    const vids = [video(1), video(2), video(3)];
    const result = build(
      baseInput({
        template,
        videos: vids,
        images: [],
        requiredVideos: 3,
        requiredImages: 0,
        contentPlan: [vids[0], vids[1], vids[2]],
        totalSlots: 3,
      }),
    );

    const creatives = result.ad_sets.map((as: any) => as.ads[0].creative);
    expect(videoIdOf(creatives[0])).toBe('meta-video-1');
    expect(videoIdOf(creatives[1])).toBe('meta-video-2');
    expect(videoIdOf(creatives[2])).toBe('meta-video-3');
  });

  it('ô thiếu content (null trong plan) giữ DẤU Ô TRỐNG: token + placeholder, không ăn nhầm media', () => {
    const vids = [video(1), video(2)];
    const result = build(
      baseInput({
        template: dupTokenTemplate(3),
        videos: vids,
        images: [],
        requiredVideos: 2,
        requiredImages: 0,
        isComplete: false,
        contentPlan: [vids[0], null, vids[1]],
        totalSlots: 3,
      }),
    );

    const creatives = result.ad_sets.map((as: any) => as.ads[0].creative);
    expect(videoIdOf(creatives[0])).toBe('meta-video-1');
    expect(videoIdOf(creatives[2])).toBe('meta-video-2');
    // Ô giữa trống: giữ token placeholder cho publish gate EMPTY_SLOT + lượt gom sau.
    expect(videoIdOf(creatives[1])).toMatch(/^VIDEO_\d+$/);
    expect(creatives[1].placeholder).toBe(true);
    // Chỉ asset THẬT SỰ được đặt vào ô mới bị tính "đã dùng".
    expect(new Set(result.automation_used_assets)).toEqual(
      new Set(['asset-v1', 'asset-v2']),
    );
  });

  it('thiếu contentPlan → ném lỗi (nhánh gán theo token của mẫu đã bị loại bỏ)', () => {
    expect(() =>
      build(
        baseInput({
          template: dupTokenTemplate(3),
          videos: [video(1)],
          images: [],
          requiredVideos: 3,
          requiredImages: 0,
        }),
      ),
    ).toThrow(/contentPlan/);
  });

  it('mẫu lành mạnh (token phân biệt sẵn) cho kết quả y hệt: asset theo đúng thứ tự ô', () => {
    const template = dupTokenTemplate(3);
    template.data.ad_sets.forEach((as: any, i: number) => {
      const vd = as.ads[0].creative.object_story_spec.video_data;
      vd.video_id = `VIDEO_${i + 1}`;
      vd.image_id = `VIDEO_${i + 1}`;
    });
    const vids = [video(1), video(2), video(3)];
    const result = build(
      baseInput({
        template,
        videos: vids,
        images: [],
        requiredVideos: 3,
        requiredImages: 0,
        contentPlan: [vids[0], vids[1], vids[2]],
        totalSlots: 3,
      }),
    );

    const creatives = result.ad_sets.map((as: any) => as.ads[0].creative);
    expect(videoIdOf(creatives[0])).toBe('meta-video-1');
    expect(videoIdOf(creatives[1])).toBe('meta-video-2');
    expect(videoIdOf(creatives[2])).toBe('meta-video-3');
  });
});
