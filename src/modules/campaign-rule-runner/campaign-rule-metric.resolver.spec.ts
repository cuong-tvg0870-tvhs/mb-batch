// Khẳng định resolver metric budget-schedule cho 5 metric Meta mới + parity allow-list:
//   - count (view_content / add_payment_info / initiate_checkout): pickAction lấy phần
//     tử ĐẦU khớp theo chuỗi ưu tiên, KHÔNG cộng dồn nhiều biểu diễn cùng 1 sự kiện.
//   - value (registrations_completed_value): đọc từ action_values (không phải actions).
//   - average_website_purchase_value = Website purchase value / Website purchases;
//     count=0 hoặc thiếu → null (KHÔNG coi chia-0 là hợp lệ — giống pickCost).
//   - BUDGET_SCHEDULE_SUPPORTED_METRICS chứa đủ 5 key mới + 'cost_per_result' (vá sót).
//   - BUDGET_SCHEDULE_CUSTOM_BASE (= SUPPORTED trừ daily/lifetime_budget) chứa 5 key mới.
import {
  resolveMetric,
  BUDGET_SCHEDULE_SUPPORTED_METRICS,
  BUDGET_SCHEDULE_CUSTOM_BASE,
} from './campaign-rule-metric.resolver';

// Dựng 1 hàng action-stats kiểu Meta [{action_type, value}]
const a = (action_type: string, value: number | string) => ({ action_type, value });

describe('resolveMetric — 5 metric Meta mới', () => {
  it('website_content_views: ưu tiên "view_content" trước fb_pixel, KHÔNG cộng dồn', () => {
    const insight = {
      actions: [
        a('offsite_conversion.fb_pixel_view_content', 40),
        a('view_content', 37),
        a('omni_view_content', 41),
      ],
    };
    // 3 biểu diễn cùng sự kiện → chỉ lấy 'view_content' (đầu chuỗi ưu tiên) = 37
    expect(resolveMetric('website_content_views', insight, null)).toBe(37);
  });

  it('website_content_views: fallback fb_pixel khi thiếu generic/omni', () => {
    const insight = {
      actions: [a('offsite_conversion.fb_pixel_view_content', '12')],
    };
    expect(resolveMetric('website_content_views', insight, null)).toBe(12);
  });

  it('website_adds_of_payment_info: đếm từ actions theo chuỗi add_payment_info', () => {
    const insight = {
      actions: [
        a('add_payment_info', 5),
        a('offsite_conversion.fb_pixel_add_payment_info', 6),
      ],
    };
    expect(resolveMetric('website_adds_of_payment_info', insight, null)).toBe(5);
  });

  it('website_checkouts_initiated: dùng chung chuỗi initiate_checkout', () => {
    const insight = {
      actions: [
        a('omni_initiated_checkout', 9),
        a('initiate_checkout', 8),
      ],
    };
    // 'initiate_checkout' đứng đầu chuỗi ưu tiên → 8, không lấy omni=9
    expect(resolveMetric('website_checkouts_initiated', insight, null)).toBe(8);
    // và khớp với checkouts_initiated (cùng chuỗi AT)
    expect(resolveMetric('checkouts_initiated', insight, null)).toBe(8);
  });

  it('registrations_completed_value: đọc từ action_values, KHÔNG phải actions', () => {
    const insight = {
      actions: [a('complete_registration', 3)],
      action_values: [a('complete_registration', 450000)],
    };
    expect(resolveMetric('registrations_completed_value', insight, null)).toBe(450000);
  });

  it('registrations_completed_value: thiếu action_values → null (không âm thầm 0)', () => {
    const insight = { actions: [a('complete_registration', 3)] };
    expect(resolveMetric('registrations_completed_value', insight, null)).toBeNull();
  });

  describe('average_website_purchase_value = value / count', () => {
    it('tính đúng từ họ website pixel (fb_pixel_purchase)', () => {
      const insight = {
        actions: [a('offsite_conversion.fb_pixel_purchase', 4)],
        action_values: [a('offsite_conversion.fb_pixel_purchase', 1000000)],
      };
      expect(resolveMetric('average_website_purchase_value', insight, null)).toBe(250000);
    });

    it('count=0 → null (KHÔNG coi chia-0 là hợp lệ)', () => {
      const insight = {
        actions: [a('offsite_conversion.fb_pixel_purchase', 0)],
        action_values: [a('offsite_conversion.fb_pixel_purchase', 1000000)],
      };
      expect(resolveMetric('average_website_purchase_value', insight, null)).toBeNull();
    });

    it('thiếu count → null', () => {
      const insight = {
        action_values: [a('offsite_conversion.fb_pixel_purchase', 1000000)],
      };
      expect(resolveMetric('average_website_purchase_value', insight, null)).toBeNull();
    });

    it('thiếu value → null', () => {
      const insight = {
        actions: [a('offsite_conversion.fb_pixel_purchase', 4)],
      };
      expect(resolveMetric('average_website_purchase_value', insight, null)).toBeNull();
    });

    it('KHÔNG lấy nhầm họ omni (omni_purchase) cho cột website', () => {
      const insight = {
        actions: [a('omni_purchase', 10)],
        action_values: [a('omni_purchase', 9999)],
      };
      expect(resolveMetric('average_website_purchase_value', insight, null)).toBeNull();
    });
  });
});

// Họ "mua hàng TRÊN Meta" (Lượt mua hàng trên Meta = onsite_conversion.purchase):
// phần bù của họ website trong omni. Chỉ nhận ĐÚNG action_type này — KHÔNG lấy
// onsite_app_purchase (đã gộp app) cũng KHÔNG lấy omni/website.
describe('resolveMetric — họ mua hàng trên Meta (onsite)', () => {
  const insight = {
    spend: 1_000_000,
    actions: [
      a('omni_purchase', 10),
      a('offsite_conversion.fb_pixel_purchase', 6),
      a('onsite_app_purchase', 5),
      a('onsite_conversion.purchase', 4),
    ],
    action_values: [
      a('omni_purchase', 25_000_000),
      a('offsite_conversion.fb_pixel_purchase', 15_000_000),
      a('onsite_app_purchase', 11_000_000),
      a('onsite_conversion.purchase', 10_000_000),
    ],
  };

  it('meta_purchases: đúng onsite_conversion.purchase, không lẫn omni/website/app', () => {
    expect(resolveMetric('meta_purchases', insight, null)).toBe(4);
  });

  it('meta_purchase_value: đọc action_values của onsite_conversion.purchase', () => {
    expect(resolveMetric('meta_purchase_value', insight, null)).toBe(10_000_000);
  });

  it('cost_per_meta_purchase: ưu tiên cost_per_action_type, fallback spend/count', () => {
    // Meta KHÔNG trả cost_per_action_type cho onsite_conversion.purchase (đối chiếu prod)
    // → fallback 1.000.000 / 4 = 250.000
    expect(resolveMetric('cost_per_meta_purchase', insight, null)).toBe(250_000);
    // nếu Meta có trả thì lấy thẳng số của Meta
    const withCost = {
      ...insight,
      cost_per_action_type: [a('onsite_conversion.purchase', 240_000)],
    };
    expect(resolveMetric('cost_per_meta_purchase', withCost, null)).toBe(240_000);
  });

  it('meta_purchase_roas: TỰ TÍNH = meta value / spend', () => {
    expect(resolveMetric('meta_purchase_roas', insight, null)).toBe(10);
  });

  it('average_meta_purchase_value = meta value / meta purchases', () => {
    expect(resolveMetric('average_meta_purchase_value', insight, null)).toBe(2_500_000);
  });

  it('thiếu onsite_conversion.purchase → null (KHÔNG rơi về omni/website)', () => {
    const onlyWeb = {
      spend: 1_000_000,
      actions: [a('offsite_conversion.fb_pixel_purchase', 6)],
      action_values: [a('offsite_conversion.fb_pixel_purchase', 15_000_000)],
    };
    expect(resolveMetric('meta_purchases', onlyWeb, null)).toBeNull();
    expect(resolveMetric('meta_purchase_value', onlyWeb, null)).toBeNull();
    expect(resolveMetric('meta_purchase_roas', onlyWeb, null)).toBeNull();
    expect(resolveMetric('average_meta_purchase_value', onlyWeb, null)).toBeNull();
    expect(resolveMetric('cost_per_meta_purchase', onlyWeb, null)).toBeNull();
  });

  it('spend<=0 / count=0 → null (không coi chia-0 là hợp lệ)', () => {
    const zeroSpend = { ...insight, spend: 0 };
    expect(resolveMetric('meta_purchase_roas', zeroSpend, null)).toBeNull();
    const zeroCount = {
      spend: 1_000_000,
      actions: [a('onsite_conversion.purchase', 0)],
      action_values: [a('onsite_conversion.purchase', 10_000_000)],
    };
    expect(resolveMetric('average_meta_purchase_value', zeroCount, null)).toBeNull();
    expect(resolveMetric('cost_per_meta_purchase', zeroCount, null)).toBeNull();
  });

  it('họ omni/website KHÔNG đổi số khi thêm họ Meta (không hồi quy)', () => {
    expect(resolveMetric('purchases', insight, null)).toBe(10);
    expect(resolveMetric('website_purchases', insight, null)).toBe(6);
    expect(resolveMetric('purchases_value', insight, null)).toBe(25_000_000);
    expect(resolveMetric('website_purchase_value', insight, null)).toBe(15_000_000);
  });

  it('allow-list: 5 key họ Meta có ở SUPPORTED và CUSTOM_BASE', () => {
    for (const k of [
      'meta_purchases',
      'meta_purchase_value',
      'cost_per_meta_purchase',
      'meta_purchase_roas',
      'average_meta_purchase_value',
    ]) {
      expect(BUDGET_SCHEDULE_SUPPORTED_METRICS).toContain(k);
      expect(BUDGET_SCHEDULE_CUSTOM_BASE).toContain(k);
    }
  });
});

describe('allow-list parity (BUDGET_SCHEDULE_*)', () => {
  const NEW_KEYS = [
    'website_content_views',
    'website_checkouts_initiated',
    'website_adds_of_payment_info',
    'average_website_purchase_value',
    'registrations_completed_value',
  ];

  it('SUPPORTED chứa đủ 5 key mới + cost_per_result (vá sót parity)', () => {
    for (const k of [...NEW_KEYS, 'cost_per_result']) {
      expect(BUDGET_SCHEDULE_SUPPORTED_METRICS).toContain(k);
    }
  });

  it('CUSTOM_BASE chứa 5 key mới (= SUPPORTED trừ daily/lifetime_budget)', () => {
    for (const k of NEW_KEYS) {
      expect(BUDGET_SCHEDULE_CUSTOM_BASE).toContain(k);
    }
    // các key MINOR-unit vẫn bị loại khỏi base custom metric
    expect(BUDGET_SCHEDULE_CUSTOM_BASE).not.toContain('daily_budget');
    expect(BUDGET_SCHEDULE_CUSTOM_BASE).not.toContain('lifetime_budget');
  });
});
