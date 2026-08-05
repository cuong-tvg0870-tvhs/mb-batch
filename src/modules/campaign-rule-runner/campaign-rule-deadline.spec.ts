import { CampaignRuleRunnerService } from './campaign-rule-runner.service';
import {
  INSIGHT_RATELIMIT_SLEEP_MS,
  META_CALL_TIMEOUT_MS,
  RULE_LOCK_TTL_SECONDS,
  RULE_RUN_MAX_WALL_MS,
  RULE_RUN_SAFETY_MARGIN_MS,
} from './campaign-rule-runner.constants';

/**
 * Bất biến sống-còn: một lượt chạy rule KHÔNG BAO GIỜ được kéo dài quá TTL của khóa
 * `crr:<ruleId>`. Khóa không tự gia hạn — nếu nó hết hạn giữa chừng thì replica khác
 * chiếm được và chạy cùng rule cùng entity → tạo 2 khung tăng ngân sách chồng nhau
 * → BƠM NGÂN SÁCH HAI LẦN. Đây chính là regression đã suýt lọt: backoff rate-limit
 * mặc định 60s (60+120+180 = 360s) dài hơn TTL 300s.
 */
describe('bất biến hạn chót lượt chạy rule', () => {
  // retriesWithinDeadline chỉ đọc this.runDeadlineAt nên không cần dựng DI.
  const makeService = (remainingMs: number): CampaignRuleRunnerService => {
    const svc = Object.create(
      CampaignRuleRunnerService.prototype,
    ) as CampaignRuleRunnerService;
    (svc as any).runDeadlineAt = Date.now() + remainingMs;
    return svc;
  };

  const retriesFor = (remainingMs: number): number =>
    (makeService(remainingMs) as any).retriesWithinDeadline();

  /** Worst-case của một lần gọi insight với k retry (nhánh rate-limit, chậm nhất). */
  const worstCaseMs = (k: number): number =>
    (INSIGHT_RATELIMIT_SLEEP_MS * k * (k + 1)) / 2 + (k + 1) * META_CALL_TIMEOUT_MS;

  it('hạn chót phải nhỏ hơn TTL khóa (nếu không, vá này vô nghĩa)', () => {
    expect(RULE_RUN_MAX_WALL_MS).toBeLessThan(RULE_LOCK_TTL_SECONDS * 1000);
    expect(RULE_RUN_SAFETY_MARGIN_MS).toBeGreaterThan(0);
  });

  it('backoff cũ (60s) sẽ vượt TTL — chốt lại lý do phải rút xuống', () => {
    // 60000 × (1+2+3) = 360s > 300s TTL. Giữ phép tính này trong test để ai đó
    // nâng lại initialSleepMs sẽ thấy ngay vì sao không được.
    const oldSleepTotalMs = 60_000 * (1 + 2 + 3);
    expect(oldSleepTotalMs).toBeGreaterThan(RULE_LOCK_TTL_SECONDS * 1000);
    // Backoff mới thì nằm gọn trong hạn chót.
    expect(worstCaseMs(3)).toBeLessThanOrEqual(RULE_RUN_MAX_WALL_MS);
  });

  it('còn nhiều thời gian → cho phép tối đa 3 lần retry', () => {
    expect(retriesFor(RULE_RUN_MAX_WALL_MS)).toBe(3);
  });

  it('sát hạn chót → cắt hẳn retry (gọi 1 lần rồi thôi)', () => {
    expect(retriesFor(0)).toBe(0);
    expect(retriesFor(-60_000)).toBe(0); // đã quá hạn
    expect(retriesFor(worstCaseMs(1) - 1)).toBe(0);
  });

  it('số retry giảm dần theo thời gian còn lại', () => {
    expect(retriesFor(worstCaseMs(3))).toBe(3);
    expect(retriesFor(worstCaseMs(3) - 1)).toBe(2);
    expect(retriesFor(worstCaseMs(2))).toBe(2);
    expect(retriesFor(worstCaseMs(2) - 1)).toBe(1);
    expect(retriesFor(worstCaseMs(1))).toBe(1);
  });

  it('BẤT BIẾN: với mọi thời gian còn lại, worst-case không vượt quá nó', () => {
    // Quét dày để không bỏ sót biên nào.
    for (let remainingMs = 0; remainingMs <= 300_000; remainingMs += 1_000) {
      const k = retriesFor(remainingMs);
      if (k > 0) {
        expect(worstCaseMs(k)).toBeLessThanOrEqual(remainingMs);
      }
    }
  });
});
