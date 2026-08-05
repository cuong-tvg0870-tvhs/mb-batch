import { CampaignRuleRunnerService } from './campaign-rule-runner.service';
import {
  retryDedupeKey,
  SLOT_RETRY_MAX_ATTEMPTS,
  SLOT_RETRY_WINDOW_MS,
} from './campaign-rule-schedule.util';

/**
 * Chốt an toàn cho cơ chế THỬ LẠI slot (code tiêu tiền thật — thử lại sai = bơm ngân
 * sách hai lần). Chỉ được thử lại khi lượt trước FAILED kèm dấu `insightRetry` (lỗi TẠM
 * THỜI ở khâu đọc insight, tức CHƯA gọi Meta ghi gì).
 */

const RULE = { id: 'rule-1', accountId: 'act_1' };
const NOW = new Date('2026-08-04T11:07:00.000Z');
const SLOT = new Date('2026-08-04T11:00:00.000Z');

function makeService(lastRun: any) {
  const prisma = {
    campaignRuleRun: { findFirst: jest.fn().mockResolvedValue(lastRun) },
  } as any;
  return new CampaignRuleRunnerService(prisma, {} as any, {} as any);
}

function findRetryableRun(lastRun: any) {
  const service = makeService(lastRun);
  return (service as any).findRetryableRun(RULE, NOW);
}

const failedTransient = (attempt = 0, scheduledFor: Date = SLOT) => ({
  status: 'FAILED',
  scheduledFor,
  ruleSnapshot: {
    name: 'r',
    insightRetry: { retryable: true, attempt, stage: 'FETCH_INSIGHT' },
  },
});

describe('findRetryableRun — cửa an toàn của cơ chế thử lại', () => {
  it('chưa từng chạy → không thử lại', async () => {
    await expect(findRetryableRun(null)).resolves.toBeNull();
  });

  it('lượt trước COMPLETED → không thử lại', async () => {
    await expect(
      findRetryableRun({ status: 'COMPLETED', scheduledFor: SLOT, ruleSnapshot: {} }),
    ).resolves.toBeNull();
  });

  it('FAILED nhưng KHÔNG có dấu insightRetry (vd lỗi khi đã đẩy Meta, hoặc run "áp lịch tay" của mb-ads) → KHÔNG thử lại', async () => {
    await expect(
      findRetryableRun({ status: 'FAILED', scheduledFor: SLOT, ruleSnapshot: { name: 'r' } }),
    ).resolves.toBeNull();
  });

  it('FAILED + lỗi tạm thời ở khâu đọc insight → thử lại lần 1 trên ĐÚNG slot cũ', async () => {
    await expect(findRetryableRun(failedTransient(0))).resolves.toEqual({
      aligned: SLOT,
      attempt: 1,
    });
  });

  it('đã thử lại chạm trần → dừng (không quét vô hạn)', async () => {
    await expect(
      findRetryableRun(failedTransient(SLOT_RETRY_MAX_ATTEMPTS)),
    ).resolves.toBeNull();
  });

  it('quá cửa sổ thử lại → dừng', async () => {
    const old = new Date(NOW.getTime() - SLOT_RETRY_WINDOW_MS - 60_000);
    await expect(findRetryableRun(failedTransient(0, old))).resolves.toBeNull();
  });
});

describe('retryDedupeKey', () => {
  it('suffix tất định theo số lần thử → 2 replica sinh cùng khóa (unique index chặn chồng)', () => {
    expect(retryDedupeKey('rule-1:act_1:2026-08-04T11:00:00.000Z', 1)).toBe(
      'rule-1:act_1:2026-08-04T11:00:00.000Z#retry1',
    );
    expect(retryDedupeKey('k', 2)).not.toBe(retryDedupeKey('k', 1));
  });
});
