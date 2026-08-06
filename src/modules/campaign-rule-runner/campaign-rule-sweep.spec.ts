import {
  CampaignRuleRunnerService,
  RUN_DEADLINE_STORE,
} from './campaign-rule-runner.service';
import {
  ruleScanAccountConcurrency,
  RULE_RUN_MAX_WALL_MS,
  TICK_SWEEP_MAX_WALL_MS,
  TICK_MAX_WALL_MS,
} from './campaign-rule-runner.constants';

const CONCURRENCY = ruleScanAccountConcurrency();

/**
 * Sự cố prod 06/08/2026 — "rule ACTIVE, đúng lịch, nhưng nhật ký đứng im nhiều ngày".
 *
 * Gốc: `runDueRules` duyệt TUẦN TỰ toàn bộ rule ACTIVE. Một lượt quét kéo 9–46 phút
 * (đo trên prod) trong khi cửa sổ bắt slot chỉ 15 phút ⇒ rule cuối hàng rớt khỏi cửa
 * sổ và MẤT HẲN slot. Vì `findMany` không có `orderBy`, thứ tự heap gần như cố định ⇒
 * luôn cùng một nhóm rule bị bỏ ⇒ đói kinh niên. Bộ test này khóa lại ba tính chất đã
 * sửa: song song theo TKQC, xếp hàng công bằng, và dừng sạch trước trần.
 */
describe('lượt quét campaign rule', () => {
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

  /** Rule tối giản: chỉ các trường mà vòng quét đụng tới. */
  const rule = (id: string, accountId: string) => ({
    id,
    name: `rule ${id}`,
    accountId,
    schedule: { type: 'SPECIFIC' },
  });

  /**
   * Service dựng bằng Object.create để khỏi phải bắc DI — vòng quét chỉ cần `prisma`
   * và `logger`. `processRule` được thay bằng bản ghi lại thứ tự/độ đồng thời.
   */
  const makeService = (
    rules: any[],
    lastRunRows: { ruleId: string; _max: { scheduledFor: Date | null } }[],
    onProcess: (rule: any) => Promise<void>,
    opts: { groupByThrows?: boolean } = {},
  ) => {
    const svc = Object.create(
      CampaignRuleRunnerService.prototype,
    ) as CampaignRuleRunnerService;
    const logs: string[] = [];
    (svc as any).logger = {
      log: (m: string) => logs.push(m),
      warn: (m: string) => logs.push(m),
      error: (m: string) => logs.push(m),
    };
    (svc as any).metaInitialized = true;
    (svc as any).prisma = {
      campaignRule: { findMany: async () => rules },
      campaignRuleRun: {
        groupBy: async () => {
          if (opts.groupByThrows) throw new Error('DB down');
          return lastRunRows;
        },
      },
    };
    (svc as any).processRule = onProcess;
    return { svc, logs };
  };

  const at = (iso: string) => new Date(iso);

  it('xếp rule ĐÓI NHẤT trước — trong cùng TKQC', async () => {
    const rules = [
      rule('r-moi-chay', 'act_1'),
      rule('r-doi-nhat', 'act_1'),
      rule('r-giua', 'act_1'),
      rule('r-chua-chay-bao-gio', 'act_1'),
    ];
    const order: string[] = [];
    const { svc } = makeService(
      rules,
      [
        { ruleId: 'r-moi-chay', _max: { scheduledFor: at('2026-08-06T10:00:00Z') } },
        { ruleId: 'r-doi-nhat', _max: { scheduledFor: at('2026-08-01T10:00:00Z') } },
        { ruleId: 'r-giua', _max: { scheduledFor: at('2026-08-04T10:00:00Z') } },
      ],
      async (r) => {
        order.push(r.id);
      },
    );

    await svc.runDueRules();

    // Rule chưa chạy bao giờ = đói nhất, rồi tới lỡ lâu nhất.
    expect(order).toEqual([
      'r-chua-chay-bao-gio',
      'r-doi-nhat',
      'r-giua',
      'r-moi-chay',
    ]);
  });

  it('xếp TKQC có rule đói nhất lên trước (khi số TKQC > số luồng)', async () => {
    const accounts = Array.from({ length: CONCURRENCY + 3 }, (_, i) =>
      String(i),
    );
    // act_0 mới chạy nhất, act_N đói nhất → thứ tự nhận việc phải là N..0.
    const rules = accounts.map((i) => rule(`r-${i}`, `act_${i}`));
    const lastRun = accounts.map((i) => ({
      ruleId: `r-${i}`,
      _max: { scheduledFor: new Date(2_000_000_000_000 - Number(i) * 60_000) },
    }));
    const started: string[] = [];
    const { svc } = makeService(rules, lastRun, async (r) => {
      started.push(r.accountId);
      await sleep(1);
    });

    await svc.runDueRules();

    const expected = [...accounts].reverse().map((i) => `act_${i}`);
    expect(started).toEqual(expected);
  });

  it('MỘT TKQC chỉ chạy một rule tại một thời điểm (giữ hạn mức Graph API theo account)', async () => {
    const rules = [
      ...Array.from({ length: 5 }, (_, i) => rule(`a${i}`, 'act_1')),
      ...Array.from({ length: 5 }, (_, i) => rule(`b${i}`, 'act_2')),
    ];
    const inFlight = new Map<string, number>();
    let maxPerAccount = 0;
    const { svc } = makeService(rules, [], async (r) => {
      const n = (inFlight.get(r.accountId) ?? 0) + 1;
      inFlight.set(r.accountId, n);
      maxPerAccount = Math.max(maxPerAccount, n);
      await sleep(2);
      inFlight.set(r.accountId, n - 1);
    });

    await svc.runDueRules();

    expect(maxPerAccount).toBe(1);
  });

  it('chạy song song nhiều TKQC nhưng không vượt trần luồng', async () => {
    const accountCount = CONCURRENCY + 5;
    const rules = Array.from({ length: accountCount }, (_, i) =>
      rule(`r${i}`, `act_${i}`),
    );
    let inFlight = 0;
    let maxInFlight = 0;
    const { svc } = makeService(rules, [], async () => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await sleep(5);
      inFlight -= 1;
    });

    await svc.runDueRules();

    expect(maxInFlight).toBe(CONCURRENCY);
    // và phải THỰC SỰ song song, nếu không thì vá này vô nghĩa.
    expect(maxInFlight).toBeGreaterThan(1);
  });

  it('chạm trần thời gian → dừng sạch, KHÔNG im lặng: log rõ số rule còn dở', async () => {
    const rules = Array.from({ length: 20 }, (_, i) => rule(`r${i}`, `act_${i % 2}`));
    const realNow = Date.now();
    let virtualNow = realNow;
    const spy = jest.spyOn(Date, 'now').mockImplementation(() => virtualNow);
    const processed: string[] = [];
    try {
      const { svc, logs } = makeService(rules, [], async (r) => {
        processed.push(r.id);
        // mỗi rule "ngốn" 1/4 trần → quá 4 rule/hàng là chạm hạn.
        virtualNow += TICK_SWEEP_MAX_WALL_MS / 4;
      });

      await svc.runDueRules();

      expect(processed.length).toBeLessThan(rules.length);
      const alarm = logs.find((l) => l.includes('CÒN'));
      expect(alarm).toBeDefined();
      expect(alarm).toContain(`${rules.length - processed.length} rule chưa quét`);
    } finally {
      spy.mockRestore();
    }
  });

  it('trần lượt quét phải NHỎ HƠN trần cướp lượt (nếu không, hai lượt chạy chồng nhau)', () => {
    expect(TICK_SWEEP_MAX_WALL_MS).toBeLessThan(TICK_MAX_WALL_MS);
    expect(TICK_SWEEP_MAX_WALL_MS).toBeGreaterThan(0);
  });

  it('không nạp được mốc chạy gần nhất → KHÔNG coi mọi rule là "chưa chạy bao giờ"', async () => {
    // Nếu rơi về null, mọi rule INTERVAL sẽ đến hạn ngay lập tức ⇒ chạy sai lịch,
    // và với rule tăng ngân sách thì đó là tiền thật.
    const rules = [rule('r1', 'act_1')];
    const seen: any[] = [];
    const { svc } = makeService(rules, [], async (_r: any) => {}, {
      groupByThrows: true,
    });
    (svc as any).processRule = async (_r: any, _now: Date, ctx: any) => {
      seen.push(ctx);
    };

    await svc.runDueRules();

    expect(seen).toHaveLength(1);
    expect(seen[0].lastRunAt).toBeUndefined();
  });
});

/**
 * Hạn chót lượt chạy phải thuộc về TỪNG lượt, không dùng chung cả service. Đây là chốt
 * chặn chống bơm ngân sách hai lần: nếu lượt sau ghi đè hạn chót của lượt trước thì lượt
 * trước có thể ngủ quá TTL khóa `crr:<ruleId>` (300s), khóa chết giữa chừng và một lượt
 * khác chạy cùng rule cùng entity → hai khung tăng ngân sách chồng nhau.
 */
describe('hạn chót lượt chạy khi quét song song', () => {
  it('hai lượt chạy đồng thời giữ hạn chót RIÊNG', async () => {
    const svc = Object.create(
      CampaignRuleRunnerService.prototype,
    ) as CampaignRuleRunnerService;
    (svc as any).logger = { log() {}, warn() {}, error() {} };

    const deadlines: Record<string, number> = {};
    /** Đọc LẠI hạn chót lúc CẢ HAI lượt đang cùng bay — đây mới là phép thử thật. */
    const deadlinesWhileOverlapping: Record<string, number> = {};
    let release: (() => void) | null = null;
    const bothInFlight = new Promise<void>((r) => (release = r));
    let pending = 2;

    (svc as any).prisma = {
      campaignRuleRun: {
        create: async ({ data }: any) => {
          // Đọc hạn chót TRONG chuỗi async của chính lượt này.
          deadlines[data.ruleId] = (svc as any).deadlineAt();
          pending -= 1;
          if (pending === 0) release?.();
          await bothInFlight; // giữ cả hai cùng bay để tái hiện đúng cảnh song song
          deadlinesWhileOverlapping[data.ruleId] = (svc as any).deadlineAt();
          throw Object.assign(new Error('dup'), { code: 'P2002' });
        },
      },
    };

    const start = Date.now();
    const first = (svc as any).executeRun(
      { id: 'r1', level: 'ADSET' },
      'Asia/Ho_Chi_Minh',
      new Date(),
      'key1',
      new Date(),
    );
    await new Promise((r) => setTimeout(r, 50));
    const second = (svc as any).executeRun(
      { id: 'r2', level: 'ADSET' },
      'Asia/Ho_Chi_Minh',
      new Date(),
      'key2',
      new Date(),
    );
    await Promise.all([first, second]);

    expect(deadlines.r1).toBeGreaterThanOrEqual(start + RULE_RUN_MAX_WALL_MS);
    // Lượt sau bắt đầu muộn hơn ⇒ hạn chót phải MUỘN HƠN hẳn, không bị/không làm lây
    // sang lượt trước. Với field dùng chung, hai giá trị này bằng nhau.
    expect(deadlines.r2 - deadlines.r1).toBeGreaterThanOrEqual(40);
    // Và mốc của lượt 1 KHÔNG bị lượt 2 kéo theo, dù lượt 2 mở sau và vẫn đang chạy.
    expect(deadlinesWhileOverlapping.r1).toBe(deadlines.r1);
    expect(deadlinesWhileOverlapping.r2).toBe(deadlines.r2);
  });

  it('ngoài lượt chạy thì không có hạn chót (không tự bịa mốc)', () => {
    const svc = Object.create(
      CampaignRuleRunnerService.prototype,
    ) as CampaignRuleRunnerService;
    expect((svc as any).deadlineAt()).toBe(Number.POSITIVE_INFINITY);
    RUN_DEADLINE_STORE.run({ at: 123 }, () => {
      expect((svc as any).deadlineAt()).toBe(123);
    });
  });
});
