// Chốt fix sự cố prod 05/08/2026: engine campaign-rule đứng 5h45 (188 rule ACTIVE không
// có LẤY MỘT dòng run nào) trong khi process mb-batch vẫn sống và meta-sync vẫn ghi DB.
//
// Gốc: cờ chống chồng tick là boolean, chỉ nhả trong `finally`. Một lượt runDueRules()
// treo vĩnh viễn (socket Meta không timeout ở code cũ) ⇒ `finally` không bao giờ chạy ⇒
// cờ mắc true mãi ⇒ mọi tick sau bị bỏ, âm thầm, tới khi restart process.
//
// Test khoá 3 bất biến của watchdog:
//   1. Tick đang chạy BÌNH THƯỜNG (dưới trần) vẫn được chống chồng như cũ.
//   2. Tick TREO quá trần thì tick sau CƯỚP được lượt → engine tự hồi phục.
//   3. Lượt bị cướp settle muộn KHÔNG được xoá cờ của lượt đang chạy (nếu không, hai lượt
//      chồng nhau thật sự).
import { TICK_MAX_WALL_MS } from './campaign-rule-runner.constants';
import { CampaignRuleRunnerScheduler } from './campaign-rule-runner.scheduler';

describe('CampaignRuleRunnerScheduler — watchdog nhả cờ khi tick treo', () => {
  let now = 1_754_000_000_000; // mốc bất kỳ, test tự tiến đồng hồ
  let nowSpy: jest.SpyInstance;

  beforeEach(() => {
    now = 1_754_000_000_000;
    nowSpy = jest.spyOn(Date, 'now').mockImplementation(() => now);
  });
  afterEach(() => nowSpy.mockRestore());

  /** Scheduler với service giả: runDueRules trả promise do test tự resolve. */
  function make() {
    const calls: Array<{ resolve: () => void }> = [];
    const service = {
      runDueRules: jest.fn(
        () => new Promise<void>((resolve) => calls.push({ resolve })),
      ),
    };
    const scheduler = new CampaignRuleRunnerScheduler(service as any);
    return { scheduler, service, calls };
  }

  it('tick trước chưa xong nhưng CHƯA quá trần → bỏ qua lượt này (chống chồng như cũ)', async () => {
    const { scheduler, service } = make();
    void scheduler.tick();
    expect(service.runDueRules).toHaveBeenCalledTimes(1);

    now += TICK_MAX_WALL_MS - 1000; // vẫn dưới trần
    await scheduler.tick();
    expect(service.runDueRules).toHaveBeenCalledTimes(1); // không chạy thêm
  });

  it('tick trước TREO quá trần → tick sau cướp lượt, engine tự hồi phục', async () => {
    const { scheduler, service } = make();
    void scheduler.tick(); // lượt này sẽ không bao giờ settle
    expect(service.runDueRules).toHaveBeenCalledTimes(1);

    now += TICK_MAX_WALL_MS; // chạm trần
    void scheduler.tick();
    expect(service.runDueRules).toHaveBeenCalledTimes(2);
  });

  it('lượt bị cướp settle MUỘN không xoá cờ của lượt đang chạy', async () => {
    const { scheduler, service, calls } = make();
    void scheduler.tick(); // lượt A (sẽ bị cướp)
    now += TICK_MAX_WALL_MS;
    void scheduler.tick(); // lượt B cướp lượt

    // A settle muộn → finally của A chạy, nhưng cờ đang thuộc B nên KHÔNG được nhả.
    calls[0].resolve();
    await Promise.resolve();
    await Promise.resolve();

    // Ngay sau đó có tick mới: B vẫn đang chạy và chưa quá trần → phải bị bỏ qua.
    await scheduler.tick();
    expect(service.runDueRules).toHaveBeenCalledTimes(2);
  });

  it('tick chạy xong bình thường → nhả cờ, tick kế tiếp chạy tiếp', async () => {
    const { scheduler, service, calls } = make();
    const first = scheduler.tick();
    calls[0].resolve();
    await first;

    // Lượt thứ hai cố tình chưa settle → chỉ kiểm tra nó ĐƯỢC phép chạy (cờ đã nhả).
    void scheduler.tick();
    expect(service.runDueRules).toHaveBeenCalledTimes(2);
  });

  it('runDueRules throw → cờ vẫn được nhả (không đứng vì một lỗi)', async () => {
    const service = {
      runDueRules: jest.fn().mockRejectedValue(new Error('Meta down')),
    };
    const scheduler = new CampaignRuleRunnerScheduler(service as any);
    await scheduler.tick();
    await scheduler.tick();
    expect(service.runDueRules).toHaveBeenCalledTimes(2);
  });
});
