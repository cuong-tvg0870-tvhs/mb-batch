// Chốt fix sự cố prod 05/08/2026: engine campaign-rule đứng 5h45 mà KHÔNG có lỗi nào.
//
// Gốc: runExclusive tự nhận là FAIL-OPEN khi "Redis không truy cập được", nhưng nhánh đó
// chỉ chạy khi Redis NÉM LỖI. ioredis mặc định bật enableOfflineQueue: kết nối chết/wedged
// thì lệnh bị XẾP HÀNG chờ reconnect, `await client.set(...)` treo VÔ HẠN — cron đứng im,
// cờ chống chồng tick của scheduler mắc luôn.
//
// Khoanh vùng được nhờ dữ liệu prod: 0 dòng campaign_rule_run status=RUNNING sau mốc treo
// ⇒ treo TRƯỚC khi tạo dòng run ⇒ không phải lời gọi Meta, chỉ còn lệnh Redis này là await
// không có trần thời gian trên đường đi đó.
import { DistributedLockService, REDIS_OP_TIMEOUT_MS } from './distributed-lock.service';

/** Queue giả: `client` do test cấu hình để mô phỏng Redis lành/treo/lỗi. */
function makeService(client: any) {
  const queue: any = { client };
  return new DistributedLockService(queue);
}

describe('DistributedLockService — Redis treo không được làm đứng cron', () => {
  beforeEach(() => jest.useFakeTimers());
  afterEach(() => jest.useRealTimers());

  it('Redis TREO (lệnh không bao giờ settle) → fail-open, `fn` VẪN chạy', async () => {
    const fn = jest.fn().mockResolvedValue(undefined);
    const service = makeService({
      set: () => new Promise(() => {}), // treo vĩnh viễn, y hệt offline queue của ioredis
      eval: jest.fn(),
    });

    const promise = service.runExclusive('crr:rule-1', 300, fn);
    // Chưa tới trần thì vẫn đang chờ Redis.
    await jest.advanceTimersByTimeAsync(REDIS_OP_TIMEOUT_MS - 1);
    expect(fn).not.toHaveBeenCalled();

    await jest.advanceTimersByTimeAsync(2);
    await expect(promise).resolves.toBe(true);
    expect(fn).toHaveBeenCalledTimes(1); // đây là bất biến quan trọng nhất
  });

  it('Redis lành + giành được khóa → chạy `fn` rồi NHẢ khóa', async () => {
    const fn = jest.fn().mockResolvedValue(undefined);
    const evalFn = jest.fn().mockResolvedValue(1);
    const service = makeService({
      set: jest.fn().mockResolvedValue('OK'),
      eval: evalFn,
    });

    await expect(service.runExclusive('k', 300, fn)).resolves.toBe(true);
    expect(fn).toHaveBeenCalledTimes(1);
    expect(evalFn).toHaveBeenCalledTimes(1);
  });

  it('instance khác đang giữ khóa → KHÔNG chạy `fn` (chống chồng vẫn nguyên)', async () => {
    const fn = jest.fn();
    const service = makeService({
      set: jest.fn().mockResolvedValue(null), // NX thất bại
      eval: jest.fn(),
    });

    await expect(service.runExclusive('k', 300, fn)).resolves.toBe(false);
    expect(fn).not.toHaveBeenCalled();
  });

  it('Redis NÉM LỖI → fail-open như trước (không đổi hành vi cũ)', async () => {
    const fn = jest.fn().mockResolvedValue(undefined);
    const service = makeService({
      set: jest.fn().mockRejectedValue(new Error('ECONNREFUSED')),
      eval: jest.fn(),
    });

    await expect(service.runExclusive('k', 300, fn)).resolves.toBe(true);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('NHẢ khóa bị treo → không làm treo lượt chạy (fn đã xong thì trả về)', async () => {
    const fn = jest.fn().mockResolvedValue(undefined);
    const service = makeService({
      set: jest.fn().mockResolvedValue('OK'),
      eval: () => new Promise(() => {}), // treo ở bước nhả khóa
    });

    const promise = service.runExclusive('k', 300, fn);
    await jest.advanceTimersByTimeAsync(REDIS_OP_TIMEOUT_MS + 1);
    await expect(promise).resolves.toBe(true);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('lỗi từ `fn` vẫn ném ra ngoài (không bị timeout che mất)', async () => {
    const service = makeService({
      set: jest.fn().mockResolvedValue('OK'),
      eval: jest.fn().mockResolvedValue(1),
    });

    await expect(
      service.runExclusive('k', 300, () => Promise.reject(new Error('job lỗi'))),
    ).rejects.toThrow('job lỗi');
  });
});
