// Test cơ chế P0: rollback lịch Meta là BEST-EFFORT (không throw) và phải BÁO ĐÚNG
// failedIds — các HDP xoá-hụt vẫn LIVE — để activator giữ ownership vào
// appliedMetaScheduleIds (tạo một phần → bước sau lỗi → rollback trả errors → không bỏ rơi).
const mockDelete = jest.fn();

jest.mock('facebook-nodejs-business-sdk', () => ({
  HighDemandPeriod: jest
    .fn()
    .mockImplementation((id: string) => ({ delete: () => mockDelete(id) })),
  Campaign: jest.fn(),
  AdSet: jest.fn(),
  FacebookAdsApi: { init: jest.fn() },
}));

import { deleteBudgetSchedules } from './campaign-rule-executor';

describe('deleteBudgetSchedules — best-effort + failedIds (giữ ownership khi rollback lỗi)', () => {
  beforeEach(() => mockDelete.mockReset());

  it('xoá được hết → removed=n, errors rỗng, failedIds rỗng', async () => {
    mockDelete.mockResolvedValue(undefined);
    const r = await deleteBudgetSchedules(['a', 'b', 'c']);
    expect(r.removed).toBe(3);
    expect(r.errors).toHaveLength(0);
    expect(r.failedIds).toEqual([]);
  });

  it('xoá HỤT một phần → failedIds đúng các id lỗi (KHÔNG throw)', async () => {
    mockDelete.mockImplementation((id: string) =>
      id === 'b'
        ? Promise.reject(new Error('Meta 500'))
        : Promise.resolve(undefined),
    );
    const r = await deleteBudgetSchedules(['a', 'b', 'c']);
    expect(r.removed).toBe(2);
    expect(r.failedIds).toEqual(['b']); // chỉ 'b' còn LIVE → phải giữ ownership
    expect(r.errors).toHaveLength(1);
  });

  it('mọi id lỗi → removed=0, failedIds = tất cả (rollback thất bại hoàn toàn)', async () => {
    mockDelete.mockRejectedValue(new Error('Meta down'));
    const r = await deleteBudgetSchedules(['x', 'y']);
    expect(r.removed).toBe(0);
    expect(r.failedIds).toEqual(['x', 'y']);
    expect(r.errors).toHaveLength(2);
  });

  it('danh sách rỗng → không gọi Meta', async () => {
    const r = await deleteBudgetSchedules([]);
    expect(mockDelete).not.toHaveBeenCalled();
    expect(r).toEqual({ removed: 0, errors: [], failedIds: [] });
  });
});
