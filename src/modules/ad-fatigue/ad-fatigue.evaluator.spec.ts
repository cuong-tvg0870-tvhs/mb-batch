import { AdFatigueStatus } from '@prisma/client';
import { describe, expect, it } from '@jest/globals';
import { AdFatigueThresholds } from './ad-fatigue.constants';
import {
  evaluateAdFatigue,
  resolveAdFatigueLifecycle,
} from './ad-fatigue.evaluator';

const thresholds: AdFatigueThresholds = {
  watchFrequency: 2,
  fatiguedFrequency: 3,
  watchCtrDropRate: 0.15,
  fatiguedCtrDropRate: 0.25,
  minImpressionsPerWindow: 1000,
};

describe('evaluateAdFatigue', () => {
  it('trả về INSUFFICIENT_DATA khi chưa đủ lượt hiển thị', () => {
    const result = evaluateAdFatigue(
      {
        frequency: 4,
        current: { impressions: 999, clicks: 10, spend: 100 },
        previous: { impressions: 2000, clicks: 40 },
      },
      thresholds,
    );

    expect(result.status).toBe(AdFatigueStatus.INSUFFICIENT_DATA);
    expect(result.score).toBe(0);
    expect(result.ctrDropRate).toBeNull();
    expect(result.reasons[0]).toContain('thấp hơn ngưỡng tối thiểu');
  });

  it('trả về INSUFFICIENT_DATA khi chưa có DAY_7 frequency', () => {
    const result = evaluateAdFatigue(
      {
        frequency: null,
        current: { impressions: 2000, clicks: 20 },
        previous: { impressions: 2000, clicks: 40 },
      },
      thresholds,
    );

    expect(result.status).toBe(AdFatigueStatus.INSUFFICIENT_DATA);
    expect(result.reasons).toContain(
      'Chưa có tần suất 7 ngày để đánh giá mức độ lặp lại quảng cáo.',
    );
  });

  it('trả về HEALTHY khi chỉ frequency cao nhưng CTR chưa giảm đủ', () => {
    const result = evaluateAdFatigue(
      {
        frequency: 3.5,
        current: { impressions: 2000, clicks: 36 },
        previous: { impressions: 2000, clicks: 40 },
      },
      thresholds,
    );

    expect(result.status).toBe(AdFatigueStatus.HEALTHY);
    expect(result.ctrDropRate).toBe(0.1);
    expect(result.score).toBeLessThan(60);
  });

  it('trả về WATCH đúng tại ngưỡng frequency 2 và CTR giảm 15%', () => {
    const result = evaluateAdFatigue(
      {
        frequency: 2,
        current: { impressions: 2000, clicks: 34 },
        previous: { impressions: 2000, clicks: 40 },
      },
      thresholds,
    );

    expect(result.status).toBe(AdFatigueStatus.WATCH);
    expect(result.currentCtr).toBe(1.7);
    expect(result.previousCtr).toBe(2);
    expect(result.ctrDropRate).toBe(0.15);
    expect(result.score).toBe(60);
    expect(result.reasons.join(' ')).toContain('xấp xỉ');
  });

  it('trả về FATIGUED đúng tại ngưỡng frequency 3 và CTR giảm 25%', () => {
    const result = evaluateAdFatigue(
      {
        frequency: 3,
        current: { impressions: 2000, clicks: 30, spend: 125.5 },
        previous: { impressions: 2000, clicks: 40 },
      },
      thresholds,
    );

    expect(result.status).toBe(AdFatigueStatus.FATIGUED);
    expect(result.ctrDropRate).toBe(0.25);
    expect(result.score).toBe(90);
    expect(result.currentSpend).toBe(125.5);
  });

  it('không coi CTR tăng là suy giảm', () => {
    const result = evaluateAdFatigue(
      {
        frequency: 4,
        current: { impressions: 2000, clicks: 50 },
        previous: { impressions: 2000, clicks: 40 },
      },
      thresholds,
    );

    expect(result.status).toBe(AdFatigueStatus.HEALTHY);
    expect(result.ctrDropRate).toBe(0);
    expect(result.score).toBe(0);
  });

  it('không đóng cảnh báo đang mở chỉ vì kỳ mới thiếu dữ liệu', () => {
    const detectedAt = new Date('2026-07-10T00:00:00.000Z');
    const evaluatedAt = new Date('2026-07-16T00:00:00.000Z');

    expect(
      resolveAdFatigueLifecycle(
        {
          status: AdFatigueStatus.FATIGUED,
          detectedAt,
          resolvedAt: null,
        },
        AdFatigueStatus.INSUFFICIENT_DATA,
        evaluatedAt,
      ),
    ).toEqual({ detectedAt, resolvedAt: null });
  });

  it('chỉ đóng cảnh báo đang mở khi đánh giá mới là HEALTHY', () => {
    const detectedAt = new Date('2026-07-10T00:00:00.000Z');
    const evaluatedAt = new Date('2026-07-16T00:00:00.000Z');

    expect(
      resolveAdFatigueLifecycle(
        {
          status: AdFatigueStatus.INSUFFICIENT_DATA,
          detectedAt,
          resolvedAt: null,
        },
        AdFatigueStatus.HEALTHY,
        evaluatedAt,
      ),
    ).toEqual({ detectedAt, resolvedAt: evaluatedAt });
  });
});
