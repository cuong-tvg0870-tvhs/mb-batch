import { normalizePendingAutomation } from './pending-automation.util';

// Test PARITY: bản util mb-batch phải khớp hành vi mb-ads (2 bản là mirror).
const P = (
  timeStart: string,
  timeEnd: string,
  budgetValueType = 'MULTIPLIER',
  budgetValue = 1.5,
) => ({ timeStart, timeEnd, budgetValueType, budgetValue });
const sched = (periods: any[]) => ({
  version: 1,
  entries: [{ uid: 's1', kind: 'SCHEDULE', level: 'ADSET', periods }],
});

describe('normalizePendingAutomation (mb-batch parity)', () => {
  it('validate: MULTIPLIER>1, end>start, >=15p, date hợp lệ', () => {
    expect(normalizePendingAutomation(sched([P('2026-07-25T18:00', '2026-07-25T20:00', 'MULTIPLIER', 1.5)]))?.entries[0].periods).toHaveLength(1);
    expect(normalizePendingAutomation(sched([P('2026-07-25T18:00', '2026-07-25T20:00', 'MULTIPLIER', 1)]))).toBeNull();
    expect(normalizePendingAutomation(sched([P('2026-07-25T20:00', '2026-07-25T18:00')]))).toBeNull();
    expect(normalizePendingAutomation(sched([P('2026-07-25T18:00', '2026-07-25T18:14')]))).toBeNull();
    expect(normalizePendingAutomation(sched([P('xxx', '2026-07-25T18:15')]))).toBeNull();
  });

  it('bỏ khung chồng chéo', () => {
    const c = normalizePendingAutomation(
      sched([
        P('2026-07-25T18:00', '2026-07-25T19:00'),
        P('2026-07-25T18:30', '2026-07-25T19:30'),
      ]),
    );
    expect(c?.entries[0].periods).toHaveLength(1);
  });

  it('mỗi config chỉ 1 SCHEDULE', () => {
    const c = normalizePendingAutomation({
      version: 1,
      entries: [
        { uid: 'a', kind: 'SCHEDULE', level: 'ADSET', periods: [P('2026-07-25T18:00', '2026-07-25T19:00')] },
        { uid: 'b', kind: 'SCHEDULE', level: 'ADSET', periods: [P('2026-07-25T20:00', '2026-07-25T21:00')] },
      ],
    });
    expect(c?.entries).toHaveLength(1);
  });

  it('RULE cần schedule + task', () => {
    expect(
      normalizePendingAutomation({
        version: 1,
        entries: [{ uid: 'r', kind: 'RULE', level: 'ADSET', rule: { name: 'x', tasks: [{}] } }],
      }),
    ).toBeNull(); // thiếu schedule
    expect(
      normalizePendingAutomation({
        version: 1,
        entries: [{ uid: 'r', kind: 'RULE', level: 'ADSET', rule: { name: 'x', schedule: {}, tasks: [{}] } }],
      })?.entries,
    ).toHaveLength(1);
  });

  it('keepApplied strip/keep applied* (gồm appliedMetaScheduleIds)', () => {
    const raw = {
      version: 1,
      entries: [
        {
          uid: 's1',
          kind: 'SCHEDULE',
          level: 'ADSET',
          periods: [P('2026-07-25T18:00', '2026-07-25T19:00')],
          appliedRuleIds: ['r'],
          appliedMetaScheduleIds: ['hdp'],
        },
      ],
    };
    expect(normalizePendingAutomation(raw, false)!.entries[0].appliedMetaScheduleIds).toBeUndefined();
    expect(normalizePendingAutomation(raw, true)!.entries[0].appliedMetaScheduleIds).toEqual(['hdp']);
  });
});
