import { randomUUID } from 'node:crypto';
import { Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { AdSet, Campaign } from 'facebook-nodejs-business-sdk';
import {
  buildSpecs,
  deleteBudgetSchedules,
  executeBudgetSchedule,
  fetchBudgetSchedulesStrict,
} from '../campaign-rule-runner/campaign-rule-executor';
import {
  CAMPAIGN_TARGET_SENTINEL,
  normalizePendingAutomation,
  PendingAutomationEntry,
} from './pending-automation.util';

const logger = new Logger('PendingAutomationActivator');

// ---------------------------------------------------------------------------
// flattenTaskTree — COPY của mb-ads campaign-rule/campaign-rule-tree.util.ts (parity).
// ---------------------------------------------------------------------------
interface RuleConditionNode {
  id?: string;
  kind: 'condition';
  compareType?: any;
  params?: Record<string, unknown>;
}
interface RuleGroupNode {
  id?: string;
  kind: 'group';
  operator?: any;
  children?: Array<RuleGroupNode | RuleConditionNode>;
}
interface FlatGroup {
  id: string;
  taskId: string;
  parentGroupId: string | null;
  rootForTaskId: string | null;
  operator: any;
  position: number;
}
interface FlatCondition {
  id: string;
  groupId: string;
  compareType: any;
  params: unknown;
  position: number;
}

function flattenTaskTree(
  taskId: string,
  root: RuleGroupNode,
): { groups: FlatGroup[]; conditions: FlatCondition[] } {
  const groups: FlatGroup[] = [];
  const conditions: FlatCondition[] = [];
  const walkGroup = (
    group: RuleGroupNode,
    parentGroupId: string | null,
    index: number,
  ): void => {
    const isRoot = parentGroupId === null;
    const groupId = randomUUID();
    groups.push({
      id: groupId,
      taskId,
      parentGroupId,
      rootForTaskId: isRoot ? taskId : null,
      operator: group.operator ?? 'AND',
      position: index,
    });
    (group.children ?? []).forEach((child, childIndex) => {
      if ((child as RuleGroupNode).kind === 'group') {
        walkGroup(child as RuleGroupNode, groupId, childIndex);
      } else {
        const cond = child as RuleConditionNode;
        conditions.push({
          id: randomUUID(),
          groupId,
          compareType: cond.compareType ?? 'VALUE',
          params: cond.params ?? {},
          position: childIndex,
        });
      }
    });
  };
  walkGroup(root, null, 0);
  return { groups, conditions };
}

/** Rolling budget-schedule PHẢI tự chạy (đêm không ai duyệt) → ép autoExecute (parity mb-ads). */
function hasRollingTask(rule: Record<string, any>): boolean {
  const tasks = Array.isArray(rule?.tasks) ? rule.tasks : [];
  return tasks.some(
    (t: any) =>
      t?.kind === 'BUDGET_SCHEDULE_BUMP' && t?.params?.mode === 'ROLLING',
  );
}

/**
 * Vật chất hoá config pendingAutomation của 1 camp automation vừa publish (mb-batch). Idempotent
 * theo TỪNG ĐÍCH (appliedAdSetIds): ABO fail 1 phần retry đúng ad set thiếu; đích đã xong không
 * áp lại (không nhân đôi lịch Meta / rule). Cấp CBO/ABO suy từ `isCbo` THẬT, không tin entry.level.
 * Persist per-entry để crash giữa chừng không mất idempotency-key. Best-effort — lỗi lưu vào entry.
 */
export async function activatePendingAutomationForCampaign(params: {
  prisma: any;
  systemCampaignId: string;
  userId?: string | null;
  accountId: string;
  campaignMetaId: string;
  isCbo: boolean;
  campaignPendingAutomation: any; // raw từ SystemCampaign.pendingAutomation (chỉ dùng khi CBO)
}): Promise<{ failed: number; errors: string[] }> {
  const summary = { failed: 0, errors: [] as string[] };
  const { prisma, systemCampaignId, accountId, campaignMetaId, isCbo } = params;
  if (!accountId || !campaignMetaId) return summary;

  const account = await prisma.account
    .findUnique({ where: { id: accountId }, select: { timezone: true } })
    .catch(() => null);
  const tz: string = account?.timezone || 'Asia/Ho_Chi_Minh';
  const ctx = { accountId, campaignMetaId, tz, userId: params.userId ?? null };

  if (isCbo) {
    // CBO: cấu hình cấp CHIẾN DỊCH (SystemCampaign.pendingAutomation).
    const config = normalizePendingAutomation(
      params.campaignPendingAutomation,
      true,
    );
    if (config && config.entries.length) {
      const r = await materializeConfig(
        prisma,
        ctx,
        config,
        { level: 'CAMPAIGN', adSetId: null, coverageKey: CAMPAIGN_TARGET_SENTINEL },
        (updated) =>
          prisma.systemCampaign.update({
            where: { id: systemCampaignId },
            data: { pendingAutomation: updated },
          }),
      );
      summary.failed += r.failed;
      summary.errors.push(...r.errors);
    }
    return summary;
  }

  // ABO: mỗi nhóm 1 cấu hình RIÊNG trong SystemAdSet.data.pendingAutomation → áp cho đúng
  // ad set đó (đã có meta_id sau publish).
  const adSets = await prisma.systemAdSet.findMany({
    where: { campaignId: systemCampaignId },
  });
  for (const adSet of adSets) {
    if (!adSet.meta_id) continue;
    const config = normalizePendingAutomation(
      (adSet.data as any)?.pendingAutomation,
      true,
    );
    if (!config || !config.entries.length) continue;
    const adSetMetaId = adSet.meta_id;
    const r = await materializeConfig(
      prisma,
      ctx,
      config,
      { level: 'ADSET', adSetId: adSetMetaId, coverageKey: adSetMetaId },
      (updated) =>
        prisma.systemAdSet.update({
          where: { id: adSet.id },
          data: { data: { ...(adSet.data as any), pendingAutomation: updated } },
        }),
    );
    summary.failed += r.failed;
    summary.errors.push(...r.errors);
  }
  return summary;
}

/**
 * Vật chất hoá 1 config vào 1 ĐÍCH duy nhất (campaign hoặc 1 ad set). Idempotent theo
 * coverageKey; lỗi → KHÔNG phủ (retry lần sau); persist per-entry qua callback.
 */
async function materializeConfig(
  prisma: any,
  ctx: {
    accountId: string;
    campaignMetaId: string;
    tz: string;
    userId: string | null;
  },
  config: { version: number; entries: PendingAutomationEntry[] },
  target: {
    level: 'CAMPAIGN' | 'ADSET';
    adSetId: string | null;
    coverageKey: string;
  },
  persist: (updated: {
    version: number;
    entries: PendingAutomationEntry[];
  }) => Promise<unknown>,
): Promise<{ failed: number; errors: string[] }> {
  const entries: PendingAutomationEntry[] = [...config.entries];
  const errors: string[] = [];
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    const covered = new Set(entry.appliedAdSetIds ?? []);
    if (covered.has(target.coverageKey)) continue;

    let result: ApplyResult | null = null;
    let error: string | undefined;
    // HDP còn sót do rollback lỗi (từ makeApplyError) — phải GIỮ ownership dù đích này LỖI.
    let leftoverScheduleIds: string[] = [];
    try {
      result = await applyTarget(prisma, ctx, entry, target.level, target.adSetId);
    } catch (e: any) {
      logger.warn(
        `Áp pendingAutomation entry ${entry.uid} đích ${target.coverageKey} (camp ${ctx.campaignMetaId}) lỗi: ${e?.message ?? e}`,
      );
      error = String(e?.message ?? e);
      leftoverScheduleIds = Array.isArray(e?.leftoverScheduleIds)
        ? (e.leftoverScheduleIds as string[])
        : [];
    }

    const applied: PendingAutomationEntry = {
      ...entry,
      appliedRuleIds: [
        ...(entry.appliedRuleIds ?? []),
        ...(result?.ruleId ? [result.ruleId] : []),
      ],
      // Gộp: đã lưu + vừa tạo (thành công) + sót-rollback (LỖI) — dedup để không phình vô hạn.
      appliedMetaScheduleIds: Array.from(
        new Set([
          ...(entry.appliedMetaScheduleIds ?? []),
          ...(result?.metaScheduleIds ?? []),
          ...leftoverScheduleIds,
        ]),
      ),
      appliedAdSetIds: error
        ? entry.appliedAdSetIds ?? []
        : [...(entry.appliedAdSetIds ?? []), target.coverageKey],
      appliedAt: new Date().toISOString(),
      appliedError: error,
    };
    entries[i] = applied;

    // Persist LỖI = trạng thái đã-áp CHƯA lưu → coi là LỖI (không báo thành công sai). Retry sẽ
    // reprocess an toàn (reconcile theo id đã tạo / giống-hệt nên không nhân đôi).
    let persistError: string | undefined;
    try {
      await persist({ ...config, entries });
    } catch (e: any) {
      persistError = `Lưu trạng thái tự-động-hoá lỗi (camp ${ctx.campaignMetaId}): ${e?.message ?? e}`;
      logger.warn(persistError);
      entries[i] = {
        ...applied,
        appliedAdSetIds: entry.appliedAdSetIds ?? [], // gỡ covered vì DB chưa lưu
        appliedError: persistError,
      };
    }

    const finalError = error ?? persistError;
    if (finalError) errors.push(finalError);
  }
  return { failed: errors.length, errors };
}

interface ApplyResult {
  ruleId: string | null;
  metaScheduleIds: string[]; // id HDP đã tạo (SCHEDULE) — để ownership reconcile ở retry
}

/** Lỗi áp pending-automation kèm `leftoverScheduleIds` = HDP đã tạo nhưng rollback KHÔNG
 *  xoá được (vẫn LIVE trên Meta). Caller phải giữ các id này vào appliedMetaScheduleIds để
 *  retry sau còn ownership mà dọn/adopt — nếu bỏ rơi, lịch sót có thể tăng ngân sách. */
type ApplyError = Error & { leftoverScheduleIds?: string[] };
function makeApplyError(message: string, leftoverScheduleIds: string[] = []): ApplyError {
  const err = new Error(
    leftoverScheduleIds.length
      ? `${message} | Rollback LỖI, còn sót ${leftoverScheduleIds.length} lịch trên Meta (giữ ownership để retry dọn).`
      : message,
  ) as ApplyError;
  err.leftoverScheduleIds = leftoverScheduleIds;
  return err;
}

export const scheduleKey = (
  ts: number,
  te: number,
  bv: number | null | undefined,
  bt: string | null | undefined,
): string => `${ts}_${te}_${bv ?? ''}_${bt ?? ''}`;

interface LiveSchedule {
  id: string;
  time_start: number;
  time_end: number;
  budget_value: number | null;
  budget_value_type: string | null;
}
interface DesiredSpec {
  time_start: number;
  time_end: number;
  budget_value: number;
  budget_value_type: string;
}

/**
 * Tính danh sách id HDP CẦN XOÁ (reconcile APPEND-ONLY, an toàn cho lịch tay — PARITY với
 * mb-ads reconcileTarget):
 *  - CHỈ xoá HDP do MÌNH tạo (`ownedIds`) mà KHÔNG còn nằm trong bộ mong muốn (owned && !desired)
 *    → dọn khung cũ của hệ thống khi cấu hình đổi.
 *  - KHÔNG BAO GIỜ đụng lịch ngoại lai (không owned) — kể cả khi trùng khung/mức: lịch giống-hệt
 *    được GIỮ NGUYÊN + adopt (xem computeSpecsToCreate), không xoá-tạo-lại (tránh chiếm quyền lịch
 *    tay + khe trống).
 * Hàm THUẦN để unit-test.
 */
export function computeSchedulesToDelete(
  live: LiveSchedule[],
  specs: DesiredSpec[],
  ownedIds: string[],
): string[] {
  const desiredKeys = new Set(
    specs.map((s) =>
      scheduleKey(s.time_start, s.time_end, s.budget_value, s.budget_value_type),
    ),
  );
  const owned = new Set(ownedIds);
  return live
    .filter(
      (w) =>
        owned.has(w.id) &&
        !desiredKeys.has(
          scheduleKey(
            w.time_start,
            w.time_end,
            w.budget_value,
            w.budget_value_type,
          ),
        ),
    )
    .map((w) => w.id);
}

/**
 * Tính các spec CẦN TẠO MỚI: chỉ những spec CHƯA có khung giống-hệt trên Meta. Spec đã có (dù do
 * mình hay do người dùng tạo tay) → ADOPT (bỏ qua, không tạo trùng). Hàm THUẦN để unit-test.
 */
export function computeSpecsToCreate(
  live: LiveSchedule[],
  specs: DesiredSpec[],
): DesiredSpec[] {
  const liveKeys = new Set(
    live.map((w) =>
      scheduleKey(w.time_start, w.time_end, w.budget_value, w.budget_value_type),
    ),
  );
  return specs.filter(
    (s) =>
      !liveKeys.has(
        scheduleKey(s.time_start, s.time_end, s.budget_value, s.budget_value_type),
      ),
  );
}

/** Áp 1 entry vào 1 ĐÍCH. Trả ruleId + id HDP đã tạo (SCHEDULE). */
async function applyTarget(
  prisma: any,
  ctx: {
    accountId: string;
    campaignMetaId: string;
    tz: string;
    userId: string | null;
  },
  entry: PendingAutomationEntry,
  level: 'CAMPAIGN' | 'ADSET',
  adSetId: string | null,
): Promise<ApplyResult> {
  const entityId = adSetId ?? ctx.campaignMetaId;

  if (entry.kind === 'SCHEDULE') {
    const specs = buildSpecs(entry.periods, null, ctx.tz);
    // Khung rỗng SAU khi căn mốc 15' (vd 09:01–09:16 → 09:15–09:15) → THROW để sinh warning,
    // KHÔNG return null (caller sẽ mark covered = báo thành công sai).
    if (specs.length === 0) {
      throw new Error(
        'Khung giờ không hợp lệ (rỗng sau khi căn mốc 15 phút) → không tạo được lịch.',
      );
    }
    const enabled = entry.scheduleEnabled !== false;

    // RECONCILE APPEND-ONLY (idempotent, an toàn retry, KHÔNG đụng lịch tay — PARITY mb-ads):
    //  1) Đọc lịch hiện có — ĐỌC LỖI → THROW (abort, tránh tạo mù → nhân đôi).
    //  2) XOÁ chỉ HDP do MÌNH tạo (id đã lưu) mà KHÔNG còn mong muốn (owned && !desired). Xoá
    //     LỖI → THROW. KHÔNG đụng lịch ngoại lai.
    //  3) Tạo chỉ khung CÒN THIẾU (khung đã có giống-hệt → ADOPT, không tạo trùng). Lỗi tạo (kể
    //     cả Meta từ chối vì đè lịch tay) → rollback phần vừa tạo + throw.
    //  4) Đặt master toggle 1 LẦN duy nhất ở đây (executeBudgetSchedule gọi manageToggle=false).
    let live: Awaited<ReturnType<typeof fetchBudgetSchedulesStrict>>;
    try {
      live = await fetchBudgetSchedulesStrict(level, entityId);
    } catch (e: any) {
      throw new Error(
        `Không đọc được lịch hiện có (${level} ${entityId}) → bỏ qua tránh nhân đôi: ${e?.message ?? e}`,
      );
    }
    const deleteIds = computeSchedulesToDelete(
      live,
      specs,
      entry.appliedMetaScheduleIds ?? [],
    );
    if (deleteIds.length) {
      const del = await deleteBudgetSchedules(deleteIds);
      if (del.errors.length) {
        throw new Error(
          `Xoá lịch cũ của hệ thống lỗi (${level} ${entityId}) → bỏ qua tránh nhân đôi: ${del.errors.join('; ')}`,
        );
      }
    }

    // Chỉ tạo khung CHƯA có trên Meta (adopt khung giống-hệt). Không còn khung thiếu → không gọi
    // Meta tạo (vẫn đặt toggle bên dưới).
    const toCreate = computeSpecsToCreate(live, specs);
    let metaScheduleIds: string[] = [];
    if (toCreate.length) {
      // manageToggle=false: activator là nơi DUY NHẤT đặt cờ (bước 4) → không bật/tắt 2 lần.
      const res = await executeBudgetSchedule(level, entityId, toCreate, false);
      if (!res.ok) {
        // Rollback phần vừa tạo. deleteBudgetSchedules KHÔNG throw (best-effort) → phải
        // đọc failedIds: lịch xoá-hụt vẫn LIVE trên Meta → đính vào lỗi để caller GIỮ
        // ownership (appliedMetaScheduleIds), retry sau còn dọn/adopt được.
        let leftover: string[] = [];
        if (res.scheduleIds?.length) {
          const rb = await deleteBudgetSchedules(res.scheduleIds).catch(() => ({
            failedIds: res.scheduleIds ?? [],
          }));
          leftover = rb.failedIds ?? [];
        }
        throw makeApplyError(
          res.error?.message || 'Đẩy budget schedule lên Meta lỗi.',
          leftover,
        );
      }
      metaScheduleIds = res.scheduleIds ?? [];
    }

    // Đặt master toggle THEO ĐÚNG scheduleEnabled. LỖI → ROLLBACK các HDP vừa tạo (tránh lịch
    // BẬT ngoài ý muốn → tăng NS) rồi THROW (không mark covered → retry sạch nhờ ownership).
    try {
      if (level === 'CAMPAIGN')
        await new Campaign(entityId).update([], {
          is_budget_schedule_enabled: enabled,
        });
      else
        await new AdSet(entityId).update([], {
          is_budget_schedule_enabled: enabled,
        });
    } catch (e: any) {
      // Toggle lỗi → rollback HDP vừa tạo (tránh lịch BẬT ngoài ý muốn). Lịch xoá-hụt vẫn
      // LIVE → giữ ownership qua leftover để retry dọn (nếu bỏ rơi mà master toggle vốn
      // đang bật thì lịch sót có thể thực sự tăng ngân sách).
      let leftover: string[] = [];
      if (metaScheduleIds.length) {
        const rb = await deleteBudgetSchedules(metaScheduleIds).catch(() => ({
          failedIds: metaScheduleIds,
        }));
        leftover = rb.failedIds ?? [];
      }
      throw makeApplyError(
        `Đặt is_budget_schedule_enabled=${enabled} (${level} ${entityId}) lỗi: ${e?.message ?? e}`,
        leftover,
      );
    }

    // Tracking row: BEST-EFFORT (chỉ để màn detail hiển thị). Row lỗi KHÔNG un-cover — lịch đã
    // lên Meta ĐÚNG rồi → vẫn trả metaScheduleIds để đích được covered (không retry đẩy Meta lại).
    let ruleId: string | null = null;
    try {
      const rule = await prisma.campaignRule.create({
        data: {
          name: entry.name?.trim() || 'Lịch tăng ngân sách (tự động từ mẫu)',
          level,
          status: enabled ? 'ACTIVE' : 'PAUSED',
          accountId: ctx.accountId,
          campaignId: ctx.campaignMetaId,
          adSetId,
          timezone: ctx.tz,
          createdById: ctx.userId ?? undefined,
          tasks: {
            create: [
              {
                kind: 'BUDGET_SCHEDULE_BUMP',
                params: { periods: entry.periods } as any,
                position: 0,
              },
            ],
          },
        },
      });
      ruleId = rule.id;
    } catch (rowErr: any) {
      logger.warn(
        `Tạo tracking row cho lịch (camp ${ctx.campaignMetaId}) lỗi: ${rowErr?.message ?? rowErr}.`,
      );
    }
    return { ruleId, metaScheduleIds };
  }

  // RULE (điều kiện): DEDUP theo UID ENTRY — đóng dấu __paUid vào task[0].params (KHÔNG theo
  // name: 2 entry trùng tên sẽ gộp nhầm; còn tái dùng cả rule tạo tay cùng tên). Marker vô hại
  // với runner. Chặn nhân đôi khi appliedAdSetIds lỡ chưa kịp persist (crash/lỗi sau commit).
  const paUid = entry.uid;
  const rawTasks = Array.isArray((entry.rule as any)?.tasks)
    ? (entry.rule as any).tasks
    : [];
  const stampedRule = {
    ...(entry.rule ?? {}),
    tasks: rawTasks.map((t: any, i: number) =>
      i === 0 ? { ...t, params: { ...(t?.params ?? {}), __paUid: paUid } } : t,
    ),
  };
  const existing = await prisma.campaignRule.findFirst({
    where: {
      campaignId: ctx.campaignMetaId,
      adSetId: adSetId ?? null,
      level,
      deletedAt: null,
      tasks: { some: { params: { path: ['__paUid'], equals: paUid } } },
    },
    select: { id: true },
  });
  if (existing) return { ruleId: existing.id as string, metaScheduleIds: [] };

  const ruleId = await createConditionalRule(prisma, stampedRule, {
    accountId: ctx.accountId,
    campaignId: ctx.campaignMetaId,
    adSetId,
    level,
    timezone: ctx.tz,
    userId: ctx.userId,
  });
  return { ruleId, metaScheduleIds: [] };
}

/** Tạo CampaignRule điều kiện đầy đủ — MIRROR mb-ads campaign-rule.service create()+writeConfig(). */
async function createConditionalRule(
  prisma: any,
  rule: Record<string, any>,
  scope: {
    accountId: string;
    campaignId: string;
    adSetId: string | null;
    level: 'CAMPAIGN' | 'ADSET';
    timezone: string;
    userId: string | null;
  },
): Promise<string> {
  let recipients: string[] = Array.isArray(rule.notifyUserIds)
    ? Array.from(new Set(rule.notifyUserIds.filter((x: any) => typeof x === 'string')))
    : [];
  // Default-to-creator (parity resolveNotifyRecipients): bật thông báo mà không chỉ người
  // nhận → gửi cho người tạo.
  const notifyOn =
    rule.notifyOnMatch === true ||
    rule.notifyOnExecute === true ||
    rule.notifyOnError === true ||
    rule.notifyErrorsOnly === true;
  if (notifyOn && recipients.length === 0 && scope.userId) {
    recipients = [scope.userId];
  }

  // Rolling PHẢI tự chạy (parity mb-ads campaign-rule.service:942).
  const autoExecute = hasRollingTask(rule) ? true : rule.autoExecute === true;

  return prisma.$transaction(
    async (tx: any) => {
    const created = await tx.campaignRule.create({
      data: {
        name: String(rule.name || 'Quy tắc tự động (từ mẫu)'),
        level: scope.level,
        status: 'ACTIVE',
        accountId: scope.accountId,
        campaignId: scope.campaignId,
        adSetId: scope.adSetId,
        timezone: rule.timezone || scope.timezone,
        autoExecute,
        notifyErrorsOnly: rule.notifyErrorsOnly === true,
        notifyOnMatch: rule.notifyOnMatch === true,
        notifyOnExecute: rule.notifyOnExecute === true,
        notifyOnError: rule.notifyOnError === true,
        ...(rule.filterGroupOperator
          ? { filterGroupOperator: rule.filterGroupOperator }
          : {}),
        useAdSetAttributionWindow: rule.useAdSetAttributionWindow ?? true,
        attributionWindow: rule.attributionWindow ?? null,
        createdById: scope.userId ?? undefined,
        ...(recipients.length
          ? { notifyUsers: { create: recipients.map((uid) => ({ userId: uid })) } }
          : {}),
      },
    });

    if (rule.schedule) {
      const s = rule.schedule;
      await tx.campaignRuleSchedule.create({
        data: {
          ruleId: created.id,
          type: s.type ?? undefined,
          interval: s.interval ?? null,
          specificSlots:
            s.specificSlots == null
              ? Prisma.JsonNull
              : (s.specificSlots as Prisma.InputJsonValue),
          useDateRange: s.useDateRange ?? false,
          dateFrom: s.dateFrom ? new Date(s.dateFrom) : null,
          dateTo: s.dateTo ? new Date(s.dateTo) : null,
        },
      });
    }

    for (const [groupIndex, group] of (
      (rule.filterGroups ?? []) as any[]
    ).entries()) {
      await tx.campaignRuleFilterGroup.create({
        data: {
          ruleId: created.id,
          operator: group.operator ?? undefined,
          position: groupIndex,
          filters: {
            create: ((group.filters ?? []) as any[]).map((f, idx) => ({
              field: f.field,
              operator: f.operator,
              values: (f.values ?? []) as Prisma.InputJsonValue,
              timeframe: f.timeframe ?? null,
              position: idx,
            })),
          },
        },
      });
    }

    for (const [taskIndex, task] of ((rule.tasks ?? []) as any[]).entries()) {
      const createdTask = await tx.campaignRuleTask.create({
        data: {
          ruleId: created.id,
          kind: task.kind,
          description: task.description ?? null,
          params:
            task.params == null
              ? Prisma.JsonNull
              : (task.params as Prisma.InputJsonValue),
          position: taskIndex,
        },
      });

      if (task.rootGroup) {
        const { groups, conditions } = flattenTaskTree(
          createdTask.id,
          task.rootGroup as RuleGroupNode,
        );
        for (const g of groups) {
          await tx.campaignRuleTaskGroup.create({
            data: {
              id: g.id,
              taskId: g.taskId,
              rootForTaskId: g.rootForTaskId,
              parentGroupId: g.parentGroupId,
              operator: g.operator,
              position: g.position,
            },
          });
        }
        if (conditions.length > 0) {
          await tx.campaignRuleTaskCondition.createMany({
            data: conditions.map((c) => ({
              id: c.id,
              groupId: c.groupId,
              compareType: c.compareType,
              params: c.params as Prisma.InputJsonValue,
              position: c.position,
            })),
          });
        }
      }
    }

    return created.id as string;
    },
    // Parity mb-ads (campaign-rule.service create): cây điều kiện nhiều bước cần > 5s mặc định.
    { timeout: 30_000 },
  );
}
