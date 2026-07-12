import { Injectable, Logger } from '@nestjs/common';
import { AdSet, Campaign, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import { PrismaService } from '../prisma/prisma.service';

// Đồng bộ NGƯỢC budget schedules từ Meta về DB: mỗi campaign giữ 1 bản ghi
// campaign_rule "gương" (syncedFromMeta=true) phản chiếu đúng các budget schedule
// đang có trên Meta. Gọi từ các job sync campaign (manual + batch).
type MetaSchedule = {
  id: string;
  time_start: string;
  time_end: string;
  budget_value: number | string;
  budget_value_type: string;
};

type Period = {
  timeStart: string;
  timeEnd: string;
  budgetValueType: string;
  budgetValue: number;
};

// Meta trả ISO có tz (…+0000) → wall-clock Asia/Ho_Chi_Minh "YYYY-MM-DDTHH:mm".
function toVnLocal(iso: string): string {
  const d = new Date(iso);
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Ho_Chi_Minh',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(d);
  const g = (t: string) => parts.find((p) => p.type === t)?.value ?? '00';
  let hour = g('hour');
  if (hour === '24') hour = '00';
  return `${g('year')}-${g('month')}-${g('day')}T${hour}:${g('minute')}`;
}

function metaToPeriod(s: MetaSchedule): Period {
  return {
    timeStart: toVnLocal(s.time_start),
    timeEnd: toVnLocal(s.time_end),
    budgetValueType: s.budget_value_type,
    budgetValue: Number(s.budget_value),
  };
}

@Injectable()
export class CampaignRuleSyncService {
  private readonly logger = new Logger(CampaignRuleSyncService.name);

  constructor(private prisma: PrismaService) {}

  /**
   * Kéo budget schedules hiện tại của campaign từ Meta và upsert bản ghi "gương".
   * CBO → schedules ở campaign; ABO → ở từng ad set. Meta rỗng → xoá bản gương.
   * KHÔNG throw (được gọi trong luồng sync) — lỗi chỉ log.
   */
  async reconcileFromMeta(campaignId: string): Promise<void> {
    try {
      const campaign = await this.prisma.campaign.findUnique({
        where: { id: campaignId },
        select: {
          id: true,
          name: true,
          accountId: true,
          dailyBudget: true,
          lifetimeBudget: true,
        },
      });
      if (!campaign) return;

      const isCbo = !!(campaign.dailyBudget || campaign.lifetimeBudget);
      const level: 'CAMPAIGN' | 'ADSET' = isCbo ? 'CAMPAIGN' : 'ADSET';

      FacebookAdsApi.init(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

      // anyEnabled = có ít nhất 1 đối tượng đang BẬT lịch (is_budget_schedule_enabled)
      // → status bản gương ACTIVE/PAUSED phản chiếu đúng công tắc trên Meta.
      let anyEnabled = false;
      const entities: { id: string; name: string; schedules: MetaSchedule[] }[] =
        [];
      if (isCbo) {
        const scheds = await this.fetchSchedules('campaign', campaign.id);
        if (scheds.length) {
          entities.push({
            id: campaign.id,
            name: campaign.name ?? campaign.id,
            schedules: scheds,
          });
          if (await this.fetchEnabled('campaign', campaign.id)) anyEnabled = true;
        }
      } else {
        const adsets = await this.prisma.adSet.findMany({
          where: { campaignId: campaign.id },
          select: { id: true, name: true },
        });
        for (const a of adsets) {
          const scheds = await this.fetchSchedules('adset', a.id);
          if (scheds.length) {
            entities.push({ id: a.id, name: a.name ?? a.id, schedules: scheds });
            if (await this.fetchEnabled('adset', a.id)) anyEnabled = true;
          }
        }
      }

      const status: 'ACTIVE' | 'PAUSED' = anyEnabled ? 'ACTIVE' : 'PAUSED';
      const totalSchedules = entities.reduce((n, e) => n + e.schedules.length, 0);
      const existing = await this.prisma.campaignRule.findFirst({
        where: { campaignId: campaign.id, syncedFromMeta: true, deletedAt: null },
        select: { id: true },
      });

      // Meta không còn schedule → xoá bản gương nếu có.
      if (totalSchedules === 0) {
        if (existing)
          await this.prisma.campaignRule.delete({ where: { id: existing.id } });
        return;
      }

      const repPeriods = entities.flatMap((e) => e.schedules.map(metaToPeriod));

      let ruleId = existing?.id;
      let taskId: string;
      if (!ruleId) {
        const rule = await this.prisma.campaignRule.create({
          data: {
            name: 'Lịch tăng ngân sách (đồng bộ từ Meta)',
            level,
            status,
            accountId: campaign.accountId,
            campaignId: campaign.id,
            timezone: 'account',
            autoExecute: false,
            syncedFromMeta: true,
            tasks: {
              create: [
                {
                  kind: 'BUDGET_SCHEDULE_BUMP',
                  position: 0,
                  params: { periods: repPeriods },
                },
              ],
            },
          },
          include: { tasks: true },
        });
        ruleId = rule.id;
        taskId = rule.tasks[0].id;
        await this.prisma.campaignRuleTaskGroup.create({
          data: { taskId, rootForTaskId: taskId, operator: 'AND', position: 0 },
        });
      } else {
        await this.prisma.campaignRule.update({
          where: { id: ruleId },
          data: { level, status },
        });
        const task = await this.prisma.campaignRuleTask.findFirst({
          where: { ruleId },
          orderBy: { position: 'asc' },
          select: { id: true },
        });
        if (task) {
          taskId = task.id;
          await this.prisma.campaignRuleTask.update({
            where: { id: taskId },
            data: { params: { periods: repPeriods } },
          });
        } else {
          const created = await this.prisma.campaignRuleTask.create({
            data: {
              ruleId,
              kind: 'BUDGET_SCHEDULE_BUMP',
              position: 0,
              params: { periods: repPeriods },
            },
          });
          taskId = created.id;
        }
        // Làm mới lịch sử run của bản gương để phản ánh trạng thái Meta hiện tại.
        await this.prisma.campaignRuleRun.deleteMany({ where: { ruleId } });
      }

      const run = await this.prisma.campaignRuleRun.create({
        data: {
          ruleId,
          accountId: campaign.accountId,
          scheduledFor: new Date(),
          startedAt: new Date(),
          finishedAt: new Date(),
          status: 'COMPLETED',
          entitiesScanned: entities.length,
          matchedCount: entities.length,
        },
      });
      for (const e of entities) {
        await this.prisma.campaignRuleRunItem.create({
          data: {
            runId: run.id,
            taskId,
            taskKind: 'BUDGET_SCHEDULE_BUMP',
            level,
            entityId: e.id,
            entityName: e.name,
            status: 'EXECUTED',
            snapshot: {},
            changePreview: { periods: e.schedules.map(metaToPeriod) },
            metaBudgetScheduleIds: e.schedules.map((s) => String(s.id)),
            executedAt: new Date(),
          },
        });
      }
    } catch (err) {
      this.logger.warn(
        `reconcile budget schedules campaign ${campaignId} lỗi: ${(err as Error)?.message}`,
      );
    }
  }

  // Đọc cờ is_budget_schedule_enabled của entity (công tắc lịch trên Meta).
  private async fetchEnabled(
    kind: 'campaign' | 'adset',
    id: string,
  ): Promise<boolean> {
    try {
      const entity = kind === 'campaign' ? new Campaign(id) : new AdSet(id);
      const res = (await entity.get(['is_budget_schedule_enabled'])) as {
        is_budget_schedule_enabled?: boolean;
        _data?: { is_budget_schedule_enabled?: boolean };
      };
      return !!(
        res?.is_budget_schedule_enabled ??
        res?._data?.is_budget_schedule_enabled
      );
    } catch {
      // Không đọc được cờ → coi như đang bật (có schedule là chính) để không tắt nhầm.
      return true;
    }
  }

  private async fetchSchedules(
    kind: 'campaign' | 'adset',
    id: string,
  ): Promise<MetaSchedule[]> {
    try {
      // SDK types cho union Campaign|AdSet thiếu getBudgetSchedules → cast.
      const target: {
        getBudgetSchedules: (fields: string[], params: object) => Promise<unknown[]>;
      } = (kind === 'campaign' ? new Campaign(id) : new AdSet(id)) as never;
      const cursor = await target.getBudgetSchedules(
        ['id', 'time_start', 'time_end', 'budget_value', 'budget_value_type'],
        { limit: 100 },
      );
      return ((cursor as unknown as { _data?: MetaSchedule }[]) || []).map(
        (s) => (s as { _data?: MetaSchedule })._data ?? (s as unknown as MetaSchedule),
      );
    } catch {
      return [];
    }
  }
}
