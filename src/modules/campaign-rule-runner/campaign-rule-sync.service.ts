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
   * Kéo budget schedules hiện tại của campaign từ Meta và đồng bộ vào bản ghi lịch
   * DUY NHẤT của campaign (cùng bản ghi mà user "gửi ngay" dùng — KHÔNG tạo bản
   * gương thứ hai). CBO → schedules ở campaign; ABO → ở từng ad set.
   * An toàn: nếu ĐỌC Meta lỗi thì KHÔNG đụng dữ liệu (tránh xoá nhầm lịch user).
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
      // → status ACTIVE/PAUSED phản chiếu đúng công tắc trên Meta.
      let anyEnabled = false;
      let readFailed = false;
      const entities: { id: string; name: string; schedules: MetaSchedule[] }[] =
        [];
      const collect = async (
        kind: 'campaign' | 'adset',
        id: string,
        name: string,
      ) => {
        const { schedules, failed } = await this.fetchSchedules(kind, id);
        if (failed) {
          readFailed = true;
          return;
        }
        if (schedules.length) {
          entities.push({ id, name, schedules });
          if (await this.fetchEnabled(kind, id)) anyEnabled = true;
        }
      };
      if (isCbo) {
        await collect('campaign', campaign.id, campaign.name ?? campaign.id);
      } else {
        const adsets = await this.prisma.adSet.findMany({
          where: { campaignId: campaign.id },
          select: { id: true, name: true },
        });
        for (const a of adsets) await collect('adset', a.id, a.name ?? a.id);
      }

      // Đọc Meta lỗi ở bất kỳ đối tượng nào → bỏ qua vòng này, giữ nguyên DB.
      if (readFailed) return;

      const status: 'ACTIVE' | 'PAUSED' = anyEnabled ? 'ACTIVE' : 'PAUSED';
      const totalSchedules = entities.reduce((n, e) => n + e.schedules.length, 0);

      // Bản ghi lịch DUY NHẤT (mọi bản không-điều-kiện) + gộp trùng nếu lỡ có nhiều.
      const canonical = await this.findOrCollapseCanonical(campaign.id);

      // Meta không còn schedule: bản gương thuần → xoá; bản do user tạo → chỉ TẮT
      // (giữ record + khung để không mất dữ liệu người dùng).
      if (totalSchedules === 0) {
        if (canonical) {
          if (canonical.syncedFromMeta) {
            await this.prisma.campaignRule.update({
              where: { id: canonical.id },
              data: { deletedAt: new Date() },
            });
          } else {
            await this.prisma.campaignRule.update({
              where: { id: canonical.id },
              data: { status: 'PAUSED' },
            });
          }
        }
        return;
      }

      const repPeriods = entities.flatMap((e) => e.schedules.map(metaToPeriod));

      let ruleId = canonical?.id;
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
        // Giữ nguyên "nguồn gốc" (syncedFromMeta) của bản ghi user — chỉ cập nhật
        // cấp + trạng thái theo Meta.
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
        // Làm mới lịch sử run để phản ánh trạng thái Meta hiện tại.
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

  // Bản ghi lịch DUY NHẤT (không điều kiện) của campaign; nếu có nhiều bản trùng
  // → giữ bản do user tạo (syncedFromMeta=false) rồi tới bản cũ nhất, xoá mềm phần dư.
  private async findOrCollapseCanonical(campaignId: string) {
    const rows = await this.prisma.campaignRule.findMany({
      where: { campaignId, schedule: { is: null }, deletedAt: null },
      orderBy: [{ syncedFromMeta: 'asc' }, { createdAt: 'asc' }],
      select: { id: true, syncedFromMeta: true },
    });
    if (rows.length === 0) return null;
    const [keep, ...extras] = rows;
    if (extras.length) {
      await this.prisma.campaignRule.updateMany({
        where: { id: { in: extras.map((e) => e.id) } },
        data: { deletedAt: new Date() },
      });
    }
    return keep;
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

  // Đọc budget schedules của 1 target. `failed=true` khi gọi Meta lỗi (phân biệt
  // với "không có schedule") để luồng reconcile không xoá nhầm dữ liệu.
  private async fetchSchedules(
    kind: 'campaign' | 'adset',
    id: string,
  ): Promise<{ schedules: MetaSchedule[]; failed: boolean }> {
    try {
      // SDK types cho union Campaign|AdSet thiếu getBudgetSchedules → cast.
      const target: {
        getBudgetSchedules: (fields: string[], params: object) => Promise<unknown[]>;
      } = (kind === 'campaign' ? new Campaign(id) : new AdSet(id)) as never;
      const cursor = await target.getBudgetSchedules(
        ['id', 'time_start', 'time_end', 'budget_value', 'budget_value_type'],
        { limit: 100 },
      );
      const schedules = ((cursor as unknown as { _data?: MetaSchedule }[]) || []).map(
        (s) => (s as { _data?: MetaSchedule })._data ?? (s as unknown as MetaSchedule),
      );
      return { schedules, failed: false };
    } catch (err) {
      this.logger.warn(
        `đọc budget schedules ${kind} ${id} lỗi: ${(err as Error)?.message}`,
      );
      return { schedules: [], failed: true };
    }
  }
}
