import { Injectable, Logger } from '@nestjs/common';
import { AdSet, Campaign, FacebookAdsApi } from 'facebook-nodejs-business-sdk';
import { PrismaService } from '../prisma/prisma.service';
import { normalizeAccountTz, toAccountWallClock } from './campaign-rule-tz.util';

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

// Meta trả ISO có tz (…+0000)/unix → wall-clock "YYYY-MM-DDTHH:mm" theo múi giờ TKQC.
function metaToPeriod(s: MetaSchedule, tz: string): Period {
  return {
    timeStart: toAccountWallClock(s.time_start, tz),
    timeEnd: toAccountWallClock(s.time_end, tz),
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
   * theo TỪNG SCOPE: CBO → 1 bản ghi cấp campaign; ABO → 1 bản ghi RIÊNG cho MỖI
   * nhóm quảng cáo (mỗi nhóm lịch riêng, giống Meta). Dùng chung bản ghi mà user
   * "gửi ngay" tạo — KHÔNG tạo bản gương thứ hai.
   * An toàn: nếu ĐỌC Meta lỗi ở scope nào thì bỏ qua scope đó (không xoá nhầm).
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
      FacebookAdsApi.init(process.env.SDK_FACEBOOK_ACCESS_TOKEN!);

      // Múi giờ TKQC — dùng khi quy đổi mốc giờ Meta ↔ wall-clock lưu DB.
      const tz = await this.getAccountTz(campaign.accountId);

      if (isCbo) {
        await this.syncScope(campaign.accountId, campaign.id, null, 'CAMPAIGN', {
          kind: 'campaign',
          id: campaign.id,
          name: campaign.name ?? campaign.id,
        }, tz);
      } else {
        const adsets = await this.prisma.adSet.findMany({
          where: { campaignId: campaign.id },
          select: { id: true, name: true },
        });
        for (const a of adsets) {
          await this.syncScope(campaign.accountId, campaign.id, a.id, 'ADSET', {
            kind: 'adset',
            id: a.id,
            name: a.name ?? a.id,
          }, tz);
        }
      }
    } catch (err) {
      this.logger.warn(
        `reconcile budget schedules campaign ${campaignId} lỗi: ${(err as Error)?.message}`,
      );
    }
  }

  // Đồng bộ lịch của MỘT scope (campaign hoặc 1 nhóm) từ Meta vào đúng 1 bản ghi.
  private async syncScope(
    accountId: string,
    campaignId: string,
    adSetId: string | null,
    level: 'CAMPAIGN' | 'ADSET',
    entity: { kind: 'campaign' | 'adset'; id: string; name: string },
    tz: string,
  ): Promise<void> {
    const { schedules, failed } = await this.fetchSchedules(entity.kind, entity.id);
    if (failed) return; // đọc lỗi → giữ nguyên DB scope này

    const canonical = await this.findOrCollapseCanonical(campaignId, adSetId);

    // Meta không còn schedule: bản gương thuần → xoá; bản do user tạo → chỉ TẮT.
    if (schedules.length === 0) {
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

    const enabled = await this.fetchEnabled(entity.kind, entity.id);
    const status: 'ACTIVE' | 'PAUSED' = enabled ? 'ACTIVE' : 'PAUSED';
    const repPeriods = schedules.map((s) => metaToPeriod(s, tz));

    let ruleId = canonical?.id;
    let taskId: string;
    if (!ruleId) {
      const rule = await this.prisma.campaignRule.create({
        data: {
          name: adSetId
            ? `Lịch tăng ngân sách nhóm "${entity.name}" (đồng bộ từ Meta)`
            : 'Lịch tăng ngân sách (đồng bộ từ Meta)',
          level,
          status,
          accountId,
          campaignId,
          adSetId,
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
        accountId,
        scheduledFor: new Date(),
        startedAt: new Date(),
        finishedAt: new Date(),
        status: 'COMPLETED',
        entitiesScanned: 1,
        matchedCount: 1,
      },
    });
    await this.prisma.campaignRuleRunItem.create({
      data: {
        runId: run.id,
        taskId,
        taskKind: 'BUDGET_SCHEDULE_BUMP',
        level,
        entityId: entity.id,
        entityName: entity.name,
        status: 'EXECUTED',
        snapshot: {},
        changePreview: { periods: repPeriods },
        metaBudgetScheduleIds: schedules.map((s) => String(s.id)),
        executedAt: new Date(),
      },
    });
  }

  // Múi giờ IANA của TKQC (Account.timezone) — mặc định Asia/Ho_Chi_Minh nếu chưa lưu.
  private async getAccountTz(accountId: string): Promise<string> {
    const account = await this.prisma.account.findUnique({
      where: { id: accountId },
      select: { timezone: true },
    });
    return normalizeAccountTz(account?.timezone);
  }

  // Bản ghi lịch DUY NHẤT (không điều kiện) của 1 scope (campaign nếu adSetId null,
  // hoặc 1 nhóm nếu có adSetId); nhiều bản trùng → giữ bản do user tạo/cũ nhất, xoá
  // mềm phần dư.
  private async findOrCollapseCanonical(campaignId: string, adSetId: string | null) {
    const rows = await this.prisma.campaignRule.findMany({
      where: { campaignId, adSetId, schedule: { is: null }, deletedAt: null },
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
