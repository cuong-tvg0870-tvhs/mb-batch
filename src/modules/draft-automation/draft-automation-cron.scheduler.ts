import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron, SchedulerRegistry } from '@nestjs/schedule';
import { CronJob } from 'cron';
import { PrismaService } from '../prisma/prisma.service';
import { DraftAutomationScheduler } from './draft-automation.scheduler';

const JOB_PREFIX = 'draft-automation-template';
const SCHEDULE_SCAN_CRON = '*/30 * * * *';
const NEXT_RUN_DELAY_MINUTES = 30;

function parseValidDate(value: any): Date | undefined {
  if (!value) return undefined;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? undefined : date;
}

@Injectable()
export class DraftAutomationCronScheduler implements OnModuleInit {
  private readonly logger = new Logger(DraftAutomationCronScheduler.name);
  private readonly scheduledRuns = new Map<string, string>();

  constructor(
    private readonly prisma: PrismaService,
    private readonly schedulerRegistry: SchedulerRegistry,
    private readonly runner: DraftAutomationScheduler,
  ) {}

  async onModuleInit() {
    this.logger.log('DraftAutomationCronScheduler initialized.');
    await this.reconcileAutomationSchedules();
  }

  @Cron(SCHEDULE_SCAN_CRON)
  async reconcileAutomationSchedules() {
    const templates = await this.prisma.templateCampaign.findMany({
      where: { deletedAt: null },
    });
    const activeTemplateIds = new Set<string>();

    for (const template of templates) {
      const automation = this.runner.normalizeAutomation(
        (template.data as any)?.automation,
      );
      const jobName = this.getJobName(template.id);

      if (
        automation?.enabled !== true ||
        !automation?.folderId ||
        automation?.status === 'PAUSED' ||
        automation?.status === 'DISABLED' ||
        automation?.status === 'COMPLETED'
      ) {
        this.unschedule(jobName);
        continue;
      }

      const nextRunAt = parseValidDate(automation.nextRunAt);
      if (!nextRunAt) {
        this.unschedule(jobName);
        continue;
      }

      activeTemplateIds.add(template.id);
      if (
        this.hasJob(jobName) &&
        automation.status === 'SCHEDULED' &&
        automation.nextRunAt === this.scheduledRuns.get(jobName)
      ) {
        continue;
      }

      await this.scheduleTemplate(template.id, nextRunAt, automation);
    }

    for (const jobName of [...this.scheduledRuns.keys()]) {
      const templateId = jobName.replace(`${JOB_PREFIX}-`, '');
      if (!activeTemplateIds.has(templateId)) {
        this.unschedule(jobName);
      }
    }
  }

  private getJobName(templateId: string) {
    return `${JOB_PREFIX}-${templateId}`;
  }

  private hasJob(jobName: string) {
    try {
      this.schedulerRegistry.getCronJob(jobName);
      return true;
    } catch {
      return false;
    }
  }

  private unschedule(jobName: string) {
    if (!this.hasJob(jobName)) {
      this.scheduledRuns.delete(jobName);
      return;
    }

    const job = this.schedulerRegistry.getCronJob(jobName);
    job.stop();
    this.schedulerRegistry.deleteCronJob(jobName);
    this.scheduledRuns.delete(jobName);
    this.logger.log(`Unscheduled ${jobName}`);
  }

  private getNextRunAt(from: Date) {
    return new Date(from.getTime() + NEXT_RUN_DELAY_MINUTES * 60 * 1000);
  }

  private async scheduleTemplate(
    templateId: string,
    nextRunAtInput: Date,
    automation: any,
  ) {
    const jobName = this.getJobName(templateId);
    this.unschedule(jobName);

    const nextRunAt =
      nextRunAtInput.getTime() <= Date.now()
        ? new Date(Date.now() + 1000)
        : nextRunAtInput;

    const job = new CronJob(nextRunAt, async () => {
      await this.runTemplateJob(templateId);
    });

    this.schedulerRegistry.addCronJob(jobName, job);
    this.scheduledRuns.set(jobName, nextRunAt.toISOString());
    job.start();

    await this.updateAutomationState(templateId, {
      ...automation,
      status: 'SCHEDULED',
      nextRunAt: nextRunAt.toISOString(),
      scheduledAt: new Date().toISOString(),
    });

    this.logger.log(
      `Scheduled draft automation template ${templateId} at ${nextRunAt.toISOString()}`,
    );
  }

  private async runTemplateJob(templateId: string) {
    const jobName = this.getJobName(templateId);
    this.unschedule(jobName);

    const template = await this.prisma.templateCampaign.findFirst({
      where: { id: templateId, deletedAt: null },
    });
    if (!template) return;

    const automation = this.runner.normalizeAutomation(
      (template.data as any)?.automation,
    );
    if (
      automation?.enabled !== true ||
      !automation?.folderId ||
      automation?.status === 'DISABLED' ||
      automation?.status === 'PAUSED'
    ) {
      await this.updateAutomationState(templateId, {
        ...automation,
        status: automation?.status === 'PAUSED' ? 'PAUSED' : 'DISABLED',
        nextRunAt: null,
      });
      return;
    }

    await this.updateAutomationState(templateId, {
      ...automation,
      status: 'RUNNING',
      nextRunAt: null,
      lastStartedAt: new Date().toISOString(),
    });

    await this.runner.processAutomation(templateId);

    const refreshed = await this.prisma.templateCampaign.findFirst({
      where: { id: templateId, deletedAt: null },
    });
    if (!refreshed) return;

    const refreshedAutomation = this.runner.normalizeAutomation(
      (refreshed.data as any)?.automation,
    );

    if (
      refreshedAutomation?.enabled !== true ||
      refreshedAutomation?.status === 'PAUSED' ||
      refreshedAutomation?.status === 'DISABLED' ||
      refreshedAutomation?.status === 'COMPLETED'
    ) {
      return;
    }

    await this.scheduleTemplate(
      templateId,
      this.getNextRunAt(new Date()),
      refreshedAutomation,
    );
  }

  private async updateAutomationState(templateId: string, automation: any) {
    const template = await this.prisma.templateCampaign.findUnique({
      where: { id: templateId },
    });
    if (!template) return;

    const data = ((template.data || {}) as any) || {};
    await this.prisma.templateCampaign.update({
      where: { id: templateId },
      data: {
        data: {
          ...data,
          automation,
        },
      },
    });
  }
}
