import { Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { PlanningStatus, PlanningType } from '@prisma/client';
import { Job } from 'bull';
import { PrismaService } from 'src/modules/prisma/prisma.service';
import { TaskService } from './task.service';

@Processor('meta-sync')
export class TaskProcessor {
  private readonly logger = new Logger(TaskProcessor.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly taskService: TaskService,
  ) {}

  @Process('run-planning')
  async handle(job: Job<{ planningId: string }>) {
    const { planningId } = job.data;
    this.logger.log(`▶️ Run planning ${planningId}`);

    /**
     * 1️⃣ Load plan
     */
    const plan = await this.prisma.planning.findUnique({
      where: { id: planningId },
    });

    // 🛑 bị xoá / disable khi đang WAITING
    if (!plan || !plan.enabled) {
      this.logger.warn(`⏭ Skip planning ${planningId} (disabled / deleted)`);
      return;
    }

    /**
     * 2️⃣ Lock WAITING / FAILED → RUNNING
     */
    const locked = await this.prisma.planning.updateMany({
      where: {
        id: planningId,
        enabled: true,
        status: {
          in: [PlanningStatus.WAITING, PlanningStatus.FAILED],
        },
      },
      data: {
        status: PlanningStatus.RUNNING,
        lastRunAt: new Date(),
      },
    });

    if (!locked.count) {
      this.logger.warn(
        `⏭ Skip planning ${planningId} (state changed / already running)`,
      );
      return;
    }

    /**
     * 3️⃣ Execute theo type
     */
    try {
      switch (plan.type) {
        case PlanningType.SYNC_CAMPAIGN:
          await this.taskService.syncCampaign(plan);
          break;

        case PlanningType.SYNC_INSIGHT:
          await this.taskService.syncInsight(plan);
          break;

        case PlanningType.RULE_CAMPAIGN:
          await this.taskService.autoToggleCampaign(plan);
          break;

        default:
          this.logger.warn(`⚠️ Unknown planning type ${plan.type}`);
      }

      /**
       * 4️⃣ Done → RUNNING → IDLE
       * Nếu plan đã bị disable trong lúc chạy → không schedule tiếp
       */
      const freshPlan = await this.prisma.planning.findUnique({
        where: { id: planningId },
        select: { enabled: true, schedule: true },
      });

      await this.prisma.planning.update({
        where: { id: planningId },
        data: {
          status: PlanningStatus.IDLE,
          nextRunAt: freshPlan?.enabled
            ? this.taskService.calculateNextRun(plan.schedule)
            : null,
          // finishedAt: new Date(), // nếu có field
        },
      });

      this.logger.log(`✅ Planning ${planningId} done`);
    } catch (error) {
      /**
       * 5️⃣ Error → RUNNING → FAILED
       */
      await this.prisma.planning.update({
        where: { id: planningId },
        data: {
          status: PlanningStatus.FAILED,
          // lastError: String(error?.message || error),
        },
      });

      this.logger.error(
        `❌ Planning ${planningId} failed`,
        error?.stack || error,
      );

      // ⚠️ throw để Bull retry
      throw error;
    }
  }
}
