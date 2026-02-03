import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { PlanningStatus } from '@prisma/client';
import { Queue } from 'bull';
import { PrismaService } from 'src/modules/prisma/prisma.service';
import { TaskService } from './task.service';

@Injectable()
export class TaskCron {
  private readonly logger = new Logger(TaskCron.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly taskService: TaskService,
    @InjectQueue('meta-sync')
    private readonly queue: Queue,
  ) {}

  /**
   * Scan planning & enqueue job đúng giờ
   * IDLE → WAITING
   */
  @Cron(CronExpression.EVERY_MINUTE)
  async scanPlanning() {
    const now = Date.now();
    const lookAheadMs = 60_000;

    this.logger.log('⏰ Scan planning...');

    const plans = await this.prisma.planning.findMany({
      where: {
        enabled: true,
        status: { in: [PlanningStatus.IDLE, PlanningStatus.FAILED] },
        nextRunAt: {
          lte: new Date(now + lookAheadMs),
        },
      },
    });

    for (const plan of plans) {
      const jobId = `planning:${plan.id}`;
      const delay = Math.max(plan.nextRunAt.getTime() - now, 0);

      const existingJob = await this.queue.getJob(jobId);
      if (existingJob) continue;

      const locked = await this.prisma.planning.updateMany({
        where: {
          id: plan.id,
          enabled: true,
          status: PlanningStatus.IDLE,
        },
        data: {
          status: PlanningStatus.WAITING,
          lastRunAt: new Date(),
        },
      });

      if (!locked.count) continue;

      await this.queue.add(
        'run-planning',
        { planningId: plan.id },
        {
          jobId,
          delay,
          attempts: 3,
          backoff: { type: 'exponential', delay: 5000 },
          removeOnComplete: true,
        },
      );

      this.logger.log(`📤 Enqueued planning ${plan.id} (delay=${delay})`);
    }
  }

  /**
   * Reconcile zombie state
   * RUNNING / WAITING nhưng không có job
   */
  @Cron(CronExpression.EVERY_5_SECONDS)
  async reconcilePlanning() {
    const now = new Date();
    this.logger.log('🧹 Reconcile planning state...');

    const plans = await this.prisma.planning.findMany({
      where: {
        enabled: true,
        OR: [
          {
            status: {
              in: [PlanningStatus.WAITING, PlanningStatus.RUNNING],
            },
          },
          { status: PlanningStatus.IDLE, nextRunAt: { lte: now } },
        ],
      },
    });

    for (const plan of plans) {
      const jobId = `planning:${plan.id}`;
      const job = await this.queue.getJob(jobId);

      // CASE 1: zombie WAITING / RUNNING
      if (
        plan.status === PlanningStatus.WAITING ||
        plan.status === PlanningStatus.RUNNING
      ) {
        if (!job) {
          await this.prisma.planning.update({
            where: { id: plan.id },
            data: {
              status: PlanningStatus.IDLE,
              nextRunAt:
                plan.nextRunAt && plan.nextRunAt > now
                  ? plan.nextRunAt
                  : this.taskService.calculateNextRun(plan.schedule),
            },
          });

          this.logger.warn(`🧟 Reset planning ${plan.id} → IDLE`);
        }
        continue;
      }

      // CASE 2: IDLE overdue
      if (
        plan.status === PlanningStatus.IDLE &&
        plan.nextRunAt <= now &&
        !job
      ) {
        const locked = await this.prisma.planning.updateMany({
          where: {
            id: plan.id,
            enabled: true,
            status: PlanningStatus.IDLE,
          },
          data: { status: PlanningStatus.WAITING },
        });

        if (!locked.count) continue;

        await this.queue.add(
          'run-planning',
          { planningId: plan.id },
          {
            jobId,
            delay: 0,
            removeOnComplete: true,
          },
        );

        this.logger.log(`⏰ Re-enqueue overdue planning ${plan.id}`);
      }
    }
  }
}
