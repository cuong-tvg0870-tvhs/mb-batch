import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class DraftCleanupScheduler {
  private readonly logger = new Logger(DraftCleanupScheduler.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly batchRunLogger: BatchRunLoggerService,
  ) {}

  // Run everyday at 2:00 AM Asia/Ho_Chi_Minh timezone
  @Cron('0 2 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async cleanupOldDrafts() {
    try {
      await this.batchRunLogger.track(
        'CLEANUP_OLD_DRAFTS',
        'draft-automation',
        async (ctx) => {
          this.logger.log('Starting cleanup of old draft campaigns...');

          const sevenDaysAgo = new Date();
          sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

          // Find all draft campaigns (meta_id is null) that have not been updated in the last 7 days
          // and are not used as templates. Loại trừ draft đang publish dở.
          const candidates = await this.prisma.systemCampaign.findMany({
            where: {
              meta_id: null,
              isPublishing: false,
              updatedAt: { lt: sevenDaysAgo },
              templateCampaigns: {
                none: {},
              },
            },
            select: {
              id: true,
              publishHistories: { select: { steps: true } },
            },
          });

          // An toàn orphan: nếu một publishHistory có bước 'campaign' kèm metaId thì
          // chiến dịch ĐÃ được tạo trên Meta dù cột meta_id đang null (crash giữa
          // createCampaign và ghi DB). Xoá CỨNG sẽ mất dấu vĩnh viễn campaign đang
          // tiêu tiền → giữ lại để xử lý thủ công, chỉ xoá các draft không có dấu vết Meta.
          const orphanIds: string[] = [];
          const campaignIds = candidates
            .filter((c) => {
              const hasMetaFootprint = (c.publishHistories || []).some(
                (h) =>
                  Array.isArray(h.steps) &&
                  (h.steps as any[]).some(
                    (s) => s?.key === 'campaign' && s?.metaId,
                  ),
              );
              if (hasMetaFootprint) {
                orphanIds.push(c.id);
                return false;
              }
              return true;
            })
            .map((c) => c.id);

          if (orphanIds.length > 0) {
            this.logger.warn(
              `Bỏ qua xoá ${orphanIds.length} draft nghi orphan (đã tạo campaign Meta nhưng thiếu meta_id): ${orphanIds.join(', ')}`,
            );
          }

          if (campaignIds.length === 0) {
            this.logger.log('No old draft campaigns found for cleanup.');
            ctx.skip('No old draft campaigns found for cleanup.');
            return;
          }

          this.logger.log(
            `Found ${campaignIds.length} draft campaigns to clean up.`,
          );

          await this.prisma.$transaction(async (tx) => {
            // 1. Delete creatives belonging to ads in these campaigns
            await tx.systemCreative.deleteMany({
              where: {
                ad: {
                  adSet: {
                    campaignId: { in: campaignIds },
                  },
                },
              },
            });

            // 2. Delete ads in these campaigns
            await tx.systemAd.deleteMany({
              where: {
                adSet: {
                  campaignId: { in: campaignIds },
                },
              },
            });

            // 3. Delete adsets in these campaigns
            await tx.systemAdSet.deleteMany({
              where: {
                campaignId: { in: campaignIds },
              },
            });

            // 4. Delete campaigns
            const deleteResult = await tx.systemCampaign.deleteMany({
              where: {
                id: { in: campaignIds },
              },
            });

            ctx.setTotal(campaignIds.length);
            ctx.addSuccess(deleteResult.count);
            this.logger.log(
              `Successfully deleted ${deleteResult.count} old draft campaigns.`,
            );
          });
        },
      );
    } catch (error) {
      this.logger.error('Failed to cleanup old draft campaigns:', error);
    }
  }
}
