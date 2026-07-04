import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { AppConfigReader } from '../app-config/app-config.reader';
import { BatchRunLoggerService } from '../batch-run-log/batch-run-logger.service';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class DraftCleanupScheduler {
  private readonly logger = new Logger(DraftCleanupScheduler.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly batchRunLogger: BatchRunLoggerService,
    private readonly appConfig: AppConfigReader,
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

          // Số ngày giữ nháp cấu hình runtime (SystemConfig draft_cleanup_days, mặc định 7).
          const cleanupDays = await this.appConfig.getNumber(
            'draft_cleanup_days',
            7,
          );
          const cutoff = new Date();
          cutoff.setDate(cutoff.getDate() - cleanupDays);

          // Find all draft campaigns (meta_id is null) chưa cập nhật trong cleanupDays ngày
          // và không dùng làm template. Loại trừ draft đang publish dở.
          const candidates = await this.prisma.systemCampaign.findMany({
            where: {
              meta_id: null,
              isPublishing: false,
              updatedAt: { lt: cutoff },
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

          // A9: KHÔNG xoá nháp do tự động hóa sinh ra mà VẪN được tham chiếu — để
          // (a) link "xem nháp đã tạo" trong lịch sử không hỏng, và (b) giữ nháp
          // DRAFT_ONLY đang chờ người duyệt / nháp đang gom dở của automation đang chạy.
          let cleanableIds = campaignIds;
          if (campaignIds.length > 0) {
            const [referencedByHistory, inProgress] = await Promise.all([
              this.prisma.draftAutomationHistory.findMany({
                where: { generatedCampaignId: { in: cleanableIds } },
                select: { generatedCampaignId: true },
              }),
              this.prisma.draftAutomation.findMany({
                where: {
                  inProgressDraftId: { in: cleanableIds },
                  deletedAt: null,
                },
                select: { inProgressDraftId: true },
              }),
            ]);
            const keep = new Set<string>(
              [
                ...referencedByHistory.map((h) => h.generatedCampaignId),
                ...inProgress.map((a) => a.inProgressDraftId),
              ].filter((id): id is string => !!id),
            );
            cleanableIds = campaignIds.filter((id) => !keep.has(id));
            const skipped = campaignIds.length - cleanableIds.length;
            if (skipped > 0) {
              this.logger.log(
                `Giữ lại ${skipped} nháp tự động còn tham chiếu (lịch sử / đang gom dở) — không xoá.`,
              );
            }
          }

          if (cleanableIds.length === 0) {
            this.logger.log('No old draft campaigns found for cleanup.');
            ctx.skip('No old draft campaigns found for cleanup.');
            return;
          }

          this.logger.log(
            `Found ${cleanableIds.length} draft campaigns to clean up.`,
          );

          await this.prisma.$transaction(async (tx) => {
            // 1. Delete creatives belonging to ads in these campaigns
            await tx.systemCreative.deleteMany({
              where: {
                ad: {
                  adSet: {
                    campaignId: { in: cleanableIds },
                  },
                },
              },
            });

            // 2. Delete ads in these campaigns
            await tx.systemAd.deleteMany({
              where: {
                adSet: {
                  campaignId: { in: cleanableIds },
                },
              },
            });

            // 3. Delete adsets in these campaigns
            await tx.systemAdSet.deleteMany({
              where: {
                campaignId: { in: cleanableIds },
              },
            });

            // 4. Delete campaigns
            const deleteResult = await tx.systemCampaign.deleteMany({
              where: {
                id: { in: cleanableIds },
              },
            });

            ctx.setTotal(cleanableIds.length);
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
