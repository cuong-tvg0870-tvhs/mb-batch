import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class DraftCleanupScheduler {
  private readonly logger = new Logger(DraftCleanupScheduler.name);

  constructor(private readonly prisma: PrismaService) {}

  // Run everyday at 2:00 AM Asia/Ho_Chi_Minh timezone
  @Cron('0 2 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async cleanupOldDrafts() {
    this.logger.log('Starting cleanup of old draft campaigns...');

    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

    try {
      // Find all draft campaigns (meta_id is null) that have not been updated in the last 7 days
      // and are not used as templates
      const campaignsToDelete = await this.prisma.systemCampaign.findMany({
        where: {
          meta_id: null,
          updatedAt: { lt: sevenDaysAgo },
          templateCampaigns: {
            none: {},
          },
        },
        select: { id: true },
      });

      const campaignIds = campaignsToDelete.map((c) => c.id);

      if (campaignIds.length === 0) {
        this.logger.log('No old draft campaigns found for cleanup.');
        return;
      }

      this.logger.log(`Found ${campaignIds.length} draft campaigns to clean up.`);

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

        this.logger.log(
          `Successfully deleted ${deleteResult.count} old draft campaigns.`,
        );
      });
    } catch (error) {
      this.logger.error('Failed to cleanup old draft campaigns:', error);
    }
  }
}
