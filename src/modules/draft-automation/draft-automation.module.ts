import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { DraftAutomationCronScheduler } from './draft-automation-cron.scheduler';
import { DraftAutomationMetaPublisherService } from './draft-automation-meta-publisher.service';
import { DraftAutomationScheduler } from './draft-automation.scheduler';
import { DraftCleanupScheduler } from './draft-cleanup.scheduler';

@Module({
  imports: [PrismaModule],
  controllers: [],
  providers: [
    DraftAutomationCronScheduler,
    DraftAutomationScheduler,
    DraftAutomationMetaPublisherService,
    DraftCleanupScheduler,
  ],
})
export class DraftAutomationModule {}
