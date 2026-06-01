import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { DraftAutomationMetaPublisherService } from './draft-automation-meta-publisher.service';
import { DraftAutomationScheduler } from './draft-automation.scheduler';

@Module({
  imports: [PrismaModule],
  providers: [DraftAutomationScheduler, DraftAutomationMetaPublisherService],
})
export class DraftAutomationModule {}
