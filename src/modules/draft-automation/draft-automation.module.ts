import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { DraftAutomationScheduler } from './draft-automation.scheduler';

@Module({
  imports: [PrismaModule],
  providers: [DraftAutomationScheduler],
})
export class DraftAutomationModule {}
