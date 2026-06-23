import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { HelpAiScheduler } from './help-ai.scheduler';
import { HelpAiService } from './help-ai.service';

@Module({
  imports: [PrismaModule],
  providers: [HelpAiService, HelpAiScheduler],
  exports: [HelpAiService],
})
export class HelpAiModule {}
