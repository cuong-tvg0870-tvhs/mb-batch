import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { GeminiApiKeyManager } from './gemini-api-key-manager.service';
import { HelpAiScheduler } from './help-ai.scheduler';
import { HelpAiService } from './help-ai.service';

@Module({
  imports: [PrismaModule],
  providers: [HelpAiService, HelpAiScheduler, GeminiApiKeyManager],
  exports: [HelpAiService],
})
export class HelpAiModule {}
