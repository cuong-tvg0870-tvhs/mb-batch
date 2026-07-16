import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { AdFatigueService } from './ad-fatigue.service';

@Module({
  imports: [PrismaModule],
  providers: [AdFatigueService],
  exports: [AdFatigueService],
})
export class AdFatigueModule {}
