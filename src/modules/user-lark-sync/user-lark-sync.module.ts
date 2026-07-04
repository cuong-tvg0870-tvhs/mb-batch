import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { LarkContactService } from './lark-contact.service';
import { UserLarkSyncScheduler } from './user-lark-sync.scheduler';
import { UserLarkSyncService } from './user-lark-sync.service';

@Module({
  imports: [PrismaModule],
  providers: [LarkContactService, UserLarkSyncService, UserLarkSyncScheduler],
  exports: [UserLarkSyncService],
})
export class UserLarkSyncModule {}
