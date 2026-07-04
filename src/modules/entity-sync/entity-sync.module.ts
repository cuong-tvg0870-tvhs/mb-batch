import { BullModule } from '@nestjs/bull';
import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { ENTITY_SYNC_QUEUE } from './entity-sync.constants';
import { EntitySyncProcessor } from './entity-sync.processor';
import { EntitySyncScheduler } from './entity-sync.scheduler';
import { EntitySyncService } from './entity-sync.service';

@Module({
  imports: [
    PrismaModule,
    BullModule.registerQueue({ name: ENTITY_SYNC_QUEUE }),
  ],
  providers: [EntitySyncService, EntitySyncProcessor, EntitySyncScheduler],
  exports: [EntitySyncService],
})
export class EntitySyncModule {}
