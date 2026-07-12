import { BullModule } from '@nestjs/bull';
import { Global, Module } from '@nestjs/common';
import {
  DISTRIBUTED_LOCK_QUEUE,
  DistributedLockService,
} from './distributed-lock.service';

/**
 * Cung cấp DistributedLockService toàn ứng dụng (Global) để bất kỳ cron inline nào
 * cũng inject dùng chống double-run cross-replica mà không cần import module này.
 * Đăng ký một Bull queue rỗng chỉ để lấy kết nối Redis dùng chung.
 */
@Global()
@Module({
  imports: [BullModule.registerQueue({ name: DISTRIBUTED_LOCK_QUEUE })],
  providers: [DistributedLockService],
  exports: [DistributedLockService],
})
export class DistributedLockModule {}
