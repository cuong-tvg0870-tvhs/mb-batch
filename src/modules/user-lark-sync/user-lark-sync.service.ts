import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import {
  LarkContactService,
  larkContactToUserData,
} from './lark-contact.service';

/**
 * Đồng bộ định kỳ User ↔ danh bạ Lark (cron). Quét mọi user chưa xoá, tra Lark
 * theo email, cập nhật avatar/mã NV/open_id... Chạy tuần tự để nhẹ rate-limit.
 *
 * ⚠️ PARITY: logic tương đương UserService.backfillFromLark ở mb-ads.
 */
@Injectable()
export class UserLarkSyncService {
  private readonly logger = new Logger(UserLarkSyncService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly larkContact: LarkContactService,
  ) {}

  async syncAll() {
    const users = await this.prisma.user.findMany({
      where: { deletedAt: null },
      select: { id: true, email: true },
    });

    // Nạp danh bạ MỘT LẦN (fresh) rồi khớp cục bộ (email = enterprise_email).
    const directory = await this.larkContact.getDirectoryMap(true);

    const syncedAt = new Date();
    let updated = 0;
    let notFound = 0;
    let failed = 0;

    for (const u of users) {
      const lark = directory.get((u.email || '').trim().toLowerCase());
      if (!lark) {
        notFound++;
        continue;
      }
      try {
        await this.prisma.user.update({
          where: { id: u.id },
          data: larkContactToUserData(lark, syncedAt),
        });
        updated++;
      } catch (e: any) {
        failed++;
        this.logger.warn(
          `Sync Lark thất bại cho ${u.email}: ${String(e?.message || e)}`,
        );
      }
    }

    const summary = {
      total: users.length,
      updated,
      notFound,
      failed,
    };
    this.logger.log(`✅ User↔Lark sync xong: ${JSON.stringify(summary)}`);
    return summary;
  }
}
