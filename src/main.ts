import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // Graceful shutdown: khi container nhận SIGTERM/SIGINT (mỗi lần DEPLOY LẠI),
  // NestFactory sẽ gọi onModuleDestroy trên MỌI provider trước khi process thoát:
  //   - Bull `queue.close()` CHỜ job đang chạy hoàn tất rồi mới đóng kết nối Redis.
  //   - PrismaService.$disconnect() đóng pool Postgres sạch sẽ.
  // Thiếu dòng này thì SIGTERM làm node thoát NGAY, cắt ngang mọi job đang chạy dở.
  // Lưu ý: orchestrator vẫn có grace-period; job quá dài có thể bị SIGKILL — nhưng
  // job qua Bull sẽ được re-deliver/retry khi container mới lên nên không mất việc.
  app.enableShutdownHooks();

  const port = Number(process.env.PORT) || 3000;
  await app.listen(port);
  Logger.log(`===== mb-batch worker running on ${port} =====`, 'Bootstrap');
}
bootstrap();
