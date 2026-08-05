import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import axios from 'axios';
import * as http from 'http';
import * as https from 'https';
import { AppModule } from './app.module';

// Meta Graph API qua facebook-nodejs-business-sdk gọi axios(options) mà KHÔNG set
// timeout (node_modules/facebook-nodejs-business-sdk/src/http.js) → mặc định axios
// timeout=0 (vô hạn). Khi socket chết nửa chừng (ECONNRESET/socket hang up/DNS) mà
// không có response, request treo tới khi OS tự kill TCP (~2 phút) — đứng luôn cả
// tick của rule runner (*/5) chạy trong cùng process. SDK KHÔNG có axios riêng
// trong node_modules của nó (đã kiểm tra: không có node_modules/axios lồng bên
// trong facebook-nodejs-business-sdk) nên `require('axios')` của SDK và của app
// cùng resolve về 1 instance singleton ở root — set default ở đây có hiệu lực cho
// cả các call Graph API bên trong SDK.
axios.defaults.timeout = 30000;

// Bật keep-alive cho cả 2 module http/https: nếu không, mỗi request ra Meta Graph
// (rule runner, insight-sync, media-sync...) đều bắt tay TCP+TLS mới → tốn thời
// gian + dễ dồn cục vào phút cron chẵn. keepAlive tái dùng socket giữa các request.
// maxSockets=128: trần chủ động tránh cạn ephemeral port, cao hơn nhu cầu thực tế của job song song.
// (ép any: TS coi `globalAgent` trên namespace import là binding chỉ-đọc, Node cho phép gán.)
(http as any).globalAgent = new http.Agent({
  keepAlive: true,
  keepAliveMsecs: 30000,
  maxSockets: 128,
});
(https as any).globalAgent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 30000,
  maxSockets: 128,
});

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
