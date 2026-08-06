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
//
// TUYỆT ĐỐI KHÔNG gán đè `http.globalAgent = new Agent(...)`.
// `import * as http` + esModuleInterop ⇒ tsc emit `__importStar(require('http'))`,
// tạo một BẢN SAO namespace mà `__createBinding` định nghĩa từng thuộc tính bằng
// `Object.defineProperty(..., { get })` — CHỈ CÓ GETTER. Gán vào bản sao đó ném
// `TypeError: Cannot set property globalAgent of #<Object> which has only a getter`
// NGAY LÚC NẠP MODULE, trước cả bootstrap, nên không log nào của app kịp chạy.
// (`#<Object>` chứ không phải `#<Module>` chính là dấu vết của bản sao này — bản
// thân Node vẫn cho gán `http.globalAgent` bình thường.)
// Không bẫy được ở máy dev: `tsc --noEmit` không THỰC THI, và local không ai chạy
// `dist/src/main.js`. Chỉ container mới nạp đúng file đó.
// Sự cố prod 05/08/2026: container mb-batch crash-loop 1102 lần, mb-batch chết 25 giờ
// (meta-sync + rule runner + automation) mà pipeline CI/CD vẫn báo xanh.
const KEEP_ALIVE = { keepAlive: true, keepAliveMsecs: 30000, maxSockets: 128 };

// Đường chính: mọi call Graph API đi qua instance axios singleton ở trên.
axios.defaults.httpAgent = new http.Agent(KEEP_ALIVE);
axios.defaults.httpsAgent = new https.Agent(KEEP_ALIVE);

// Phần còn lại (client MinIO, http thuần...) vẫn dùng agent mặc định của Node → sửa
// TẠI CHỖ trên instance sẵn có. `keepAlive`/`keepAliveMsecs` là property THẬT của
// Agent lúc chạy (constructor gán từ options, nên sửa `agent.options` sau đó vô tác
// dụng) nhưng @types/node không khai báo → phải nới kiểu.
// try/catch là BÀI HỌC từ chính sự cố trên: đây chỉ là tinh chỉnh hiệu năng, không
// đáng để làm chết cả service nếu một bản Node sau này khoá các thuộc tính này lại.
type TunableAgent = http.Agent & { keepAlive?: boolean; keepAliveMsecs?: number };
try {
  for (const agent of [http.globalAgent, https.globalAgent] as TunableAgent[]) {
    agent.keepAlive = KEEP_ALIVE.keepAlive;
    agent.keepAliveMsecs = KEEP_ALIVE.keepAliveMsecs;
    agent.maxSockets = KEEP_ALIVE.maxSockets;
  }
} catch (error) {
  console.warn(
    `Không chỉnh được keep-alive cho globalAgent (bỏ qua, axios đã có agent riêng): ${
      (error as Error)?.message ?? error
    }`,
  );
}

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
