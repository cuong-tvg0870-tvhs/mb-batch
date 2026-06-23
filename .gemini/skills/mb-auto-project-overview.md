# MB Auto — Tổng Quan Dự Án (Project Knowledge Base)

> **Mục đích**: Cung cấp context tổng quan cho AI assistant khi làm việc ở cả 3 workspace. File này được đồng bộ ở cả 3 workspace.

---

## 1. Tổng Quan Hệ Thống

**MB Auto** là nền tảng quản lý quảng cáo Meta Ads nội bộ cho **Thành Vinh Holding (TVHS)**.
**Domain chính**: `https://ads.3fastvn.com`

### Kiến trúc 3 service:
```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│    mb-frontend      │     │      mb-ads          │     │     mb-batch         │
│   (Next.js 16)      │────▶│   (NestJS 11 API)    │     │  (NestJS 11 Worker)  │
│   Port: 3000        │     │   Port: 8000/3003     │     │   Port: 3030         │
│   UI Dashboard      │     │   REST API Backend    │     │   Cron & Queue Jobs   │
└─────────────────────┘     └──────────┬──────────┘     └──────────┬──────────┘
                                       │                            │
                            ┌──────────▼──────────┐     ┌──────────▼──────────┐
                            │   PostgreSQL DB      │     │   Redis (Bull Queue)  │
                            │   (Prisma ORM v7)    │     │   Port: 6379          │
                            └──────────────────────┘     └──────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
             Meta Graph API     Google Drive API    Lark Bitable API
              (v24.0)          (Service Account)    (REST API)
```

---

## 2. Các Service & Tech Stack Cốt Lõi

### A. MB-Frontend (Next.js 16)
- **Path**: `mb-frontend` (Port 3000)
- **Tech Stack**: Next.js 16 (App Router), React 19, Tailwind CSS v4, Radix/Shadcn, Zustand, SWR + Axios, React Hook Form + Zod.
- **Styling Rule**: Cấu hình Tailwind v4 theme trực tiếp trong file CSS (sử dụng `@theme`), KHÔNG dùng `tailwind.config.js`.
- **API Pattern**: Gọi `/api/*` -> API Route proxy -> Backend `BACKEND_INTERNAL_URL` (http://localhost:8000).
- **Zustand Stores**: `useAuthStore` (auth-storage), `useUIStore` (sidebar, theme), `useDataStore` (table references), `useSelectedAssetsStore` (campaign launching).

### B. MB-Ads (NestJS 11 Backend API)
- **Path**: `mb-ads` (Port 8000/3003)
- **Tech Stack**: NestJS 11, Prisma v7 (PostgreSQL), passport-jwt, facebook-nodejs-business-sdk (v24.0.1), googleapis.
- **Key Modules**:
  - `meta`: Core Meta SDK interaction (sync, publish, upload).
  - `draft-campaign` (94KB): CRUD drafts, templates, logic publish to Meta.
  - `data-sync`, `medias`, `drive`, `larkbase`, `data-export`, `export-presets`: Sync, export and manage media assets & insights (Google Sheets & Drive integration), and manage saved export presets.
  - `auth`, `user`, `permission` (RBAC), `accounts`, `project`, `account-proposal`: User management, access control & account allocation proposals.
  - `help`: Chatbot hướng dẫn sử dụng dashboard, lưu hội thoại và dùng AI provider key pool (Gemini/DeepSeek) trong DB để tự chuyển key khi quota/rate limit.
- **Auth Guard**: `@UseGuards(JwtAuthGuard)` + `@CurrentUser()`.
- **Export Endpoints**: `POST /cids/export-sheet` (Lark content records), `POST /insights/admin/creatives/export-sheet` (Creative performance insights), `POST /data-export/drive-sheet` (quản lý xuất động Insights và Creative Assets ra Google Sheet), và các API CRUD cho cấu hình xuất `/export-presets` (GET, POST, DELETE).

### C. MB-Batch (NestJS 11 Background Worker)
- **Path**: `mb-batch` (Port 3030)
- **Tech Stack**: NestJS 11, `@nestjs/schedule` (Cron), `@nestjs/bull` (Redis Queue), Prisma v7.
- **Flow**: Scheduler (@Cron) -> Bull Queue -> Processor -> Service. Timezone `Asia/Ho_Chi_Minh`. Concurrency control via `p-limit(4)`.
- **Sync Groups**:
  - `meta-sync`: Incremental campaigns core sync (hourly).
  - `insight-sync`: Sync Meta performance insights (TODAY hourly, 3D 6h/once, 7D 8h/once, MAX daily, Audience daily).
  - `media-sync`: Folders, creative assets, source URLs from Drive to Meta.
  - `meta-media-sync`: Đồng bộ hình ảnh/video hàng ngày, kèm cronjob riêng xử lý các video lỗi (status = ERROR/hệ thống) bằng System config token/cookies qua lô truy vấn (batch query) và fallback tự động lùi hàng chờ.
  - `lark-sync`: Fetch Lark records & Drive Permission Audit (every 30m).
  - `draft-automation`: Generate drafts from templates based on schedule.
  - `help-ai`: Build knowledge snapshot cho chatbot helper và thaw/reset trạng thái AI provider API key định kỳ.

---

## 3. Quy Tắc Bắt Buộc: Thay Đổi Database Schema (Prisma)

- **Không tự ý chạy Migration**: AI tuyệt đối KHÔNG được chạy `npx prisma migrate dev` hay `yarn migration:run`... Việc chạy migration do USER thực hiện thủ công.
- **Đồng bộ hóa Schema 2 chiều**: Cần đồng bộ file `schema.prisma` ở cả 3 repo (`mb-ads`, `mb-batch`, `mb-database`) sau khi sửa đổi.
  - File `mb-database/prisma/schema.prisma` **phải có** cấu hình `output = "../src/generated/prisma"` trong block `generator client`.
  - File `mb-ads/prisma/schema.prisma` và `mb-batch/prisma/schema.prisma` **không được có** dòng cấu hình `output` này.
  - Sau khi đồng bộ, AI gọi `npx prisma generate` trong các thư mục đích có sẵn `node_modules` để sinh client code mới.

---

## 4. Các Pattern Kiến Trúc Cốt Lõi

1. **Mirror + Draft**: Dữ liệu Meta được mirror locally. Draft system cho phép chỉnh sửa/mô phỏng template và tạo drafts trước khi push thay đổi lên Meta (chỉ push các thay đổi thực sự).
2. **Reuse ID (Insight Sync)**: Tái sử dụng ID insight cũ để ghi đè dữ liệu (TODAY, 3D, 7D, MAX), loại bỏ 99% lệnh ghi vào Campaign/AdSet/Ad/Creative, tối ưu hóa sort/filter của Prisma.
3. **Daily Insights Upsert**: Lưu dữ liệu daily insights thông qua Native `upsert` trên Postgres composite keys `(entityId, dateStart, range)`.
4. **Recently Paused Retention**: Tiếp tục sync thực thể tạm dừng (paused) trong 3 ngày tiếp theo để bắt conversion trễ (attribution lag).
5. **Creative Placeholder Substitution**: Tự động thay thế các placeholder `VIDEO_1`, `IMAGE_1` trong TemplateCampaign bằng media asset thực tế từ folder chỉ định. Bản nháp chạy thử được xử lý in-process trên `mb-ads` thay vì gọi sang `mb-batch`.
6. **Folder-based Permission for CIDs**: User không phải admin chỉ xem được LarkRecords (CIDs) thuộc các folder mà họ có quyền (qua Project manager/leader hoặc FolderMember). Logic lọc bằng clause `OR` ngay dưới DB để phân trang nhanh chóng.
7. **Creative Asset Mapping during Sync**: Tự động mapping `CreativeAsset` khớp với `imageHash`/`videoId` từ Meta, đồng thời map `SystemCampaign` tương ứng qua `meta_id` để phục vụ lọc điều kiện automation.
8. **Automation History & Template Visibility Filter**: Để bảo mật thông tin, màn hình lịch sử tự động hoá (`/dashboard/automations/history`) và các endpoint của nó (`/automations/templates` và `/automations/draft-history`) được lọc theo user đang đăng nhập (chỉ hiển thị những mẫu và lịch sử chạy thuộc về user đó), ngoại trừ user có quyền `ADMIN` sẽ xem được toàn bộ.
9. **Advantage+ Audience Age Range Control**: Cho phép nhập khoảng tuổi `age_min` và `age_max` (13–65) trên UI bản nháp cho nhóm quảng cáo thường, kể cả khi bật Advantage+ Audience. Payload publish luôn gửi cả hai giá trị tuổi theo UI; các special ad categories vẫn khóa 18–65 theo policy.
10. **Google Sheets Export & Cache Optimization & Presets & Spreadsheet ID constraint & Relative Dates & Custom Fields/Formulas**: Hỗ trợ xuất dữ liệu ra Google Sheets quy mô lớn (lên tới 10k-20k dòng) bằng cách tối ưu hóa hiệu năng, sử dụng bộ nhớ đệm thư mục cha, hỗ trợ đổi tên/sắp xếp cột kéo thả từ client, tự động chèn base metrics cần thiết để tính toán chỉ số tự định nghĩa (Custom Metrics) thông qua công thức động (Google Sheets Formulas) thay vì số tĩnh, cho phép người dùng tự định nghĩa cột trích xuất dữ liệu từ `rawPayload` qua JSON Path hoặc cột công thức số học dựa trên 2 cột số khác, hỗ trợ lọc dữ liệu linh hoạt (Dynamic Filters), lưu/phục hồi các cấu hình xuất file (Export Presets) của người dùng hỗ trợ cả Ngày tương đối (Relative Dates - tự động tính toán lại mốc thời gian động theo ngày hiện hành khi load preset), đồng thời bắt buộc luôn xuất dữ liệu vào một file Google Sheet duy nhất theo cấu hình `GOOGLE_EXPORT_SPREADSHEET_ID` (mặc định ID `1JcElzgCbvte63MuT_d9n_li6jkgdja4biW7pAbxvuY8`) dưới dạng ghi đè hoặc tạo tab (trang tính) mới trong chính file đó.
11. **AdVideo Sync & Active Prioritization & System Fallback**: Ưu tiên đồng bộ các video thuộc chiến dịch/nhóm quảng cáo/quảng cáo/creative đang hoạt động (`ACTIVE`) trước các video lỗi/hết hạn khác. Luồng đồng bộ video lỗi chạy qua token/cookies hệ thống kết hợp batch query và fallback lẻ, hỗ trợ trigger qua API `POST /data-sync/ad-videos-error` truyền limit và hiển thị card đồng bộ riêng trên UI Dashboard.
12. **UI Truncation & Layout Stability & Premium Card/Tooltip Layout**: Tự động thu gọn các tên tài sản Meta/Google Drive khi hiển thị trực tiếp và đặc biệt là chuyển đổi các dòng cấu hình phân quyền bật/tắt thành cấu hình dạng Card Layout hai phần (Switch + Tên tự động xuống dòng ở trên, thông tin Quyền/Trạng thái ở dưới) kết hợp `Tooltip` của `shadcn` và thuộc tính `title` để hiển thị đầy đủ thông tin nhất cho người dùng trên mọi màn hình phân quyền và tạo dự án.
13. **Help AI Provider Key Pool**: Gemini và DeepSeek API keys được lưu trong bảng `HelpAiApiKey` theo cột `provider` thay vì chỉ xoay vòng từ env. Mỗi request ghi usage/trạng thái key, tự chuyển sang key/provider khác khi gặp quota/rate limit, đóng băng key đến `blockedUntil`, và tự mở lại khi qua giờ reset/cooldown. UI ADMIN quản lý key tại `/dashboard/api-keys`.

---

## 5. Lưu Ý Khi Phát Triển

- **Cập nhật Project Overview**: Sau khi có thay đổi code có ý nghĩa, bắt buộc cập nhật file `mb-auto-project-overview.md` này và đồng bộ sang cả 3 workspace:
  - `mb-ads`: `.gemini/skills/mb-auto-project-overview.md`
  - `mb-batch`: `/Users/cuongdangquoc/Documents/thanhvinh-source.nosync/new/mb-batch/.gemini/skills/mb-auto-project-overview.md`
  - `mb-frontend`: `/Users/cuongdangquoc/Documents/thanhvinh-source.nosync/new/mb-frontend/.gemini/skills/mb-auto-project-overview.md`
- **Error Alerts**: Khi gặp Meta auth/rate limit errors (codes 190, 102, 17, 4), hệ thống tự động clear config và gửi mail alert.
- **Language**: Toàn bộ UI labels, enum descriptions, cron descriptions dùng Tiếng Việt. Mặc định dark mode.
