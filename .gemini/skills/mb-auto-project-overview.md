# MB Auto — Tổng Quan Dự Án (Project Knowledge Base)

> **Mục đích file này**: Cung cấp context tổng quan cho AI assistant để hiểu toàn bộ hệ thống MB Auto khi làm việc với bất kỳ repo nào. File này được đồng bộ ở cả 3 workspace.

---

## 1. Tổng Quan Hệ Thống

**MB Auto** (MB Automation) là nền tảng quản lý quảng cáo Meta (Facebook) Ads nội bộ cho **Thành Vinh Holding (TVHS)**. Hệ thống cho phép team ads vận hành, tự động hóa, và tối ưu các chiến dịch quảng cáo Meta ở quy mô lớn.

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

## 2. MB-Frontend (Next.js 16)

**Path**: `/home/thispc/Documents/thanhvinh/MB Auto/dev/mb-frontend`

### Tech Stack
| Công nghệ | Version | Vai trò |
|---|---|---|
| Next.js (App Router) | 16.0.3 | Framework |
| React | 19.2.0 | UI Library |
| TypeScript | 5.x | Ngôn ngữ |
| Tailwind CSS v4 | 4.1.9 | Styling (dùng `@theme` directive, KHÔNG dùng tailwind.config.js) |
| Radix UI / Shadcn | latest | UI Components |
| Zustand | 5.0.12 | State Management (persisted stores) |
| SWR + Axios | 2.4.1 / 1.13.4 | Data Fetching |
| React Hook Form + Zod | latest | Form Validation |
| Recharts | 3.8.1 | Charts |
| Framer Motion | 12.29.0 | Animations |
| @google/genai | 1.33.0 | AI Ad Suggestions |
| next-themes | 0.4.6 | Dark/Light mode |

### API Pattern
- Frontend gọi `/api/*` → Next.js API Route proxy → Backend `BACKEND_INTERNAL_URL` (http://localhost:8000)
- JWT Bearer token từ `localStorage`, auto-logout khi 401
- Helper functions: `get`, `post`, `put`, `patch`, `del`, `upload` trong `lib/api.ts`

### Routing Structure
```
/login                          → Đăng nhập
/auth/facebook/callback         → Facebook OAuth
/dashboard                      → Dashboard chính
  /activities                   → Lịch sử hoạt động
  /ad-accounts                  → Quản lý tài khoản QC
  /campaigns + [id]             → Quản lý Campaign
  /ad-sets + [id]               → Quản lý Ad Set
  /ads + [id]                   → Quản lý Ad
  /creatives + [id]             → Quản lý Creative
  /cids                         → Quản lý CID (Content ID)
  /automations + /history       → Automation Rules
  /draft/campaigns              → Draft Campaigns
  /draft/templates              → Campaign Templates
  /folders + [id]               → Creative Folders
  /medias/gallery|images|videos → Media Library
  /projects                     → Quản lý Projects
  /data-sync                    → Data Sync từ Meta
  /suggest-ads                  → AI gợi ý quảng cáo
  /users + [id]                 → Quản lý Users
  /settings + /permissions      → Cài đặt & Phân quyền
  /profile                      → Hồ sơ cá nhân
  /feedbacks + [id]             → Đóng góp ý kiến (Khen/Chê/Đề xuất) & chi tiết đóng góp
  /help + [slug]                → Trung tâm Trợ giúp (User Guide Viewer)
  /insights                     → Quản trị Insight (ADMIN)
    /campaigns + [id]           → Campaign Insight
    /ad-sets + [id]             → Adset Insight
    /ads + [id]                 → Ad Insight
    /creatives + [id]           → Creative Insight
```

### Zustand Stores
| Store | Key | Chức năng |
|---|---|---|
| `useAuthStore` | `auth-storage` | JWT token, user profile, login/logout |
| `useUIStore` | `ui-storage` | Sidebar state, theme |
| `useDataStore` | `table-reference` | Cấu hình bảng (columns, filters, sorting) |
| `useSelectedAssetsStore` | `selected-assets-storage` | Assets đã chọn cho campaign launch |
| `useGallerySelectedAssetsStore` | `gallery-selected-assets-storage` | Gallery asset selections |

### File quan trọng & kích thước lớn
- `components/draft/create.tsx` — **243KB** — Campaign draft builder (file lớn nhất)
- `components/common/data-table.tsx` — **83KB** — Generic data table
- `components/campaign/clone-button.tsx` — **40KB** — Campaign cloning
- `components/draft/constants.ts` — **25KB** — Draft constants
- `components/draft/locationData.ts` — **125KB** — Location targeting data

---

## 3. MB-Ads (NestJS 11 API Backend)

**Path**: `/home/thispc/Documents/thanhvinh/MB Auto/dev/mb-ads`

### Tech Stack
| Công nghệ | Version | Vai trò |
|---|---|---|
| NestJS | 11.0.1 | API Framework |
| Prisma | 7.0.1 | ORM (PostgreSQL) |
| passport-jwt | 11.0.1 | JWT Authentication |
| facebook-nodejs-business-sdk | 24.0.1 | Meta Ads SDK |
| googleapis | 171.4.0 | Google Drive |
| nodemailer + @nestjs-modules/mailer | latest | Email alerts |
| @nestjs/swagger | 11.2.3 | API Documentation |

### Module Architecture (24 NestJS modules)

#### Core Ad Operations
| Module | Service Size | Chức năng |
|---|---|---|
| **meta** | 66KB | Core Meta SDK — sync accounts/pages/pixels, publish campaigns, upload media |
| **campaigns** | — | CRUD campaigns đã sync từ Meta |
| **adsets** | — | Ad Set management |
| **ads** | — | Ad management |
| **creatives** | — | Creative management |
| **insights** | — | Performance analytics + Admin Insights raw data endpoints |
| **dashboard** | — | Dashboard aggregation |

#### Campaign Builder
| Module | Service Size | Chức năng |
|---|---|---|
| **draft-campaign** | **94KB** | Full campaign builder — CRUD drafts, templates, publish to Meta |
| **campaign-sync-service** | 17KB | Upsert synced campaign data |

#### Data & Media
| Module | Chức năng |
|---|---|
| **data-sync** (28KB) | Sync Meta folders/assets/videos, refresh expired URLs |
| **medias** | Media asset/folder management + Meta Graph API wrapper |
| **drive** (18KB) | Google Drive integration |
| **larkbase** | Lark Bitable data fetching |
| **web-hook** | Lark webhook receiver |
| **mail** | Email notifications (Handlebars templates) |

#### User & Organization
| Module | Chức năng |
|---|---|
| **auth** | JWT login, token refresh |
| **user** | User CRUD, profile, activity |
| **permission** | RBAC permission checking |
| **accounts** | Ad account + member management |
| **project** (26KB) | Project grouping — accounts, fanpages, catalogs, folders |
| **cid-content** | Content ID tracking |
| **dropdown** | Configurable dropdown data |
| **saved-filters** | User-saved table filters |
| **automation-history** | Automation execution logs |
| **contributions** | Ghi nhận & xử lý đóng góp ý kiến (Khen/Chê/Đề xuất) |

### API Endpoints (83 endpoints)
Chính gồm: Auth (1), User (11), Contributions (7), Account (5), Campaign (6), AdSet (3), Ad (3), Creatives (2), Draft-Campaigns (17 — bao gồm API chạy thử mô mỏng template automation trực tiếp trên DB, và API POST /draft-campaigns/drafts/bulk-delete để xóa nhanh nhiều bản nháp), Meta (4), Media (12 — đã thêm GET /medias/assets/:id/usage để lấy số bản nháp & publish của image/video), Dropdown (7), Insights (3 + 8 Admin endpoints), Dashboard (1), Drive (2), CID (2)

### Meta Integration Pattern
1. **Hai cơ chế auth**: SDK token (`SDK_FACEBOOK_ACCESS_TOKEN`) + Per-user `MetaConnection` (AES-256-GCM encrypted)
2. **Mirror + Draft Architecture**: Dữ liệu Meta được sync/mirror locally → Draft system song song cho phép build campaign trước khi publish
3. **Change Detection**: Diff local drafts vs Meta data, chỉ push thay đổi thực sự
4. **Error Handling**: Auto-clear config + gửi email alert khi gặp auth/rate-limit errors (codes 190, 102, 17, 4)

---

## 4. MB-Batch (NestJS 11 Worker Service)

**Path**: `/home/thispc/Documents/thanhvinh/MB Auto/dev/mb-batch`

### Tech Stack
| Công nghệ | Version | Vai trò |
|---|---|---|
| NestJS | 11 | Worker Framework |
| @nestjs/schedule | latest | Cron scheduling |
| @nestjs/bull | latest | Redis job queues |
| Prisma | 7.0 | ORM (PostgreSQL) |
| facebook-nodejs-business-sdk | 24 | Meta API |
| googleapis | latest | Google Drive API |
| p-limit | latest | Concurrency control |

### Architecture Pattern
```
Scheduler (@Cron) → Bull Queue → Processor → Service
```
- Retry: 3 lần với exponential backoff
- Job IDs: timestamp buckets cho idempotency
- Timezone: `Asia/Ho_Chi_Minh`

### Scheduled Jobs

#### meta-sync (Campaign Core Data)
| Cron | Job | Mô tả |
|---|---|---|
| `5 * * * *` (mỗi giờ :05) | SYNC_CAMPAIGN_CORE | Incremental sync campaigns/adsets/ads/creatives. 14-day lookback, concurrency 4 accounts |

#### insight-sync (Performance Insights)
| Cron | Job | Mô tả |
|---|---|---|
| `0 * * * *` (mỗi giờ) | TODAY sync | Insights hôm nay |
| `0 */6 * * *` (6h/lần) | DAY_3 sync | 3 ngày gần nhất (bao gồm cập nhật DAILY của 3 ngày để sửa lag attribution) |
| `0 */8 * * *` (8h/lần) | DAY_7 sync | 7 ngày gần nhất |
| `15 2 * * *` (2:15 AM) | MAX sync | Lifetime insights |
| `0 3 * * *` (3:00 AM) | Missing DAILY | Tìm và sync bù các ngày DAILY bị thiếu theo lô 50 IDs (tối ưu hóa API calls) |
| `35 4 * * *` (4:35 AM) | Audience sync | Age/gender demographic breakdowns |
| `10 0 * * *` (12:10 AM) | Inactive Sliding Window | Trượt ngày locally cho thực thể PAUSED/ARCHIVED quá 3 ngày (slideInactiveInsights) |

#### media-sync (Creative Library)
| Cron | Mô tả |
|---|---|
| `0 * * * *` | Sync folder structure từ Meta |
| `3 * * * *` | Sync creative assets |
| `6 * * * *` | Sync video source URLs |
| `0 */2 * * *` | Refresh expired media URLs |

#### meta-media-sync (Ad Images/Videos)
| Cron | Mô tả |
|---|---|
| `5 19,20,21 * * *` | Sync ad images (buổi tối) |
| `5 20,21,22 * * *` | Sync ad videos (buổi tối - đồng bộ đầy đủ các trường source, title, length, createdTime, thumbnails, rawPayload) |
| `0 0 * * *` | Recalculate URL expiry dates |

#### lark-sync (Lark ↔ Google Drive)
| Cron | Mô tả |
|---|---|
| Mỗi 30 phút | Fetch Lark records → Targeted Drive Permission Audit with Exponential Backoff → Map assets |

#### draft-automation (Campaign Auto-generation)
| Cron | Mô tả |
|---|---|
| `*/30 * * * *` | Reconcile automation schedules — register/unregister dynamic cron jobs |
| `0 2 * * *` | Tự động dọn dẹp các bản nháp không cập nhật trong 7 ngày gần nhất |
| Dynamic (per template) | Select unused assets → Replace placeholders (VIDEO_1, IMAGE_1) → Generate drafts → Optional publish |

---

## 5. Database Schema (Prisma — 2,537 lines, 65+ models)

### Cơ chế đồng bộ Schema (Schema Syncing)
Prisma schema có thể được chỉnh sửa bởi AI tại bất kỳ repo nào (`mb-ads`, `mb-batch`, hoặc `mb-database`), nhưng **bắt buộc phải đồng bộ sang 2 repo còn lại ngay sau đó**:
- **Quy tắc Generator Block**:
  - File schema tại `mb-database` **phải giữ lại** dòng `output = "../src/generated/prisma"` trong block `generator client`.
  - File schema tại `mb-ads` và `mb-batch` **phải loại bỏ** dòng `output` này.
- **Auto Generate Client**: AI cần gọi `npx prisma generate` tại các thư mục đích có sẵn `node_modules` để cập nhật Prisma Client cục bộ.
- **Ràng buộc Migration**: AI **TUYỆT ĐỐI KHÔNG ĐƯỢC tự ý chạy lệnh migration** (`yarn migration:run`, `npx prisma migrate dev`...). Lệnh này chỉ do USER chạy thủ công.

### Core Domains

#### User & Auth
- **User** — email/password, roles (ADMIN/EMPLOYEE), hierarchical (parent/children)
- **UserActivity** — API audit log
- **Permission**, **RolePermission** — RBAC system
- **Contribution** — Đóng góp ý kiến từ người dùng (Khen/Chê/Đề xuất), hiển thị công khai cho mọi user (ẩn danh/mask tên nếu không phải admin/creator) đi kèm chat thread thảo luận và bình chọn "Hữu ích".

#### Meta Ad Hierarchy (Mirrored)
```
Account → Campaign → AdSet → Ad → Creative
                                    ↓
                              CidGroup (Content ID tracking)
```
- Mỗi entity có denormalized insight metrics (impressions, clicks, spend, ROAS, CTR...)
- Insight snapshots: TODAY, DAY_3, DAY_7, MAX, DAILY

#### Insight Models (Time-series)
- **CampaignInsight**, **AdSetInsight**, **AdInsight**, **CreativeInsight**
- **CampaignAudienceInsight**, **AdsetAudienceInsight**, **AdAudienceInsight** — age/gender breakdown

#### Draft System (Song song với Meta hierarchy)
```
TemplateCampaign → SystemCampaign → SystemAdSet → SystemAd → SystemCreative
                                                                    ↓
                                                            PublishHistory
```

#### Creative Asset Management
- **CreativeFolder** — Hierarchical tree (Project → Brand → Product)
- **CreativeAsset** — Images/videos with Drive integration
- **CreativeAssetMapping** — Maps Meta creatives to internal assets
- **LarkRecord** — Lark Bitable records (CID, project, brand, employee, drive URL)
- **DriveFile** — Google Drive file metadata

#### Automation Rule Engine (Birch-style)
```
AutomationCategory → AutomationFolder → AutomationRule
                                              ↓
                    AutomationSchedule (INTERVAL / SPECIFIC time)
                    AutomationFilterGroup → AutomationFilter (AND/OR)
                    AutomationTask → AutomationTaskGroup → AutomationTaskCondition
                    AutomationRuleRun → AutomationRuleRunItem (execution logs)
```
- Task types: START, PAUSE, DUPLICATE, NOTIFY, INCREASE_BUDGET, DECREASE_BUDGET, SET_SPENDING_LIMITS...
- Custom metrics & timeframes

#### Projects & Organization
- **Project** → ProjectMember, ProjectAccount, ProjectFanpage, ProjectFolder
- **FolderMember** — Folder-level access control

#### CID Performance Classification
- Statuses: TEST → NEED_SPEND → SCALE_P1 → SCALE_P2 → REVIEW → OFF
- Auto-assigned dựa trên ROAS/spend/CTR thresholds

---

## 6. External Integrations

| Service | Mục đích | API Version |
|---|---|---|
| **Meta Graph API** | Core — quản lý campaigns, insights, media, creatives | v24.0 |
| **Google Drive API** | Quản lý creative assets qua service account | v3 |
| **Lark Bitable API** | Sync data CID, project, brand, employee | REST |
| **Redis** | Bull queue backend cho batch jobs | — |
| **Gmail SMTP** | Email alerts khi Meta auth errors | — |
| **PostgreSQL** | Primary database (103.15.51.52) | — |

---

## 7. Key Architectural Patterns

1. **Mirror + Draft**: Meta data mirrored locally; Draft system cho phép build/test trước khi publish
2. **Change Detection**: Diff-based publishing, chỉ push thay đổi thực sự lên Meta
3. **Scheduler → Queue → Processor → Service**: Cron triggers Bull jobs với retry, idempotency
4. **Multi-range Insight Snapshots**: 4 pre-computed snapshots (3D, 7D, MAX, TODAY) per entity
5. **Birch-style Automation**: Full rule engine với filters, conditions, scheduling, confirmation workflow
6. **Denormalized Metrics**: Insight data stored trên cả insight records VÀ trực tiếp trên Campaign/AdSet/Ad
7. **Creative Placeholder Substitution**: Templates dùng VIDEO_1, IMAGE_1 → auto-replace với assets từ folders. Nếu ad creative không được chọn slot trước trong template, hệ thống sẽ tự động gán slot tương ứng (VIDEO_x, IMAGE_x) theo mediaType và số lượng yêu cầu để tự động lấp đầy. Hỗ trợ kích hoạt chạy thử dưới dạng mô phỏng trực tiếp trên API backend, và nếu đủ điều kiện, giao diện sẽ hỏi để cho phép tạo bản nháp chiến dịch ngay lập tức ("Chạy thử ngay"). Khi người dùng kích hoạt tạo bản nháp chạy thật, backend `mb-ads` tự xử lý logic mapping và lưu DB song song trực tiếp (in-process) thay vì gọi HTTP sang `mb-batch`.
8. **Incremental Sync**: `updated_time` filtering + `lastFetchedAt` tracking per account
9. **Concurrency Control**: `p-limit(4)` cho parallel Meta API calls
10. **Creative Asset Mapping during Sync**: Khi đồng bộ dữ liệu chiến dịch từ Meta (trong cả `mb-batch` và `mb-ads`), hệ thống tự động tìm kiếm `CreativeAsset` khớp với `imageHash` hoặc `videoId` của creative và tự động chèn các bản ghi `CreativeAssetMapping`. Đồng thời, các chiến dịch được đồng bộ từ Meta cũng được tự động liên kết ngược lại với `SystemCampaign` tương ứng (qua `systemCampaignId`) bằng cách tìm kiếm theo `meta_id`. Điều này đảm bảo tính năng kiểm tra điều kiện/lọc loại trừ của tự động hóa (automation exclusion check) hoạt động chính xác và không bị chọn lại các ảnh/video đã được sử dụng.
11. **Optimized Insight Synchronization (Reuse ID & Concurrency)**: Dữ liệu insight theo range tổng hợp (TODAY, 3D, 7D, MAX) sử dụng cơ chế *Tái sử dụng ID (Reuse ID)*. Thay vì xóa-chèn bản ghi mới rồi cập nhật lại bảng cha, worker ghi đè dữ liệu trực tiếp vào ID insight cũ, giúp loại bỏ 99% lệnh ghi vào Campaign/AdSet/Ad/Creative và bảo toàn các trường quan hệ 1-1 cho Prisma sort/filter hoạt động mượt mà. Đồng thời, dữ liệu DAILY được cập nhật bằng lệnh Native `upsert` trên Postgres composite keys `(entityId, dateStart, range)` để tránh phình dung lượng và đảm bảo tính nguyên tử (atomicity). Chunk API calls được thực thi song song giới hạn luồng (concurrency limit = 3) để tối ưu thời gian.
    - **Recently Paused Retention (3 Days)**: Thực thể tạm dừng vẫn được đồng bộ từ Meta API trong vòng 3 ngày sau khi pause để bắt được các chuyển đổi trễ (attribution lag) và chi tiêu phát sinh.
    - **Local Inactive Sliding Window**: Sau 3 ngày pause, thực thể chuyển sang cơ chế trượt ngày locally (`slideInactiveInsights`). Hệ thống tự động tính toán các range TODAY, 3D, 7D dựa trên dữ liệu bảng `DAILY` trong DB mà không cần gọi API Meta, xóa bỏ hoàn toàn chi phí API cho các quảng cáo đã dừng.
    - **Dynamic Creative Aggregation**: CreativeInsight được tổng hợp động trực tiếp từ dữ liệu `DAILY` và `MAX` của AdInsight trực thuộc. Các range TODAY, 3D (yesterday-2 đến yesterday), 7D (yesterday-6 đến yesterday) được tính toán hoàn toàn bằng local code (loại bỏ việc gọi API Meta và khắc phục lỗi ngày kết thúc dừng ở ngày ad bị tắt).

---

## 8. Coding Conventions

### Backend (mb-ads & mb-batch)
- **PrismaService**: Import từ `src/prisma/prisma.service.ts` (mb-ads) hoặc `src/modules/prisma/prisma.service.ts` (mb-batch)
- **Auth Guard**: `@UseGuards(JwtAuthGuard)` + `@CurrentUser()` decorator
- **Validation**: `class-validator` + `class-transformer` trên tất cả Request DTOs
- **Meta SDK**: `facebook-nodejs-business-sdk` v24.0.1, KHÔNG hardcode tokens

### Frontend (mb-frontend)
- **Tailwind CSS v4**: Dùng `@theme` directive trong CSS, KHÔNG dùng `tailwind.config.js`
- **State**: Zustand stores (primary) + Context/Reducer pattern (legacy)
- **Forms**: React Hook Form + Zod schemas từ `lib/schemas.ts`
- **API**: SWR cho data fetching, Axios cho API calls
- **UI Language**: Vietnamese (tất cả labels, enum descriptions, cron descriptions bằng tiếng Việt)
- **Dark theme**: Mặc định dark mode, oklch color space

---

## 9. Deployment

- **Docker**: Multi-stage builds, Node 20 Alpine, `dumb-init`, non-root user `nestjs:1001`
- **Frontend**: `output: "standalone"` cho minimal Docker image
- **Docker Compose**: Service chính `mb-api` port 3003 (có commented-out services: user, batch, socket, vps)

---

## 10. Lưu Ý Khi Phát Triển

> [!IMPORTANT]
> - Schema Prisma được **chia sẻ giữa 3 repo (đồng bộ hai chiều)** — AI được sửa ở bất kỳ repo nào nhưng phải đồng bộ thủ công sang các repo còn lại (giữ `output` ở `mb-database`, loại bỏ ở `mb-ads` và `mb-batch`). AI tuyệt đối không chạy lệnh migration.
> - Meta API tokens được mã hóa AES-256-GCM trong `MetaConnection` — không bao giờ log/expose
> - Draft campaign builder (`draft-campaign.service.ts` 94KB, `create.tsx` 243KB) là phần phức tạp nhất — cần cẩn thận khi sửa đổi
> - Tất cả batch jobs chạy timezone `Asia/Ho_Chi_Minh`
> - Frontend không có test files — cần verify manual
> - `typescript.ignoreBuildErrors: true` trong next.config — có thể có TS errors ẩn
