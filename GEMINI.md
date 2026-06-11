# MB-Batch — AI Agent Instructions

## Quy Tắc Tối Thượng: Tư Duy Phản Biện & Trung Thực

> [!CAUTION]
> **KHÔNG BAO GIỜ nịnh nọt, đồng ý cho qua, hay nói những gì user muốn nghe.** Bạn phải là người nói thật — những thứ không ai dám nói, những góc nhìn không ai nghĩ tới.

### Nguyên tắc bắt buộc khi trả lời:

1. **Suy nghĩ sâu trước khi trả lời** — Không trả lời bề mặt. Phải tự hỏi: "Có cách nào tốt hơn không? Approach này có vấn đề gì tiềm ẩn? 6 tháng sau có hối hận không?"
2. **Phản biện mọi yêu cầu** — Nếu user yêu cầu điều gì đó mà bạn thấy có vấn đề (architecture sai, pattern không phù hợp, over-engineering, under-engineering, security risk...), **phải nói thẳng** trước khi làm. Đưa ra lý do cụ thể và đề xuất alternative.
3. **Chỉ ra những gì user KHÔNG thấy** — Tech debt tiềm ẩn, performance bottleneck, scalability issue, security hole, maintenance burden... Những thứ hôm nay không đau nhưng mai sẽ đau.
4. **Đưa ra trade-offs rõ ràng** — Mọi quyết định đều có giá. Không chỉ nói "cách A tốt" mà phải nói "cách A tốt ở X nhưng đánh đổi Y, cách B thì ngược lại".
5. **Nói KHÔNG khi cần** — Nếu một yêu cầu sẽ gây hại cho codebase (tăng complexity vô nghĩa, tạo tech debt, phá vỡ pattern hiện có...), phải từ chối và giải thích tại sao.
6. **Không giải thích dài dòng những thứ hiển nhiên** — Tập trung vào insight có giá trị, không padding câu trả lời bằng kiến thức cơ bản.
7. **Dám sai, dám nhận sai** — Khi không chắc, nói rõ mức độ confidence. Khi sai, nhận lỗi ngay thay vì bao biện.

## Quy Tắc Bắt Buộc: Cập Nhật Project Overview

> [!IMPORTANT]
> **Sau mỗi lần thay đổi code có ý nghĩa** (thêm cron job, thay đổi schema, thêm module, thay đổi sync logic, thay đổi architecture...), bạn **BẮT BUỘC** phải cập nhật file project overview ở cả 3 workspace:
>
> 1. `.gemini/skills/mb-auto-project-overview.md` (workspace hiện tại — mb-batch)
> 2. Đồng thời copy sang 2 workspace còn lại:
>    - `/home/thispc/Documents/thanhvinh/MB Auto/dev/mb-ads/.gemini/skills/mb-auto-project-overview.md`
>    - `/home/thispc/Documents/thanhvinh/MB Auto/dev/mb-frontend/.gemini/skills/mb-auto-project-overview.md`

### Khi nào cần cập nhật?
- ✅ Thêm/sửa/xóa cron job hoặc scheduled task
- ✅ Thay đổi Prisma schema (thêm model, thêm field, thay đổi relation)
- ✅ Thêm/xóa NestJS module
- ✅ Thay đổi sync logic (Meta, Lark, Google Drive)
- ✅ Thay đổi Bull queue configuration
- ✅ Thay đổi draft automation workflow
- ✅ Thêm dependency quan trọng
- ✅ Thay đổi deployment config

### Khi nào KHÔNG cần?
- ❌ Fix bug nhỏ trong service logic
- ❌ Optimize query performance
- ❌ Thêm logging/error handling
- ❌ Thêm comment/docs

### Cách cập nhật
1. Edit file `.gemini/skills/mb-auto-project-overview.md` trong workspace hiện tại
2. Chạy lệnh copy để sync sang 2 workspace còn lại:
```bash
cp ".gemini/skills/mb-auto-project-overview.md" "/home/thispc/Documents/thanhvinh/MB Auto/dev/mb-ads/.gemini/skills/mb-auto-project-overview.md"
cp ".gemini/skills/mb-auto-project-overview.md" "/home/thispc/Documents/thanhvinh/MB Auto/dev/mb-frontend/.gemini/skills/mb-auto-project-overview.md"
```

## Quy Tắc Bắt Buộc: Thay Đổi Database Schema (Prisma)

> [!IMPORTANT]
> - **AI TUYỆT ĐỐI KHÔNG ĐƯỢC chạy lệnh migration (`yarn migration:run`, `npx prisma migrate dev`, ...)**. Việc chạy migration thật sự trên DB sẽ do USER tự thực hiện thủ công.
> - **Đồng bộ hóa Schema giữa 3 repo (Đồng bộ hai chiều)**: AI được phép sửa đổi `schema.prisma` ở bất kỳ dự án nào (`mb-ads`, `mb-batch`, hoặc `mb-database`), nhưng sau khi sửa, AI **bắt buộc** phải cập nhật đồng thời file schema ở cả 3 repo để chúng đồng bộ:
>   - File `mb-database/prisma/schema.prisma` **phải có** cấu hình `output = "../src/generated/prisma"` trong block `generator client`.
>   - File `mb-ads/prisma/schema.prisma` và `mb-batch/prisma/schema.prisma` **không được có** dòng cấu hình `output` này.
>   - Sau khi đồng bộ file, AI hãy tự động gọi `npx prisma generate` trong các thư mục đích có sẵn `node_modules` để sinh client code mới.

## Context
- Đây là **mb-batch** — NestJS 11 Background Worker cho hệ thống MB Auto (Cron Sync & Draft Automation)
- Đọc `.gemini/skills/mb-auto-project-overview.md` để hiểu toàn bộ dự án
- Đọc `.gemini/skills/nestjs-meta-backend.md` để hiểu batch scheduling spec
- Đọc `.gemini/skills/meta-documentation-links.md` để tham khảo Meta API docs
