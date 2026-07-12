# ========================
# builder: install deps (cached) + generate prisma + build nest
# ========================
FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json yarn.lock* ./
# --mount=type=cache keeps the Yarn cache across builds → no re-download on repeat builds
# target phải đúng thư mục cache thật của yarn v1 (`yarn cache dir` = /usr/local/share/.cache/yarn)
# Inline registry/timeout so every stage gets the same config
RUN --mount=type=cache,target=/usr/local/share/.cache/yarn,sharing=locked \
    yarn install --frozen-lockfile \
                 --registry https://registry.npmjs.org \
                 --network-timeout 600000

COPY . .

# Prisma chỉ cần DATABASE_URL hợp lệ về format
RUN DATABASE_URL="postgresql://user:pass@localhost:5432/db" \
    npx prisma generate

# nest/tsc build ăn >2GB heap → vượt giới hạn V8 mặc định trong container gây
# "heap out of memory". Nâng heap cho riêng bước build.
RUN NODE_OPTIONS=--max-old-space-size=4096 yarn build


# ========================
# prod-deps: chỉ production deps
# ========================
FROM node:20-alpine AS prod-deps
WORKDIR /app

COPY package.json yarn.lock* ./
RUN --mount=type=cache,target=/usr/local/share/.cache/yarn,sharing=locked \
    yarn install --frozen-lockfile --production \
                 --registry https://registry.npmjs.org \
                 --network-timeout 600000

# copy Prisma client đã generate
COPY --from=builder /app/node_modules/.prisma ./node_modules/.prisma


# ========================
# runtime: image chạy thật
# ========================
FROM node:20-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production

# init + non-root user
RUN apk add --no-cache dumb-init \
 && addgroup -g 1001 -S nodejs \
 && adduser -S nestjs -u 1001

COPY --from=prod-deps /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/src/modules/help-ai/knowledge ./dist/src/modules/help-ai/knowledge
COPY --from=builder /app/prisma ./prisma
COPY package.json ./

USER nestjs

# Cổng lấy từ env PORT (compose server đặt PORT=8000); fallback 3000 cho chạy local.
# Healthcheck đọc CHÍNH env đó để luôn khớp cổng app đang lắng nghe. start-period rộng
# để chờ boot + startup-sync xong. Dùng global fetch của Node 20 (không cần curl).
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||3000)+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

# dumb-init forward SIGTERM → node nhận tín hiệu → NestFactory chạy graceful shutdown
# (Bull drain job đang chạy, Prisma disconnect) trước khi thoát.
ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "dist/src/main.js"]
