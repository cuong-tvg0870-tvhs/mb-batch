# ========================
# builder: install deps (cached) + generate prisma + build nest
# ========================
FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json yarn.lock* ./
# --mount=type=cache keeps the Yarn cache across builds → no re-download on repeat builds
# Inline registry/timeout so every stage gets the same config
RUN --mount=type=cache,target=/root/.yarn \
    yarn install --frozen-lockfile \
                 --registry https://registry.npmjs.org \
                 --network-timeout 600000

COPY . .

# Prisma chỉ cần DATABASE_URL hợp lệ về format
RUN DATABASE_URL="postgresql://user:pass@localhost:5432/db" \
    npx prisma generate

RUN yarn build


# ========================
# prod-deps: chỉ production deps
# ========================
FROM node:20-alpine AS prod-deps
WORKDIR /app

COPY package.json yarn.lock* ./
RUN --mount=type=cache,target=/root/.yarn \
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
COPY --from=builder /app/prisma ./prisma
COPY package.json ./

USER nestjs

EXPOSE 3000
ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "dist/src/main.js"]
