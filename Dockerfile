# ========================
# deps: cài full deps để build
# ========================
FROM node:20-alpine AS deps
WORKDIR /app

COPY package.json yarn.lock* ./
RUN yarn install --frozen-lockfile


# ========================
# builder: generate prisma + build nest
# ========================
FROM node:20-alpine AS builder
WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
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
RUN yarn install --frozen-lockfile --production

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
