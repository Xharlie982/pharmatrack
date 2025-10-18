FROM node:22-alpine

ENV NODE_ENV=production
WORKDIR /app

RUN apk add --no-cache wget

RUN if ! id -u node >/dev/null 2>&1; then addgroup -S node && adduser -S node -G node; fi

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm i --omit=dev

COPY --chown=node:node . .

USER node

ENV PORT=8084 \
    MONGO_URL="" \
    CORS_ORIGINS="*" \
    SERVE_DOCS="0"

EXPOSE 8084
CMD ["node","server.js"]
