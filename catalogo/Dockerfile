FROM node:22-alpine
ENV NODE_ENV=production
WORKDIR /app

RUN apk add --no-cache wget

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm i --omit=dev

COPY . .
USER node  # la imagen ya trae el usuario node

ENV PORT=8084
ENV MONGO_URL=""
ENV CORS_ORIGINS="*"
ENV SERVE_DOCS="0"

EXPOSE 8084
CMD ["node","server.js"]
