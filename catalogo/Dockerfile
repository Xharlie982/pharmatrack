FROM node:22-alpine

ENV NODE_ENV=production
WORKDIR /app

# wget para healthchecks del compose
RUN apk add --no-cache wget

# Instala dependencias primero para aprovechar cache
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm i --omit=dev

# Copia el resto del código
COPY . .

# ✅ Asegura que el usuario 'node' existe y que /app le pertenece
# (en la mayoría de imágenes existe, pero esto lo vuelve a prueba de balas)
RUN if ! id -u node >/dev/null 2>&1; then addgroup -S node && adduser -S node -G node; fi && \
    chown -R node:node /app

# Corre como no-root
USER node

# Env por defecto (puedes sobreescribirlos desde el compose o .env)
ENV PORT=8084 \
    MONGO_URL="" \
    CORS_ORIGINS="*" \
    SERVE_DOCS="0"

EXPOSE 8084
CMD ["node","server.js"]
