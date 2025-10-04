FROM node:22-alpine

ENV NODE_ENV=production
WORKDIR /app

# wget para healthchecks
RUN apk add --no-cache wget

# Asegura que exista el usuario 'node' (en la mayoría de imágenes ya existe,
# pero esto lo hace robusto si cambia la base)
RUN if ! id -u node >/dev/null 2>&1; then addgroup -S node && adduser -S node -G node; fi

# Instala dependencias primero para aprovechar cache
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm i --omit=dev

# Copia el resto del código (incluye ./docs/catalogo.yaml)
COPY --chown=node:node . .

# Corre como no-root
USER node

# Env por defecto (puedes sobreescribir desde compose/.env)
ENV PORT=8084 \
    MONGO_URL="" \
    CORS_ORIGINS="*" \
    SERVE_DOCS="0"

EXPOSE 8084
CMD ["node","server.js"]
