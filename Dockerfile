ARG NODE_VERSION=26.5.1
ARG PNPM_VERSION=11.15.1

FROM node:${NODE_VERSION}-alpine AS dependencies

ARG PNPM_VERSION
RUN npm install --global pnpm@${PNPM_VERSION}
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY patches ./patches
RUN pnpm install --frozen-lockfile

FROM dependencies AS build

COPY eslint.config.mjs tsconfig.json tsconfig.build.json ./
COPY src ./src
COPY test ./test
RUN pnpm lint && pnpm typecheck && pnpm build && pnpm test && pnpm prune --prod

FROM alpine:3.24 AS runtime

ARG NODE_VERSION
RUN apk upgrade --no-cache \
    && apk add --no-cache nodejs-current=${NODE_VERSION}-r0 \
    && addgroup -S -g 10001 app \
    && adduser -S -D -u 10001 -G app -h /home/app app \
    && mkdir -p /app /home/app \
    && chown -R app:app /app /home/app

ENV HOME=/home/app \
    HOST=0.0.0.0 \
    NODE_ENV=production \
    PORT=3000
WORKDIR /app
COPY --from=build --chown=app:app /app/package.json ./package.json
COPY --from=build --chown=app:app /app/node_modules ./node_modules
COPY --from=build --chown=app:app /app/dist ./dist

USER app
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=5s --start-period=180s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||3000)+'/health').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
CMD ["node", "dist/src/index.js"]
