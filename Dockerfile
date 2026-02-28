FROM oven/bun:1.2.22-alpine AS build

WORKDIR /src

COPY package.json ./
RUN bun install --production
COPY src ./src

RUN bun build --bundle --target=node --format=esm --outfile /out/traefik-registrator.mjs ./src/main.ts

FROM node:22-alpine

RUN apk add --no-cache ca-certificates
COPY --from=build /out/traefik-registrator.mjs /usr/local/bin/traefik-registrator.mjs

ENTRYPOINT ["node", "/usr/local/bin/traefik-registrator.mjs"]
