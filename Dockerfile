FROM oven/bun:1.2.22-alpine AS build

WORKDIR /src

COPY package.json ./
RUN bun install --production
COPY src ./src

RUN bun build --bundle --target=node --format=esm --outfile /out/traefik-registrator.mjs ./src/main.ts

FROM alpine:3.20

ARG TARGETARCH
ARG BUN_VERSION=1.2.22

RUN apk add --no-cache ca-certificates libstdc++ curl unzip && \
    case "${TARGETARCH}" in \
      amd64) bun_asset="bun-linux-x64-musl-baseline.zip" ;; \
      arm64) bun_asset="bun-linux-aarch64-musl.zip" ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/oven-sh/bun/releases/download/bun-v${BUN_VERSION}/${bun_asset}" -o /tmp/bun.zip && \
    unzip -q /tmp/bun.zip -d /tmp && \
    mv /tmp/bun-linux-*/bun /usr/local/bin/bun && \
    chmod +x /usr/local/bin/bun && \
    rm -rf /tmp/bun.zip /tmp/bun-linux-*

COPY --from=build /out/traefik-registrator.mjs /usr/local/bin/traefik-registrator.mjs

ENTRYPOINT ["bun", "/usr/local/bin/traefik-registrator.mjs"]
