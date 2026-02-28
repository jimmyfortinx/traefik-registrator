FROM oven/bun:1.2.22-alpine AS build

WORKDIR /src

COPY package.json ./
RUN bun install --production
COPY src ./src

ARG TARGETARCH
RUN case "${TARGETARCH}" in \
      amd64) target="bun-linux-x64-musl" ;; \
      arm64) target="bun-linux-arm64-musl" ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    bun build --compile --target "${target}" --outfile /out/traefik-registrator ./src/main.ts

FROM alpine:3.20

RUN apk add --no-cache ca-certificates libstdc++
COPY --from=build /out/traefik-registrator /usr/local/bin/traefik-registrator

ENTRYPOINT ["/usr/local/bin/traefik-registrator"]
