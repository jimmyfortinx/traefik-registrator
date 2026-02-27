FROM golang:1.24-alpine AS build

RUN apk add --no-cache ca-certificates
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/traefik-registrator .

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/traefik-registrator /traefik-registrator

ENTRYPOINT ["/traefik-registrator"]
