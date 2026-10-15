# syntax=docker/dockerfile:1.7
FROM golang:1.25.12-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/log-viewer ./cmd/log-viewer

FROM alpine:3.22
RUN apk add --no-cache ca-certificates curl \
    && addgroup -g 65532 -S app \
    && adduser -u 65532 -S -G app -h /home/app app
COPY --from=build /out/log-viewer /usr/local/bin/log-viewer
USER 65532:65532
WORKDIR /home/app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/log-viewer"]
