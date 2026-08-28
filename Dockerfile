# Multi-platform Docker build for TGX
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

ARG VERSION="dev"
ARG COMMIT="unknown"
ARG COMMIT_DATE="unknown"

WORKDIR /

COPY . .

ARG TARGETOS
ARG TARGETARCH

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
    -ldflags "-s -w  \
    -X github.com/Hittlert/TGX/pkg/consts.Version=${VERSION}  \
    -X github.com/Hittlert/TGX/pkg/consts.Commit=${COMMIT}  \
    -X github.com/Hittlert/TGX/pkg/consts.CommitDate=${COMMIT_DATE}" \
    -o tgx

FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=builder /tgx /usr/bin/tgx
RUN ln -s /usr/bin/tgx /usr/bin/tg-downloader && ln -s /usr/bin/tgx /usr/bin/tdl

ENTRYPOINT ["tgx"]
