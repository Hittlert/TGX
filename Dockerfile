# https://www.docker.com/blog/faster-multi-platform-builds-dockerfile-cross-compilation-guide/
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

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
