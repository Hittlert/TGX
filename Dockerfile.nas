FROM golang:1.25-alpine AS builder

ARG VERSION="dev"
ARG COMMIT="unknown"
ARG COMMIT_DATE="unknown"

WORKDIR /src
COPY . .

RUN go build -trimpath \
    -ldflags "-s -w \
    -X github.com/Hittlert/TGX/pkg/consts.Version=${VERSION} \
    -X github.com/Hittlert/TGX/pkg/consts.Commit=${COMMIT} \
    -X github.com/Hittlert/TGX/pkg/consts.CommitDate=${COMMIT_DATE}" \
    -o /tgx

RUN go build -trimpath -ldflags "-s -w" \
    -o /tgx-import-pyrogram-session ./tools/import-pyrogram-session

FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=builder /tgx /usr/bin/tgx
RUN ln -s /usr/bin/tgx /usr/bin/tg-downloader && ln -s /usr/bin/tgx /usr/bin/tdl
COPY --from=builder /tgx-import-pyrogram-session /usr/bin/tgx-import-pyrogram-session

ENTRYPOINT ["tgx"]
