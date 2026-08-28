FROM golang:1.25-alpine AS builder

ARG VERSION="dev"
ARG COMMIT="unknown"
ARG COMMIT_DATE="unknown"

WORKDIR /src
COPY . .

RUN go build -trimpath \
    -ldflags "-s -w \
    -X github.com/Hittlert/TG_Downloader/pkg/consts.Version=${VERSION} \
    -X github.com/Hittlert/TG_Downloader/pkg/consts.Commit=${COMMIT} \
    -X github.com/Hittlert/TG_Downloader/pkg/consts.CommitDate=${COMMIT_DATE}" \
    -o /tdl

RUN go build -trimpath -ldflags "-s -w" \
    -o /tdl-import-pyrogram-session ./tools/import-pyrogram-session

FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=builder /tdl /usr/bin/tdl
COPY --from=builder /tdl-import-pyrogram-session /usr/bin/tdl-import-pyrogram-session

ENTRYPOINT ["tdl"]
