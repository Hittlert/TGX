FROM alpine:latest

COPY bin/xray /usr/local/bin/xray
COPY config.json /etc/xray/config.json

RUN chmod +x /usr/local/bin/xray

ENTRYPOINT ["/usr/local/bin/xray"]
CMD ["run", "-c", "/etc/xray/config.json"]
