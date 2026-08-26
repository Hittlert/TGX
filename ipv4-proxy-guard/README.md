# Telegram IPv4 CONNECT Guard

This small HTTP CONNECT proxy rejects IPv6 target literals before forwarding
IPv4 Telegram DC connections to the configured upstream proxy. It exists
because a proxied IPv6 target bypasses container IPv6 sysctls: the upstream
proxy, rather than the container, performs the target connection.

Runtime configuration:

- `UPSTREAM_PROXY`: required `http://host:port` upstream proxy URL.
- `LISTEN_ADDR`: optional listen address; defaults to `0.0.0.0:18081`.
- `GET /healthz`: health and connection counters.
- `healthcheck --url URL`: container healthcheck command.
