# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

FROM rust:1.97.1-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa AS builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends cmake=3.25.1-1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release --locked --bins

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS runtime

LABEL org.opencontainers.image.title="market-stream-gateway" \
    org.opencontainers.image.source="https://github.com/kutpat/market-stream-gateway"

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder --chmod=0555 /build/target/release/market-stream-gateway /usr/local/bin/market-stream-gateway
COPY --from=builder --chmod=0555 /build/target/release/market-stream-healthcheck /usr/local/bin/market-stream-healthcheck

USER 10001:10001
WORKDIR /app
ENV MSG_BIND=0.0.0.0:8080 \
    MSG_LOG_FORMAT=json \
    RUST_LOG=market_stream_gateway=info \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=4s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/market-stream-healthcheck"]

STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/market-stream-gateway"]
