# syntax=docker/dockerfile:1
FROM rust:1-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --locked --release -p blowfish && cp target/release/blowfish /blowfish

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 blowfish && useradd --uid 10001 --gid blowfish --no-create-home blowfish \
    && mkdir /data && chown blowfish:blowfish /data
COPY --from=builder /blowfish /usr/local/bin/blowfish
USER 10001:10001
ENV BLOWFISH_STORAGE_URL=file:///data BLOWFISH_LISTEN=0.0.0.0:8000
EXPOSE 8000
VOLUME ["/data"]
# Authenticated deployments should supply the super key through this environment variable.
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail --silent --output /dev/null -H "Authorization: Bearer ${BLOWFISH_ADMIN_KEY}" http://127.0.0.1:8000/ || exit 1
ENTRYPOINT ["blowfish"]
CMD ["serve"]
