# syntax=docker/dockerfile:1
FROM rust:1-slim-trixie AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates cmake clang libclang-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    if [ "$(uname -m)" = x86_64 ]; then export CFLAGS="-march=x86-64-v3" CXXFLAGS="-march=x86-64-v3"; fi && \
    # arm64 (Apple Silicon, Graviton): numkong's NEON dot-product kernels need clang (GCC rejects the inlined
    # intrinsics), and its SME kernels need `__arm_get_current_vg`, which the Linux runtime libraries don't provide.
    if [ "$(uname -m)" = aarch64 ]; then \
        export CC=clang CXX=clang++; \
        for t in SME SME2 SME2P1 SMEF64 SMEHALF SMEBF16 SMEBI32 SMELUT2 SMEFA64; do export NK_TARGET_$t=0; done; \
    fi && \
    cargo build --locked --release -p blowfish --features local-models && cp target/release/blowfish /blowfish

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 blowfish && useradd --uid 10001 --gid blowfish --no-create-home blowfish \
    && mkdir /data && chown blowfish:blowfish /data
COPY --from=builder /blowfish /usr/local/bin/blowfish
USER 10001:10001
ENV BLOWFISH_STORAGE_URL=file:///data BLOWFISH_LISTEN=0.0.0.0:8000 BLOWFISH_MODELS_CACHE_DIR=/data/models
EXPOSE 8000
VOLUME ["/data"]
# Authenticated deployments should supply the super key through this environment variable.
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail --silent --output /dev/null -H "Authorization: Bearer ${BLOWFISH_ADMIN_KEY}" http://127.0.0.1:8000/ || exit 1
ENTRYPOINT ["blowfish"]
CMD ["serve"]
