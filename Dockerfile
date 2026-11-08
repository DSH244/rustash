# syntax=docker/dockerfile:1
# Rustash: QStash-compatible Rust service backed by SQLite.
# Multi-stage build; BuildKit cache mounts make source-only changes incremental.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
# Copy the pin before the sources so the toolchain layer is cached independently of code
# changes. Without this the image would compile with whatever `rust:1-slim-bookworm`
# happens to be today (1.99.1 at the time of writing) while CI compiles with the 1.98.1
# pinned in rust-toolchain.toml — the same source building differently in two places.
COPY rust-toolchain.toml ./
RUN rustup toolchain install
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/src/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --locked --bin rustash --bin rs-check \
    && cp /src/target/release/rustash /rustash \
    && cp /src/target/release/rs-check /rs-check

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin rustash \
    && install -d -o 10001 -g 10001 /data
COPY --from=build /rustash /usr/local/bin/rustash
COPY --from=build /rs-check /usr/local/bin/rs-check
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=5 \
  CMD ["curl","--fail","--silent","http://127.0.0.1:8080/v2/health"]
CMD ["/usr/local/bin/rustash"]
