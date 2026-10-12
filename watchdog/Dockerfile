# Watchdog Dockerfile

FROM rust:1.74-slim AS builder

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy Cargo files
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

# Build release
RUN cargo build --release

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/watchdog /app/watchdog
COPY config.toml /config/config.toml

EXPOSE 8080

CMD ["/app/watchdog", "--config", "/config/config.toml"]