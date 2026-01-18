# Build stage
FROM rust:latest AS builder

WORKDIR /build

# Use Docker BuildKit cache mounts for faster builds
RUN --mount=type=bind,source=src,target=/build/src \
    --mount=type=bind,source=Cargo.toml,target=/build/Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=/build/Cargo.lock \
    --mount=type=cache,id=cachey-rust,sharing=locked,target=/build/target \
    --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,sharing=locked,target=/usr/local/cargo/git \
    cargo build --locked --release --bin server

# Copy the binary from the cache volume
RUN --mount=type=cache,id=cachey-rust,sharing=locked,target=/cache \
    mkdir -p /build/target/release/ && \
    cp /cache/release/server /build/target/release/server

# Runtime stage
FROM ubuntu:latest

# Install ca-certificates for HTTPS requests
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /build/target/release/server /app/cachey

# Run the server
ENTRYPOINT ["./cachey"]
