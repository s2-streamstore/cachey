# List available commands
default:
    @just --list

# Build the server binary
build:
    cargo build --locked --release --bin server

# Run clippy linter
clippy:
    cargo clippy --workspace --all-features --all-targets -- -D warnings --allow deprecated

# Format code with rustfmt
fmt:
    cargo +nightly fmt

# Run tests with nextest
test:
    cargo nextest run --all-features

# Run the server locally
run:
    cargo run --locked --release --bin server

# Verify Cargo.lock is up-to-date
check-locked:
    cargo metadata --locked --format-version 1 >/dev/null

# Clean build artifacts
clean:
    cargo clean
