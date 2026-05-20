# List available commands
default:
    @just --list

# Build the server binary
build:
    cargo build --locked --release --bin server

# Run clippy linter
clippy:
    cargo clippy --workspace --all-features --all-targets -- -D warnings --allow deprecated

# Ensure cargo-deny is installed
_ensure-deny:
    @cargo deny --version > /dev/null 2>&1 || cargo install cargo-deny

# Run cargo-deny checks
deny *args: _ensure-deny
    cargo deny check {{args}}

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
