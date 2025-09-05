# List available commands
default:
    @just --list

# Build the server binary
build:
    cargo build --release --bin server

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
    cargo run --release --bin server

# Clean build artifacts
clean:
    cargo clean

# Create and push a release tag
release TAG:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Creating release tag: {{TAG}}"
    git tag {{TAG}}
    git push origin {{TAG}}
    echo "✓ Tag {{TAG}} created and pushed - release workflow should start"
