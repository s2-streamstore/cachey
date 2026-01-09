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

# Create and push a release tag (tag only, no version bump)
tag TAG:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Creating release tag: {{TAG}}"
    git tag {{TAG}}
    git push origin {{TAG}}
    echo "✓ Tag {{TAG}} created and pushed - release workflow should start"

# Full release: bump version, commit, tag, push
release VERSION:
    #!/usr/bin/env bash
    set -euo pipefail

    # Validate version format
    if ! [[ "{{VERSION}}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "❌ Invalid version format. Use semver: X.Y.Z"
        exit 1
    fi

    # Must be on main branch
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$BRANCH" != "main" ]]; then
        echo "❌ Must be on main branch (currently on: $BRANCH)"
        exit 1
    fi

    # Check for clean working directory
    if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "❌ Working directory not clean. Commit or stash changes first."
        exit 1
    fi

    # Pull latest
    git pull --ff-only

    echo "📦 Releasing version {{VERSION}}..."

    # Bump version in Cargo.toml
    sed -i '' 's/^version = "[^"]*"/version = "{{VERSION}}"/' Cargo.toml

    # Update Cargo.lock
    cargo check --quiet 2>/dev/null || true

    # Commit and tag
    git add Cargo.toml Cargo.lock
    git commit -m "release: {{VERSION}}"
    git tag "{{VERSION}}"

    # Push commit and tag
    git push && git push origin "{{VERSION}}"

    echo "✅ Released {{VERSION}} - workflow started"
    echo "   https://github.com/s2-streamstore/cachey/actions"
