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

# Clean up a test release (tag, GitHub release, and all related GHCR images)
clean-release TAG:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Cleaning up release: {{TAG}}"

    # Delete git tag locally and remotely
    echo "Deleting git tag..."
    git tag -d {{TAG}} || echo "Local tag not found"
    git push origin --delete {{TAG}} || echo "Remote tag not found"

    # Delete GitHub release
    echo "Deleting GitHub release..."
    gh release delete {{TAG}} --yes || echo "Release not found"

    # Delete GHCR images - main tag and architecture-specific variants
    echo "Deleting GHCR images..."

    # Function to delete a specific image tag
    delete_image_tag() {
        local tag=$1
        echo "  Trying to delete: $tag"
        local version_id=$(gh api /orgs/s2-streamstore/packages/container/cachey/versions --jq ".[] | select(.metadata.container.tags[]? == \"$tag\") | .id" 2>/dev/null || echo "")
        if [ -n "$version_id" ]; then
            gh api --method DELETE "/orgs/s2-streamstore/packages/container/cachey/versions/$version_id" && echo "    ✓ Deleted $tag" || echo "    ✗ Failed to delete $tag"
        else
            echo "    - Not found: $tag"
        fi
    }

    # Delete main tag and architecture-specific variants
    delete_image_tag "{{TAG}}"
    delete_image_tag "{{TAG}}-x86-64"
    delete_image_tag "{{TAG}}-arm64"

    echo "Cleanup complete for {{TAG}}"
