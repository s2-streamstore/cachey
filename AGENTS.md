cachey is a read-through cache for S3-compatible object storage

- NO TRIVIAL COMMENTS
- Follow Rust idioms and best practices
- Latest Rust features can be used
- Descriptive variable and function names
- No wildcard imports
- Explicit error handling with `Result<T, E>` over panics
- Use `eyre::Report` when the specific error is not as important
- Use custom error types using `thiserror` for domain-specific errors
- Format: `cargo +nightly fmt`
- Lint: `cargo clippy --all-features --all-targets -- -D warnings --allow deprecated`
- Place unit tests in the same file using `#[cfg(test)]` modules
- Integration tests go in the `tests/` directory
- Run tests with: `cargo nextest run`
- Add dependencies to `Cargo.toml`
- Prefer well-maintained crates from crates.io
- Be mindful of allocations in hot paths
- Prefer structured logging
- Provide helpful error messages
