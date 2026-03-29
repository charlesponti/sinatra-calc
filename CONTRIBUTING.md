# Contributing

## Local Setup

```bash
cargo build
cargo test
```

## Quality Checks

Before submitting changes, run:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

## Notes

- Keep changes focused and covered by tests when practical.
- Prefer small, reviewable commits.
- Avoid adding new dependencies unless they materially improve the crate.