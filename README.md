# Geo

`geo` is a CLI for geolocation lookups and CSV geocoding via OpenStreetMap Nominatim.

Repository: `github.com/charlesponti/geo`

## Requirements

- Rust stable
## Install

```bash
cargo build --release
```

## Usage

```bash
cargo run -- --help
cargo run -- geocode "New York, NY"
cargo run -- geocode-csv --file input.csv --column Name --output output.csv
```

The binary name is `geo`.

## Development

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

## Repository Layout

- `src/` application code
- `tests/` integration tests
- `.github/workflows/` CI

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for local development and contribution guidance.

## License

MIT. See [LICENSE](LICENSE).
