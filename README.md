# n8n-rust-v5 (Release: `n8n-rust-v5.1`)

High-performance single-crate native Rust implementation of n8n engine & Axum UI server serving prebuilt Vue `editor-ui`.

## Features
- **Single-Crate Architecture**: Clean and lean root `Cargo.toml` without multi-crate workspace bloat.
- **Native Axum + Tokio**: Async HTTP/WebSocket ready backend compiled directly to native machine code.
- **Official n8n Vue Editor-UI**: Extracted from stable `n8n-editor-ui@2.43.4` (n8n 2.43.5) with zero node_modules or TypeScript devDependencies in repo.
- **Template Placeholder Normalization**: Clean on-the-fly replacement of `/{{BASE_PATH}}/`, `%CONFIG_TAGS%`, and `{{CSP_NONCE}}` for seamless browser rendering.
- **SPA Fallback Routing**: All frontend navigation routes properly route to `index.html`.
- **Pre-configured REST Endpoints**:
  - `GET /healthz` and `GET /rest/healthz`: Returns `{"status":"ok"}`
  - `GET /rest/settings`: Returns complete n8n configuration object expected by Vue stores
  - `GET /rest/login`: Returns 401 Unauthorized for initial guest state

## Quick Start

### Prerequisites
- Rust 1.80+ (Tested on Rust 1.98.1)

### Build and Run
```bash
# Run server (default: 0.0.0.0:5678)
cargo run

# Custom port
N8N_PORT=8080 cargo run
```

Open browser at `http://localhost:5678/`.

### Run Tests and Quality Verification
```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```
