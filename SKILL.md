---
name: zero-trust
description: Zero-Trust Protocol and Forensics Enforcement for n8n-rust-v5
---

# ZERO-TRUST AUDITING & ANTI-CHEAT PROTOCOL (n8n-rust-v5)

Setiap agent, sub-agent, dan proses implementasi WAJIB mematuhi protokol Zero-Trust berikut tanpa kompromi:

1. **DILARANG KERAS (STRICTLY FORBIDDEN) Hardcode / Mocking Nilai Asersi**:
   - Dilarang keras melakukan hardcode/mocking nilai asersi pada node yang salah demi membuat test terlihat lulus.
   - Contoh terlarang: menyelipkan `"codeExecuted": true` di parameter `Set`, atau menduplikasi payload `pinData` ke dalam `parameters.data` pada node `manualTrigger`.
   - Asersi `codeExecuted` wajib 100% murni dihasilkan dari eksekusi aktual `NodeKind::Code`.
   - Data injeksi `pinData` wajib murni diuji melalui `pinData` mapping tanpa pre-populated `parameters.data`.

2. **DILARANG KERAS Menambah Logika Tanpa Test Coverage Eksplisit**:
   - Dilarang menambahkan cabang logika baru di `src/` (misalnya penanganan struktur `"conditions"` pada `Switch` dan `Filter`, atau evaluasi ekspresi per-item pada `Set`) tanpa menulis skenario pengujian eksplisit yang benar-benar mengeksekusi dan memvalidasi cabang tersebut di `tests/integration_tests.rs`.

3. **DILARANG KERAS Truncation Kode / Diff**:
   - Dilarang keras memotong output kode atau diff dengan `...`, `// ...`, `/* ... */`, atau representasi abstrak lainnya.
   - Semua laporan diff dan bukti kode wajib 100% verbatim dari disk.

4. **KONTRAK PELAPORAN SUB-AGENT**:
   - Setiap sub-agent wajib membaca protokol ini di awal sesi mereka.
   - Setiap sub-agent wajib melaporkan rentang nomor baris yang dimodifikasi (`start_line` hingga `end_line`) beserta bukti konkret verbatim dari disk kepada Main Agent.

5. **FAIL-FAST & ZERO WARNING / ZERO UNWRAP**:
   - `cargo clippy --all-targets -- -D warnings` wajib 0 error dan 0 warning.
   - Dilarang keras menggunakan `.unwrap()` pada seluruh kode produksi `src/`.
   - Dilarang melemahkan atau menghapus asersi di `tests/`.
