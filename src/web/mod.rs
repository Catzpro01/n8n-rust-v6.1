use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode, Uri},
    response::{Html, IntoResponse, Json, Response},
    Router,
};
use dashmap::DashMap;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const HYPERSCALE_HUD_SNIPPET: &str = r##"<!-- HyperScale Spatial Jumper & Inspector HUD -->
<style>
#hyperscale-hud {
  position: fixed;
  bottom: 24px;
  right: 24px;
  z-index: 999998;
  display: none;
  align-items: center;
  gap: 10px;
  background: rgba(26, 26, 46, 0.94);
  border: 1px solid #e5484d;
  border-radius: 9999px;
  padding: 6px 16px;
  box-shadow: 0 8px 30px rgba(0, 0, 0, 0.55);
  backdrop-filter: blur(12px);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  font-size: 12px;
  color: #f1f5f9;
  user-select: none;
}
#hs-jump-btn:hover { background: #ff5c62 !important; }
#hs-inspect-btn:hover { background: #35355c !important; }
#hs-inspect-close:hover { color: #f1f5f9 !important; }
#hs-node-input:focus { border-color: #e5484d !important; outline: none; }
#hs-node-input::-webkit-outer-spin-button,
#hs-node-input::-webkit-inner-spin-button {
  -webkit-appearance: none;
  margin: 0;
}
#hs-node-input[type=number] {
  -moz-appearance: textfield;
}
#hs-inspect-modal {
  position: fixed;
  bottom: 74px;
  right: 24px;
  width: 480px;
  max-width: calc(100vw - 48px);
  max-height: 480px;
  background: rgba(22, 22, 38, 0.98);
  border: 1px solid #e5484d;
  border-radius: 10px;
  box-shadow: 0 12px 36px rgba(0, 0, 0, 0.65);
  z-index: 999999;
  display: none;
  flex-direction: column;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  color: #e2e8f0;
  backdrop-filter: blur(12px);
  overflow: hidden;
}
</style>
<div id="hyperscale-hud">
  <span style="font-weight: 700; color: #ff6b6b; display: inline-flex; align-items: center; gap: 4px; white-space: nowrap;">
    ⚡ HyperScale 1,000,000 Real Nodes Active
  </span>
  <span style="color: #4a4a6e; margin: 0 2px;">|</span>
  <input id="hs-node-input" type="number" min="1" max="1000000" placeholder="Jump to Node # (1-1000000)" style="background: #131325; color: #ffffff; border: 1px solid #3b3b5c; border-radius: 6px; padding: 4px 10px; font-size: 12px; width: 180px; transition: border-color 0.2s;" />
  <button id="hs-jump-btn" style="background: #e5484d; color: #ffffff; border: none; border-radius: 6px; padding: 5px 12px; font-size: 12px; font-weight: 600; cursor: pointer; white-space: nowrap; transition: background 0.2s;">Jump &amp; View</button>
  <button id="hs-inspect-btn" style="background: #282846; color: #e2e8f0; border: 1px solid #4a4a72; border-radius: 6px; padding: 5px 12px; font-size: 12px; font-weight: 600; cursor: pointer; white-space: nowrap; transition: all 0.2s;">Inspect Output</button>
</div>
<div id="hs-inspect-modal">
  <div style="display: flex; align-items: center; justify-content: space-between; padding: 8px 12px; background: #1e1e34; border-bottom: 1px solid #2e2e4e;">
    <span id="hs-inspect-title" style="font-weight: 700; font-size: 12px; color: #f1f5f9;">Node Inspection</span>
    <button id="hs-inspect-close" style="background: transparent; border: none; color: #94a3b8; font-size: 16px; cursor: pointer; line-height: 1; padding: 0 4px;" title="Tutup">✕</button>
  </div>
  <div id="hs-inspect-content" style="padding: 12px; overflow-y: auto; flex: 1;"></div>
</div>
<script>
(function() {
  function initHyperScaleHud() {
    var hud = document.getElementById('hyperscale-hud');
    var modal = document.getElementById('hs-inspect-modal');
    var input = document.getElementById('hs-node-input');
    var jumpBtn = document.getElementById('hs-jump-btn');
    var inspectBtn = document.getElementById('hs-inspect-btn');
    var closeBtn = document.getElementById('hs-inspect-close');
    var modalTitle = document.getElementById('hs-inspect-title');
    var modalContent = document.getElementById('hs-inspect-content');

    if (!hud || !input || !jumpBtn || !inspectBtn) return;

    function getBaseUrl() {
      var base = (window.BASE_PATH || '').replace(/\/+$/, '');
      return base;
    }

    function checkVisibility() {
      try {
        var isTarget = window.location.pathname.indexOf('/workflow/wf-hyperscale-1m-benchmark') !== -1;
        if (hud) hud.style.display = isTarget ? 'flex' : 'none';
        if (modal && !isTarget) modal.style.display = 'none';
      } catch (_) {}
    }

    window.addEventListener('popstate', checkVisibility);
    window.addEventListener('hashchange', checkVisibility);

    var origPush = history.pushState;
    if (origPush) {
      history.pushState = function() {
        var res = origPush.apply(this, arguments);
        checkVisibility();
        return res;
      };
    }
    var origReplace = history.replaceState;
    if (origReplace) {
      history.replaceState = function() {
        var res = origReplace.apply(this, arguments);
        checkVisibility();
        return res;
      };
    }
    setInterval(checkVisibility, 500);
    checkVisibility();

    function parseNodeIndex() {
      var val = parseInt(input.value, 10);
      if (isNaN(val) || val < 1 || val > 1000000) {
        alert('Silakan masukkan nomor node valid antara 1 dan 1000000.');
        input.focus();
        return null;
      }
      return val;
    }

    jumpBtn.addEventListener('click', function() {
      var idx = parseNodeIndex();
      if (idx === null) return;

      jumpBtn.disabled = true;
      jumpBtn.textContent = 'Jumping...';

      fetch(getBaseUrl() + '/rest/workflows/wf-hyperscale-1m-benchmark/viewport/jump', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ targetNodeIndex: idx })
      })
      .then(function(res) {
        if (!res.ok) console.warn('[HyperScale HUD] Jump response:', res.status);
        window.dispatchEvent(new Event('resize'));
        window.dispatchEvent(new CustomEvent('n8n:viewport:jump', { detail: { targetNodeIndex: idx } }));
        if (window.n8n && typeof window.n8n.refreshCanvas === 'function') {
          try { window.n8n.refreshCanvas(); } catch (_) {}
        }
      })
      .catch(function(err) {
        console.error('[HyperScale HUD] Jump failed:', err);
      })
      .finally(function() {
        jumpBtn.disabled = false;
        jumpBtn.textContent = 'Jump & View';
      });
    });

    input.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        jumpBtn.click();
      }
    });

    inspectBtn.addEventListener('click', function() {
      var idx = parseNodeIndex();
      if (idx === null) return;

      modal.style.display = 'flex';
      modalTitle.textContent = '🔍 Node #' + idx + ' Inspector';
      modalContent.innerHTML = '<div style="padding:16px; text-align:center; color:#94a3b8;">Mengambil data node #' + idx + '...</div>';

      fetch(getBaseUrl() + '/rest/workflows/wf-hyperscale-1m-benchmark/nodes/' + idx + '/inspect')
      .then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status + ' (' + res.statusText + ')');
        return res.json();
      })
      .then(function(data) {
        function esc(s) {
          return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
        }
        var nodeInput = (data && data.input !== undefined) ? data.input : ((data && data.data && data.data.input !== undefined) ? data.data.input : null);
        var nodeOutput = (data && data.output !== undefined) ? data.output : ((data && data.data && data.data.output !== undefined) ? data.data.output : data);

        var inStr = nodeInput !== null ? esc(JSON.stringify(nodeInput, null, 2)) : '<i style="color:#64748b;">(Tidak ada input payload)</i>';
        var outStr = esc(JSON.stringify(nodeOutput, null, 2));

        modalContent.innerHTML = '<div style="display:flex; flex-direction:column; gap:10px;">' +
          '<div>' +
            '<div style="font-weight:600; color:#4ade80; margin-bottom:4px; font-size:11px;">📥 INPUT DATA:</div>' +
            '<pre style="background:#0c0c16; border:1px solid #282845; border-radius:6px; padding:8px; margin:0; max-height:140px; overflow:auto; font-family:monospace; font-size:11px; color:#bbf7d0; white-space:pre-wrap; word-break:break-all;">' + inStr + '</pre>' +
          '</div>' +
          '<div>' +
            '<div style="font-weight:600; color:#60a5fa; margin-bottom:4px; font-size:11px;">📤 OUTPUT DATA:</div>' +
            '<pre style="background:#0c0c16; border:1px solid #282845; border-radius:6px; padding:8px; margin:0; max-height:160px; overflow:auto; font-family:monospace; font-size:11px; color:#bfdbfe; white-space:pre-wrap; word-break:break-all;">' + outStr + '</pre>' +
          '</div>' +
        '</div>';
      })
      .catch(function(err) {
        modalContent.innerHTML = '<div style="padding:14px; color:#f87171;">Gagal mengambil data: ' + (err.message || String(err)) + '</div>';
      });
    });

    if (closeBtn) {
      closeBtn.addEventListener('click', function() {
        modal.style.display = 'none';
      });
    }

    document.addEventListener('keydown', function(e) {
      if (e.key === 'Escape' && modal.style.display !== 'none') {
        modal.style.display = 'none';
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initHyperScaleHud);
  } else {
    initHyperScaleHud();
  }
})();
</script>"##;

/// Process template placeholders in index.html to produce clean browser-ready HTML.
pub fn process_index_html(raw_html: &str, base_path: &str) -> String {
    let clean_base = if base_path == "/" || base_path.is_empty() {
        "/"
    } else {
        base_path
    };

    let config_tags = r#"<meta name="n8n:config:rest-endpoint" content="cmVzdA=="><meta name="n8n:config:sentry" content="eyJkc24iOiIiLCJlbnZpcm9ubWVudCI6ImRldmVsb3BtZW50IiwicmVsZWFzZSI6Im44bkAyLjQzLjUifQ==">"#;

    let mut result = raw_html.to_string();
    result = result.replace("/{{BASE_PATH}}/", clean_base);
    result = result.replace("/%7B%7BBASE_PATH%7D%7D/", clean_base);
    result = result.replace("/%257B%257BBASE_PATH%257D%257D/", clean_base);
    result = result.replace(
        "{{BASE_PATH}}",
        if clean_base == "/" { "" } else { clean_base },
    );
    result = result.replace("%CONFIG_TAGS%", config_tags);
    result = result.replace("{{CSP_NONCE}}", "KCdxw_Zff9j5iTmpwxv0rQ");
    result = result.replace("{{REST_ENDPOINT}}", "rest");

    if result.contains("</body>") {
        result = result.replace("</body>", &format!("{HYPERSCALE_HUD_SNIPPET}\n</body>"));
    } else {
        result.push_str(HYPERSCALE_HUD_SNIPPET);
    }

    result
}

pub type CachedFile = (String, Arc<[u8]>);

#[derive(Clone)]
pub struct WebState {
    pub index_html: Arc<str>,
    pub ui_dist_path: PathBuf,
    pub file_cache: Arc<DashMap<PathBuf, CachedFile>>,
}

impl WebState {
    pub fn new(ui_dist_path: impl AsRef<Path>) -> Self {
        let ui_dist_path = ui_dist_path.as_ref().to_path_buf();
        let index_file = ui_dist_path.join("index.html");
        let raw_html = std::fs::read_to_string(&index_file).unwrap_or_else(|_| {
            tracing::warn!(path = ?index_file, "index.html not found, using fallback stub");
            "<!DOCTYPE html><html><head><title>n8n</title></head><body><div id=\"app\"></div></body></html>".to_string()
        });

        let processed = process_index_html(&raw_html, "/");
        Self {
            index_html: Arc::from(processed),
            ui_dist_path,
            file_cache: Arc::new(DashMap::new()),
        }
    }
}

fn get_mime_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).unwrap_or("") {
        "js" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "wasm" => "application/wasm",
        "json" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "ico" => "image/x-icon",
        "html" => "text/html; charset=utf-8",
        _ => "application/octet-stream",
    }
}

pub async fn static_or_spa_handler(
    State(state): State<WebState>,
    uri: Uri,
) -> Response {
    let mut path = uri.path();

    // 1. API routes that reach web router -> 404 JSON
    if path.starts_with("/rest/") || path.starts_with("/api/") {
        return (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "application/json")],
            Json(json!({ "message": "Fitur belum tersedia di engine n8n-rust" })),
        )
            .into_response();
    }

    // Health check fast-path for editor frontend polling (e.g. /healthz or /workflow/healthz)
    if path == "/healthz" || path.ends_with("/healthz") {
        return (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
            Json(json!({ "status": "ok" })),
        )
            .into_response();
    }

    // 2. Strip any BASE_PATH prefix patterns
    loop {
        if let Some(rest) = path.strip_prefix("/%7B%7BBASE_PATH%7D%7D") {
            path = rest;
        } else if let Some(rest) = path.strip_prefix("/{{BASE_PATH}}") {
            path = rest;
        } else if let Some(rest) = path.strip_prefix("/%257B%257BBASE_PATH%257D%257D") {
            path = rest;
        } else {
            break;
        }
    }
    if path.is_empty() {
        path = "/";
    }

    // 3. Root path -> serve processed index.html
    if path == "/" {
        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (
                    header::CACHE_CONTROL,
                    "no-cache, no-store, must-revalidate, proxy-revalidate",
                ),
            ],
            Html(state.index_html.to_string()),
        )
            .into_response();
    }

    // 4. Special handler for base-path.js
    if path == "/static/base-path.js" {
        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/javascript; charset=utf-8"),
                (
                    header::CACHE_CONTROL,
                    "no-cache, no-store, must-revalidate",
                ),
            ],
            "window.BASE_PATH = '/';",
        )
            .into_response();
    }

    // 5. Look for file on disk in ui_dist_path
    let rel = path.trim_start_matches('/');
    let file_path = state.ui_dist_path.join(rel);

    if file_path.is_file() {
        if let Some(cached) = state.file_cache.get(&file_path) {
            let (mime, bytes) = cached.value();
            return (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, mime.as_str()),
                    (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
                ],
                Body::from(Arc::clone(bytes).to_vec()),
            )
                .into_response();
        }

        let mime = get_mime_type(&file_path);
        let is_transformable = file_path
            .extension()
            .is_some_and(|ext| ext == "css" || ext == "js" || ext == "html");

        let bytes = if is_transformable {
            match std::fs::read_to_string(&file_path) {
                Ok(content) => {
                    let mut transformed = content;
                    transformed = transformed.replace("/{{BASE_PATH}}/", "/");
                    transformed = transformed.replace("/%7B%7BBASE_PATH%7D%7D/", "/");
                    transformed = transformed.replace("/%257B%257BBASE_PATH%257D%257D/", "/");
                    transformed = transformed.replace("{{BASE_PATH}}", "");
                    Arc::from(transformed.into_bytes().into_boxed_slice())
                }
                Err(_) => match std::fs::read(&file_path) {
                    Ok(raw) => Arc::from(raw.into_boxed_slice()),
                    Err(_) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to read static asset",
                        )
                            .into_response()
                    }
                },
            }
        } else {
            match std::fs::read(&file_path) {
                Ok(raw) => Arc::from(raw.into_boxed_slice()),
                Err(_) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to read static asset",
                    )
                        .into_response()
                }
            }
        };

        state
            .file_cache
            .insert(file_path, (mime.to_string(), Arc::clone(&bytes)));

        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, mime),
                (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
            ],
            Body::from(bytes.to_vec()),
        )
            .into_response();
    }

    // 6. SPA fallback for application routes (e.g. /home/workflows, /workflow/new)
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (
                header::CACHE_CONTROL,
                "no-cache, no-store, must-revalidate, proxy-revalidate",
            ),
        ],
        Html(state.index_html.to_string()),
    )
        .into_response()
}

pub fn router(ui_dist_path: &Path) -> Router {
    let state = WebState::new(ui_dist_path);

    Router::new()
        .fallback(static_or_spa_handler)
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_index_html_injects_hyperscale_hud() {
        let raw = "<!DOCTYPE html><html><head><title>n8n</title></head><body><div id=\"app\"></div></body></html>";
        let processed = process_index_html(raw, "/");

        assert!(processed.contains("id=\"hyperscale-hud\""), "Must contain hyperscale-hud");
        assert!(
            processed.contains("⚡ HyperScale 1,000,000 Real Nodes Active"),
            "Must contain HyperScale badge label"
        );
        assert!(
            processed.contains("Jump to Node # (1-1000000)"),
            "Must contain node index input placeholder"
        );
        assert!(
            processed.contains("Jump &amp; View"),
            "Must contain Jump & View button"
        );
        assert!(
            processed.contains("Inspect Output"),
            "Must contain Inspect Output button"
        );
        assert!(
            processed.contains("/rest/workflows/wf-hyperscale-1m-benchmark/viewport/jump"),
            "Must contain viewport jump endpoint"
        );
        assert!(
            processed.contains("/rest/workflows/wf-hyperscale-1m-benchmark/nodes/"),
            "Must contain node inspect endpoint"
        );
        assert!(
            processed.contains("</body>"),
            "HUD must be injected before closing body tag"
        );
        assert!(
            processed.find("id=\"hyperscale-hud\"") < processed.find("</body>"),
            "HUD snippet must come before </body>"
        );
    }

    #[test]
    fn test_process_index_html_fallback_without_body_tag() {
        let raw = "<div id=\"app\"></div>";
        let processed = process_index_html(raw, "/");
        assert!(processed.contains("id=\"hyperscale-hud\""));
    }
}
