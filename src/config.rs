use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub ui_dist_path: PathBuf,
    pub db_path: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let port = std::env::var("N8N_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(5678);

        let host = std::env::var("N8N_LISTEN_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string());

        let ui_dist_path = std::env::var("N8N_UI_DIST")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("ui-dist"));

        let db_path = std::env::var("N8N_DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("n8n_data.sqlite"));

        Self {
            host,
            port,
            ui_dist_path,
            db_path,
        }
    }
}

impl ServerConfig {
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}
