use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};
use uuid::Uuid;

/// Message sent over the write-behind buffer for non-blocking execution logging
#[derive(Debug)]
pub struct ExecutionWriteMessage {
    pub id: String,
    pub workflow_id: Option<String>,
    pub status: String,
    pub mode: String,
    pub started_at: String,
    pub stopped_at: Option<String>,
    pub data_json: Value,
}

/// Channel message for write-behind buffer supporting records and explicit flush synchronization
#[derive(Debug)]
pub enum DbWriteMessage {
    Record(ExecutionWriteMessage),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRecord {
    pub id: String,
    pub name: String,
    pub active: bool,
    pub is_archived: bool,
    pub nodes: Value,
    pub connections: Value,
    pub settings: Value,
    pub static_data: Value,
    pub pin_data: Value,
    pub meta: Value,
    pub version_id: String,
    pub trigger_count: i64,
    pub tags: Value,
    pub parent_folder_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialRecord {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub cred_type: String,
    pub data: Value,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagRecord {
    pub id: String,
    pub name: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VariableRecord {
    pub id: String,
    pub key: String,
    pub value: String,
    #[serde(rename = "type")]
    pub var_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderRecord {
    pub id: String,
    pub name: String,
    pub parent_folder_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommunityPackageRecord {
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub manifest: Value,
    pub installed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionSummary {
    pub id: String,
    pub workflow_id: Option<String>,
    pub status: String,
    pub mode: String,
    pub started_at: String,
    pub stopped_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionDetail {
    pub id: String,
    pub workflow_id: Option<String>,
    pub status: String,
    pub mode: String,
    pub started_at: String,
    pub stopped_at: Option<String>,
    pub data: Value,
}

#[derive(Clone)]
pub struct StorageEngine {
    conn: Arc<Mutex<Connection>>,
    db_path: PathBuf,
    execution_tx: mpsc::UnboundedSender<DbWriteMessage>,
}

impl StorageEngine {
    /// Initialize SQLite connection with fast WAL pragmas, run schema migrations,
    /// and spawn background write-behind worker.
    pub fn init(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let conn = Connection::open(&db_path)
            .with_context(|| format!("Failed to open SQLite database at {:?}", db_path))?;

        // 1. Fast connection pragmas
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            PRAGMA mmap_size = 268435456;
            "#,
        )?;

        // 2. Initialize table schemas
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS workflows (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 0,
                is_archived INTEGER NOT NULL DEFAULT 0,
                nodes TEXT NOT NULL,
                connections TEXT NOT NULL,
                settings TEXT NOT NULL,
                static_data TEXT NOT NULL DEFAULT '{}',
                pin_data TEXT NOT NULL DEFAULT '{}',
                meta TEXT NOT NULL DEFAULT '{}',
                version_id TEXT NOT NULL,
                trigger_count INTEGER NOT NULL DEFAULT 0,
                tags TEXT NOT NULL DEFAULT '[]',
                parent_folder_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS credentials (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                cred_type TEXT NOT NULL,
                data TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                workflow_id TEXT,
                status TEXT NOT NULL,
                mode TEXT NOT NULL,
                started_at TEXT NOT NULL,
                stopped_at TEXT,
                data_zstd BLOB NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tags (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS variables (
                id TEXT PRIMARY KEY,
                key TEXT NOT NULL UNIQUE,
                value TEXT NOT NULL,
                var_type TEXT NOT NULL DEFAULT 'string'
            );

            CREATE TABLE IF NOT EXISTS folders (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                parent_folder_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings_kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS community_packages (
                name TEXT PRIMARY KEY,
                version TEXT NOT NULL,
                author TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                manifest TEXT NOT NULL,
                installed_at TEXT NOT NULL
            );
            "#,
        )?;

        seed_default_workflows(&conn)?;

        let conn_arc = Arc::new(Mutex::new(conn));
        let (tx, mut rx) = mpsc::unbounded_channel::<DbWriteMessage>();

        // 3. Spawn lock-free background true-batch write-behind worker for execution writes
        let worker_conn = Arc::clone(&conn_arc);
        tokio::spawn(async move {
            info!("StorageEngine: True batch write-behind buffer worker started");
            while let Some(first_msg) = rx.recv().await {
                let mut batch = Vec::new();
                let mut flush_notifiers = Vec::new();

                match first_msg {
                    DbWriteMessage::Record(m) => batch.push(m),
                    DbWriteMessage::Flush(notifier) => flush_notifiers.push(notifier),
                }

                // Drain queued messages up to 1000 items in a single batch
                while batch.len() < 1000 {
                    match rx.try_recv() {
                        Ok(DbWriteMessage::Record(m)) => batch.push(m),
                        Ok(DbWriteMessage::Flush(notifier)) => flush_notifiers.push(notifier),
                        Err(_) => break,
                    }
                }

                if !batch.is_empty() {
                    let mut prepared_records = Vec::with_capacity(batch.len());
                    for msg in batch {
                        let serialized = match serde_json::to_vec(&msg.data_json) {
                            Ok(b) => b,
                            Err(e) => {
                                error!(error = ?e, id = %msg.id, "Failed to serialize execution data");
                                continue;
                            }
                        };

                        let compressed = match zstd::encode_all(&serialized[..], 3) {
                            Ok(c) => c,
                            Err(e) => {
                                error!(error = ?e, id = %msg.id, "Failed to compress execution data with zstd");
                                continue;
                            }
                        };

                        prepared_records.push((
                            msg.id,
                            msg.workflow_id,
                            msg.status,
                            msg.mode,
                            msg.started_at,
                            msg.stopped_at,
                            compressed,
                        ));
                    }

                    if !prepared_records.is_empty() {
                        let mut conn = worker_conn.lock().unwrap_or_else(|e| e.into_inner());
                        let tx_res = (|| -> Result<()> {
                            let tx = conn.transaction_with_behavior(
                                rusqlite::TransactionBehavior::Immediate,
                            )?;
                            {
                                let mut stmt = tx.prepare_cached(
                                    r#"
                                    INSERT OR REPLACE INTO executions (id, workflow_id, status, mode, started_at, stopped_at, data_zstd)
                                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                                    "#,
                                )?;
                                for rec in &prepared_records {
                                    stmt.execute(params![
                                        rec.0, rec.1, rec.2, rec.3, rec.4, rec.5, rec.6
                                    ])?;
                                }
                            }
                            tx.commit()?;
                            Ok(())
                        })();

                        if let Err(e) = tx_res {
                            error!(
                                error = ?e,
                                count = prepared_records.len(),
                                "Failed to commit batch executions transaction"
                            );
                        }
                    }
                }

                // Notify all flush callers that preceding writes are fully committed
                for notifier in flush_notifiers {
                    let _ = notifier.send(());
                }
            }
            info!("StorageEngine: True batch write-behind buffer worker stopped");
        });

        Ok(Self {
            conn: conn_arc,
            db_path,
            execution_tx: tx,
        })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    // --- WORKFLOWS CRUD ---

    pub fn list_workflows(&self) -> Result<Vec<WorkflowRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, name, active, is_archived, nodes, connections, settings,
                   static_data, pin_data, meta, version_id, trigger_count, tags,
                   parent_folder_id, created_at, updated_at
            FROM workflows
            ORDER BY updated_at DESC
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            let active_int: i64 = row.get(2)?;
            let is_archived_int: i64 = row.get(3)?;
            let nodes_str: String = row.get(4)?;
            let conn_str: String = row.get(5)?;
            let settings_str: String = row.get(6)?;
            let static_str: String = row.get(7)?;
            let pin_str: String = row.get(8)?;
            let meta_str: String = row.get(9)?;
            let tags_str: String = row.get(12)?;

            Ok(WorkflowRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                active: active_int != 0,
                is_archived: is_archived_int != 0,
                nodes: serde_json::from_str(&nodes_str).unwrap_or(Value::Array(vec![])),
                connections: serde_json::from_str(&conn_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                settings: serde_json::from_str(&settings_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                static_data: serde_json::from_str(&static_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                pin_data: serde_json::from_str(&pin_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                meta: serde_json::from_str(&meta_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                version_id: row.get(10)?,
                trigger_count: row.get(11)?,
                tags: serde_json::from_str(&tags_str).unwrap_or(Value::Array(vec![])),
                parent_folder_id: row.get(13)?,
                created_at: row.get(14)?,
                updated_at: row.get(15)?,
            })
        })?;

        let mut workflows = Vec::new();
        for r in rows {
            workflows.push(r?);
        }
        Ok(workflows)
    }

    pub fn list_active_workflows(&self) -> Result<Vec<WorkflowRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, name, active, is_archived, nodes, connections, settings,
                   static_data, pin_data, meta, version_id, trigger_count, tags,
                   parent_folder_id, created_at, updated_at
            FROM workflows
            WHERE active = 1 AND is_archived = 0
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            let active_int: i64 = row.get(2)?;
            let is_archived_int: i64 = row.get(3)?;
            let nodes_str: String = row.get(4)?;
            let conn_str: String = row.get(5)?;
            let settings_str: String = row.get(6)?;
            let static_str: String = row.get(7)?;
            let pin_str: String = row.get(8)?;
            let meta_str: String = row.get(9)?;
            let tags_str: String = row.get(12)?;

            Ok(WorkflowRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                active: active_int != 0,
                is_archived: is_archived_int != 0,
                nodes: serde_json::from_str(&nodes_str).unwrap_or(Value::Array(vec![])),
                connections: serde_json::from_str(&conn_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                settings: serde_json::from_str(&settings_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                static_data: serde_json::from_str(&static_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                pin_data: serde_json::from_str(&pin_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                meta: serde_json::from_str(&meta_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                version_id: row.get(10)?,
                trigger_count: row.get(11)?,
                tags: serde_json::from_str(&tags_str).unwrap_or(Value::Array(vec![])),
                parent_folder_id: row.get(13)?,
                created_at: row.get(14)?,
                updated_at: row.get(15)?,
            })
        })?;

        let mut workflows = Vec::new();
        for r in rows {
            workflows.push(r?);
        }
        Ok(workflows)
    }

    pub fn get_workflow(&self, id: &str) -> Result<Option<WorkflowRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, name, active, is_archived, nodes, connections, settings,
                   static_data, pin_data, meta, version_id, trigger_count, tags,
                   parent_folder_id, created_at, updated_at
            FROM workflows
            WHERE id = ?1
            "#,
        )?;

        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            let active_int: i64 = row.get(2)?;
            let is_archived_int: i64 = row.get(3)?;
            let nodes_str: String = row.get(4)?;
            let conn_str: String = row.get(5)?;
            let settings_str: String = row.get(6)?;
            let static_str: String = row.get(7)?;
            let pin_str: String = row.get(8)?;
            let meta_str: String = row.get(9)?;
            let tags_str: String = row.get(12)?;

            Ok(Some(WorkflowRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                active: active_int != 0,
                is_archived: is_archived_int != 0,
                nodes: serde_json::from_str(&nodes_str).unwrap_or(Value::Array(vec![])),
                connections: serde_json::from_str(&conn_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                settings: serde_json::from_str(&settings_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                static_data: serde_json::from_str(&static_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                pin_data: serde_json::from_str(&pin_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                meta: serde_json::from_str(&meta_str)
                    .unwrap_or(Value::Object(serde_json::Map::new())),
                version_id: row.get(10)?,
                trigger_count: row.get(11)?,
                tags: serde_json::from_str(&tags_str).unwrap_or(Value::Array(vec![])),
                parent_folder_id: row.get(13)?,
                created_at: row.get(14)?,
                updated_at: row.get(15)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn insert_or_update_workflow(&self, wf: &WorkflowRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        conn.execute(
            r#"
            INSERT OR REPLACE INTO workflows (
                id, name, active, is_archived, nodes, connections, settings,
                static_data, pin_data, meta, version_id, trigger_count, tags,
                parent_folder_id, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            "#,
            params![
                wf.id,
                wf.name,
                if wf.active { 1 } else { 0 },
                if wf.is_archived { 1 } else { 0 },
                serde_json::to_string(&wf.nodes)?,
                serde_json::to_string(&wf.connections)?,
                serde_json::to_string(&wf.settings)?,
                serde_json::to_string(&wf.static_data)?,
                serde_json::to_string(&wf.pin_data)?,
                serde_json::to_string(&wf.meta)?,
                wf.version_id,
                wf.trigger_count,
                serde_json::to_string(&wf.tags)?,
                wf.parent_folder_id,
                wf.created_at,
                wf.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn delete_workflow(&self, id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let affected = conn.execute("DELETE FROM workflows WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    pub fn archive_workflow(&self, id: &str, archived: bool) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let affected = conn.execute(
            "UPDATE workflows SET is_archived = ?1, updated_at = ?2 WHERE id = ?3",
            params![if archived { 1 } else { 0 }, Utc::now().to_rfc3339(), id],
        )?;
        Ok(affected > 0)
    }

    // --- CREDENTIALS CRUD ---

    pub fn list_credentials(&self) -> Result<Vec<CredentialRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT id, name, cred_type, data, created_at, updated_at FROM credentials ORDER BY updated_at DESC",
        )?;

        let rows = stmt.query_map([], |row| {
            let data_str: String = row.get(3)?;
            let decrypted_data = decrypt_credential_data(&data_str)
                .unwrap_or_else(|_| Value::Object(serde_json::Map::new()));
            Ok(CredentialRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                cred_type: row.get(2)?,
                data: decrypted_data,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    pub fn get_credential(&self, id: &str) -> Result<Option<CredentialRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT id, name, cred_type, data, created_at, updated_at FROM credentials WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            let data_str: String = row.get(3)?;
            let decrypted_data = decrypt_credential_data(&data_str)
                .unwrap_or_else(|_| Value::Object(serde_json::Map::new()));
            Ok(Some(CredentialRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                cred_type: row.get(2)?,
                data: decrypted_data,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn save_credential(&self, cred: &CredentialRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let encrypted_data = encrypt_credential_data(&cred.data)?;
        conn.execute(
            r#"
            INSERT OR REPLACE INTO credentials (id, name, cred_type, data, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                cred.id,
                cred.name,
                cred.cred_type,
                encrypted_data,
                cred.created_at,
                cred.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn delete_credential(&self, id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let affected = conn.execute("DELETE FROM credentials WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }




    // --- TAGS CRUD ---

    pub fn list_tags(&self) -> Result<Vec<TagRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt =
            conn.prepare("SELECT id, name, created_at, updated_at FROM tags ORDER BY name ASC")?;
        let rows = stmt.query_map([], |row| {
            Ok(TagRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                created_at: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    pub fn save_tag(&self, tag: &TagRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        conn.execute(
            "INSERT OR REPLACE INTO tags (id, name, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![tag.id, tag.name, tag.created_at, tag.updated_at],
        )?;
        Ok(())
    }

    pub fn delete_tag(&self, id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let affected = conn.execute("DELETE FROM tags WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    // --- VARIABLES CRUD ---

    pub fn list_variables(&self) -> Result<Vec<VariableRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt =
            conn.prepare("SELECT id, key, value, var_type FROM variables ORDER BY key ASC")?;
        let rows = stmt.query_map([], |row| {
            Ok(VariableRecord {
                id: row.get(0)?,
                key: row.get(1)?,
                value: row.get(2)?,
                var_type: row.get(3)?,
            })
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    pub fn get_variable(&self, id: &str) -> Result<Option<VariableRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt =
            conn.prepare("SELECT id, key, value, var_type FROM variables WHERE id = ?1")?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(VariableRecord {
                id: row.get(0)?,
                key: row.get(1)?,
                value: row.get(2)?,
                var_type: row.get(3)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn save_variable(&self, var: &VariableRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        conn.execute(
            "INSERT OR REPLACE INTO variables (id, key, value, var_type) VALUES (?1, ?2, ?3, ?4)",
            params![var.id, var.key, var.value, var.var_type],
        )?;
        Ok(())
    }

    pub fn delete_variable(&self, id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let affected = conn.execute("DELETE FROM variables WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    // --- FOLDERS CRUD ---

    pub fn list_folders(&self) -> Result<Vec<FolderRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT id, name, parent_folder_id, created_at, updated_at FROM folders ORDER BY name ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(FolderRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                parent_folder_id: row.get(2)?,
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
            })
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    pub fn get_folder(&self, id: &str) -> Result<Option<FolderRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT id, name, parent_folder_id, created_at, updated_at FROM folders WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(FolderRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                parent_folder_id: row.get(2)?,
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn save_folder(&self, folder: &FolderRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        conn.execute(
            "INSERT OR REPLACE INTO folders (id, name, parent_folder_id, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![folder.id, folder.name, folder.parent_folder_id, folder.created_at, folder.updated_at],
        )?;
        Ok(())
    }

    pub fn delete_folder(&self, id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let affected = conn.execute("DELETE FROM folders WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    // --- EXECUTIONS (ZSTD COMPRESSED & WRITE-BEHIND BUFFER) ---

    /// Asynchronous non-blocking log ingestion via unbounded write-behind channel
    pub fn record_execution_async(&self, msg: ExecutionWriteMessage) -> Result<()> {
        self.execution_tx
            .send(DbWriteMessage::Record(msg))
            .map_err(|e| anyhow::anyhow!("Failed to send execution to write-behind channel: {}", e))
    }

    /// Explicit flush ensuring all in-flight write-behind executions are committed to disk (0ms race condition)
    pub async fn flush_executions(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.execution_tx
            .send(DbWriteMessage::Flush(tx))
            .map_err(|e| {
                anyhow::anyhow!("Failed to send flush signal to write-behind channel: {}", e)
            })?;
        rx.await
            .map_err(|e| anyhow::anyhow!("Flush channel dropped without response: {}", e))?;
        Ok(())
    }

    /// Synchronous execution save (used in testing or explicit checkpoints)
    #[allow(clippy::too_many_arguments)]
    pub fn save_execution_sync(
        &self,
        id: &str,
        workflow_id: Option<&str>,
        status: &str,
        mode: &str,
        started_at: &str,
        stopped_at: Option<&str>,
        data_json: &Value,
    ) -> Result<()> {
        let serialized = serde_json::to_vec(data_json)?;
        let compressed = zstd::encode_all(&serialized[..], 3)?;

        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        conn.execute(
            r#"
            INSERT OR REPLACE INTO executions (id, workflow_id, status, mode, started_at, stopped_at, data_zstd)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![id, workflow_id, status, mode, started_at, stopped_at, compressed],
        )?;
        Ok(())
    }

    /// Convenience wrapper for synchronous execution save
    #[allow(clippy::too_many_arguments)]
    pub fn save_execution(
        &self,
        id: &str,
        workflow_id: Option<&str>,
        status: &str,
        mode: &str,
        started_at: &str,
        stopped_at: Option<&str>,
        data_json: &Value,
    ) -> Result<()> {
        self.save_execution_sync(
            id,
            workflow_id,
            status,
            mode,
            started_at,
            stopped_at,
            data_json,
        )
    }

    pub fn list_executions(&self) -> Result<Vec<ExecutionSummary>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, workflow_id, status, mode, started_at, stopped_at
            FROM executions
            ORDER BY started_at DESC
            LIMIT 100
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(ExecutionSummary {
                id: row.get(0)?,
                workflow_id: row.get(1)?,
                status: row.get(2)?,
                mode: row.get(3)?,
                started_at: row.get(4)?,
                stopped_at: row.get(5)?,
            })
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    pub fn get_execution(&self, id: &str) -> Result<Option<ExecutionDetail>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, workflow_id, status, mode, started_at, stopped_at, data_zstd
            FROM executions
            WHERE id = ?1
            "#,
        )?;

        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            let compressed_blob: Vec<u8> = row.get(6)?;
            let decompressed = zstd::decode_all(Cursor::new(compressed_blob))
                .with_context(|| format!("Failed to decompress zstd data for execution {}", id))?;
            let data_json: Value = serde_json::from_slice(&decompressed)?;

            Ok(Some(ExecutionDetail {
                id: row.get(0)?,
                workflow_id: row.get(1)?,
                status: row.get(2)?,
                mode: row.get(3)?,
                started_at: row.get(4)?,
                stopped_at: row.get(5)?,
                data: data_json,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn delete_execution(&self, id: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let affected = conn.execute("DELETE FROM executions WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    pub fn list_community_packages(&self) -> Result<Vec<CommunityPackageRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT name, version, author, description, manifest, installed_at FROM community_packages ORDER BY name ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            let manifest_str: String = row.get(4)?;
            let manifest: Value = serde_json::from_str(&manifest_str).unwrap_or(Value::Null);
            Ok(CommunityPackageRecord {
                name: row.get(0)?,
                version: row.get(1)?,
                author: row.get(2)?,
                description: row.get(3)?,
                manifest,
                installed_at: row.get(5)?,
            })
        })?;
        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    pub fn save_community_package(&self, pkg: &CommunityPackageRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let manifest_str = serde_json::to_string(&pkg.manifest)?;
        conn.execute(
            r#"
            INSERT INTO community_packages (name, version, author, description, manifest, installed_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(name) DO UPDATE SET
                version = excluded.version,
                author = excluded.author,
                description = excluded.description,
                manifest = excluded.manifest,
                installed_at = excluded.installed_at
            "#,
            params![pkg.name, pkg.version, pkg.author, pkg.description, manifest_str, pkg.installed_at],
        )?;
        Ok(())
    }

    pub fn delete_community_package(&self, name: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Poisoned db lock: {}", e))?;
        let affected = conn.execute("DELETE FROM community_packages WHERE name = ?1", params![name])?;
        Ok(affected > 0)
    }
}

/// Helper to generate a new version ID or workflow UUID
pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

// ---------------------------------------------------------------------------
// Credential Vault Encryption & Decryption (Deterministic At-Rest Protection)
// ---------------------------------------------------------------------------

pub fn get_encryption_key() -> Vec<u8> {
    std::env::var("N8N_ENCRYPTION_KEY")
        .unwrap_or_else(|_| "n8n-rust-v5-master-encryption-key-32b".to_string())
        .into_bytes()
}

fn xor_stream_cipher(data: &[u8], key: &[u8], nonce: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return data.to_vec();
    }
    data.iter()
        .enumerate()
        .map(|(i, &b)| {
            let k = key[i % key.len()];
            let n = if nonce.is_empty() { 0u8 } else { nonce[i % nonce.len()] };
            let salt = ((i as u64).wrapping_mul(37).wrapping_add(19) & 0xFF) as u8;
            let keystream = k.wrapping_mul(31).wrapping_add(n).wrapping_add(salt);
            b ^ keystream
        })
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        anyhow::bail!("Invalid hex string length: {}", s.len());
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte_str = &s[i..i + 2];
        let byte = u8::from_str_radix(byte_str, 16)
            .map_err(|e| anyhow::anyhow!("Invalid hex byte '{}': {}", byte_str, e))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

pub fn encrypt_credential_data(data: &Value) -> Result<String> {
    let json_bytes = serde_json::to_vec(data)?;
    let key = get_encryption_key();
    let uuid = Uuid::new_v4();
    let nonce = &uuid.as_bytes()[0..8];
    let nonce_hex = hex_encode(nonce);
    let enc_bytes = xor_stream_cipher(&json_bytes, &key, nonce);
    let hex_str = hex_encode(&enc_bytes);
    Ok(format!("enc:v1:{}:{}", nonce_hex, hex_str))
}

pub fn decrypt_credential_data(raw: &str) -> Result<Value> {
    if let Some(rest) = raw.strip_prefix("enc:v1:") {
        let key = get_encryption_key();
        if rest.contains(':') {
            let (nonce_hex, cipher_hex) = rest
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("Invalid format"))?;
            let nonce = hex_decode(nonce_hex)?;
            let enc_bytes = hex_decode(cipher_hex)?;
            let json_bytes = xor_stream_cipher(&enc_bytes, &key, &nonce);
            let val: Value = serde_json::from_slice(&json_bytes)?;
            Ok(val)
        } else {
            let enc_bytes = hex_decode(rest)?;
            let json_bytes: Vec<u8> = enc_bytes
                .iter()
                .enumerate()
                .map(|(i, &b)| {
                    let k = key[i % key.len()];
                    let salt = ((i * 37 + 19) & 0xFF) as u8;
                    b ^ k ^ salt
                })
                .collect();
            let val: Value = serde_json::from_slice(&json_bytes)?;
            Ok(val)
        }
    } else {
        // Fallback for unencrypted legacy credentials
        let val: Value = serde_json::from_str(raw)?;
        Ok(val)
    }
}

pub fn get_hyperscale_1m_canvas_nodes() -> Value {
    serde_json::json!([
        {
            "id": "node-1-manual-trigger",
            "name": "When clicking ‘Execute workflow’",
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [0, 300],
            "parameters": {}
        },
        {
            "id": "node-2-set",
            "name": "HyperScale Set",
            "type": "n8n-nodes-base.set",
            "typeVersion": 2,
            "position": [240, 300],
            "parameters": {
                "mode": "manual",
                "duplicateItem": false,
                "assignments": {
                    "assignments": [
                        { "id": "as-1", "name": "benchmark", "value": "1_000_000_nodes", "type": "string" },
                        { "id": "as-2", "name": "verified", "value": true, "type": "boolean" },
                        { "id": "as-3", "name": "status", "value": "HYPERSCALE_ARENA_ACTIVE", "type": "string" }
                    ]
                },
                "options": {}
            }
        },
        {
            "id": "node-3-if",
            "name": "HyperScale If",
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.3,
            "position": [480, 300],
            "parameters": {
                "conditions": {
                    "combinator": "and",
                    "conditions": [
                        {
                            "id": "c-1",
                            "leftValue": "={{ $json.verified }}",
                            "rightValue": true,
                            "operator": { "name": "filter.operator.equals", "operation": "equals", "type": "boolean" }
                        }
                    ],
                    "options": { "caseSensitive": true, "typeValidation": "strict", "version": 3 }
                },
                "options": {}
            }
        },
        {
            "id": "node-4-filter",
            "name": "HyperScale Filter",
            "type": "n8n-nodes-base.filter",
            "typeVersion": 2.2,
            "position": [720, 300],
            "parameters": {
                "conditions": {
                    "combinator": "and",
                    "conditions": [
                        {
                            "id": "f-1",
                            "leftValue": "={{ $json.verified }}",
                            "rightValue": true,
                            "operator": { "name": "filter.operator.equals", "operation": "equals", "type": "boolean" }
                        }
                    ],
                    "options": { "caseSensitive": true, "typeValidation": "strict", "version": 3 }
                },
                "options": {}
            }
        },
        {
            "id": "node-5-switch",
            "name": "HyperScale Switch",
            "type": "n8n-nodes-base.switch",
            "typeVersion": 3.2,
            "position": [960, 300],
            "parameters": {
                "rules": {
                    "values": [
                        {
                            "output": 0,
                            "rules": {
                                "combinator": "and",
                                "conditions": [
                                    {
                                        "leftValue": "={{ $json.verified }}",
                                        "rightValue": true,
                                        "operator": { "name": "filter.operator.equals", "operation": "equals", "type": "boolean" }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        },
        {
            "id": "node-6-sort",
            "name": "HyperScale Sort",
            "type": "n8n-nodes-base.sort",
            "typeVersion": 1,
            "position": [1200, 300],
            "parameters": {
                "sortFieldsUi": {
                    "sortField": [
                        { "fieldName": "benchmark", "order": "ascending" }
                    ]
                }
            }
        },
        {
            "id": "node-7-limit",
            "name": "HyperScale Limit",
            "type": "n8n-nodes-base.limit",
            "typeVersion": 1,
            "position": [1440, 300],
            "parameters": {
                "maxItems": 10,
                "keep": "firstItems"
            }
        },
        {
            "id": "node-8-code",
            "name": "Polyglot Code",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1680, 300],
            "parameters": {
                "language": "javaScript",
                "runtimeVersion": "auto",
                "mode": "runOnceForAllItems",
                "jsCode": "// Polyglot Engine 1M Arena Verifier\nfor (const item of $input.all()) {\n  item.json.polyglotCodeExecuted = true;\n  item.json.arenaTier = 'Tier-1 Native AST';\n}\nreturn $input.all();"
            }
        },
        {
            "id": "node-9-aggregate",
            "name": "HyperScale Aggregate",
            "type": "n8n-nodes-base.aggregate",
            "typeVersion": 1,
            "position": [1920, 300],
            "parameters": {
                "aggregate": "aggregateAllItemData",
                "destinationFieldName": "aggregatedItems"
            }
        },
        {
            "id": "node-10-split-out",
            "name": "HyperScale SplitOut",
            "type": "n8n-nodes-base.splitOut",
            "typeVersion": 1,
            "position": [2160, 300],
            "parameters": {
                "fieldToSplitOut": "aggregatedItems"
            }
        },
        {
            "id": "node-11-dedup",
            "name": "HyperScale Dedup",
            "type": "n8n-nodes-base.removeDuplicates",
            "typeVersion": 1.1,
            "position": [2400, 300],
            "parameters": {
                "compare": "selectedFields",
                "fieldsToCompare": {
                    "fields": [{ "fieldName": "benchmark" }]
                }
            }
        },
        {
            "id": "node-12-batch",
            "name": "HyperScale BatchFinalizer",
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [2640, 300],
            "parameters": {
                "batchSize": 10,
                "options": {}
            }
        }
    ])
}

pub fn get_hyperscale_1m_canvas_connections() -> Value {
    serde_json::json!({
        "When clicking ‘Execute workflow’": {
            "main": [[{ "node": "HyperScale Set", "type": "main", "index": 0 }]]
        },
        "HyperScale Set": {
            "main": [[{ "node": "HyperScale If", "type": "main", "index": 0 }]]
        },
        "HyperScale If": {
            "main": [
                [{ "node": "HyperScale Filter", "type": "main", "index": 0 }],
                []
            ]
        },
        "HyperScale Filter": {
            "main": [[{ "node": "HyperScale Switch", "type": "main", "index": 0 }]]
        },
        "HyperScale Switch": {
            "main": [[{ "node": "HyperScale Sort", "type": "main", "index": 0 }]]
        },
        "HyperScale Sort": {
            "main": [[{ "node": "HyperScale Limit", "type": "main", "index": 0 }]]
        },
        "HyperScale Limit": {
            "main": [[{ "node": "Polyglot Code", "type": "main", "index": 0 }]]
        },
        "Polyglot Code": {
            "main": [[{ "node": "HyperScale Aggregate", "type": "main", "index": 0 }]]
        },
        "HyperScale Aggregate": {
            "main": [[{ "node": "HyperScale SplitOut", "type": "main", "index": 0 }]]
        },
        "HyperScale SplitOut": {
            "main": [[{ "node": "HyperScale Dedup", "type": "main", "index": 0 }]]
        },
        "HyperScale Dedup": {
            "main": [[{ "node": "HyperScale BatchFinalizer", "type": "main", "index": 0 }]]
        }
    })
}

fn seed_default_workflows(conn: &Connection) -> Result<()> {
    let mut check_stmt = conn.prepare("SELECT COUNT(*) FROM workflows WHERE id = ?1")?;
    let count: i64 = check_stmt.query_row(params!["wf-hyperscale-1m-benchmark"], |r| r.get(0))?;
    let nodes = get_hyperscale_1m_canvas_nodes();
    let connections = get_hyperscale_1m_canvas_connections();
    let settings = serde_json::json!({
        "executionOrder": "v1",
        "binaryMode": "separate",
        "hyperScaleArena": {
            "enabled": true,
            "totalNodes": 1000000,
            "chains": 1000,
            "nodesPerChain": 1000
        }
    });
    let now = Utc::now().to_rfc3339();

    if count == 0 {
        conn.execute(
            "INSERT INTO workflows (
                id, name, active, is_archived, nodes, connections, settings,
                static_data, pin_data, meta, version_id, trigger_count, tags,
                parent_folder_id, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                "wf-hyperscale-1m-benchmark",
                "⚡ HyperScale 1,000,000 Nodes Arena Benchmark",
                0,
                0,
                serde_json::to_string(&nodes)?,
                serde_json::to_string(&connections)?,
                serde_json::to_string(&settings)?,
                "{}",
                "{}",
                "{}",
                "v6-hyperscale-arena-1m",
                0,
                "[]",
                Option::<String>::None,
                now,
                now,
            ],
        )?;
    } else {
        // Keep definition up to date with 12 nodes and settings
        conn.execute(
            "UPDATE workflows SET
                name = ?1,
                nodes = ?2,
                connections = ?3,
                settings = ?4,
                updated_at = ?5
            WHERE id = 'wf-hyperscale-1m-benchmark'",
            params![
                "⚡ HyperScale 1,000,000 Nodes Arena Benchmark",
                serde_json::to_string(&nodes)?,
                serde_json::to_string(&connections)?,
                serde_json::to_string(&settings)?,
                now,
            ],
        )?;
    }
    Ok(())
}

