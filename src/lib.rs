use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OpenFlags, params};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize)]
pub struct Candidate {
    pub parent_thread_id: String,
    pub thread_id: String,
    pub status: String,
    pub agent_nickname: Option<String>,
    pub title: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputPayload {
    pub mode: &'static str,
    pub count: usize,
    pub threads: Vec<Candidate>,
    pub fallback_used: bool,
}

pub fn default_codex_home() -> PathBuf {
    if let Ok(value) = std::env::var("CODEX_HOME") {
        return PathBuf::from(value);
    }

    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".codex")
}

pub fn load_candidates(
    db_path: &Path,
    parent_thread_id: Option<&str>,
    all_closed_subagents: bool,
) -> Result<Vec<Candidate>> {
    if !all_closed_subagents && parent_thread_id.is_none() {
        bail!("missing parent thread id: pass --parent-thread-id or set CODEX_THREAD_ID");
    }

    let conn = open_read_connection(db_path)?;
    let mut query = String::from(
        "
        SELECT
            e.parent_thread_id,
            e.child_thread_id AS thread_id,
            e.status,
            t.agent_nickname,
            t.title
        FROM thread_spawn_edges e
        JOIN threads t ON t.id = e.child_thread_id
        WHERE e.status = 'closed'
          AND t.archived = 0
          AND t.agent_nickname IS NOT NULL
        ",
    );

    if !all_closed_subagents {
        query.push_str(" AND e.parent_thread_id = ?1");
    }
    query.push_str(" ORDER BY t.created_at ASC");

    let mut stmt = conn.prepare(&query)?;
    let rows = if all_closed_subagents {
        stmt.query_map([], candidate_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?
    } else {
        stmt.query_map(params![parent_thread_id.unwrap()], candidate_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };

    Ok(rows)
}

fn candidate_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Candidate> {
    Ok(Candidate {
        parent_thread_id: row.get("parent_thread_id")?,
        thread_id: row.get("thread_id")?,
        status: row.get("status")?,
        agent_nickname: row.get("agent_nickname")?,
        title: row.get("title")?,
    })
}

pub fn unarchived_candidate_ids(db_path: &Path, candidates: &[Candidate]) -> Result<Vec<String>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let conn = open_read_connection(db_path)?;
    let mut stmt = conn.prepare("SELECT archived FROM threads WHERE id = ?1")?;
    let mut unarchived = Vec::new();

    for candidate in candidates {
        let archived: Option<i64> = stmt
            .query_row(params![candidate.thread_id], |row| row.get(0))
            .optional()
            .with_context(|| format!("failed to verify thread {}", candidate.thread_id))?;

        if archived != Some(1) {
            unarchived.push(candidate.thread_id.clone());
        }
    }

    Ok(unarchived)
}

pub fn direct_archive(db_path: &Path, thread_ids: &[String]) -> Result<usize> {
    if thread_ids.is_empty() {
        return Ok(0);
    }

    let mut conn = open_write_connection(db_path)?;
    conn.busy_timeout(Duration::from_secs(30))?;

    let now = unix_timestamp_seconds()?;
    let tx = conn.transaction()?;
    let mut changed = 0usize;
    {
        let mut stmt = tx.prepare(
            "
            UPDATE threads
            SET archived = 1,
                archived_at = COALESCE(archived_at, ?1)
            WHERE id = ?2
              AND archived = 0
            ",
        )?;

        for thread_id in thread_ids {
            changed += stmt.execute(params![now, thread_id])?;
        }
    }
    tx.commit()?;

    Ok(changed)
}

pub fn archive_with_verification(
    db_path: &Path,
    candidates: &[Candidate],
    timeout: Duration,
    direct_sqlite_only: bool,
) -> Result<bool> {
    let mut fallback_used = direct_sqlite_only;

    if !direct_sqlite_only && let Err(err) = archive_via_app_server(candidates, timeout) {
        fallback_used = true;
        eprintln!("app-server archive failed, falling back to direct SQLite update: {err:#}");
    }

    let remaining = unarchived_candidate_ids(db_path, candidates)?;
    if !remaining.is_empty() {
        fallback_used = true;
        direct_archive(db_path, &remaining)?;
    }

    let still_unarchived = unarchived_candidate_ids(db_path, candidates)?;
    if !still_unarchived.is_empty() {
        bail!(
            "archive verification failed; still unarchived: {}",
            still_unarchived.join(", ")
        );
    }

    Ok(fallback_used)
}

pub fn render_text(candidates: &[Candidate], dry_run: bool) {
    let action = if dry_run { "would archive" } else { "archived" };
    if candidates.is_empty() {
        println!("no closed unarchived subagent threads found");
        return;
    }

    println!("{action} {} subagent thread(s):", candidates.len());
    for item in candidates {
        let nickname = item.agent_nickname.as_deref().unwrap_or("unknown");
        println!(
            "- {nickname}: {} (parent {})",
            item.thread_id, item.parent_thread_id
        );
    }
}

fn archive_via_app_server(candidates: &[Candidate], timeout: Duration) -> Result<()> {
    let mut client = AppServerClient::spawn(timeout)?;
    client.initialize()?;
    for item in candidates {
        client.archive_thread(&item.thread_id)?;
    }
    client.close();
    Ok(())
}

struct AppServerClient {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout_rx: mpsc::Receiver<Result<String, String>>,
    stderr_buffer: Arc<Mutex<String>>,
    timeout: Duration,
    next_id: u64,
}

impl AppServerClient {
    fn spawn(timeout: Duration) -> Result<Self> {
        let mut child = Command::new("codex")
            .args(["app-server", "--listen", "stdio://"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to start `codex app-server --listen stdio://`")?;

        let stdin = child
            .stdin
            .take()
            .context("failed to open app-server stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("failed to open app-server stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("failed to open app-server stderr")?;

        let (stdout_tx, stdout_rx) = mpsc::channel();
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(line) => {
                        if stdout_tx.send(Ok(line)).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = stdout_tx.send(Err(err.to_string()));
                        break;
                    }
                }
            }
        });

        let stderr_buffer = Arc::new(Mutex::new(String::new()));
        let stderr_for_thread = Arc::clone(&stderr_buffer);
        thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines().map_while(Result::ok) {
                if let Ok(mut buffer) = stderr_for_thread.lock() {
                    if !buffer.is_empty() {
                        buffer.push('\n');
                    }
                    buffer.push_str(&line);
                }
            }
        });

        Ok(Self {
            child,
            stdin: Some(stdin),
            stdout_rx,
            stderr_buffer,
            timeout,
            next_id: 1,
        })
    }

    fn initialize(&mut self) -> Result<Value> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "codex-archive-subagent-threads",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": null,
            }),
        )
    }

    fn archive_thread(&mut self, thread_id: &str) -> Result<()> {
        self.request("thread/archive", json!({ "threadId": thread_id }))?;
        Ok(())
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let request_id = self.next_id.to_string();
        self.next_id += 1;

        let payload = json!({
            "method": method,
            "id": request_id,
            "params": params,
        });

        let stdin = self
            .stdin
            .as_mut()
            .context("app-server stdin is already closed")?;
        serde_json::to_writer(&mut *stdin, &payload)?;
        stdin.write_all(b"\n")?;
        stdin.flush()?;

        let deadline = Instant::now() + self.timeout;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| self.timeout_error(method))?;

            let line = self
                .stdout_rx
                .recv_timeout(remaining)
                .map_err(|err| match err {
                    mpsc::RecvTimeoutError::Timeout => self.timeout_error(method),
                    mpsc::RecvTimeoutError::Disconnected => {
                        anyhow!(
                            "app-server stdout closed while handling {method}: {}",
                            self.stderr()
                        )
                    }
                })?
                .map_err(|err| anyhow!("failed to read app-server stdout: {err}"))?;

            let message: Value = serde_json::from_str(&line)
                .with_context(|| format!("invalid app-server JSON response: {line}"))?;
            if message.get("id").and_then(Value::as_str) != Some(request_id.as_str()) {
                continue;
            }

            if let Some(error) = message.get("error") {
                bail!("app-server error on {method}: {error}");
            }

            return Ok(message.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    fn timeout_error(&self, method: &str) -> anyhow::Error {
        anyhow!(
            "timed out waiting for app-server response to {method} after {}s: {}",
            self.timeout.as_secs(),
            self.stderr()
        )
    }

    fn stderr(&self) -> String {
        let stderr = self
            .stderr_buffer
            .lock()
            .map(|value| value.clone())
            .unwrap_or_default();
        if stderr.is_empty() {
            "<no stderr>".to_string()
        } else {
            stderr
        }
    }

    fn close(&mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => thread::sleep(Duration::from_millis(25)),
                Err(_) => break,
            }
        }

        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for AppServerClient {
    fn drop(&mut self) {
        self.close();
    }
}

fn open_read_connection(db_path: &Path) -> Result<Connection> {
    ensure_db_exists(db_path)?;
    let conn = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| {
        format!(
            "failed to open Codex state database at {}",
            db_path.display()
        )
    })?;
    conn.busy_timeout(Duration::from_secs(30))?;
    Ok(conn)
}

fn open_write_connection(db_path: &Path) -> Result<Connection> {
    ensure_db_exists(db_path)?;
    let conn = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| {
        format!(
            "failed to open Codex state database at {}",
            db_path.display()
        )
    })?;
    conn.busy_timeout(Duration::from_secs(30))?;
    Ok(conn)
}

fn ensure_db_exists(db_path: &Path) -> Result<()> {
    if !db_path.exists() {
        bail!("Codex state database does not exist: {}", db_path.display());
    }
    Ok(())
}

fn unix_timestamp_seconds() -> Result<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_secs() as i64)
}

trait OptionalRow<T> {
    fn optional(self) -> rusqlite::Result<Option<T>>;
}

impl<T> OptionalRow<T> for rusqlite::Result<T> {
    fn optional(self) -> rusqlite::Result<Option<T>> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fixture_db() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("state_5.sqlite");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE threads (
                id TEXT PRIMARY KEY,
                rollout_path TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                model_provider TEXT NOT NULL DEFAULT '',
                cwd TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL,
                sandbox_policy TEXT NOT NULL DEFAULT '',
                approval_mode TEXT NOT NULL DEFAULT '',
                tokens_used INTEGER NOT NULL DEFAULT 0,
                has_user_event INTEGER NOT NULL DEFAULT 0,
                archived INTEGER NOT NULL DEFAULT 0,
                archived_at INTEGER,
                agent_nickname TEXT
            );
            CREATE TABLE thread_spawn_edges (
                parent_thread_id TEXT NOT NULL,
                child_thread_id TEXT NOT NULL PRIMARY KEY,
                status TEXT NOT NULL
            );
            ",
        )
        .unwrap();

        conn.execute(
            "INSERT INTO threads (id, created_at, updated_at, title, archived, agent_nickname)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params!["child-closed", 1, 1, "Closed", 0, "Ada"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO threads (id, created_at, updated_at, title, archived, agent_nickname)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params!["child-open", 2, 2, "Open", 0, "Grace"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO threads (id, created_at, updated_at, title, archived, agent_nickname)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params!["child-archived", 3, 3, "Archived", 1, "Linus"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO threads (id, created_at, updated_at, title, archived, agent_nickname)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                "ordinary-thread",
                4,
                4,
                "Ordinary",
                0,
                Option::<String>::None
            ],
        )
        .unwrap();

        for (child, status) in [
            ("child-closed", "closed"),
            ("child-open", "running"),
            ("child-archived", "closed"),
            ("ordinary-thread", "closed"),
        ] {
            conn.execute(
                "INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status)
                 VALUES (?1, ?2, ?3)",
                params!["parent-1", child, status],
            )
            .unwrap();
        }

        drop(conn);
        (temp_dir, db_path)
    }

    #[test]
    fn load_candidates_filters_to_closed_unarchived_subagents() {
        let (_temp_dir, db_path) = fixture_db();

        let candidates = load_candidates(&db_path, Some("parent-1"), false).unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].thread_id, "child-closed");
    }

    #[test]
    fn load_candidates_requires_parent_without_all_flag() {
        let (_temp_dir, db_path) = fixture_db();

        let error = load_candidates(&db_path, None, false).unwrap_err();

        assert!(
            error.to_string().contains("missing parent thread id"),
            "{error:#}"
        );
    }

    #[test]
    fn direct_archive_sets_archived_and_archived_at() {
        let (_temp_dir, db_path) = fixture_db();

        direct_archive(&db_path, &["child-closed".to_string()]).unwrap();

        let conn = Connection::open(db_path).unwrap();
        let (archived, archived_at): (i64, Option<i64>) = conn
            .query_row(
                "SELECT archived, archived_at FROM threads WHERE id = 'child-closed'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(archived, 1);
        assert!(archived_at.is_some());
    }
}
