use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use codex_archive_subagent_threads::{
    OutputPayload, archive_with_verification, default_codex_home, load_candidates, render_text,
};

#[derive(Debug, Parser)]
#[command(
    about = "Archive closed Codex subagent threads and verify the SQLite state",
    version
)]
struct Args {
    /// Parent thread id. Defaults to $CODEX_THREAD_ID.
    #[arg(long)]
    parent_thread_id: Option<String>,

    /// Archive every closed, unarchived subagent thread across all parents.
    #[arg(long)]
    all_closed_subagents: bool,

    /// Codex home directory. Defaults to $CODEX_HOME or ~/.codex.
    #[arg(long)]
    codex_home: Option<std::path::PathBuf>,

    /// Per-request timeout when talking to codex app-server.
    #[arg(long, default_value_t = 10)]
    timeout_seconds: u64,

    /// Print what would be archived without sending archive requests.
    #[arg(long)]
    dry_run: bool,

    /// Print the result as JSON.
    #[arg(long)]
    json: bool,

    /// Skip app-server and archive directly in the Codex SQLite database.
    #[arg(long)]
    direct_sqlite_only: bool,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    let codex_home = args.codex_home.unwrap_or_else(default_codex_home);
    let db_path = codex_home.join("state_5.sqlite");
    let parent_thread_id = args
        .parent_thread_id
        .or_else(|| std::env::var("CODEX_THREAD_ID").ok());

    let candidates = load_candidates(
        &db_path,
        parent_thread_id.as_deref(),
        args.all_closed_subagents,
    )
    .with_context(|| format!("failed to load candidates from {}", db_path.display()))?;

    if args.dry_run || candidates.is_empty() {
        let payload = OutputPayload {
            mode: if args.dry_run { "dry-run" } else { "noop" },
            count: candidates.len(),
            threads: candidates.clone(),
            fallback_used: false,
        };
        if args.json {
            println!("{}", serde_json::to_string_pretty(&payload)?);
        } else {
            render_text(&candidates, args.dry_run);
        }
        return Ok(());
    }

    let fallback_used = archive_with_verification(
        &db_path,
        &candidates,
        Duration::from_secs(args.timeout_seconds),
        args.direct_sqlite_only,
    )?;

    let payload = OutputPayload {
        mode: "archived",
        count: candidates.len(),
        threads: candidates.clone(),
        fallback_used,
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        render_text(&candidates, false);
    }

    Ok(())
}
