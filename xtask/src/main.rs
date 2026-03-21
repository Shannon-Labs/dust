use anyhow::{Context, Result, anyhow};
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Task {
    Ci,
    Smoke,
    Fmt,
    Check,
}

fn parse_task(arg: Option<String>) -> Result<Task> {
    match arg.as_deref().unwrap_or("ci") {
        "ci" => Ok(Task::Ci),
        "smoke" => Ok(Task::Smoke),
        "fmt" => Ok(Task::Fmt),
        "check" => Ok(Task::Check),
        other => Err(anyhow!("unknown xtask `{other}`")),
    }
}

fn run(cmd: &str, args: &[&str], cwd: Option<&PathBuf>) -> Result<()> {
    let mut command = Command::new(cmd);
    command.args(args);
    if let Some(dir) = cwd {
        command.current_dir(dir);
    }

    let status = command
        .status()
        .with_context(|| format!("failed to start `{cmd}`"))?;

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("`{cmd}` exited with {status}"))
    }
}

fn cargo(args: &[&str]) -> Result<()> {
    run("cargo", args, None)
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("xtask manifest directory has no parent"))
}

fn cargo_run_workspace(args: &[&str], cwd: Option<&PathBuf>) -> Result<()> {
    let manifest = workspace_root()?.join("Cargo.toml");
    let manifest_str = manifest
        .to_str()
        .ok_or_else(|| anyhow!("workspace manifest path is not valid UTF-8"))?;

    let mut owned_args = vec!["run", "--manifest-path", manifest_str];
    owned_args.extend_from_slice(args);
    run("cargo", &owned_args, cwd)
}

fn smoke() -> Result<()> {
    let temp = tempfile::TempDir::new().context("failed to create temp dir")?;
    let root = temp.path().to_path_buf();
    let root_str = root
        .to_str()
        .ok_or_else(|| anyhow!("temporary directory path is not valid UTF-8"))?;

    cargo_run_workspace(&["-p", "dust-cli", "--", "init", root_str, "--force"], None)?;
    cargo_run_workspace(&["-p", "dust-cli", "--", "doctor", root_str], None)?;
    cargo_run_workspace(&["-p", "dust-cli", "--", "query", "select 1"], Some(&root))?;
    cargo_run_workspace(
        &["-p", "dust-cli", "--", "explain", "select 1"],
        Some(&root),
    )?;
    // DDL execution
    cargo_run_workspace(
        &[
            "-p",
            "dust-cli",
            "--",
            "query",
            "create table audit_log (id integer primary key, payload text not null)",
        ],
        Some(&root),
    )?;
    cargo_run_workspace(
        &[
            "-p",
            "dust-cli",
            "--",
            "explain",
            "create unique index audit_log_payload_idx on audit_log using columnar (payload)",
        ],
        Some(&root),
    )?;
    // INSERT + SELECT round-trip (single batch — no cross-process persistence yet)
    cargo_run_workspace(
        &[
            "-p",
            "dust-cli",
            "--",
            "query",
            "create table smoke_test (id integer, name text); insert into smoke_test (id, name) values (1, 'hello'), (2, 'world'); select * from smoke_test",
        ],
        Some(&root),
    )?;
    // Column projection in a batch
    cargo_run_workspace(
        &[
            "-p",
            "dust-cli",
            "--",
            "query",
            "create table t2 (a integer, b text, c text); insert into t2 (a, b, c) values (1, 'x', 'y'); select b, c from t2",
        ],
        Some(&root),
    )?;
    Ok(())
}

fn main() -> Result<()> {
    match parse_task(std::env::args().nth(1))? {
        Task::Ci => {
            cargo(&["fmt", "--check"])?;
            cargo(&[
                "clippy",
                "--workspace",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ])?;
            cargo(&["test", "--workspace"])?;
        }
        Task::Smoke => smoke()?,
        Task::Fmt => cargo(&["fmt"])?,
        Task::Check => {
            cargo(&["fmt", "--check"])?;
            cargo(&["test", "-p", "dust-testing", "-p", "xtask"])?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Task, parse_task};

    #[test]
    fn parses_known_tasks() {
        assert_eq!(parse_task(Some("ci".to_string())).unwrap(), Task::Ci);
        assert_eq!(parse_task(Some("smoke".to_string())).unwrap(), Task::Smoke);
        assert_eq!(parse_task(Some("fmt".to_string())).unwrap(), Task::Fmt);
        assert_eq!(parse_task(Some("check".to_string())).unwrap(), Task::Check);
    }

    #[test]
    fn defaults_to_ci() {
        assert_eq!(parse_task(None).unwrap(), Task::Ci);
    }
}
