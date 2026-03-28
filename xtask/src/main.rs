use anyhow::{Context, Result, anyhow, ensure};
use clap::{Args, Parser, Subcommand};
use pulldown_cmark::{Options, Parser as MarkdownParser, html};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    task: Option<Task>,
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
enum Task {
    Ci,
    Smoke,
    Fmt,
    Check,
    Site,
    ReleaseParity(ReleaseParityArgs),
}

#[derive(Debug, Args, Clone, PartialEq, Eq)]
struct ReleaseParityArgs {
    #[arg(long)]
    archive: PathBuf,
}

fn parse_task(args: &[&str]) -> Result<Task> {
    let cli = Cli::try_parse_from(std::iter::once("xtask").chain(args.iter().copied()))
        .map_err(|err| anyhow!(err.to_string()))?;
    Ok(cli.task.unwrap_or(Task::Ci))
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

fn run_capture(cmd: &str, args: &[&str], cwd: Option<&PathBuf>) -> Result<String> {
    let mut command = Command::new(cmd);
    command.args(args);
    if let Some(dir) = cwd {
        command.current_dir(dir);
    }

    let output = command
        .output()
        .with_context(|| format!("failed to start `{cmd}`"))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        Err(anyhow!("`{cmd}` failed: {detail}"))
    }
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

fn cargo_capture_workspace(args: &[&str], cwd: Option<&PathBuf>) -> Result<String> {
    let manifest = workspace_root()?.join("Cargo.toml");
    let manifest_str = manifest
        .to_str()
        .ok_or_else(|| anyhow!("workspace manifest path is not valid UTF-8"))?;

    let mut owned_args = vec!["run", "--manifest-path", manifest_str];
    owned_args.extend_from_slice(args);
    run_capture("cargo", &owned_args, cwd)
}

fn run_path(cmd: &Path, args: &[&str], cwd: Option<&PathBuf>) -> Result<()> {
    let mut command = Command::new(cmd);
    command.args(args);
    if let Some(dir) = cwd {
        command.current_dir(dir);
    }

    let status = command
        .status()
        .with_context(|| format!("failed to start `{}`", cmd.display()))?;

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("`{}` exited with {status}", cmd.display()))
    }
}

fn run_capture_path(cmd: &Path, args: &[&str], cwd: Option<&PathBuf>) -> Result<String> {
    let mut command = Command::new(cmd);
    command.args(args);
    if let Some(dir) = cwd {
        command.current_dir(dir);
    }

    let output = command
        .output()
        .with_context(|| format!("failed to start `{}`", cmd.display()))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        Err(anyhow!("`{}` failed: {detail}", cmd.display()))
    }
}

enum DustRunner {
    CargoWorkspace,
    Installed(PathBuf),
}

struct InstalledBinary {
    _tempdir: tempfile::TempDir,
    path: PathBuf,
}

fn dust_run(runner: &DustRunner, args: &[&str], cwd: Option<&PathBuf>) -> Result<()> {
    match runner {
        DustRunner::CargoWorkspace => {
            let mut owned_args = vec!["-p", "dust-cli", "--"];
            owned_args.extend_from_slice(args);
            cargo_run_workspace(&owned_args, cwd)
        }
        DustRunner::Installed(path) => run_path(path, args, cwd),
    }
}

fn dust_capture(runner: &DustRunner, args: &[&str], cwd: Option<&PathBuf>) -> Result<String> {
    match runner {
        DustRunner::CargoWorkspace => {
            let mut owned_args = vec!["-p", "dust-cli", "--"];
            owned_args.extend_from_slice(args);
            cargo_capture_workspace(&owned_args, cwd)
        }
        DustRunner::Installed(path) => run_capture_path(path, args, cwd),
    }
}

#[derive(Debug, Clone, Copy)]
struct DocsPage {
    source: &'static str,
    output: &'static str,
    title: &'static str,
    summary: &'static str,
    section: &'static str,
}

#[derive(Debug, Serialize)]
struct SearchEntry {
    title: String,
    url: String,
    section: String,
    text: String,
}

const DOCS_PAGES: &[DocsPage] = &[
    DocsPage {
        source: "docs/quickstart.md",
        output: "docs/index.html",
        title: "Quickstart",
        summary: "Install Dust, run your first query, and branch a local database in minutes.",
        section: "Get Started",
    },
    DocsPage {
        source: "README.md",
        output: "docs/overview/index.html",
        title: "Overview",
        summary: "Positioning, current status, and the product wedge for Dust.",
        section: "Get Started",
    },
    DocsPage {
        source: "docs/cli.md",
        output: "docs/cli/index.html",
        title: "CLI Reference",
        summary: "Command-by-command reference for the Dust CLI surface.",
        section: "Reference",
    },
    DocsPage {
        source: "docs/architecture.md",
        output: "docs/architecture/index.html",
        title: "Architecture",
        summary: "Core crates, storage model, and the local-first workflow boundaries.",
        section: "Reference",
    },
    DocsPage {
        source: "docs/roadmap.md",
        output: "docs/roadmap/index.html",
        title: "Roadmap",
        summary: "Near-term priorities, longer-term bets, and what Dust is not trying to be.",
        section: "Product",
    },
    DocsPage {
        source: "docs/faq.md",
        output: "docs/faq/index.html",
        title: "FAQ",
        summary: "Answers to the most common launch and adoption questions.",
        section: "Product",
    },
    DocsPage {
        source: "docs/pricing.md",
        output: "docs/pricing/index.html",
        title: "Pricing",
        summary: "Packaging for Free, Team, and Enterprise with honest beta boundaries.",
        section: "Product",
    },
    DocsPage {
        source: "docs/waitlist.md",
        output: "docs/waitlist/index.html",
        title: "Waitlist",
        summary: "What the Team beta intake needs to capture and how to route it.",
        section: "Product",
    },
    DocsPage {
        source: "docs/support.md",
        output: "docs/support/index.html",
        title: "Support",
        summary: "Public support, docs feedback, bug routing, and launch intake surfaces.",
        section: "Product",
    },
    DocsPage {
        source: "docs/python-client.md",
        output: "docs/python-client/index.html",
        title: "Python Client",
        summary: "Thin Python bindings over the stable Dust CLI JSON surface.",
        section: "SDKs",
    },
    DocsPage {
        source: "docs/rfcs/plugin-registry.md",
        output: "docs/rfcs/plugin-registry/index.html",
        title: "Plugin Model RFC",
        summary: "Public extension points, capability gates, and future registry shape.",
        section: "RFCs",
    },
    DocsPage {
        source: "docs/rfcs/compatibility-and-release.md",
        output: "docs/rfcs/compatibility-and-release/index.html",
        title: "Compatibility RFC",
        summary: "1.0 compatibility guarantees, packaging, and release distribution policy.",
        section: "RFCs",
    },
    DocsPage {
        source: "docs/launch-post.md",
        output: "docs/launch-post/index.html",
        title: "Launch Narrative",
        summary: "The public story for why Dust exists and where it fits.",
        section: "Launch",
    },
    DocsPage {
        source: "docs/launch-checklist.md",
        output: "docs/launch-checklist/index.html",
        title: "Launch Checklist",
        summary: "Go/no-go checks, day-zero smoke tests, and rollback guidance.",
        section: "Launch",
    },
    DocsPage {
        source: "docs/ops/launch-infrastructure.md",
        output: "docs/ops/launch-infrastructure/index.html",
        title: "Launch Infrastructure",
        summary: "Hosting targets, DNS, preview deployments, and secret contracts.",
        section: "Launch",
    },
    DocsPage {
        source: "docs/ops/commercial-ops-runbook.md",
        output: "docs/ops/commercial-ops-runbook/index.html",
        title: "Commercial Ops Runbook",
        summary: "Invite-only beta operations for onboarding, support, and refunds.",
        section: "Launch",
    },
];

fn release_archive_name(version: &str, platform: &str, arch: &str) -> String {
    format!("dust-{version}-{platform}-{arch}.tar.gz")
}

fn current_platform() -> Result<&'static str> {
    match std::env::consts::OS {
        "linux" => Ok("linux"),
        "macos" => Ok("darwin"),
        other => Err(anyhow!(
            "unsupported platform `{other}` for release parity smoke"
        )),
    }
}

fn current_arch() -> Result<&'static str> {
    match std::env::consts::ARCH {
        "x86_64" => Ok("x86_64"),
        "aarch64" => Ok("aarch64"),
        other => Err(anyhow!(
            "unsupported architecture `{other}` for release parity smoke"
        )),
    }
}

fn documented_top_level_commands() -> Result<BTreeSet<String>> {
    let cli_doc = fs::read_to_string(workspace_root()?.join("docs/cli.md"))
        .context("failed to read docs/cli.md for command parity check")?;
    let mut commands = BTreeSet::new();

    for line in cli_doc.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("- `dust ") else {
            continue;
        };

        let token = rest.split_whitespace().next().unwrap_or("");
        let command = token
            .split('`')
            .next()
            .unwrap_or(token)
            .trim_matches(|ch| ch == '[' || ch == ']');
        if !command.is_empty() {
            commands.insert(command.to_string());
        }
    }

    ensure!(
        !commands.is_empty(),
        "docs/cli.md did not yield any documented top-level commands"
    );
    Ok(commands)
}

fn help_top_level_commands(help: &str) -> BTreeSet<String> {
    let mut commands = BTreeSet::new();
    let mut in_commands = false;

    for line in help.lines() {
        let trimmed = line.trim();
        if trimmed == "Commands:" {
            in_commands = true;
            continue;
        }
        if !in_commands {
            continue;
        }
        if trimmed == "Options:" {
            break;
        }
        if trimmed.is_empty() {
            continue;
        }
        if let Some(name) = trimmed.split_whitespace().next() {
            if name != "help" {
                commands.insert(name.to_string());
            }
        }
    }

    commands
}

fn assert_cli_surface_matches_docs(help: &str) -> Result<()> {
    let documented = documented_top_level_commands()?;
    let observed = help_top_level_commands(help);

    let missing: Vec<_> = documented.difference(&observed).cloned().collect();
    ensure!(
        missing.is_empty(),
        "installed CLI is missing documented commands: {}",
        missing.join(", ")
    );

    let undocumented: Vec<_> = observed.difference(&documented).cloned().collect();
    ensure!(
        undocumented.is_empty(),
        "installed CLI exposes undocumented commands: {}",
        undocumented.join(", ")
    );

    Ok(())
}

fn install_release_archive(archive: &Path) -> Result<InstalledBinary> {
    let version = format!("v{}", env!("CARGO_PKG_VERSION"));
    let expected_name = release_archive_name(&version, current_platform()?, current_arch()?);
    let archive_name = archive
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow!(
                "release archive path is not valid UTF-8: {}",
                archive.display()
            )
        })?;
    ensure!(
        archive_name == expected_name,
        "release archive `{archive_name}` does not match expected host asset `{expected_name}`"
    );

    let tempdir = tempfile::TempDir::new().context("failed to create release parity temp dir")?;
    let release_dir = tempdir.path().join("releases").join(&version);
    fs::create_dir_all(&release_dir).with_context(|| {
        format!(
            "failed to create staged release directory {}",
            release_dir.display()
        )
    })?;
    let staged_archive = release_dir.join(&expected_name);
    fs::copy(archive, &staged_archive).with_context(|| {
        format!(
            "failed to stage release archive {} -> {}",
            archive.display(),
            staged_archive.display()
        )
    })?;

    let install_dir = tempdir.path().join("bin");
    let base_url = format!("file://{}", release_dir.display());
    let installer = workspace_root()?.join("install.sh");
    let status = Command::new("sh")
        .arg(&installer)
        .env("DUST_BASE_URL", &base_url)
        .env("DUST_INSTALL_DIR", &install_dir)
        .env("DUST_VERSION", &version)
        .status()
        .with_context(|| format!("failed to start installer {}", installer.display()))?;
    ensure!(status.success(), "installer exited with {status}");

    let installed = install_dir.join("dust");
    ensure!(
        installed.exists(),
        "installer did not produce {}",
        installed.display()
    );

    Ok(InstalledBinary {
        _tempdir: tempdir,
        path: installed,
    })
}

fn smoke_release_parity(args: &ReleaseParityArgs) -> Result<()> {
    let installed = install_release_archive(&args.archive)?;
    let version = dust_capture(
        &DustRunner::Installed(installed.path.clone()),
        &["version"],
        None,
    )?;
    ensure!(
        version.contains(env!("CARGO_PKG_VERSION")),
        "installed binary reported unexpected version output: {}",
        version.trim()
    );

    let help = dust_capture(
        &DustRunner::Installed(installed.path.clone()),
        &["--help"],
        None,
    )?;
    assert_cli_surface_matches_docs(&help)?;

    let runner = DustRunner::Installed(installed.path.clone());
    smoke_demo(&runner)?;
    smoke_readme_try_path(&runner)?;
    smoke_quickstart_path(&runner)?;
    smoke_inventory_sample(&runner)?;
    smoke_branch_sample(&runner)?;
    Ok(())
}

fn smoke_demo(runner: &DustRunner) -> Result<()> {
    let temp = tempfile::TempDir::new().context("failed to create demo temp dir")?;
    let demo_root = temp.path().join("dust-demo");
    let demo_root_str = demo_root
        .to_str()
        .ok_or_else(|| anyhow!("demo path is not valid UTF-8"))?;
    dust_run(runner, &["demo", demo_root_str, "--quiet"], None)?;
    ensure!(
        demo_root.join(".dust").exists(),
        "demo command did not create a Dust project"
    );
    Ok(())
}

fn smoke_readme_try_path(runner: &DustRunner) -> Result<()> {
    let temp = tempfile::TempDir::new().context("failed to create README smoke temp dir")?;
    let root = temp.path().join("myapp");
    let root_str = root
        .to_str()
        .ok_or_else(|| anyhow!("README smoke path is not valid UTF-8"))?;

    dust_run(runner, &["init", root_str], None)?;
    dust_run(runner, &["query", "-f", "db/schema.sql"], Some(&root))?;
    dust_run(runner, &["query", "SELECT 1"], Some(&root))?;
    dust_run(runner, &["branch", "create", "experiment"], Some(&root))?;
    dust_run(runner, &["branch", "switch", "experiment"], Some(&root))?;
    dust_run(runner, &["diff", "main", "experiment"], Some(&root))?;
    Ok(())
}

fn smoke_quickstart_path(runner: &DustRunner) -> Result<()> {
    const PRODUCTS_CSV: &str = "sku,name,category,unit_price_cents,stock\nWDG-001,Standard Widget,widgets,1499,340\nWDG-002,Premium Widget,widgets,2999,125\nBLT-001,M6 Bolt,fasteners,29,8400\nBLT-002,M8 Bolt,fasteners,45,6200\nGDG-001,USB-C Hub,gadgets,3495,58\nGDG-002,Bluetooth Dongle,gadgets,1299,210\nGDG-003,Portable SSD 1TB,gadgets,8999,42\n";

    let temp = tempfile::TempDir::new().context("failed to create quickstart temp dir")?;
    let root = temp.path().join("inventory-demo");
    let root_str = root
        .to_str()
        .ok_or_else(|| anyhow!("quickstart path is not valid UTF-8"))?;

    dust_run(runner, &["init", root_str], None)?;
    fs::write(root.join("products.csv"), PRODUCTS_CSV)
        .context("failed to write quickstart products.csv fixture")?;
    dust_run(
        runner,
        &["import", "products.csv", "--table", "products"],
        Some(&root),
    )?;

    let status = dust_capture(runner, &["status"], Some(&root))?;
    ensure!(
        status.contains("products"),
        "quickstart status output did not mention imported products table"
    );

    let product_count = dust_capture(
        runner,
        &["query", "SELECT count(*) AS total FROM products"],
        Some(&root),
    )?;
    ensure!(
        product_count.contains('7'),
        "quickstart query output did not report the expected row count"
    );

    dust_run(runner, &["branch", "create", "experiment"], Some(&root))?;
    dust_run(runner, &["branch", "switch", "experiment"], Some(&root))?;
    dust_run(
        runner,
        &["query", "DELETE FROM products WHERE stock > 5000"],
        Some(&root),
    )?;
    dust_run(runner, &["branch", "switch", "main"], Some(&root))?;

    let main_count = dust_capture(
        runner,
        &["query", "SELECT count(*) FROM products"],
        Some(&root),
    )?;
    ensure!(
        main_count.contains('7'),
        "quickstart branch isolation check did not preserve the main branch row count"
    );

    dust_run(runner, &["branch", "delete", "experiment"], Some(&root))?;
    Ok(())
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

    let runner = DustRunner::CargoWorkspace;
    smoke_inventory_sample(&runner)?;
    smoke_branch_sample(&runner)?;
    site()?;
    Ok(())
}

fn smoke_inventory_sample(runner: &DustRunner) -> Result<()> {
    let temp = tempfile::TempDir::new().context("failed to create inventory temp dir")?;
    let root = temp.path().join("inventory-demo");
    let root_str = root
        .to_str()
        .ok_or_else(|| anyhow!("inventory sample path is not valid UTF-8"))?;
    let sample_root = workspace_root()?.join("templates/samples/inventory-demo");

    dust_run(runner, &["init", root_str, "--force"], None)?;
    copy_dir_contents(&sample_root, &root)?;
    dust_run(runner, &["query", "-f", "db/schema.sql"], Some(&root))?;
    dust_run(runner, &["seed", "--profile", "demo"], Some(&root))?;
    let report = dust_capture(
        runner,
        &[
            "query",
            "--format",
            "json",
            "-f",
            "db/queries/reorder_report.sql",
        ],
        Some(&root),
    )?;
    ensure!(
        report.contains("Portable SSD 1TB"),
        "inventory sample query did not surface the expected low-stock item"
    );
    dust_run(runner, &["codegen"], Some(&root))?;
    ensure!(
        root.join("db/generated/queries.rs").exists(),
        "Rust codegen output was not generated for inventory sample"
    );
    ensure!(
        root.join("db/generated/queries.ts").exists(),
        "TypeScript codegen output was not generated for inventory sample"
    );
    let rust_codegen = std::fs::read_to_string(root.join("db/generated/queries.rs"))
        .context("failed to read generated Rust code")?;
    ensure!(
        rust_codegen.contains("pub struct DustClient"),
        "Rust codegen did not include the executable DustClient helper"
    );
    ensure!(
        rust_codegen.contains("pub fn products_by_category"),
        "Rust codegen did not include the products_by_category helper"
    );
    let ts_codegen = std::fs::read_to_string(root.join("db/generated/queries.ts"))
        .context("failed to read generated TypeScript code")?;
    ensure!(
        ts_codegen.contains("export class DustClient"),
        "TypeScript codegen did not include the executable DustClient helper"
    );
    ensure!(
        ts_codegen.contains("export async function products_by_category"),
        "TypeScript codegen did not include the products_by_category helper"
    );
    Ok(())
}

fn smoke_branch_sample(runner: &DustRunner) -> Result<()> {
    let temp = tempfile::TempDir::new().context("failed to create branch temp dir")?;
    let root = temp.path().join("branch-lab");
    let root_str = root
        .to_str()
        .ok_or_else(|| anyhow!("branch sample path is not valid UTF-8"))?;
    let sample_root = workspace_root()?.join("templates/samples/branch-lab");

    dust_run(runner, &["init", root_str, "--force"], None)?;
    copy_dir_contents(&sample_root, &root)?;
    dust_run(runner, &["query", "-f", "db/schema.sql"], Some(&root))?;
    dust_run(runner, &["seed", "--profile", "demo"], Some(&root))?;
    dust_run(runner, &["branch", "create", "promo-cut"], Some(&root))?;
    dust_run(runner, &["branch", "switch", "promo-cut"], Some(&root))?;
    dust_run(
        runner,
        &[
            "query",
            "INSERT INTO ledger_entries VALUES (9001, 1, 'campaign rebate', -9000, '2026-03-01')",
        ],
        Some(&root),
    )?;
    dust_run(runner, &["branch", "switch", "main"], Some(&root))?;
    let diff = dust_capture(runner, &["diff", "main", "promo-cut"], Some(&root))?;
    ensure!(
        diff.contains("ledger_entries"),
        "branch sample diff did not report the expected table delta"
    );
    Ok(())
}

fn copy_dir_contents(src: &Path, dst: &Path) -> Result<()> {
    for entry in fs::read_dir(src)
        .with_context(|| format!("failed to read template directory {}", src.display()))?
    {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            fs::create_dir_all(&to)?;
            copy_dir_contents(&from, &to)?;
        } else {
            if let Some(parent) = to.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&from, &to).with_context(|| {
                format!(
                    "failed to copy template file {} -> {}",
                    from.display(),
                    to.display()
                )
            })?;
        }
    }
    Ok(())
}

fn site() -> Result<()> {
    let root = workspace_root()?;
    let site_root = root.join("apps/www");
    let docs_root = site_root.join("docs");
    if docs_root.exists() {
        fs::remove_dir_all(&docs_root)
            .with_context(|| format!("failed to clear {}", docs_root.display()))?;
    }
    fs::create_dir_all(&docs_root)?;

    let mut search_entries = Vec::new();
    for page in DOCS_PAGES {
        let markdown_path = root.join(page.source);
        let markdown = fs::read_to_string(&markdown_path)
            .with_context(|| format!("failed to read {}", markdown_path.display()))?;
        let title = extract_title(&markdown).unwrap_or_else(|| page.title.to_string());
        let prefix = relative_prefix(page.output);
        let nav = render_docs_nav(page.output);
        let rewritten = rewrite_markdown_links(&markdown, page.output);
        let output_path = site_root.join(page.output);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            &output_path,
            render_docs_page(&title, page.summary, &rewritten, &nav, &prefix, page.output),
        )?;
        search_entries.push(SearchEntry {
            title,
            url: route_for(page.output),
            section: page.section.to_string(),
            text: markdown_to_search_text(&markdown),
        });
    }

    let search_index_path = docs_root.join("search-index.json");
    fs::write(
        &search_index_path,
        serde_json::to_string_pretty(&search_entries)?,
    )
    .with_context(|| format!("failed to write {}", search_index_path.display()))?;
    Ok(())
}

fn extract_title(markdown: &str) -> Option<String> {
    markdown.lines().find_map(|line| {
        line.strip_prefix("# ")
            .map(|title| title.trim().to_string())
    })
}

fn render_markdown(markdown: &str) -> String {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TASKLISTS);
    options.insert(Options::ENABLE_FOOTNOTES);
    options.insert(Options::ENABLE_SMART_PUNCTUATION);

    let parser = MarkdownParser::new_ext(markdown, options);
    let mut html_out = String::new();
    html::push_html(&mut html_out, parser);
    html_out
}

fn render_docs_nav(active_output: &str) -> String {
    let sections = [
        "Get Started",
        "Reference",
        "Product",
        "SDKs",
        "RFCs",
        "Launch",
    ];

    let mut html_out = String::new();
    for section in sections {
        html_out.push_str(&format!(
            "<div class=\"docs-nav-group\"><div class=\"docs-nav-label\">{section}</div>"
        ));
        for page in DOCS_PAGES.iter().filter(|page| page.section == section) {
            let class = if page.output == active_output {
                "docs-nav-link is-active"
            } else {
                "docs-nav-link"
            };
            html_out.push_str(&format!(
                "<a class=\"{class}\" href=\"{}\">{}</a>",
                relative_href(active_output, page.output),
                page.title
            ));
        }
        html_out.push_str("</div>");
    }
    html_out
}

fn render_docs_page(
    title: &str,
    summary: &str,
    markdown: &str,
    nav: &str,
    prefix: &str,
    output: &str,
) -> String {
    let content = render_markdown(markdown);
    let search_index = format!("{prefix}/docs/search-index.json");
    let home_href = format!("{prefix}/");
    let pricing_href = format!("{prefix}/pricing/");
    let waitlist_href = format!("{prefix}/waitlist/");
    let support_href = format!("{prefix}/support/");
    let docs_href = format!("{prefix}/docs/");
    let stylesheet_href = format!("{prefix}/assets/site.css");
    let script_href = format!("{prefix}/assets/site.js");

    format!(
        "<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
    <title>{title} · Dust Docs</title>
    <meta name=\"description\" content=\"{summary}\">
    <link rel=\"stylesheet\" href=\"{stylesheet_href}\">
  </head>
  <body class=\"docs-shell\" data-search-index=\"{search_index}\">
    <header class=\"site-header\">
      <a class=\"site-brand\" href=\"{home_href}\">dust</a>
      <nav class=\"site-nav\">
        <a href=\"{docs_href}\">Docs</a>
        <a href=\"{pricing_href}\">Pricing</a>
        <a href=\"{waitlist_href}\">Waitlist</a>
        <a href=\"{support_href}\">Support</a>
      </nav>
    </header>
    <main class=\"docs-layout\">
      <aside class=\"docs-sidebar\">
        <div class=\"docs-sidebar-card\">
          <div class=\"eyebrow\">Docs</div>
          <h1>{title}</h1>
          <p>{summary}</p>
          <label class=\"search-label\" for=\"docs-search\">Search docs</label>
          <input id=\"docs-search\" class=\"search-input\" type=\"search\" placeholder=\"Search quickstart, CLI, roadmap…\">
          <div id=\"docs-search-results\" class=\"search-results\"></div>
        </div>
        {nav}
      </aside>
      <article class=\"docs-article\" data-doc-path=\"{output}\">
        {content}
      </article>
    </main>
    <script src=\"{script_href}\"></script>
  </body>
</html>"
    )
}

fn relative_prefix(output: &str) -> String {
    let parent = Path::new(output)
        .parent()
        .map(|p| p.components().count())
        .unwrap_or(0);
    if parent == 0 {
        ".".to_string()
    } else {
        std::iter::repeat_n("..", parent)
            .collect::<Vec<_>>()
            .join("/")
    }
}

fn route_for(output: &str) -> String {
    format!("/{}", output.trim_end_matches("index.html"))
}

fn relative_href(from_output: &str, to_output: &str) -> String {
    format!(
        "{}/{}",
        relative_prefix(from_output),
        route_for(to_output).trim_start_matches('/')
    )
}

fn markdown_to_search_text(markdown: &str) -> String {
    markdown
        .chars()
        .map(|ch| {
            if ch.is_alphanumeric() || ch.is_whitespace() {
                ch
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn rewrite_markdown_links(markdown: &str, current_output: &str) -> String {
    let docs_links = [
        ("docs/quickstart.md", "docs/index.html"),
        ("docs/cli.md", "docs/cli/index.html"),
        ("docs/architecture.md", "docs/architecture/index.html"),
        ("docs/roadmap.md", "docs/roadmap/index.html"),
        ("docs/faq.md", "docs/faq/index.html"),
        ("docs/pricing.md", "docs/pricing/index.html"),
        ("docs/waitlist.md", "docs/waitlist/index.html"),
        ("docs/support.md", "docs/support/index.html"),
        ("docs/python-client.md", "docs/python-client/index.html"),
        ("docs/launch-post.md", "docs/launch-post/index.html"),
        (
            "docs/launch-checklist.md",
            "docs/launch-checklist/index.html",
        ),
    ];
    let repo_links = [
        (
            "templates/samples/inventory-demo",
            "https://github.com/Shannon-Labs/dust/tree/main/templates/samples/inventory-demo",
        ),
        (
            "templates/samples/branch-lab",
            "https://github.com/Shannon-Labs/dust/tree/main/templates/samples/branch-lab",
        ),
        (
            "assets/benchmarks/README.md",
            "https://github.com/Shannon-Labs/dust/blob/main/assets/benchmarks/README.md",
        ),
        (
            "CONTRIBUTING.md",
            "https://github.com/Shannon-Labs/dust/blob/main/CONTRIBUTING.md",
        ),
        (
            "apps/www",
            "https://github.com/Shannon-Labs/dust/tree/main/apps/www",
        ),
        (
            "../sdk/python",
            "https://github.com/Shannon-Labs/dust/tree/main/sdk/python",
        ),
    ];

    let mut rewritten = markdown.to_string();
    for (src, dest) in docs_links {
        rewritten = rewritten.replace(
            &format!("({src})"),
            &format!("({})", relative_href(current_output, dest)),
        );
        rewritten = rewritten.replace(
            &format!("href=\"{src}\""),
            &format!("href=\"{}\"", relative_href(current_output, dest)),
        );
    }
    for (src, dest) in repo_links {
        rewritten = rewritten.replace(&format!("({src})"), &format!("({dest})"));
    }
    rewritten
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    match parse_task(&arg_refs)? {
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
        Task::Site => site()?,
        Task::ReleaseParity(args) => smoke_release_parity(&args)?,
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{ReleaseParityArgs, Task, parse_task};

    #[test]
    fn parses_known_tasks() {
        assert_eq!(parse_task(&["ci"]).unwrap(), Task::Ci);
        assert_eq!(parse_task(&["smoke"]).unwrap(), Task::Smoke);
        assert_eq!(parse_task(&["fmt"]).unwrap(), Task::Fmt);
        assert_eq!(parse_task(&["check"]).unwrap(), Task::Check);
        assert_eq!(parse_task(&["site"]).unwrap(), Task::Site);
        assert_eq!(
            parse_task(&[
                "release-parity",
                "--archive",
                "dist/dust-v0.1.1-linux-x86_64.tar.gz"
            ])
            .unwrap(),
            Task::ReleaseParity(ReleaseParityArgs {
                archive: PathBuf::from("dist/dust-v0.1.1-linux-x86_64.tar.gz"),
            })
        );
    }

    #[test]
    fn defaults_to_ci() {
        assert_eq!(parse_task(&[]).unwrap(), Task::Ci);
    }
}
