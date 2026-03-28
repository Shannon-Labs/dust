use std::env;
use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use clap::Args;
use dust_core::ProjectPaths;
use dust_exec::{PersistentEngine, QueryOutput};
use dust_types::{DustError, Result};

use crate::demo_data;
use crate::format::print_output;
use crate::style;

#[derive(Debug, Args)]
pub struct DemoArgs {
    /// Directory to create the demo project in (default: ./dust-demo)
    pub path: Option<PathBuf>,

    /// Retained for backwards compatibility; the branch walkthrough now runs by default
    #[arg(long)]
    pub branching: bool,

    /// Create and seed the demo project without printing the walkthrough
    #[arg(long)]
    pub quiet: bool,

    /// Print the walkthrough immediately without pauses
    #[arg(long)]
    pub no_wait: bool,

    /// Delay between walkthrough beats in milliseconds
    #[arg(long, default_value = "650")]
    pub delay_ms: u64,
}

pub fn run(args: DemoArgs) -> Result<()> {
    let ui = style::stdout();
    let pacer = Pacer::new(args.no_wait, args.delay_ms);
    let root = args.path.unwrap_or_else(|| {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("dust-demo")
    });
    let total_start = Instant::now();

    let project = ProjectPaths::new(&root);
    let init_start = Instant::now();
    project.init(false)?;
    let init_elapsed = init_start.elapsed();

    let db_path = project.active_data_db_path();
    let mut engine = PersistentEngine::open(&db_path)?;

    let schema_start = Instant::now();
    engine.query(demo_data::DEMO_SCHEMA)?;
    let schema_elapsed = schema_start.elapsed();

    let seed_start = Instant::now();
    engine.query(demo_data::DEMO_SEED)?;
    engine.sync()?;
    let seed_elapsed = seed_start.elapsed();

    if args.quiet {
        println!("{}", root.display());
        return Ok(());
    }

    let table_names = engine.table_names();
    let seeded_rows = total_rows(&mut engine, &table_names);

    println!(
        "{}",
        ui.header("dust demo - the database toolchain in one binary")
    );
    println!();
    println!(
        "{} Created project at {}",
        timing_badge(&ui, init_elapsed),
        ui.path(root.display())
    );
    println!(
        "{} Created {} tables ({})",
        timing_badge(&ui, schema_elapsed),
        ui.metric(table_names.len()),
        ui.info(table_names.join(", "))
    );
    println!(
        "{} Inserted {} rows across all tables",
        timing_badge(&ui, seed_elapsed),
        ui.metric(seeded_rows)
    );

    pacer.beat();
    println!();
    println!("{}", ui.section("Query"));
    println!("{}", ui.rule(56));
    println!("{}", ui.command_line(demo_data::SHOWCASE_QUERY.command));
    println!();
    let query_start = Instant::now();
    let output = engine.query(demo_data::SHOWCASE_QUERY.sql)?;
    let query_elapsed = query_start.elapsed();
    let query_rows = row_count(&output);
    print_output(&output);
    println!(
        "{} Query returned {} rows",
        timing_badge(&ui, query_elapsed),
        ui.metric(query_rows)
    );

    pacer.beat();
    println!();
    println!("{}", ui.section("Branch"));
    println!("{}", ui.rule(56));
    let exe = env::current_exe()
        .map_err(|err| DustError::Message(format!("failed to locate current executable: {err}")))?;

    run_cli_step(
        &ui,
        &root,
        &exe,
        ["branch", "create", "experiment"],
        "Created branch `experiment`",
        &pacer,
    )?;
    run_cli_step(
        &ui,
        &root,
        &exe,
        ["branch", "switch", "experiment"],
        "Switched to `experiment`",
        &pacer,
    )?;

    let branch_mutation_start = Instant::now();
    let review_count = 5;
    let mut experiment_engine = PersistentEngine::open(&project.active_data_db_path())?;
    experiment_engine.query("CREATE TABLE reviews (id INTEGER PRIMARY KEY, reviewer TEXT NOT NULL, rating INTEGER NOT NULL)")?;
    experiment_engine.query(&insert_reviews_sql(review_count))?;
    experiment_engine.sync()?;
    println!(
        "{} Added new table `reviews` with {} rows",
        timing_badge(&ui, branch_mutation_start.elapsed()),
        ui.metric(review_count)
    );

    run_cli_step(
        &ui,
        &root,
        &exe,
        ["branch", "switch", "main"],
        "Switched back to `main`",
        &pacer,
    )?;

    pacer.beat();
    println!();
    println!("{}", ui.section("Diff"));
    println!("{}", ui.rule(56));
    println!("{}", ui.command_line("dust diff main experiment"));
    let diff_start = Instant::now();
    let diff = run_cli_capture(&root, &exe, ["diff", "main", "experiment"])?;
    print_captured_output(&diff.stdout);
    println!("{} Diff completed", timing_badge(&ui, diff_start.elapsed()));

    pacer.beat();
    println!();
    println!("{}", ui.section("Summary"));
    println!("{}", ui.rule(56));
    println!(
        "{} Total time: {}",
        timing_badge(&ui, total_start.elapsed()),
        ui.metric(format_duration(total_start.elapsed()))
    );
    println!(
        "{}",
        ui.header("Docker Postgres hasn't finished starting yet.")
    );
    println!();
    println!("{}", ui.label("Try it yourself:"));
    println!("  {}", ui.command_line(format!("cd {}", root.display())));
    println!("  {}", ui.command_line("dust shell"));

    Ok(())
}

fn total_rows(engine: &mut PersistentEngine, tables: &[String]) -> usize {
    tables
        .iter()
        .map(|table| engine.row_count(table).unwrap_or(0))
        .sum()
}

fn row_count(output: &QueryOutput) -> usize {
    match output {
        QueryOutput::Rows { rows, .. } => rows.len(),
        QueryOutput::RowsTyped { rows, .. } => rows.len(),
        QueryOutput::Message(_) => 0,
    }
}

fn insert_reviews_sql(count: usize) -> String {
    let mut sql = String::from("INSERT INTO reviews VALUES ");
    for index in 0..count {
        if index > 0 {
            sql.push_str(", ");
        }
        let id = index + 1;
        let rating = (index % 5) + 1;
        sql.push_str(&format!("({}, 'critic_{}', {})", id, id, rating));
    }
    sql
}

fn run_cli_step<const N: usize>(
    ui: &style::Palette,
    root: &Path,
    exe: &Path,
    args: [&str; N],
    summary: &str,
    pacer: &Pacer,
) -> Result<()> {
    println!("{}", ui.command_line(format!("dust {}", args.join(" "))));
    let started = Instant::now();
    let result = run_cli_capture(root, exe, args)?;
    println!("{} {}", timing_badge(ui, started.elapsed()), summary);
    if !result.stdout.trim().is_empty() {
        print_captured_output(&result.stdout);
    }
    pacer.short();
    Ok(())
}

struct CapturedCommand {
    stdout: String,
}

fn run_cli_capture<const N: usize>(
    root: &Path,
    exe: &Path,
    args: [&str; N],
) -> Result<CapturedCommand> {
    let output = Command::new(exe)
        .current_dir(root)
        .args(args)
        .output()
        .map_err(|err| DustError::Message(format!("failed to run `{}`: {err}", args.join(" "))))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let message = if !stderr.is_empty() { stderr } else { stdout };
        return Err(DustError::Message(format!(
            "`dust {}` failed: {}",
            args.join(" "),
            message
        )));
    }

    Ok(CapturedCommand {
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
    })
}

fn print_captured_output(output: &str) {
    for line in output.lines() {
        println!("  {line}");
    }
}

fn timing_badge(ui: &style::Palette, duration: Duration) -> String {
    ui.metric(format!("[{}]", format_duration(duration)))
}

fn format_duration(duration: Duration) -> String {
    if duration.as_millis() == 0 {
        "<1ms".to_string()
    } else if duration.as_secs() == 0 {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}

struct Pacer {
    enabled: bool,
    delay: Duration,
}

impl Pacer {
    fn new(no_wait: bool, delay_ms: u64) -> Self {
        Self {
            enabled: !no_wait && io::stdout().is_terminal(),
            delay: Duration::from_millis(delay_ms),
        }
    }

    fn beat(&self) {
        if self.enabled {
            thread::sleep(self.delay);
        }
    }

    fn short(&self) {
        if self.enabled {
            thread::sleep(self.delay / 2);
        }
    }
}
