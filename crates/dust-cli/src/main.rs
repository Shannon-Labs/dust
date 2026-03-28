mod commands;
mod demo_data;
mod format;
mod import_postgres;
mod import_sqlite;
mod pgwire;
pub(crate) mod project;
mod sql_quote;
mod style;
#[cfg(test)]
mod test_support;

use clap::{Parser, Subcommand};
use miette::IntoDiagnostic;

#[derive(Debug, Parser)]
#[command(name = "dust", about = "Dust: branchable local-first SQL", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run built-in benchmarks
    Bench(commands::bench::BenchArgs),
    /// Manage branches
    Branch(commands::branch::BranchArgs),
    /// Generate typed query artifacts (Rust, TypeScript)
    Codegen(commands::codegen::CodegenArgs),
    /// Create a demo project and run a timed walkthrough
    Demo(commands::demo::DemoArgs),
    /// Package a project for deployment
    Deploy(commands::deploy::DeployArgs),
    /// Start development mode with file watching, seeds, and optional pgwire server
    Dev(commands::dev::DevArgs),
    /// Show row/value differences between branches
    Diff(commands::diff::DiffArgs),
    /// Check project health
    Doctor(commands::doctor::DoctorArgs),
    /// Show the query plan for a SQL statement
    Explain(commands::explain::ExplainArgs),
    /// Export data to various formats
    Export(commands::export::ExportArgs),
    /// Import data from various formats
    Import(commands::import::ImportArgs),
    /// Create a new Dust project
    Init(commands::init::InitArgs),
    /// Lint SQL schema and query files
    Lint(commands::lint::LintArgs),
    /// Start the Language Server Protocol server
    Lsp(commands::lsp::LspArgs),
    /// Start the MCP (Model Context Protocol) server for AI agent integration
    Mcp,
    /// Merge branches with conflict detection and resolution
    Merge(commands::merge::MergeArgs),
    /// Plan, apply, and manage schema migrations
    Migrate(commands::migrate::MigrateArgs),
    /// Execute SQL queries
    Query(commands::query::QueryArgs),
    /// Push and pull branches to/from remote repositories
    Remote(commands::remote::RemoteArgs),
    /// Load SQL seed files into the current branch
    Seed(commands::seed::SeedArgs),
    /// Start an interactive SQL shell
    Shell(commands::shell::ShellArgs),
    /// Start a Postgres wire protocol server
    Serve(commands::serve::ServeArgs),
    /// Create and manage point-in-time snapshots
    Snapshot(commands::snapshot::SnapshotArgs),
    /// Show project status
    Status(commands::status::StatusArgs),
    /// Run SQL test files against ephemeral databases
    Test(commands::test::TestArgs),
    /// Print version
    Version,
}

fn main() -> miette::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Bench(args) => commands::bench::run(args).into_diagnostic()?,
        Commands::Branch(args) => commands::branch::run(args).into_diagnostic()?,
        Commands::Codegen(args) => commands::codegen::run(args).into_diagnostic()?,
        Commands::Demo(args) => commands::demo::run(args).into_diagnostic()?,
        Commands::Deploy(args) => commands::deploy::run(args).into_diagnostic()?,
        Commands::Dev(args) => commands::dev::run(args).into_diagnostic()?,
        Commands::Diff(args) => commands::diff::run(args).into_diagnostic()?,
        Commands::Doctor(args) => commands::doctor::run(args).into_diagnostic()?,
        Commands::Explain(args) => commands::explain::run(args).into_diagnostic()?,
        Commands::Export(args) => commands::export::run(args).into_diagnostic()?,
        Commands::Import(args) => commands::import::run(args).into_diagnostic()?,
        Commands::Init(args) => commands::init::run(args).into_diagnostic()?,
        Commands::Lint(args) => commands::lint::run(args).into_diagnostic()?,
        Commands::Lsp(args) => commands::lsp::run(args).into_diagnostic()?,
        Commands::Mcp => dust_mcp::run(),
        Commands::Merge(args) => commands::merge::run(args).into_diagnostic()?,
        Commands::Migrate(args) => commands::migrate::run(args).into_diagnostic()?,
        Commands::Query(args) => commands::query::run(args).into_diagnostic()?,
        Commands::Remote(args) => commands::remote::run(args).into_diagnostic()?,
        Commands::Seed(args) => commands::seed::run(args).into_diagnostic()?,
        Commands::Shell(args) => commands::shell::run(args).into_diagnostic()?,
        Commands::Serve(args) => commands::serve::run(args).into_diagnostic()?,
        Commands::Snapshot(args) => commands::snapshot::run(args).into_diagnostic()?,
        Commands::Status(args) => commands::status::run(args).into_diagnostic()?,
        Commands::Test(args) => commands::test::run(args).into_diagnostic()?,
        Commands::Version => println!("dust {}", env!("CARGO_PKG_VERSION")),
    }

    Ok(())
}
