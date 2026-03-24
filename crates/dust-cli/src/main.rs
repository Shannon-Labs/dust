mod commands;
mod demo_data;
mod format;
mod import_postgres;
mod import_sqlite;
pub(crate) mod project;

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
    /// Create a demo project with sample data and example queries
    Demo(commands::demo::DemoArgs),
    Init(commands::init::InitArgs),
    Query(commands::query::QueryArgs),
    Shell(commands::shell::ShellArgs),
    Serve(commands::serve::ServeArgs),
    Explain(commands::explain::ExplainArgs),
    Doctor(commands::doctor::DoctorArgs),
    Branch(commands::branch::BranchArgs),
    Import(commands::import::ImportArgs),
    Export(commands::export::ExportArgs),
    Status(commands::status::StatusArgs),
    /// Start the MCP (Model Context Protocol) server for AI agent integration
    Mcp,
    Version,
}

fn main() -> miette::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Demo(args) => commands::demo::run(args).into_diagnostic()?,
        Commands::Init(args) => commands::init::run(args).into_diagnostic()?,
        Commands::Query(args) => commands::query::run(args).into_diagnostic()?,
        Commands::Shell(args) => commands::shell::run(args).into_diagnostic()?,
        Commands::Serve(args) => commands::serve::run(args).into_diagnostic()?,
        Commands::Explain(args) => commands::explain::run(args).into_diagnostic()?,
        Commands::Doctor(args) => commands::doctor::run(args).into_diagnostic()?,
        Commands::Branch(args) => commands::branch::run(args).into_diagnostic()?,
        Commands::Import(args) => commands::import::run(args).into_diagnostic()?,
        Commands::Export(args) => commands::export::run(args).into_diagnostic()?,
        Commands::Status(args) => commands::status::run(args).into_diagnostic()?,
        Commands::Mcp => dust_mcp::run(),
        Commands::Version => println!("dust {}", env!("CARGO_PKG_VERSION")),
    }

    Ok(())
}
