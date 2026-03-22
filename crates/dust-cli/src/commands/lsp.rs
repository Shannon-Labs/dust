use std::path::PathBuf;
use std::sync::Mutex;

use clap::Args;
use dust_types::Result;

#[derive(Debug, Args)]
pub struct LspArgs {
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

pub fn run(_args: LspArgs) -> Result<()> {
    dust_lsp::LspServer::new().run(std::io::stdin(), Mutex::new(Box::new(std::io::stdout())));
    Ok(())
}
