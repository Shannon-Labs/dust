use std::sync::Mutex;

use dust_lsp::LspServer;

fn main() {
    LspServer::new().run(std::io::stdin(), Mutex::new(Box::new(std::io::stdout())));
}
