/// `dust serve` — Postgres wire protocol server.
///
/// Implements just enough of the pgwire v3 protocol for psql, DataGrip,
/// and Postgres drivers to connect and run queries.
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clap::Args;
use dust_exec::PersistentEngine;
use dust_types::Result;
use tokio::net::TcpListener;

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Bind address
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port
    #[arg(long, default_value = "4545")]
    pub port: u16,

    /// Project root
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

pub fn run(args: ServeArgs) -> Result<()> {
    let db_path = crate::project::find_db_path(&args.path);
    let engine = PersistentEngine::open(&db_path)?;
    let engine = Arc::new(Mutex::new(engine));

    let addr = format!("{}:{}", args.host, args.port);
    println!("dust serve listening on {addr} (Postgres wire protocol)");
    println!("Connect with: psql -h {} -p {}", args.host, args.port);

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| dust_types::DustError::Message(e.to_string()))?;

    rt.block_on(async {
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(dust_types::DustError::Io)?;

        loop {
            let (stream, peer) = listener.accept().await.map_err(dust_types::DustError::Io)?;
            let engine = Arc::clone(&engine);
            println!("Connection from {peer}");
            tokio::spawn(async move {
                if let Err(err) = crate::pgwire::handle_connection(stream, engine).await {
                    eprintln!("Connection error: {err}");
                }
            });
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::pgwire::{command_tag, frame_body_len};

    #[test]
    fn rejects_short_frames() {
        assert!(frame_body_len(0, "query").is_err());
        assert!(frame_body_len(3, "query").is_err());
        assert_eq!(frame_body_len(4, "query").unwrap(), 0);
    }

    #[test]
    fn command_tag_matches_returning_statements() {
        assert_eq!(command_tag("select * from t", 2), "SELECT 2");
        assert_eq!(command_tag("insert into t returning *", 1), "INSERT 0 1");
        assert_eq!(command_tag("update t set x = 1 returning *", 3), "UPDATE 3");
        assert_eq!(command_tag("delete from t returning *", 4), "DELETE 4");
    }
}
