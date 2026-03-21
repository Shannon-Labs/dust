use std::fs;
use std::path::Path;

use dust_types::Result;

pub trait Vfs {
    fn create_dir_all(&self, path: &Path) -> Result<()>;
    fn write(&self, path: &Path, bytes: &[u8]) -> Result<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct LocalVfs;

impl Vfs for LocalVfs {
    fn create_dir_all(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path)?;
        Ok(())
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        fs::write(path, bytes)?;
        Ok(())
    }
}
