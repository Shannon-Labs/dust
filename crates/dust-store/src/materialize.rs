use dust_types::Result;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationStrategy {
    Clone,
    Copy,
    Missing,
}

impl MaterializationStrategy {
    pub fn label(self) -> &'static str {
        match self {
            Self::Clone => "clone",
            Self::Copy => "copy",
            Self::Missing => "missing",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BranchStateMaterialization {
    pub data_db: MaterializationStrategy,
    pub schema: MaterializationStrategy,
}

pub fn materialize_branch_state(
    source_db: &Path,
    target_db: &Path,
) -> Result<BranchStateMaterialization> {
    let data_db = clone_or_copy_optional_file(source_db, target_db)?;
    let schema = clone_or_copy_optional_file(
        &source_db.with_extension("schema.toml"),
        &target_db.with_extension("schema.toml"),
    )?;

    Ok(BranchStateMaterialization { data_db, schema })
}

pub fn clone_or_copy_optional_file(
    source: &Path,
    target: &Path,
) -> Result<MaterializationStrategy> {
    if target.exists() {
        fs::remove_file(target)?;
    }
    if !source.exists() {
        return Ok(MaterializationStrategy::Missing);
    }

    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }

    match try_clone_file(source, target) {
        Ok(true) => Ok(MaterializationStrategy::Clone),
        Ok(false) => unreachable!("unsupported clone path should return an error"),
        Err(_) => {
            if target.exists() {
                fs::remove_file(target)?;
            }
            fs::copy(source, target)?;
            Ok(MaterializationStrategy::Copy)
        }
    }
}

#[cfg(target_os = "macos")]
fn try_clone_file(source: &Path, target: &Path) -> std::io::Result<bool> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;

    let rc = unsafe { libc::clonefile(source.as_ptr(), target.as_ptr(), 0) };
    if rc == 0 {
        Ok(true)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn try_clone_file(source: &Path, target: &Path) -> std::io::Result<bool> {
    use std::fs::OpenOptions;
    use std::os::fd::AsRawFd;

    const FICLONE: libc::c_ulong = 0x4004_9409;

    let source = OpenOptions::new().read(true).open(source)?;
    let target = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)?;

    let rc = unsafe { libc::ioctl(target.as_raw_fd(), FICLONE, source.as_raw_fd()) };
    if rc == 0 {
        Ok(true)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn try_clone_file(_source: &Path, _target: &Path) -> std::io::Result<bool> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "filesystem clone is not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_source_is_reported_without_creating_target() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("missing.db");
        let target = dir.path().join("target.db");

        let strategy = clone_or_copy_optional_file(&source, &target).unwrap();
        assert_eq!(strategy, MaterializationStrategy::Missing);
        assert!(!target.exists());
    }

    #[test]
    fn materialized_branch_state_preserves_db_and_schema_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("main.db");
        let target = dir.path().join("branches/feature/data.db");

        fs::write(&source, b"main-data").unwrap();
        fs::write(source.with_extension("schema.toml"), "title = 'main'\n").unwrap();

        let result = materialize_branch_state(&source, &target).unwrap();
        assert_ne!(result.data_db, MaterializationStrategy::Missing);
        assert_ne!(result.schema, MaterializationStrategy::Missing);
        assert_eq!(fs::read(&target).unwrap(), b"main-data");
        assert_eq!(
            fs::read_to_string(target.with_extension("schema.toml")).unwrap(),
            "title = 'main'\n"
        );
    }
}
