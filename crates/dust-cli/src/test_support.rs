use std::env;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

fn cwd_mutex() -> &'static Mutex<()> {
    static MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    MUTEX.get_or_init(|| Mutex::new(()))
}

pub struct CwdGuard {
    original_dir: PathBuf,
    _lock: MutexGuard<'static, ()>,
}

impl CwdGuard {
    pub fn enter(dir: impl AsRef<Path>) -> Self {
        let lock = cwd_mutex().lock().unwrap();
        let original_dir = env::current_dir().unwrap();
        env::set_current_dir(dir.as_ref()).unwrap();
        Self {
            original_dir,
            _lock: lock,
        }
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = env::set_current_dir(&self.original_dir);
    }
}
