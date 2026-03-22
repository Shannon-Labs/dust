#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = dust_exec::persistent::PersistentEngine::open(std::path::Path::new(s));
        let _ = toml::from_str::<toml::Value>(s);
    }
});
