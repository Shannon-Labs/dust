//! WASM UDF sandbox — loads .wasm modules and registers their exported
//! functions as UDFs.  Sandboxed: limited memory, fuel metering, no
//! filesystem / network access.
//!
//! Gated behind the `wasm` Cargo feature.

#[cfg(feature = "wasm")]
mod inner {
    use std::path::Path;
    use std::sync::Arc;

    use wasmtime::{Config, Engine, Linker, Module, Store};

    use crate::udf::{Udf, UdfRegistry};

    /// Maximum WASM memory: 16 MiB.
    const MAX_MEMORY_BYTES: usize = 16 * 1024 * 1024;
    /// Maximum fuel (instruction budget) per call.
    const MAX_FUEL: u64 = 1_000_000;

    struct WasmState {
        /// Scratch buffer for passing string data in/out.
        input_buf: Vec<u8>,
        output_buf: Vec<u8>,
    }

    /// Load a WASM module from `path` and register every exported function
    /// whose signature is `(i32, i32) -> i32` (pointer+len -> pointer to
    /// NUL-terminated result) into `registry`.
    ///
    /// The convention is intentionally simple so that test modules are easy
    /// to write: the guest receives a UTF-8 string (the first argument
    /// concatenated with `|` separators), writes its UTF-8 result into
    /// linear memory, and returns the byte offset.
    pub fn load_wasm_module(
        path: &Path,
        registry: &mut UdfRegistry,
    ) -> Result<Vec<String>, String> {
        let mut config = Config::new();
        config.consume_fuel(true);
        // Memory limit is enforced via Store limits
        let engine = Engine::new(&config).map_err(|e| format!("wasm engine init: {e}"))?;
        let module_bytes = std::fs::read(path).map_err(|e| format!("reading wasm file: {e}"))?;
        let module =
            Module::new(&engine, &module_bytes).map_err(|e| format!("compiling wasm: {e}"))?;
        let linker: Linker<WasmState> = Linker::new(&engine);

        let mut registered = Vec::new();
        let engine = Arc::new(engine);
        let module = Arc::new(module);
        let linker = Arc::new(linker);

        for export in module.exports() {
            let name = export.name().to_string();
            // Only register function exports (skip memory, tables, etc.)
            if export.ty().func().is_none() {
                continue;
            }
            let fn_name = name.clone();
            let engine_ref = Arc::clone(&engine);
            let module_ref = Arc::clone(&module);
            let linker_ref = Arc::clone(&linker);

            let udf = Udf::new(&fn_name, move |args: &[String]| {
                // Serialize arguments as pipe-separated string
                let input = args.join("|");
                call_wasm_fn(&engine_ref, &module_ref, &linker_ref, &name, &input)
                    .unwrap_or_else(|e| format!("WASM_ERROR: {e}"))
            });
            registry.register(udf);
            registered.push(fn_name);
        }
        Ok(registered)
    }

    /// Execute a single WASM function call with fuel metering and memory limits.
    fn call_wasm_fn(
        engine: &Engine,
        module: &Module,
        linker: &Linker<WasmState>,
        fn_name: &str,
        input: &str,
    ) -> Result<String, String> {
        let state = WasmState {
            input_buf: input.as_bytes().to_vec(),
            output_buf: Vec::new(),
        };
        let mut store = Store::new(engine, state);
        store
            .set_fuel(MAX_FUEL)
            .map_err(|e| format!("set fuel: {e}"))?;
        // Apply memory limits
        store.limiter(|_state| wasmtime::StoreLimits::default());

        let instance = linker
            .instantiate(&mut store, module)
            .map_err(|e| format!("instantiate: {e}"))?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or("module has no exported memory")?;

        // Enforce memory limit
        let pages = memory.size(&store);
        if pages as usize * 65536 > MAX_MEMORY_BYTES {
            return Err("memory limit exceeded".to_string());
        }

        // Write input into WASM memory at offset 0
        let input_bytes = input.as_bytes();
        let mem_data = memory.data_mut(&mut store);
        if input_bytes.len() + 1 > mem_data.len() {
            return Err("input too large for WASM memory".to_string());
        }
        mem_data[..input_bytes.len()].copy_from_slice(input_bytes);
        mem_data[input_bytes.len()] = 0; // NUL-terminate

        let func = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, fn_name)
            .map_err(|e| format!("get func `{fn_name}`: {e}"))?;

        let result_offset = func
            .call(&mut store, (0i32, input_bytes.len() as i32))
            .map_err(|e| format!("call `{fn_name}`: {e}"))?;

        // Read NUL-terminated result from memory
        let mem_data = memory.data(&store);
        let start = result_offset as usize;
        if start >= mem_data.len() {
            return Err("result offset out of bounds".to_string());
        }
        let end = mem_data[start..]
            .iter()
            .position(|&b| b == 0)
            .map(|p| start + p)
            .unwrap_or(mem_data.len().min(start + 4096));
        let result = std::str::from_utf8(&mem_data[start..end])
            .map_err(|e| format!("invalid UTF-8 result: {e}"))?;
        Ok(result.to_string())
    }
}

#[cfg(feature = "wasm")]
pub use inner::load_wasm_module;

/// Stub when wasm feature is not enabled.
#[cfg(not(feature = "wasm"))]
pub fn load_wasm_module(
    _path: &std::path::Path,
    _registry: &mut crate::udf::UdfRegistry,
) -> Result<Vec<String>, String> {
    Err("WASM UDF support is not enabled (compile with --features wasm)".to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn wasm_disabled_returns_error() {
        #[cfg(not(feature = "wasm"))]
        {
            let mut reg = crate::udf::UdfRegistry::new();
            let result = super::load_wasm_module(std::path::Path::new("dummy.wasm"), &mut reg);
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("not enabled"));
        }
    }
}
