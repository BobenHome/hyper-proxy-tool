use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::WasiCtxBuilder;
use wasmtime_wasi::p1::{self, WasiP1Ctx};

/// Wasm plugin input
#[derive(Serialize)]
pub struct WasmInput {
    pub path: String,
    pub headers: HashMap<String, String>,
}

/// Wasm plugin output/decision
#[derive(Deserialize)]
pub struct WasmOutput {
    pub allow: bool,
    pub status_code: u16,
    pub body: String,
}

/// Wasm host state
struct WasmHostState {
    wasi_ctx: WasiP1Ctx,
}

/// Precompiled Wasm plugin module
pub struct PluginModule {
    engine: Engine,
    module: Module,
}

impl PluginModule {
    /// Load a Wasm plugin from file
    pub fn new(path: &str) -> Result<Self, anyhow::Error> {
        let engine = Engine::default();
        let module = Module::from_file(&engine, path)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("Failed to load wasm file: {}", path))?;
        Ok(Self { engine, module })
    }

    /// Run the plugin with input JSON
    pub fn run(&self, req_json: String) -> Result<String, anyhow::Error> {
        let wasi_ctx = WasiCtxBuilder::new().inherit_stdio().build_p1();
        let state = WasmHostState { wasi_ctx };
        let mut store = Store::new(&self.engine, state);
        let mut linker = Linker::new(&self.engine);

        p1::add_to_linker_sync(&mut linker, |s: &mut WasmHostState| &mut s.wasi_ctx)?;

        let instance = linker.instantiate(&mut store, &self.module)?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow::anyhow!("Wasm module missing 'memory' export"))?;

        let alloc_func = instance.get_typed_func::<i32, i32>(&mut store, "alloc")?;
        let input_bytes = req_json.as_bytes();
        let ptr = alloc_func.call(&mut store, input_bytes.len() as i32)?;
        memory.write(&mut store, ptr as usize, input_bytes)?;

        let run_func = instance.get_typed_func::<(i32, i32), u64>(&mut store, "on_request")?;
        let packed_ptr_len = run_func.call(&mut store, (ptr, input_bytes.len() as i32))?;

        let res_ptr = (packed_ptr_len >> 32) as usize;
        let res_len = (packed_ptr_len & 0xFFFFFFFF) as usize;
        let mut res_buffer = vec![0u8; res_len];
        memory.read(&mut store, res_ptr, &mut res_buffer)?;

        Ok(String::from_utf8(res_buffer)?)
    }
}
