use std::time::Instant;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Engine, Result, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

wasmtime::component::bindgen!({
    path: "../logic/wit",
    world: "primes",
});

struct State {
    ctx: WasiCtx,
    table: ResourceTable,
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.ctx, table: &mut self.table }
    }
}

fn run(engine: &Engine, linker: &Linker<State>, path: &str, start: u64, end: u64) -> Result<u64> {
    let component = Component::from_file(engine, path)?;
    let mut store = Store::new(
        engine,
        State { ctx: WasiCtx::builder().build(), table: ResourceTable::new() },
    );
    let bindings = Primes::instantiate(&mut store, &component, linker)?;
    bindings
        .demo_primes_calcul()
        .call_how_many_prime_in_range(&mut store, start, end)
}

fn main() -> Result<()> {
    let engine = Engine::default();
    let mut linker = Linker::<State>::new(&engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;

    let modules = [
        ("Rust", "../logic/rust/target/wasm32-wasip2/release/wasm_logic_rust.wasm"),
        ("C#  ", "../logic/cs/bin/Release/net10.0/wasi-wasm/publish/WasmLogicCS.wasm"),
    ];
    let cases = [(0, 100_000, 9_592), (100_000, 1_000_000, 68_906), (1_000, 1_000_000, 78_330)];

    for (name, path) in modules {
        for (start, end, expected) in cases {
            let t = Instant::now();
            let result = run(&engine, &linker, path, start, end)?;
            let ok = if result == expected { "OK" } else { "ERREUR" };
            println!("{name} {start}-{end} = {result} (attendu {expected}) {ok} en {:?}", t.elapsed());
        }
    }
    Ok(())
}