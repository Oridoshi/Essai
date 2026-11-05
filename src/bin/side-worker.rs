use zenoh::prelude::r#async::*;
use std::sync::Arc;
use shared::*;
use uuid::Uuid;

use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Engine, Result, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

wasmtime::component::bindgen!({
    path: "wasm/logic/wit",
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

#[tokio::main]
async fn main() {
    let worker_id = Uuid::new_v4();
    let worker_name = format!("Worker_{}", worker_id);
    println!("Démarrage du {}...", worker_name);

    // INITIALISATION WASMTIME
    let wasm_engine = Arc::new(Engine::default());
    let mut wasm_linker = Linker::<State>::new(&*wasm_engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut wasm_linker).expect("Erreur lors de l'ajout de WASI au linker");
    let wasm_linker = Arc::new(wasm_linker);
    let wasm_path = "wasm/logic/cs/bin/Release/net10.0/wasi-wasm/publish/WasmLogicCS.wasm";
    let wasm_component = Arc::new(Component::from_file(&*wasm_engine, wasm_path)
        .expect("⚠️ Impossible de trouver le fichier .wasm !"));

    // CANAL DE COMMUNICATION INTERNE (Rx/Tx)
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    // INITIALISATION ZENOH
    let session = Arc::new(
        zenoh::open(zenoh::config::peer())
            .res()
            .await
            .expect("Erreur lors de l'ouverture de la session Zenoh"),
    );

    let query_key = format!("sim/calcul/premiers/{}", worker_name);
    let _queryable = session
        .as_ref()
        .declare_queryable(&query_key)
        .callback(move |query| {
            let _ = tx.send(query);
        })
        .res()
        .await
        .expect("Erreur lors de la déclaration de la queryable Zenoh");

    println!("{} is ready to receive tasks on {}.", worker_name, query_key);

    // BOUCLE D'ÉCOUTE ET D'EXÉCUTION
    let semaphore_worker = Arc::new(tokio::sync::Semaphore::new(4));

    // TÉLÉMÉTRIE (Pour que le Dashboard te voie)
    let session_telemetry = session.clone();
    let name_telemetry = worker_name.clone();
    let semaphore_telemetry = semaphore_worker.clone();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            //telemetry data
            let cpu_usage = 0; // Remplacer par la vraie valeur
            let ram_usage = 0; // Remplacer par la vraie valeur
            let max_tasks: u32 = 4; // Remplacer par la vraie valeur
            let tasks_count: u32 = max_tasks - semaphore_telemetry.available_permits() as u32;

            let heartbeat = shared::Heartbeat {
                id: worker_id,
                cpu_usage,
                ram_usage,
                tasks_count,
                max_tasks,
                status: WorkerStatus::Available,
                state: WorkerState::WarmUp,
            };

            let payload = serde_json::to_string(&heartbeat).unwrap();
            let _ = session_telemetry.as_ref().put(format!("telemetry/{}", name_telemetry), payload).res().await;
        }
    });

    while let Some(query) = rx.recv().await {
        let parameters = query.parameters().to_string(); // On convertit en String pour l'isoler
        let parts: Vec<&str> = parameters.split(&['=', '&'][..]).collect();

        if parts.len() >= 4 {
            let start: u64 = parts[1].parse().unwrap_or(0);
            let end: u64 = parts[3].parse().unwrap_or(0);

            println!("⚙️ {} a reçu une tâche : {}-{}", worker_name, start, end);

            // On prépare les clones pour le thread
            let engine_clone = wasm_engine.clone();
            let component_clone = wasm_component.clone();
            let wasm_linker_clone = wasm_linker.clone();
            let permit_future = tokio::sync::Semaphore::acquire_owned(semaphore_worker.clone());

            //tache
            tokio::spawn(async move {
                let permit = permit_future.await.unwrap();

                // 2. On délègue le calcul lourd au processeur
                let primes = tokio::task::spawn_blocking(move || {
                    let mut store = Store::new(
                        &*engine_clone,
                        State { ctx: WasiCtx::builder().build(), table: ResourceTable::new() },
                    );

                    let bindings = Primes::instantiate(&mut store, &*component_clone, &*wasm_linker_clone)
                        .expect("Erreur lors de l'instanciation du composant Wasm");

                    bindings.demo_primes_calcul()
                        .call_how_many_prime_in_range(&mut store, start, end)
                        .expect("Erreur lors de l'appel à la fonction Wasm")
                }).await.unwrap(); // On attend que le thread CPU ait fini

                println!("✅ Calcul terminé ({}-{}) : {} premiers trouvés. Envoi...", start, end, primes);

                // 3. On renvoie la réponse au Master via le réseau
                let sample = zenoh::sample::Sample::new(query.key_expr().clone(), primes.to_string());
                let _ = query.reply(Ok(sample)).res().await;

                drop(permit); // On libère la place pour la tâche suivante
            });
        } else {
            println!("⚠️ Requête reçue mais mal formatée : {}", parameters);
        }
    }
}