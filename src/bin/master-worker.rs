use zenoh::prelude::r#async::*;
use sysinfo::{System, Pid};
use tokio::sync::Semaphore;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use std::sync::Mutex;
use shared::*;

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


//lancer le webassembly
fn launch_wasm_calcul(engine: &Engine, component: &Component, linker: &Linker<State>, start: u64, end: u64) {
    let mut store = Store::new(
        engine,
        State { ctx: WasiCtx::builder().build(), table: ResourceTable::new() },
    );

    let bindings = Primes::instantiate(&mut store, &component, &linker)
        .expect("Erreur lors de l'instanciation du module Wasm");

    let prime_count = bindings
        .demo_primes_calcul()
        .call_how_many_prime_in_range(&mut store, start, end)
        .expect("Erreur lors de l'appel de la fonction Wasm");

    println!("Nombre de nombres premiers entre {} et {}: {}", start, end, prime_count);
}

#[tokio::main]
async fn main() {
    println!("Démarrage Master-Worker ...");

    // INITIALISATION WASMTIME
    let wasm_engine = Arc::new(Engine::default());
    let mut wasm_linker = Linker::<State>::new(&*wasm_engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut wasm_linker).expect("Erreur lors de l'ajout de WASI au linker");
    let wasm_linker = Arc::new(wasm_linker);
    let wasm_path = "wasm/logic/rust/target/wasm32-wasip2/release/wasm_logic_rust.wasm";
    let wasm_component = Arc::new(Component::from_file(&*wasm_engine, wasm_path)
        .expect("⚠️ Impossible de trouver le fichier .wasm !"));

    // INITIALISATION ZENOH
    let session = Arc::new(zenoh::open(zenoh::config::peer()).res().await.expect("Erreur Zenoh"));

    //var info sys (A CHANGER QUAND PASSAGE SOUS LINUX)
    let mut sys = System::new_all();
    let my_pid = Pid::from_u32(std::process::id());

    // DÉCOUVERTE DYNAMIQUE DES WORKERS
    let active_workers = Arc::new(Mutex::new(Vec::new()));
    let active_workers_clone = active_workers.clone();

    // ASTUCE ZENOH : On utilise .callback() au lieu de s'embêter avec un flux asynchrone
    let _subscriber = session.declare_subscriber("telemetry/**")
        .callback(move |sample| {
            // CORRECTION : On enlève les parenthèses, key_expr est un champ direct
            let key = sample.key_expr.as_str();

            if let Some(worker_name) = key.split('/').last() {
                let mut list = active_workers_clone.lock().unwrap();
                // Si le worker n'est pas dans notre liste, on l'ajoute
                if !list.contains(&worker_name.to_string()) {
                    println!("🔌 Nouveau Worker détecté et connecté au cluster : {}", worker_name);
                    list.push(worker_name.to_string());
                }
            }
        })
        .res()
        .await
        .unwrap();

    // VARIABLES DE LA BOUCLE DE CALCUL
    let semaphore_side_workers = Arc::new(Semaphore::new(12));
    let semaphore_local = Arc::new(Semaphore::new(5));
    let mut current_start: u64 = 200_000_000;
    let step: u64 = 2_000_000_000;
    let mut next_worker_idx = 0;

    //VERIFICATION DE LA CHARGE CPU/CALCUL/ENVOI AUX WORKERS
    loop {
        // ⚠️ CRUCIAL : Il faut laisser un peu de temps passer pour que le CPU soit mesurable
        sleep(Duration::from_millis(50)).await;

        sys.refresh_process(my_pid);
        let cpu_usage = if let Some(process) = sys.process(my_pid) {
            process.cpu_usage()
        } else {
            -1.0 // Sécurité au cas où
        };
        println!("Mon CPU est à {:.1}%", cpu_usage);

        if cpu_usage == -1.0 {
            println!("⚠️ Impossible de récupérer l'utilisation CPU !");
        }
        else if cpu_usage > 90.0 {
            println!("⚠️ CPU à {:.1}% !", cpu_usage);

            // regarde le semaphore local et si max est atteint attend, verif charge cpu si > 90% alors envoie tache en cours au worker
        }
        else if cpu_usage < 70.0 {
            if semaphore_local.available_permits() == 0 {
                println!("⚠️ Semaphore local plein ! Attente...");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }

            let permit = tokio::sync::Semaphore::acquire_owned(semaphore_local.clone()).await.unwrap();
            let engine_clone = wasm_engine.clone();
            let component_clone = wasm_component.clone();
            let linker_clone = wasm_linker.clone();
            let start = current_start;
            let end = current_start + step;

            //On utilise spawn_blocking pour les calculs lourds (CPU-bound)
            tokio::task::spawn_blocking(move || {
                println!("🟢 Lancement d'un thread local Wasm pour {}-{}...", start, end);
                launch_wasm_calcul(&*engine_clone, &*component_clone, &linker_clone, start, end);
                drop(permit); // On rend le ticket quand le calcul est fini
            });

            current_start += step;
        } else {
            //OFFLOADING AUX WORKERS

            let workers_list = active_workers.lock().unwrap().clone();

            if workers_list.is_empty() {
                println!("Aucun Worker n'est détecté sur le réseau !");
                // On attend un peu et on passe au cycle suivant sans avancer current_start
                sleep(Duration::from_millis(500)).await;
                continue;
            }

            let target_worker = workers_list[next_worker_idx % workers_list.len()].clone();
            next_worker_idx += 1;

            println!("⚠️ CPU à {:.1}% ! Délégation réseau de la plage {}-{}...", cpu_usage, current_start, current_start + step);

            let permit = tokio::sync::Semaphore::acquire_owned(semaphore_side_workers.clone()).await.unwrap();
            let session_clone = session.clone();
            let start_calc = current_start;
            let end_calc = current_start + step;

            // ASTUCE ZENOH : On utilise spawn asynchrone classique car on attend le réseau (I/O-bound)
            tokio::spawn(async move {
                // On formule l'URL avec les paramètres pour le Worker
                //VOIR SI MOYEN D'ENVOYER LE WASM
                //VOIR MODIF EN FONCTION HEARTBEAT

                let query_uri = format!("sim/calcul/premiers/{}?start={}&end={}", target_worker, start_calc, end_calc);

                let request = session_clone.get(&query_uri).timeout(Duration::from_secs(3600));

                // On utilise GET pour interroger les Workers (Queryable)
                if let Ok(replies) = request.res().await {
                    while let Ok(reply) = replies.recv_async().await {
                        if let Ok(sample) = reply.sample {
                            let result = String::from_utf8_lossy(&sample.value.payload.contiguous()).to_string();
                            println!("📥 Réponse d'un Worker ({}-{}) : {}", start_calc, end_calc, result);
                        }
                    }
                }
                drop(permit); // On rend le ticket quand le réseau a répondu
            });

            current_start += step;
        }
    }
}