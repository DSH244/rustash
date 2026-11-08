//! Rustash server binary: env config → SQLite → migrations/bootstrap → engine → HTTP.

use std::sync::Arc;

use rustash::{bootstrap, build_router, config::load_from_env, open_db, AppState, Engine};

#[tokio::main]
async fn main() {
    let config = Arc::new(load_from_env());
    config.validate();

    tracing_subscriber::fmt()
        .with_env_filter(
            config
                .log_level
                .parse()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let path = config.sqlite_path();
    tracing::info!(%path, "opening SQLite database");
    let db = open_db(&path);
    bootstrap(&db, &config).unwrap_or_else(|e| {
        tracing::error!("{e}");
        std::process::exit(1);
    });

    let engine = Engine::new(db.clone(), config.clone());
    engine.spawn();

    let state = AppState { db, config, engine };

    let addr = format!("0.0.0.0:{}", state.config.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("bind {addr}: {e}"));
    tracing::info!(%addr, "Rustash listening");

    let router = build_router(state);

    // Node's default SIGTERM/SIGINT disposition terminates the process immediately;
    // inside a container the kernel does NOT apply default dispositions to PID 1, so
    // without an explicit handler `docker stop` would leave this process alive through
    // the whole grace period — in-flight deliveries would time out and be recorded,
    // turning a clean lease-expiry (worker crash) into a transport-error retry. Exit
    // the moment the signal arrives: outstanding requests die with us, the row stays
    // leased, and the next process reclaims it as an expired lease.
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let Ok(mut term) = signal(SignalKind::terminate()) else {
            return;
        };
        let Ok(mut int) = signal(SignalKind::interrupt()) else {
            return;
        };
        tokio::select! {
            _ = term.recv() => {},
            _ = int.recv() => {},
        }
        std::process::exit(0);
    });

    axum::serve(listener, router).await.unwrap_or_else(|e| {
        tracing::error!(error = %e, "server error");
        std::process::exit(1);
    });
}
