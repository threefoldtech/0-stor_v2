use actix_web::http::{header, Method, StatusCode};
use actix_web::{
    error, get, guard, middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer, Result,
};
use futures::future::join_all;
use prometheus::{register_gauge, register_gauge_vec, Encoder, Opts, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use tokio::signal::unix::{signal, SignalKind};
use tokio::{select, time};
use zstor_v2::{config::Config, zdb::SequentialZdb};

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg_file = std::fs::File::open("./zstor.toml")?;
    let mut cfg_toml = String::new();
    cfg_file.read_to_string(&mut cfg_toml)?;
    let cfg: Config = toml::from_str(&cfg_toml)?;

    let zdbs = cfg.backends();
    let mut handles: Vec<
        tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    > = Vec::with_capacity(zdbs.len());

    let entries_gauges =
        register_gauge_vec!("entries", "entries in namespace", &["address", "namespace"])?;
    let data_size_bytes_gauges = register_gauge_vec!(
        "data_size_bytes",
        "data_size_bytes in namespace",
        &["address", "namespace"]
    )?;
    let data_limit_bytes_gauges = register_gauge_vec!(
        "data_limit_bytes",
        "data_limit_bytes in namespace",
        &["address", "namespace"]
    )?;
    let index_size_bytes_gauges = register_gauge_vec!(
        "index_size_bytes",
        "index_size_bytes in namespace",
        &["address", "namespace"]
    )?;
    let index_io_errors_gauges = register_gauge_vec!(
        "index_io_errors",
        "index_io_errors in namespace",
        &["address", "namespace"]
    )?;
    let index_faults_gauges = register_gauge_vec!(
        "index_faults",
        "index_faults in namespace",
        &["address", "namespace"]
    )?;
    let data_io_errors_gauges = register_gauge_vec!(
        "data_io_errors",
        "data_io_errors in namespace",
        &["address", "namespace"]
    )?;
    let data_faults_gauges = register_gauge_vec!(
        "data_faults",
        "data_faults in namespace",
        &["address", "namespace"]
    )?;
    let index_disk_freespace_bytes_gauges = register_gauge_vec!(
        "index_disk_freespace_bytes",
        "index_disk_freespace_bytes in namespace",
        &["address", "namespace"]
    )?;
    let data_disk_freespace_bytes_gauges = register_gauge_vec!(
        "data_disk_freespace_bytes",
        "data_disk_freespace_bytes in namespace",
        &["address", "namespace"]
    )?;

    let active_cu_gauge =
        register_gauge!("pool_active_cu", "active compute units drain in the pool")?;
    let active_su_gauge =
        register_gauge!("pool_active_su", "active storage units drain in the pool")?;
    let active_ipv4_gauge =
        register_gauge!("pool_active_ipv4", "active ipv4 units drain in the pool")?;
    let cus_gauge = register_gauge!("pool_cus", "total compute units in pool")?;
    let sus_gauge = register_gauge!("pool_sus", "total storage units in pool")?;
    let ipv4us_gauge = register_gauge!("pool_ipv4us", "total ipv4 units in pool")?;

    let client = reqwest::Client::new();

    for ci in zdbs.into_iter().cloned() {
        let mut labels = HashMap::new();
        labels.insert(
            "namespace",
            ci.namespace().as_ref().map(|s| s.as_str()).unwrap_or(""),
        );
        let address = ci.address().to_string();
        labels.insert("address", &address);

        let entries_gauge = entries_gauges.get_metric_with(&labels)?;
        let data_size_bytes_gauge = data_size_bytes_gauges.get_metric_with(&labels)?;
        let data_limit_bytes_gauge = data_limit_bytes_gauges.get_metric_with(&labels)?;
        let index_size_bytes_gauge = index_size_bytes_gauges.get_metric_with(&labels)?;
        let index_io_errors_gauge = index_io_errors_gauges.get_metric_with(&labels)?;
        let index_faults_gauge = index_faults_gauges.get_metric_with(&labels)?;
        let data_io_errors_gauge = data_io_errors_gauges.get_metric_with(&labels)?;
        let data_faults_gauge = data_faults_gauges.get_metric_with(&labels)?;
        let index_disk_freespace_bytes_gauge =
            index_disk_freespace_bytes_gauges.get_metric_with(&labels)?;
        let data_disk_freespace_bytes_gauge =
            data_disk_freespace_bytes_gauges.get_metric_with(&labels)?;

        handles.push(tokio::spawn(async move {
            let mut zdb = SequentialZdb::new(ci).await?;

            let mut interval = time::interval(time::Duration::from_secs(3));

            loop {
                interval.tick().await;
                let ns_info = zdb.ns_info().await?;

                entries_gauge.set(ns_info.entries as f64);
                data_size_bytes_gauge.set(ns_info.data_size_bytes as f64);
                data_limit_bytes_gauge.set(
                    ns_info
                        .data_limit_bytes
                        .unwrap_or_else(|| ns_info.data_disk_freespace_bytes)
                        as f64,
                );
                index_size_bytes_gauge.set(ns_info.index_size_bytes as f64);
                index_io_errors_gauge.set(ns_info.index_io_errors as f64);
                index_faults_gauge.set(ns_info.index_faults as f64);
                data_io_errors_gauge.set(ns_info.data_io_errors as f64);
                data_faults_gauge.set(ns_info.data_faults as f64);
                index_disk_freespace_bytes_gauge.set(ns_info.index_disk_freespace_bytes as f64);
                data_disk_freespace_bytes_gauge.set(ns_info.data_disk_freespace_bytes as f64);
            }
        }));
    }

    handles.push(tokio::spawn(async move {
        loop {
            let pool = client
                .get("https://explorer.testnet.grid.tf/api/v1/reservations/pools/22040")
                .send()
                .await?
                .json::<Pool>()
                .await?;

            active_cu_gauge.set(pool.active_cu);
            active_su_gauge.set(pool.active_su);
            active_ipv4_gauge.set(pool.active_ipv4);
            cus_gauge.set(pool.cus);
            sus_gauge.set(pool.sus);
            ipv4us_gauge.set(pool.ipv4us);
        }
    }));

    // let mut buffer = Vec::new();
    // let encoder = TextEncoder::new();
    // encoder.encode(&prometheus::gather(), &mut buffer)?;
    // let output = String::from_utf8(buffer)?;
    // println!("{}", output);

    let mut stream = signal(SignalKind::interrupt())?;

    select! {
    _ = HttpServer::new(|| App::new().service(metrics))
        .bind("[::]:9100")?
        .run() => {},
    _ = stream.recv() => {}
        };

    Ok(())
}

#[get("/metrics")]
async fn metrics(_: HttpRequest) -> Result<HttpResponse> {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&prometheus::gather(), &mut buffer).unwrap(); // TODO: unwrap
    let output = String::from_utf8(buffer).unwrap(); // TODO: unwrap

    Ok(HttpResponse::build(StatusCode::OK)
        .content_type("text/plain; charset=utf-8")
        .body(output))
}

#[derive(Serialize, Deserialize)]
struct Pool {
    pub pool_id: u64,
    pub cus: f64,
    pub sus: f64,
    pub ipv4us: f64,
    pub active_cu: f64,
    pub active_su: f64,
    pub active_ipv4: f64,
    pub empty_at: u64,
}
