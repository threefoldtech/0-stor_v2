use actix_web::http::StatusCode;
use actix_web::{get, App, HttpRequest, HttpResponse, HttpServer, Result};
use nix::ioctl_read;
use prometheus::{labels, opts, register_int_gauge, Encoder, TextEncoder};
use std::env;
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::thread;

const ZDBFS_IOC_MAGIC: u8 = b'E';
const ZDBFS_IOC_TYPE_MODE: u8 = 1;

ioctl_read! {
    /// Read the statistics of a 0-db-fs process
    zdbfs_read_stats,
    ZDBFS_IOC_MAGIC,
    ZDBFS_IOC_TYPE_MODE,
    types::fs_stats_t
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: zdb-prom <path>");
        std::process::exit(1);
    }
    let p = args[1].clone();

    // Setup prometheus metrics
    let labels = labels! {"mount" => &p};

    let opts = opts!("fuse_reqs", "Total amount of fuse requests", labels);
    let fuse_reqs = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "cache_hits",
        "Total amount of cache hits in the filesystem",
        labels
    );
    let cache_hits = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "cache_miss",
        "Total amount of cache misses in the filesystem",
        labels
    );
    let cache_misses = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "cache_full",
        "Total amount of times the cache was completely filled",
        labels
    );
    let cache_full = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "cache_linear_flush",
        "Total amount of times the cache was flushed as result of time",
        labels
    );
    let cache_linear_flush = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "cache_random_flush",
        "Total amount of times the cache was flushed randomly",
        labels
    );
    let cache_random_flush = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "syscall_getattr",
        "Total amount of getattr syscalls",
        labels
    );
    let syscall_getattr = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "syscall_setattr",
        "Total amount of setattr syscalls",
        labels
    );
    let syscall_setattr = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_create", "Total amount of create syscalls", labels);
    let syscall_create = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "syscall_readdir",
        "Total amount of readdir syscalls",
        labels
    );
    let syscall_readdir = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_open", "Total amount of open syscalls", labels);
    let syscall_open = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_read", "Total amount of read syscalls", labels);
    let syscall_read = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_write", "Total amount of write syscalls", labels);
    let syscall_write = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_mkdir", "Total amount of mkdir syscalls", labels);
    let syscall_mkdir = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_unlink", "Total amount of unlink syscalls", labels);
    let syscall_unlink = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_rmdir", "Total amount of rmdir syscalls", labels);
    let syscall_rmdir = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_rename", "Total amount of rename syscalls", labels);
    let syscall_rename = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_link", "Total amount of link syscalls", labels);
    let syscall_link = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "syscall_symlink",
        "Total amount of symlink syscalls",
        labels
    );
    let syscall_symlink = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "syscall_statsfs",
        "Total amount of statsfs syscalls",
        labels
    );
    let syscall_statsfs = register_int_gauge!(opts).unwrap();
    let opts = opts!("syscall_ioctl", "Total amount of ioctl syscalls", labels);
    let syscall_ioctl = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "read_bytes",
        "Total amount of bytes read from the filesystem",
        labels
    );
    let read_bytes = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "write_bytes",
        "Total amount of bytes written to the filesystem",
        labels
    );
    let write_bytes = register_int_gauge!(opts).unwrap();
    let opts = opts!(
        "fuse_errors",
        "Total amount of errors returned by fuse syscalls",
        labels
    );
    let fuse_errors = register_int_gauge!(opts).unwrap();

    let _ = thread::spawn(move || {
        // get fd
        let path = PathBuf::from(p);
        let dir_f = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .truncate(false)
            .open(&path)
            .expect("could not get file descriptor to dir");
        loop {
            let mut stats = types::fs_stats_t::default();
            unsafe {
                if let Err(e) = zdbfs_read_stats(dir_f.as_raw_fd(), &mut stats as *mut _) {
                    eprintln!("error return code {}", e);
                    std::process::exit(e.as_errno().unwrap() as i32);
                };
            }

            // Update metrics
            fuse_reqs.set(stats.fuse_reqs as i64);
            cache_hits.set(stats.cache_hit as i64);
            cache_misses.set(stats.cache_miss as i64);
            cache_full.set(stats.cache_full as i64);
            cache_linear_flush.set(stats.cache_linear_flush as i64);
            cache_random_flush.set(stats.cache_random_flush as i64);
            syscall_getattr.set(stats.syscall_getattr as i64);
            syscall_setattr.set(stats.syscall_setattr as i64);
            syscall_create.set(stats.syscall_create as i64);
            syscall_readdir.set(stats.syscall_readdir as i64);
            syscall_open.set(stats.syscall_open as i64);
            syscall_read.set(stats.syscall_read as i64);
            syscall_write.set(stats.syscall_write as i64);
            syscall_mkdir.set(stats.syscall_mkdir as i64);
            syscall_unlink.set(stats.syscall_unlink as i64);
            syscall_rmdir.set(stats.syscall_rmdir as i64);
            syscall_rename.set(stats.syscall_rename as i64);
            syscall_link.set(stats.syscall_link as i64);
            syscall_symlink.set(stats.syscall_symlink as i64);
            syscall_statsfs.set(stats.syscall_statsfs as i64);
            syscall_ioctl.set(stats.syscall_ioctl as i64);
            read_bytes.set(stats.read_bytes as i64);
            write_bytes.set(stats.write_bytes as i64);
            fuse_errors.set(stats.errors as i64);

            thread::sleep(std::time::Duration::from_secs(1));
        }
    });

    HttpServer::new(|| App::new().service(metrics))
        .bind("[::]:9100")?
        .run()
        .await
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

mod types {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
/*
*/
