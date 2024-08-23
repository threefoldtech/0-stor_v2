use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=include/types.h");

    let bindings = bindgen::Builder::default()
        .header("include/types.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .allowlist_type("fs_stats_t")
        .derive_default(true)
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("zdbfs_stats.rs"))
        .expect("couldn't write bindings");
}
