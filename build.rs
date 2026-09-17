//! Make sure the dashboard output directory exists so `rust_embed` compiles
//! before `npm run build` has ever run. An empty directory embeds nothing and
//! the daemon serves a "not built" page instead.

use std::path::Path;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|err| {
        panic!("CARGO_MANIFEST_DIR is unset: {err}");
    });
    let build_dir = Path::new(&manifest_dir).join("web").join("build");
    if let Err(err) = std::fs::create_dir_all(&build_dir) {
        panic!("create {}: {err}", build_dir.display());
    }
    // the directory mtime changes when `npm run build` replaces its contents
    println!("cargo:rerun-if-changed=web/build");
}
