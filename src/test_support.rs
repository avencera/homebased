//! Helpers shared by unit tests

use std::path::PathBuf;

/// Path of the `homebased` binary cargo built for this test run
///
/// Cargo sets `CARGO_BIN_EXE_homebased` only for integration tests. The final
/// binary usually sits beside the test binary's directory, which follows any
/// `--target-dir`; with a separate `build-dir` it does not, and the default
/// target directory is the fallback. `cargo test` builds the binary before any
/// test runs because the package has integration tests
pub(crate) fn homebased_binary() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_homebased") {
        return path.into();
    }

    let name = format!("homebased{}", std::env::consts::EXE_SUFFIX);
    let beside_tests = std::env::current_exe().ok().and_then(|exe| {
        let dir = exe.parent()?;
        let dir = if dir.ends_with("deps") {
            dir.parent()?
        } else {
            dir
        };
        Some(dir.join(&name))
    });
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let in_target = std::env::var_os("CARGO_TARGET_DIR")
        .map_or_else(
            || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"),
            PathBuf::from,
        )
        .join(profile)
        .join(&name);
    let candidates = [beside_tests, Some(in_target)];
    candidates
        .iter()
        .flatten()
        .find(|path| path.is_file())
        .cloned()
        .unwrap_or_else(|| {
            panic!("no homebased binary at {candidates:?}; set CARGO_BIN_EXE_homebased or run `cargo build` first")
        })
}
