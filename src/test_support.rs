//! Helpers shared by unit tests

use std::path::PathBuf;

/// Path of the `homebased` binary cargo built for this test run
///
/// Cargo sets `CARGO_BIN_EXE_homebased` only for integration tests, and with
/// a separate `build-dir` the unit test binary no longer sits next to the
/// final binary, so `assert_cmd`'s fallback finds nothing. The final binary
/// is still placed in the target directory, which `cargo test` fills before
/// running any test because the package has integration tests
pub(crate) fn homebased_binary() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_homebased") {
        return path.into();
    }

    let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"),
        PathBuf::from,
    );
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let path = target
        .join(profile)
        .join(format!("homebased{}", std::env::consts::EXE_SUFFIX));
    assert!(
        path.exists(),
        "no homebased binary at {}; run `cargo build` first",
        path.display()
    );
    path
}
