[default]
list:
    @just --list

# ------------------------------------------------------------------------------
# build
# ------------------------------------------------------------------------------

[group('build')]
build:
    cargo build

[group('build')]
build-release:
    cargo build --release

[private]
alias br := build-release

# ------------------------------------------------------------------------------
# test
# ------------------------------------------------------------------------------

[group('test')]
test *args="":
    cargo test {{args}}

# Real `codex`/`claude`/`grok` on PATH. Opt-in; not part of ci.
[group('test')]
smoke-cli *args="":
    cargo test --test cli_smoke -- --ignored --nocapture {{args}}

# ------------------------------------------------------------------------------
# lint
# ------------------------------------------------------------------------------

[group('lint')]
fmt *args="":
    cargo fmt --all {{args}}

[group('lint')]
clippy:
    cargo clippy --all-targets -- -D warnings

[group('ci')]
[script('bash')]
ci:
    set -euo pipefail
    just fmt -- --check
    just clippy
    just test
