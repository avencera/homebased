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
build-release: web-build
    cargo build --release

# Bump homebased crate version: major, minor, or patch
[group('build')]
bump part:
    cargo xtask bump {{part}}

# Place: local | github | gh | public
[group('build')]
release place="local":
    cargo xtask release {{place}}

[private]
alias br := build-release

# ------------------------------------------------------------------------------
# web
# ------------------------------------------------------------------------------

# Install the dashboard dependencies from the lockfile
[group('web')]
web-install:
    cd web && npm ci

# Vite dev server; proxies /v1 to a daemon on 127.0.0.1:7677
[group('web')]
web-dev *args="":
    cd web && npm run dev {{args}}

# Build web/build/, which the release binary embeds
[group('web')]
[script('bash')]
web-build:
    set -euo pipefail
    cd web
    [[ -d node_modules ]] || npm ci
    npm run build

[group('web')]
[script('bash')]
web-check:
    set -euo pipefail
    cd web
    npm run check
    npm run lint
    npm test

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
    just web-check
    just web-build
    just test
