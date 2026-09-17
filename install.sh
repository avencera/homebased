#!/bin/sh
# Install a homebased binary from GitHub Releases.
#
# One-liner (Linux and macOS):
#   curl -LSfs https://github.com/avencera/homebasd/releases/latest/download/install.sh | sh
#
# Asset names match .github/workflows/release.yml:
#   homebased-<tag>-<target>.tar.gz
#
# Heavily modified from https://github.com/japaric/trust and avencera/rustywind.

DEFAULT_GIT="avencera/homebasd"
DEFAULT_CRATE="homebased"

help() {
    cat <<EOF
Install a binary release of homebased from GitHub.

Usage:
    install.sh [options]

    curl -LSfs https://github.com/${DEFAULT_GIT}/releases/latest/download/install.sh | sh

Options:
    -h, --help      Display this message
    --git SLUG      GitHub repository (default ${DEFAULT_GIT})
    -f, --force     Overwrite an existing binary (default)
    --crate NAME    Binary name to install (default ${DEFAULT_CRATE})
    --tag TAG       Release tag (default latest)
    --to LOCATION   Install directory (default ~/.local/bin)
EOF
}

say() {
    echo "install.sh: $1"
}

say_err() {
    say "$1" >&2
}

err() {
    if [ -n "${td:-}" ]; then
        rm -rf "$td"
    fi

    say_err "ERROR $1"
    exit 1
}

need() {
    if ! command -v "$1" > /dev/null 2>&1; then
        err "need $1 (command not found)"
    fi
}

expand_dest() {
    dest=$1
    case "$dest" in
        ~)
            dest=$HOME
            ;;
        ~/*)
            dest=$HOME/${dest#~/}
            ;;
    esac
    printf '%s\n' "$dest"
}

target_triple() {
    os=$(uname -s)
    arch=$(uname -m)
    case "$arch" in
        amd64)
            arch=x86_64
            ;;
        arm64)
            arch=aarch64
            ;;
    esac

    case "$os" in
        Darwin)
            printf '%s-apple-darwin\n' "$arch"
            ;;
        Linux)
            printf '%s-unknown-linux-musl\n' "$arch"
            ;;
        *)
            return 1
            ;;
    esac
}

git=$DEFAULT_GIT
crate=$DEFAULT_CRATE
tag=""
dest=""

need_value() {
    case ${2:-} in
        "" | --*)
            err "$1 requires a value"
            ;;
    esac
}

while test $# -gt 0; do
    case $1 in
        --crate)
            need_value "$1" "$2"
            crate=$2
            shift
            ;;
        --force | -f)
            # always overwrite; accepted for rustywind-style invocations
            ;;
        --git)
            need_value "$1" "$2"
            git=$2
            shift
            ;;
        --help | -h)
            help
            exit 0
            ;;
        --tag)
            need_value "$1" "$2"
            tag=$2
            shift
            ;;
        --to)
            need_value "$1" "$2"
            dest=$2
            shift
            ;;
        *)
            err "unknown option: $1"
            ;;
    esac
    shift
done

need curl
need tar
need mkdir
need mktemp
need install
need rm
need uname

if [ -z "$HOME" ]; then
    err "HOME is not set"
fi

if [ -z "$dest" ]; then
    dest=$HOME/.local/bin
fi
dest=$(expand_dest "$dest")

url="https://github.com/$git"

say_err "GitHub repository: $url"

if [ -z "$tag" ]; then
    latest_url=$(curl -fsSL -o /dev/null -w '%{url_effective}' "$url/releases/latest") \
        || err "failed to resolve latest release for $git"
    case "$latest_url" in
        */releases/tag/*) ;;
        *)
            err "no GitHub release found for $git"
            ;;
    esac
    tag=${latest_url##*/}
    say_err "Tag: latest ($tag)"
else
    say_err "Tag: $tag"
fi

target=$(target_triple) || err "unsupported OS $(uname -s)/$(uname -m); supported: Linux and macOS"
say_err "Crate: $crate"
say_err "Target: $target"

download="$url/releases/download/$tag/$crate-$tag-$target.tar.gz"
say_err "Downloading: $download"

td=$(mktemp -d 2>/dev/null || mktemp -d -t homebased-install)
curl -fsSL "$download" -o "$td/homebased.tar.gz" \
    || err "$download does not exist; build $crate from source"
tar -C "$td" -xzf "$td/homebased.tar.gz" \
    || err "failed to extract $download"
rm -f "$td/homebased.tar.gz"

say_err "Installing to: $dest"

installed=false
for f in "$td"/*; do
    [ -e "$f" ] || continue
    [ -f "$f" ] || continue
    [ -x "$f" ] || continue

    mkdir -p "$dest" || err "failed to create $dest"
    # unlink first so a running binary can be replaced (macOS ETXTBSY)
    rm -f "$dest/$crate"
    install -m 0755 "$f" "$dest/$crate" || err "failed to install $crate to $dest"
    installed=true
    break
done

rm -rf "$td"
td=""

if [ "$installed" = false ]; then
    err "archive did not contain an executable $crate binary"
fi

case ":$PATH:" in
    *:"$dest":*) ;;
    *)
        say_err "warning: $dest is not on PATH"
        ;;
esac

say_err "installed $crate $tag to $dest/$crate"
