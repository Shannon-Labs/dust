#!/bin/sh

set -u

REPO="Shannon-Labs/dust"
BINARY="dust"
BASE_URL="${DUST_BASE_URL:-https://github.com/${REPO}/releases/latest/download}"

say() {
    printf 'dust-install: %s\n' "$*"
}

fail() {
    say "$*" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

detect_os() {
    case "$(uname -s)" in
        Linux) echo "linux" ;;
        Darwin) echo "macos" ;;
        *) fail "unsupported operating system: $(uname -s)" ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64) echo "x86_64" ;;
        arm64|aarch64) echo "aarch64" ;;
        *) fail "unsupported architecture: $(uname -m)" ;;
    esac
}

choose_install_dir() {
    if [ -n "${DUST_INSTALL_DIR:-}" ]; then
        INSTALL_DIR="$DUST_INSTALL_DIR"
        USE_SUDO=0
        return
    fi

    if [ -d /usr/local/bin ] && [ -w /usr/local/bin ]; then
        INSTALL_DIR="/usr/local/bin"
        USE_SUDO=0
        return
    fi

    if [ ! -d /usr/local/bin ] && [ -d /usr/local ] && [ -w /usr/local ]; then
        INSTALL_DIR="/usr/local/bin"
        USE_SUDO=0
        return
    fi

    if command -v sudo >/dev/null 2>&1; then
        INSTALL_DIR="/usr/local/bin"
        USE_SUDO=1
        return
    fi

    INSTALL_DIR="${HOME}/.local/bin"
    USE_SUDO=0
}

install_binary() {
    src="$1"
    dest="$INSTALL_DIR/$BINARY"

    if [ "$USE_SUDO" -eq 1 ]; then
        sudo mkdir -p "$INSTALL_DIR" || fail "unable to create $INSTALL_DIR"
        sudo cp "$src" "$dest" || fail "unable to copy binary to $dest"
        sudo chmod 755 "$dest" || fail "unable to mark $dest executable"
    else
        mkdir -p "$INSTALL_DIR" || fail "unable to create $INSTALL_DIR"
        cp "$src" "$dest" || fail "unable to copy binary to $dest"
        chmod 755 "$dest" || fail "unable to mark $dest executable"
    fi
}

main() {
    need_cmd curl
    need_cmd tar
    need_cmd mktemp

    os="$(detect_os)"
    arch="$(detect_arch)"
    choose_install_dir

    archive="dust-${arch}-${os}.tar.gz"
    url="${BASE_URL}/${archive}"
    tmpdir="$(mktemp -d 2>/dev/null || mktemp -d -t dust-install)"

    trap 'rm -rf "$tmpdir"' EXIT INT TERM

    say "downloading ${archive}"
    if ! curl -fsSL "$url" -o "$tmpdir/$archive"; then
        fail "download failed: ${url}"
    fi

    if ! tar -xzf "$tmpdir/$archive" -C "$tmpdir"; then
        fail "failed to extract ${archive}"
    fi

    if [ ! -f "$tmpdir/$BINARY" ]; then
        fail "archive did not contain ${BINARY}"
    fi

    install_binary "$tmpdir/$BINARY"

    version="$("$INSTALL_DIR/$BINARY" --version 2>/dev/null | head -n 1 || true)"
    if [ -n "$version" ]; then
        say "installed ${version} to $INSTALL_DIR/$BINARY"
    else
        say "installed $BINARY to $INSTALL_DIR/$BINARY"
    fi

    case ":$PATH:" in
        *":$INSTALL_DIR:"*) ;;
        *)
            say "$INSTALL_DIR is not on your PATH"
            say "add it with: export PATH=\"$INSTALL_DIR:\$PATH\""
            ;;
    esac
}

main "$@"
