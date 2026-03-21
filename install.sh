#!/bin/sh
# Dust installer
# Usage: curl -fsSL https://dustdb.dev/install.sh | sh
set -eu

REPO="shannon-labs/dust"
BINARY="dust"
INSTALL_DIR="${DUST_INSTALL_DIR:-$HOME/.dust/bin}"

main() {
    platform=$(detect_platform)
    arch=$(detect_arch)
    target="${platform}-${arch}"

    echo "Installing dust for ${target}..."

    # Determine latest release
    if [ -n "${DUST_VERSION:-}" ]; then
        version="$DUST_VERSION"
    else
        version=$(latest_version)
    fi

    echo "  Version: ${version}"
    echo "  Target:  ${target}"
    echo "  Install: ${INSTALL_DIR}"

    # Download
    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT

    url="https://github.com/${REPO}/releases/download/${version}/dust-${version}-${target}.tar.gz"
    echo "  Downloading ${url}..."

    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "${tmpdir}/dust.tar.gz"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "${tmpdir}/dust.tar.gz" "$url"
    else
        echo "Error: curl or wget required" >&2
        exit 1
    fi

    # Extract
    tar xzf "${tmpdir}/dust.tar.gz" -C "${tmpdir}"

    # Install
    mkdir -p "${INSTALL_DIR}"
    mv "${tmpdir}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
    chmod +x "${INSTALL_DIR}/${BINARY}"

    echo ""
    echo "Installed dust to ${INSTALL_DIR}/${BINARY}"

    # Check PATH
    case ":${PATH}:" in
        *":${INSTALL_DIR}:"*) ;;
        *)
            echo ""
            echo "Add dust to your PATH by adding this to your shell profile:"
            echo ""
            echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
            echo ""
            ;;
    esac

    echo "Run 'dust --help' to get started."
}

detect_platform() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        *)
            echo "Error: unsupported platform $(uname -s)" >&2
            exit 1
            ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)  echo "x86_64" ;;
        arm64|aarch64) echo "aarch64" ;;
        *)
            echo "Error: unsupported architecture $(uname -m)" >&2
            exit 1
            ;;
    esac
}

latest_version() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name"' \
            | head -1 \
            | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/'
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name"' \
            | head -1 \
            | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/'
    else
        echo "Error: curl or wget required" >&2
        exit 1
    fi
}

main "$@"
