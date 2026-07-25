#!/bin/sh
# Build step for `herdr plugin install`. Prefers a prebuilt release binary
# (so hosts without a Go toolchain work), and falls back to `go build` for
# local development or when no matching release asset exists.
set -eu

cd "$(dirname "$0")/.."
VERSION="$(tr -d ' \t\r\n' < VERSION)"
REPO="ninehills/herdr-command-center"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
  x86_64 | amd64) arch="amd64" ;;
  arm64 | aarch64) arch="arm64" ;;
  *) arch="$(uname -m)" ;;
esac
asset="herdr-telemetry_${os}_${arch}"
base="https://github.com/${REPO}/releases/download/v${VERSION}"

mkdir -p bin

sha256() {
  if command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  elif command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else echo ""; fi
}

if command -v curl >/dev/null 2>&1 &&
  curl -fsSL "${base}/${asset}" -o bin/herdr-telemetry.dl 2>/dev/null; then
  # verify against the release checksums when available
  if curl -fsSL "${base}/checksums.txt" -o bin/checksums.txt 2>/dev/null; then
    want="$(grep " ${asset}\$" bin/checksums.txt 2>/dev/null | cut -d' ' -f1 || true)"
    got="$(sha256 bin/herdr-telemetry.dl)"
    if [ -n "$want" ] && [ -n "$got" ] && [ "$want" != "$got" ]; then
      echo "checksum mismatch for ${asset} (want ${want}, got ${got})" >&2
      rm -f bin/herdr-telemetry.dl bin/checksums.txt
      exit 1
    fi
    rm -f bin/checksums.txt
  fi
  mv bin/herdr-telemetry.dl bin/herdr-telemetry
  chmod +x bin/herdr-telemetry
  echo "installed prebuilt herdr-telemetry v${VERSION} (${os}/${arch})"
  exit 0
fi
rm -f bin/herdr-telemetry.dl

if command -v go >/dev/null 2>&1; then
  echo "no prebuilt binary for ${os}/${arch} at v${VERSION}; building from source"
  go build -trimpath -ldflags "-s -w" -o bin/herdr-telemetry .
  exit 0
fi

echo "ERROR: no prebuilt binary for ${os}/${arch} at v${VERSION} and no Go toolchain to build from source." >&2
echo "Install Go (https://go.dev/dl/) or file an issue to add a ${os}/${arch} release build." >&2
exit 1
