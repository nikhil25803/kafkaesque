#!/usr/bin/env sh
set -eu

repository="nikhil25803/kafkaesque"
version="${VERSION:-latest}"
install_dir="${INSTALL_DIR:-${HOME}/.local/bin}"

case "$(uname -s)" in
  Linux) os="linux" ;;
  Darwin) os="darwin" ;;
  *)
    echo "Unsupported operating system: $(uname -s)" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64 | amd64) arch="amd64" ;;
  arm64 | aarch64) arch="arm64" ;;
  *)
    echo "Unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

if [ "${version}" = "latest" ]; then
  release_url="https://github.com/${repository}/releases/latest/download"
else
  case "${version}" in
    v*) ;;
    *) version="v${version}" ;;
  esac
  release_url="https://github.com/${repository}/releases/download/${version}"
fi

archive="kafkaesque_${os}_${arch}.tar.gz"
tmp_dir="$(mktemp -d)"

cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup 0 1 2 15

echo "Downloading Kafkaesque ${version} for ${os}/${arch}..."
curl -fsSL "${release_url}/${archive}" -o "${tmp_dir}/${archive}"
curl -fsSL "${release_url}/checksums.txt" -o "${tmp_dir}/checksums.txt"

expected_checksum="$(awk -v file="${archive}" '$2 == file { print $1 }' "${tmp_dir}/checksums.txt")"
if [ -z "${expected_checksum}" ]; then
  echo "No checksum found for ${archive}" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  actual_checksum="$(sha256sum "${tmp_dir}/${archive}" | awk '{ print $1 }')"
elif command -v shasum >/dev/null 2>&1; then
  actual_checksum="$(shasum -a 256 "${tmp_dir}/${archive}" | awk '{ print $1 }')"
else
  echo "A SHA-256 checksum tool is required" >&2
  exit 1
fi

if [ "${actual_checksum}" != "${expected_checksum}" ]; then
  echo "Checksum verification failed for ${archive}" >&2
  exit 1
fi

tar -xzf "${tmp_dir}/${archive}" -C "${tmp_dir}"
mkdir -p "${install_dir}"
install -m 0755 "${tmp_dir}/kafkaesque" "${install_dir}/kafkaesque"
"${install_dir}/kafkaesque" --help >/dev/null

echo "Kafkaesque installed at ${install_dir}/kafkaesque"
case ":${PATH}:" in
  *":${install_dir}:"*) ;;
  *) echo "Add ${install_dir} to PATH: export PATH=\"${install_dir}:\$PATH\"" ;;
esac
