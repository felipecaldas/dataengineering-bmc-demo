#!/usr/bin/env bash
set -euo pipefail

readonly CLI_VERSION="1.10.0"
readonly CLI_ARCHIVE="databricks_cli_${CLI_VERSION}_linux_amd64.tar.gz"
readonly CLI_SHA256="70f4c0c817c6e5e6e1450cc8489cd09902ced6ce85343cd0a31c83222939ef53"
readonly CLI_URL="https://github.com/databricks/cli/releases/download/v${CLI_VERSION}/${CLI_ARCHIVE}"
readonly INSTALL_DIR="${DATABRICKS_CLI_INSTALL_DIR:-${HOME}/.local/bin}"
readonly INSTALL_PATH="${INSTALL_DIR}/databricks"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "This installer supports the demo host's Linux x86_64 platform only." >&2
  exit 1
fi

if [[ -x "${INSTALL_PATH}" ]]; then
  installed_version="$(${INSTALL_PATH} version 2>/dev/null || true)"
  if [[ "${installed_version}" == "Databricks CLI v${CLI_VERSION}" ]]; then
    echo "Databricks CLI v${CLI_VERSION} is already installed at ${INSTALL_PATH}."
    exit 0
  fi
  echo "Refusing to replace the existing ${INSTALL_PATH} (${installed_version:-unknown version})." >&2
  echo "Move it aside or choose another DATABRICKS_CLI_INSTALL_DIR." >&2
  exit 1
fi

temp_dir="$(mktemp -d -t retail-databricks-cli.XXXXXXXX)"
cleanup() {
  if [[ -n "${temp_dir}" && -d "${temp_dir}" && "${temp_dir}" == /tmp/retail-databricks-cli.* ]]; then
    rm -rf -- "${temp_dir}"
  fi
}
trap cleanup EXIT

env -u LD_LIBRARY_PATH curl -fsSL "${CLI_URL}" -o "${temp_dir}/${CLI_ARCHIVE}"
printf '%s  %s\n' "${CLI_SHA256}" "${temp_dir}/${CLI_ARCHIVE}" | sha256sum --check --status
tar -xzf "${temp_dir}/${CLI_ARCHIVE}" -C "${temp_dir}" databricks

mkdir -p "${INSTALL_DIR}"
install -m 0755 "${temp_dir}/databricks" "${INSTALL_PATH}"
"${INSTALL_PATH}" version

if [[ ":${PATH}:" != *":${INSTALL_DIR}:"* ]]; then
  echo "Add ${INSTALL_DIR} to PATH before invoking databricks from a new shell." >&2
fi
