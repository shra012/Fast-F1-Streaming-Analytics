#!/usr/bin/env bash
#
# Install Neo4j CLI tools (cypher-shell, neo4j-admin) on an EMR master node.
# The script downloads the requested Neo4j distribution, installs dependencies,
# and exposes the client binaries without configuring or running a Neo4j server.

set -euo pipefail

NEO4J_VERSION="${NEO4J_VERSION:-5.23.0}"
NEO4J_EDITION="${NEO4J_EDITION:-community}" # community or enterprise (requires license)

NEO4J_BASE_URL="https://dist.neo4j.org"
NEO4J_TARBALL="neo4j-${NEO4J_EDITION}-${NEO4J_VERSION}-unix.tar.gz"
INSTALL_BASE="/opt/neo4j"
NEO4J_HOME="${INSTALL_BASE}/neo4j-${NEO4J_EDITION}-${NEO4J_VERSION}"
NEO4J_CURRENT="${INSTALL_BASE}/current"
TMP_TARBALL="/tmp/${NEO4J_TARBALL}"

usage() {
  cat <<EOF
Install Neo4j CLI tools on an EMR node.

Options:
  --version <semver>        Neo4j version (default: ${NEO4J_VERSION})
  --edition <community|enterprise>
                            Neo4j edition (default: ${NEO4J_EDITION})
  -h, --help                Show this help message

Environment variables:
  NEO4J_VERSION, NEO4J_EDITION can be used instead of flags.

Example:
  ./install_neo4j_on_emr.sh --version 5.23.0 --edition community
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      NEO4J_VERSION="$2"
      shift 2
      ;;
    --edition)
      NEO4J_EDITION="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

NEO4J_TARBALL="neo4j-${NEO4J_EDITION}-${NEO4J_VERSION}-unix.tar.gz"
TMP_TARBALL="/tmp/${NEO4J_TARBALL}"
NEO4J_HOME="${INSTALL_BASE}/neo4j-${NEO4J_EDITION}-${NEO4J_VERSION}"

log_info() {
  echo -e "[INFO] $*"
}

log_warn() {
  echo -e "[WARN] $*" >&2
}

log_error() {
  echo -e "[ERROR] $*" >&2
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "Required command '$1' not found"
    exit 1
  fi
}

SUDO="sudo"
if [[ $EUID -eq 0 ]]; then
  SUDO=""
elif ! sudo -n true 2>/dev/null; then
  log_error "This script requires sudo access. Re-run with sudo."
  exit 1
fi

require_cmd tar

if ! command -v yum >/dev/null 2>&1; then
  log_error "This script targets Amazon Linux/EMR nodes (yum not found)."
  exit 1
fi

install_dependencies() {
  log_info "Installing prerequisites (Java 17, wget)..."
  # Note: curl is already installed on EMR (curl-minimal), so we don't install it
  $SUDO yum install -y -q java-17-amazon-corretto-headless wget >/dev/null 2>&1 || true
  log_info "Java 17 and wget installed"
}

download_neo4j() {
  if [[ -f "${TMP_TARBALL}" ]]; then
    log_info "Reusing downloaded tarball ${TMP_TARBALL}"
    return
  fi

  local url="${NEO4J_BASE_URL}/${NEO4J_TARBALL}"
  log_info "Downloading Neo4j ${NEO4J_VERSION} (${NEO4J_EDITION})..."
  curl -L --fail -o "${TMP_TARBALL}" "${url}"
}

extract_neo4j() {
  $SUDO mkdir -p "${INSTALL_BASE}"

  if [[ -d "${NEO4J_HOME}" ]]; then
    log_info "Neo4j ${NEO4J_VERSION} already extracted at ${NEO4J_HOME}"
  else
    log_info "Extracting Neo4j to ${NEO4J_HOME}..."
    $SUDO tar -xzf "${TMP_TARBALL}" -C "${INSTALL_BASE}"
  fi

  log_info "Updating ${NEO4J_CURRENT} symlink..."
  $SUDO ln -sfn "${NEO4J_HOME}" "${NEO4J_CURRENT}"
}

install_cli_tools() {
  log_info "Installing cypher-shell for Neo4j Aura connectivity..."
  
  # Find Java 17 installation
  JAVA_17_HOME="/usr/lib/jvm/java-17-amazon-corretto.x86_64"
  if [[ ! -d "$JAVA_17_HOME" ]]; then
    log_warn "Java 17 not found at $JAVA_17_HOME, checking alternatives..."
    JAVA_17_HOME=$(ls -d /usr/lib/jvm/java-17-* 2>/dev/null | head -1 || echo "")
  fi
  
  if [[ -z "$JAVA_17_HOME" ]]; then
    log_error "Java 17 not found. Cannot configure cypher-shell."
    return 1
  fi
  
  # Backup and create wrapper script for cypher-shell that uses Java 17
  if [[ -f "${NEO4J_CURRENT}/bin/cypher-shell" ]]; then
    # Backup the original if not already backed up
    if [[ ! -f "${NEO4J_CURRENT}/bin/cypher-shell.orig" ]]; then
      log_info "Backing up original cypher-shell..."
      $SUDO cp "${NEO4J_CURRENT}/bin/cypher-shell" "${NEO4J_CURRENT}/bin/cypher-shell.orig"
    fi
    
    log_info "Creating cypher-shell wrapper with Java 17..."
    $SUDO tee /usr/local/bin/cypher-shell > /dev/null <<EOF
#!/bin/bash
export JAVA_HOME="${JAVA_17_HOME}"
export PATH="${JAVA_17_HOME}/bin:\$PATH"
exec "${NEO4J_CURRENT}/bin/cypher-shell.orig" "\$@"
EOF
    $SUDO chmod +x /usr/local/bin/cypher-shell
    log_info "cypher-shell available at: /usr/local/bin/cypher-shell (using Java 17)"
  else
    log_warn "cypher-shell not found in Neo4j installation"
  fi
  
  # Backup and create wrapper for neo4j-admin
  if [[ -f "${NEO4J_CURRENT}/bin/neo4j-admin" ]]; then
    # Backup the original if not already backed up
    if [[ ! -f "${NEO4J_CURRENT}/bin/neo4j-admin.orig" ]]; then
      log_info "Backing up original neo4j-admin..."
      $SUDO cp "${NEO4J_CURRENT}/bin/neo4j-admin" "${NEO4J_CURRENT}/bin/neo4j-admin.orig"
    fi
    
    log_info "Creating neo4j-admin wrapper with Java 17..."
    $SUDO tee /usr/local/bin/neo4j-admin > /dev/null <<EOF
#!/bin/bash
export JAVA_HOME="${JAVA_17_HOME}"
export PATH="${JAVA_17_HOME}/bin:\$PATH"
exec "${NEO4J_CURRENT}/bin/neo4j-admin.orig" "\$@"
EOF
    $SUDO chmod +x /usr/local/bin/neo4j-admin
    log_info "neo4j-admin available at: /usr/local/bin/neo4j-admin (using Java 17)"
  fi
}

print_summary() {
  cat <<EOF

Neo4j CLI installation complete!
  Version: ${NEO4J_VERSION} (${NEO4J_EDITION})
  Home:    ${NEO4J_CURRENT}

Usage examples:
  cypher-shell -a "\${NEO4J_URI}" -u "\${NEO4J_USERNAME}" -p "\${NEO4J_PASSWORD}"
  neo4j-admin database info

Next steps:
  1. Configure Neo4j credentials in neo4j/Neo4j.txt.
  2. Run ./setup_neo4j_schema.sh to create constraints and indexes via cypher-shell.
  3. Start the gold_stream Spark job with --write-to-neo4j enabled (uses cypher-shell for troubleshooting).
EOF
}

install_dependencies
download_neo4j
extract_neo4j
install_cli_tools
print_summary
