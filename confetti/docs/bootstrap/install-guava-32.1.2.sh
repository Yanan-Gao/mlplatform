#!/bin/bash
set -euo pipefail

GUAVA_VERSION="${GUAVA_VERSION:-32.1.2-jre}"
GUAVA_JAR="guava-${GUAVA_VERSION}.jar"
HADOOP_LIB_DIR="/usr/lib/hadoop/lib"
BACKUP_DIR="${HADOOP_LIB_DIR}/bootstrap-guava-backup"
DOWNLOAD_URL="https://repo1.maven.org/maven2/com/google/guava/guava/${GUAVA_VERSION}/${GUAVA_JAR}"

log() {
  echo "[bootstrap-guava] $1"
}

log "Ensuring Guava ${GUAVA_VERSION} is present in ${HADOOP_LIB_DIR}"

if [[ ! -d "${HADOOP_LIB_DIR}" ]]; then
  log "Creating ${HADOOP_LIB_DIR}"
  sudo mkdir -p "${HADOOP_LIB_DIR}"
fi

sudo mkdir -p "${BACKUP_DIR}"

# Move any preinstalled Guava jars out of the active Hadoop classpath so that the newer
# version we download below is picked first by Spark/YARN.
for jar in "${HADOOP_LIB_DIR}"/guava-*.jar; do
  if [[ -f "${jar}" && "${jar}" != "${HADOOP_LIB_DIR}/${GUAVA_JAR}" ]]; then
    log "Backing up $(basename "${jar}") to ${BACKUP_DIR}"
    sudo mv "${jar}" "${BACKUP_DIR}/"
  fi
done

if [[ ! -f "${HADOOP_LIB_DIR}/${GUAVA_JAR}" ]]; then
  TMP_JAR="/tmp/${GUAVA_JAR}"
  log "Downloading ${DOWNLOAD_URL}"
  curl -sfL "${DOWNLOAD_URL}" -o "${TMP_JAR}"
  sudo mv "${TMP_JAR}" "${HADOOP_LIB_DIR}/${GUAVA_JAR}"
  sudo chmod 644 "${HADOOP_LIB_DIR}/${GUAVA_JAR}"
  log "Installed ${GUAVA_JAR} into ${HADOOP_LIB_DIR}"
else
  log "${GUAVA_JAR} already present; skipping download"
fi

log "Guava bootstrap completed"
