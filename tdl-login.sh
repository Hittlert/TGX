#!/bin/sh
set -eu

PROJECT_DIR="/volume2/docker/telegram_media_downloader_us"
IMAGE="telegram-tdl-daemon:v0.20.3-daemon.4"
PROXY="http://192.168.79.22:6152"
NAMESPACE="production"
STATE_DIR="${PROJECT_DIR}/tdl-state"
BACKUP_DIR="${STATE_DIR}/backups"
SESSION_FILE="${STATE_DIR}/${NAMESPACE}"

umask 077
sudo mkdir -p "${STATE_DIR}" "${BACKUP_DIR}"
sudo chmod 700 "${STATE_DIR}" "${BACKUP_DIR}"

sudo docker run --rm -it \
  -v "${STATE_DIR}:/data" \
  "${IMAGE}" \
  --storage type=bolt,path=/data \
  --ns "${NAMESPACE}" \
  --proxy "${PROXY}" \
  --reconnect-timeout 0 \
  login -T qr

if ! sudo test -s "${SESSION_FILE}"; then
  echo "Login returned without creating ${SESSION_FILE}" >&2
  exit 1
fi

timestamp="$(date +%Y%m%d-%H%M%S)"
backup_file="${BACKUP_DIR}/${NAMESPACE}.${timestamp}.bolt"
sudo chmod 600 "${SESSION_FILE}"
sudo cp -p "${SESSION_FILE}" "${backup_file}"
sudo chmod 400 "${backup_file}"
sudo sha256sum "${backup_file}" | sudo tee "${backup_file}.sha256" >/dev/null
sudo chmod 400 "${backup_file}.sha256"

echo "Session persisted: ${SESSION_FILE}"
echo "Offline backup: ${backup_file}"
