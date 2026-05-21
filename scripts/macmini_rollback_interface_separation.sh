#!/bin/zsh
set -euo pipefail

ANCHOR_NAME="com.jun.server-interface-separation"
ANCHOR_FILE="/etc/pf.anchors/${ANCHOR_NAME}"
PF_CONF="/etc/pf.conf"
LAUNCHD_PLIST="/Library/LaunchDaemons/${ANCHOR_NAME}.plist"
BACKUP_SUFFIX="$(date +%Y%m%d-%H%M%S)"

if [[ ${EUID} -ne 0 ]]; then
  echo "Run with sudo: sudo /bin/zsh $0"
  exit 1
fi

cp "${PF_CONF}" "${PF_CONF}.bak.${BACKUP_SUFFIX}"
if [[ -f "${ANCHOR_FILE}" ]]; then
  cp "${ANCHOR_FILE}" "${ANCHOR_FILE}.bak.${BACKUP_SUFFIX}"
fi
if [[ -f "${LAUNCHD_PLIST}" ]]; then
  cp "${LAUNCHD_PLIST}" "${LAUNCHD_PLIST}.bak.${BACKUP_SUFFIX}"
fi

TMP_FILE="$(mktemp)"
awk -v anchor_line="anchor \"${ANCHOR_NAME}\"" \
    -v load_line="load anchor \"${ANCHOR_NAME}\" from \"${ANCHOR_FILE}\"" '
  $0 == anchor_line { next }
  $0 == load_line { next }
  $0 == "# Managed by Codex for Mac mini interface separation." { next }
  { print }
' "${PF_CONF}" > "${TMP_FILE}"
mv "${TMP_FILE}" "${PF_CONF}"

rm -f "${ANCHOR_FILE}"
/bin/launchctl bootout system/${ANCHOR_NAME} >/dev/null 2>&1 || true
rm -f "${LAUNCHD_PLIST}"

/sbin/pfctl -nf "${PF_CONF}"
/sbin/pfctl -f "${PF_CONF}" >/dev/null

echo "Rolled back interface separation rules."
