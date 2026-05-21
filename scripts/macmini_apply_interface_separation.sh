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

WIRED_IF="${WIRED_IF:-$(/usr/sbin/networksetup -listallhardwareports | awk '
  $0 == "Hardware Port: Ethernet" { getline; print $2; exit }
')}"
WIFI_IF="${WIFI_IF:-$(/usr/sbin/networksetup -listallhardwareports | awk '
  $0 == "Hardware Port: Wi-Fi" { getline; print $2; exit }
')}"

if [[ -z "${WIRED_IF}" || -z "${WIFI_IF}" ]]; then
  echo "Could not determine Ethernet/Wi-Fi interfaces."
  exit 1
fi

cp "${PF_CONF}" "${PF_CONF}.bak.${BACKUP_SUFFIX}"
if [[ -f "${ANCHOR_FILE}" ]]; then
  cp "${ANCHOR_FILE}" "${ANCHOR_FILE}.bak.${BACKUP_SUFFIX}"
fi
if [[ -f "${LAUNCHD_PLIST}" ]]; then
  cp "${LAUNCHD_PLIST}" "${LAUNCHD_PLIST}.bak.${BACKUP_SUFFIX}"
fi

cat > "${ANCHOR_FILE}" <<EOF
# Managed by Codex for Mac mini interface separation.
# Wi-Fi is reserved for internal management.
# Ethernet is reserved for published server traffic.
block drop in quick on ${WIRED_IF} proto tcp from any to any port 22
block drop in quick on ${WIFI_IF} proto tcp from any to any port { 80, 443, 18000 }
EOF

if ! grep -Fq "anchor \"${ANCHOR_NAME}\"" "${PF_CONF}"; then
  {
    echo
    echo "# Managed by Codex for Mac mini interface separation."
    echo "anchor \"${ANCHOR_NAME}\""
    echo "load anchor \"${ANCHOR_NAME}\" from \"${ANCHOR_FILE}\""
  } >> "${PF_CONF}"
fi

cat > "${LAUNCHD_PLIST}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${ANCHOR_NAME}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/sbin/pfctl</string>
    <string>-e</string>
    <string>-f</string>
    <string>/etc/pf.conf</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>
EOF

chmod 644 "${LAUNCHD_PLIST}"
chown root:wheel "${LAUNCHD_PLIST}"

/sbin/pfctl -nf "${PF_CONF}"
/sbin/pfctl -e >/dev/null 2>&1 || true
/sbin/pfctl -f "${PF_CONF}" >/dev/null
/bin/launchctl bootout system/${ANCHOR_NAME} >/dev/null 2>&1 || true
/bin/launchctl bootstrap system "${LAUNCHD_PLIST}" >/dev/null 2>&1 || true
/bin/launchctl enable system/${ANCHOR_NAME} >/dev/null 2>&1 || true
/bin/launchctl kickstart -k system/${ANCHOR_NAME} >/dev/null 2>&1 || true

echo "Applied interface separation."
echo "Ethernet interface: ${WIRED_IF}"
echo "Wi-Fi interface: ${WIFI_IF}"
echo
echo "Loaded PF rules:"
/sbin/pfctl -a "${ANCHOR_NAME}" -sr
