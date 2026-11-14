set -eu
mkdir -p /etc/systemd/journald.conf.d /var/log/journal
cat > /etc/systemd/journald.conf.d/70-request-retention.conf <<'EOF'
[Journal]
Storage=persistent
# Five-minute margin accounts for rotation and the one-minute cleanup timer.
MaxRetentionSec=604500s
MaxFileSec=60s
SystemMaxUse=512M
RuntimeMaxUse=64M
ForwardToSyslog=no
ForwardToKMsg=no
ForwardToConsole=no
ForwardToWall=no
EOF
cat > /etc/systemd/system/request-log-expiry.service <<'EOF'
[Unit]
Description=Rotate and expire request/error journals (maximum seven days)
[Service]
Type=oneshot
ExecStart=/usr/bin/journalctl --rotate
ExecStart=/usr/bin/journalctl --vacuum-time=604500s
EOF
cat > /etc/systemd/system/request-log-expiry.timer <<'EOF'
[Unit]
Description=Expire request/error logs every minute, including idle services
[Timer]
OnBootSec=15s
OnCalendar=*-*-* *:*:00
AccuracySec=1s
Persistent=true
[Install]
WantedBy=timers.target
EOF
systemctl restart systemd-journald
journalctl --flush
systemctl daemon-reload
systemctl enable --now request-log-expiry.timer
systemctl start request-log-expiry.service
systemctl is-active systemd-journald request-log-expiry.timer
