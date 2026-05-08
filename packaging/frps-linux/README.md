# frps CentOS x86-64 package

This package contains a Linux x86-64 `frps` server binary built for `x86_64-unknown-linux-musl`.

## Quick start

```bash
tar -xzf frprs-frps-centos-x86_64.tar.gz
cd frprs-frps-centos-x86_64
chmod +x frps run-frps.sh
./run-frps.sh
```

Default config:

- Control port: `7000`
- HTTP vhost: disabled
- HTTPS vhost: disabled
- Auth token: `change-me`

Edit `conf/frps.toml` before public deployment and open the required firewall/security-group ports.

## systemd

Install to `/opt/frprs-frps`:

```bash
sudo mkdir -p /opt/frprs-frps
sudo cp -r ./* /opt/frprs-frps/
sudo cp frprs-frps.service /etc/systemd/system/frprs-frps.service
sudo systemctl daemon-reload
sudo systemctl enable --now frprs-frps
sudo systemctl status frprs-frps
```

Logs:

```bash
journalctl -u frprs-frps -f
```
