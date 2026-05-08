# frps server package

Run on the server:

```powershell
.\run-frps.ps1
```

Equivalent command:

```powershell
.\frps.exe -c .\conf\frps.toml
```

Default ports in the packaged config:

- Control: `7000`
- HTTP vhost: `8080`
- HTTPS vhost: `8443`

Open these ports on the server firewall/security group when needed.

Before public deployment, enable `[auth].token` in both `frps.toml` and `frpc.toml`.
