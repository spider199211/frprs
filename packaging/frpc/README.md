# frpc client package

Run locally:

```powershell
.\run-frpc.ps1
```

Equivalent command:

```powershell
.\frpc.exe -c .\conf\frpc.toml
```

Before connecting to a real server, edit `conf/frpc.toml`:

```toml
serverAddr = "YOUR_SERVER_IP_OR_DOMAIN"
serverPort = 7000
```

The packaged config currently uses the local development value `127.0.0.1`.

Before public deployment, enable `[auth].token` in both `frps.toml` and `frpc.toml`.
