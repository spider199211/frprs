use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(rename = "bindAddr", default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(rename = "bindPort", default = "default_bind_port")]
    pub bind_port: u16,
    #[serde(rename = "proxyBindAddr", default = "default_proxy_bind_addr")]
    pub proxy_bind_addr: String,
    #[serde(rename = "vhostHTTPPort", default)]
    pub vhost_http_port: u16,
    #[serde(rename = "vhostHTTPSPort", default)]
    pub vhost_https_port: u16,
    #[serde(rename = "dashboardAddr", default = "default_dashboard_addr")]
    pub dashboard_addr: String,
    #[serde(rename = "dashboardPort", default)]
    pub dashboard_port: u16,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub plugins: ServerPluginConfig,
    #[serde(default)]
    pub transport: TransportConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClientConfig {
    #[serde(rename = "serverAddr")]
    pub server_addr: String,
    #[serde(rename = "serverPort")]
    pub server_port: u16,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(rename = "poolCount", default)]
    pub pool_count: usize,
    #[serde(default)]
    pub transport: TransportConfig,
    #[serde(default)]
    pub proxies: Vec<ProxyConfig>,
    #[serde(default)]
    pub visitors: Vec<VisitorConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AuthConfig {
    pub token: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ServerPluginConfig {
    #[serde(rename = "loginURL", default)]
    pub login_url: Option<String>,
    #[serde(rename = "newProxyURL", default)]
    pub new_proxy_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransportConfig {
    #[serde(default = "default_transport_protocol")]
    pub protocol: TransportProtocol,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            protocol: TransportProtocol::Tcp,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportProtocol {
    Tcp,
    Quic,
    Kcp,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProxyConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub proxy_type: ProxyType,
    #[serde(rename = "localIP", default = "default_local_ip")]
    pub local_ip: String,
    #[serde(rename = "localPort")]
    pub local_port: u16,
    #[serde(rename = "remotePort", default)]
    pub remote_port: u16,
    #[serde(rename = "customDomains", default)]
    pub custom_domains: Vec<String>,
    #[serde(rename = "bandwidthLimit", default)]
    pub bandwidth_limit: Option<String>,
    #[serde(default)]
    pub sk: Option<String>,
    #[serde(rename = "healthCheck", default)]
    pub health_check: Option<HealthCheckConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VisitorConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub visitor_type: ProxyType,
    #[serde(rename = "serverName")]
    pub server_name: String,
    #[serde(rename = "bindAddr", default = "default_visitor_bind_addr")]
    pub bind_addr: String,
    #[serde(rename = "bindPort")]
    pub bind_port: u16,
    #[serde(default)]
    pub sk: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    #[serde(rename = "type")]
    pub check_type: HealthCheckType,
    #[serde(
        rename = "intervalSeconds",
        default = "default_health_interval_seconds"
    )]
    pub interval_seconds: u64,
    #[serde(rename = "timeoutSeconds", default = "default_health_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(rename = "maxFailed", default = "default_health_max_failed")]
    pub max_failed: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthCheckType {
    Tcp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProxyType {
    Tcp,
    Udp,
    Http,
    Https,
    Stcp,
    Sudp,
    Xtcp,
    Tcpmux,
}

impl ProxyType {
    pub fn as_str(self) -> &'static str {
        match self {
            ProxyType::Tcp => "tcp",
            ProxyType::Udp => "udp",
            ProxyType::Http => "http",
            ProxyType::Https => "https",
            ProxyType::Stcp => "stcp",
            ProxyType::Sudp => "sudp",
            ProxyType::Xtcp => "xtcp",
            ProxyType::Tcpmux => "tcpmux",
        }
    }
}

impl ServerConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("read server config {}", path.display()))?;
        let cfg: Self = toml::from_str(&content)
            .with_context(|| format!("parse server config {}", path.display()))?;
        ensure_supported_transport(cfg.transport.protocol)?;
        Ok(cfg)
    }

    pub fn control_addr(&self) -> String {
        format!("{}:{}", self.bind_addr, self.bind_port)
    }
}

impl ClientConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("read client config {}", path.display()))?;
        let cfg: Self = toml::from_str(&content)
            .with_context(|| format!("parse client config {}", path.display()))?;
        ensure_supported_transport(cfg.transport.protocol)?;
        Ok(cfg)
    }

    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.server_addr, self.server_port)
    }
}

fn default_bind_addr() -> String {
    "0.0.0.0".to_string()
}

fn default_dashboard_addr() -> String {
    "127.0.0.1".to_string()
}

fn default_proxy_bind_addr() -> String {
    "0.0.0.0".to_string()
}

fn default_transport_protocol() -> TransportProtocol {
    TransportProtocol::Tcp
}

fn ensure_supported_transport(protocol: TransportProtocol) -> Result<()> {
    match protocol {
        TransportProtocol::Tcp | TransportProtocol::Quic | TransportProtocol::Kcp => Ok(()),
    }
}

fn default_bind_port() -> u16 {
    7000
}

fn default_local_ip() -> String {
    "127.0.0.1".to_string()
}

fn default_visitor_bind_addr() -> String {
    "127.0.0.1".to_string()
}

fn default_health_interval_seconds() -> u64 {
    10
}

fn default_health_timeout_seconds() -> u64 {
    3
}

fn default_health_max_failed() -> u32 {
    3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_frp_style_client_config() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            serverAddr = "127.0.0.1"
            serverPort = 7000

            [[proxies]]
            name = "ssh"
            type = "tcp"
            localIP = "127.0.0.1"
            localPort = 22
            remotePort = 6000
            bandwidthLimit = "1MB"
            sk = "private"

            [proxies.healthCheck]
            type = "tcp"
            intervalSeconds = 5

            [[visitors]]
            name = "ssh-visitor"
            type = "stcp"
            serverName = "ssh"
            bindPort = 16000
            sk = "private"
            "#,
        )
        .unwrap();

        assert_eq!(cfg.server_addr(), "127.0.0.1:7000");
        assert_eq!(cfg.proxies[0].proxy_type, ProxyType::Tcp);
        assert_eq!(cfg.proxies[0].bandwidth_limit.as_deref(), Some("1MB"));
        assert_eq!(cfg.proxies[0].sk.as_deref(), Some("private"));
        assert!(cfg.proxies[0].health_check.is_some());
        assert_eq!(cfg.visitors[0].server_name, "ssh");
        assert_eq!(cfg.visitors[0].bind_addr, "127.0.0.1");
    }
}
