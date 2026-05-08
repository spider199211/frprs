use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Message {
    Login {
        version: String,
        hostname: String,
        os: String,
        arch: String,
        run_id: String,
        token: Option<String>,
        pool_count: usize,
    },
    LoginResp {
        version: String,
        run_id: String,
        error: String,
    },
    NewProxy {
        proxy_name: String,
        proxy_type: String,
        remote_port: u16,
        custom_domains: Vec<String>,
        bandwidth_limit: Option<String>,
        sk: Option<String>,
    },
    NewProxyResp {
        proxy_name: String,
        remote_addr: String,
        error: String,
    },
    NewVisitorConn {
        proxy_name: String,
        sk: Option<String>,
        token: Option<String>,
    },
    NewVisitorConnResp {
        proxy_name: String,
        error: String,
    },
    CloseProxy {
        proxy_name: String,
    },
    NewWorkConn {
        run_id: String,
        token: Option<String>,
    },
    ReqWorkConn,
    StartWorkConn {
        proxy_name: String,
        src_addr: String,
        dst_addr: String,
        error: String,
    },
    UdpPacket {
        proxy_name: String,
        content: Vec<u8>,
        visitor_addr: String,
    },
    NatHoleRegister {
        transaction_id: String,
        proxy_name: String,
        role: String,
        local_addrs: Vec<String>,
    },
    NatHoleResp {
        transaction_id: String,
        proxy_name: String,
        role: String,
        observed_addr: String,
        peer_observed_addr: String,
        peer_local_addrs: Vec<String>,
        waiting: bool,
        error: String,
    },
    Ping {
        token: Option<String>,
    },
    Pong {
        error: String,
    },
}

pub async fn read_msg<R>(reader: &mut R) -> Result<Message>
where
    R: AsyncRead + Unpin,
{
    let len = reader.read_u32().await.context("read frame length")?;
    if len > MAX_FRAME_SIZE {
        bail!("frame too large: {len} bytes");
    }

    let mut buf = vec![0_u8; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .context("read frame payload")?;
    serde_json::from_slice(&buf).context("decode protocol message")
}

pub async fn write_msg<W>(writer: &mut W, msg: &Message) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let buf = serde_json::to_vec(msg).context("encode protocol message")?;
    if buf.len() > MAX_FRAME_SIZE as usize {
        bail!("frame too large: {} bytes", buf.len());
    }
    writer
        .write_u32(buf.len() as u32)
        .await
        .context("write frame length")?;
    writer
        .write_all(&buf)
        .await
        .context("write frame payload")?;
    writer.flush().await.context("flush protocol message")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn message_round_trips() {
        let (mut client, mut server) = duplex(1024);
        let sent = Message::ReqWorkConn;

        write_msg(&mut client, &sent).await.unwrap();
        let got = read_msg(&mut server).await.unwrap();

        assert!(matches!(got, Message::ReqWorkConn));
    }
}
