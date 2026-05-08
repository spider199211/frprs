use frprs::transports::{kcp, quic};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{timeout, Duration},
};

fn loopback_any_port() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_bi_stream_round_trips_bytes() {
    let server = quic::QuicServerEndpoint::bind(loopback_any_port()).unwrap();
    let server_addr = server.local_addr().unwrap();
    let server_cert = server.certificate_der();

    let server_task = tokio::spawn(async move {
        let mut stream = server.accept_bi().await.unwrap();
        let mut request = [0_u8; 10];
        stream.recv.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"hello quic");
        stream.send.write_all(b"quic ok").await.unwrap();
        stream.send.finish().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let (_endpoint, mut client_stream) = quic::connect_bi(server_addr, server_cert).await.unwrap();
    client_stream.send.write_all(b"hello quic").await.unwrap();
    let mut response = [0_u8; 7];
    timeout(
        Duration::from_secs(5),
        client_stream.recv.read_exact(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&response, b"quic ok");

    server_task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kcp_stream_round_trips_bytes() {
    let mut listener = kcp::bind(loopback_any_port()).await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 9];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"hello kcp");
        stream.write_all(b"kcp ok").await.unwrap();
        stream.flush().await.unwrap();
    });

    let mut stream = kcp::connect(server_addr).await.unwrap();
    stream.write_all(b"hello kcp").await.unwrap();
    stream.flush().await.unwrap();
    let mut response = [0_u8; 6];
    timeout(Duration::from_secs(10), stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&response, b"kcp ok");

    server_task.await.unwrap();
}
