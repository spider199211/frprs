use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use uuid::Uuid;

const BINDING_REQUEST: u16 = 0x0001;
const BINDING_SUCCESS_RESPONSE: u16 = 0x0101;
const XOR_MAPPED_ADDRESS: u16 = 0x0020;
const MAGIC_COOKIE: u32 = 0x2112_A442;
const HEADER_LEN: usize = 20;

/// 创建一个 RFC 5389 STUN Binding Request，并返回用于校验响应的事务 ID。
pub fn new_binding_request() -> ([u8; HEADER_LEN], [u8; 12]) {
    let mut transaction_id = [0_u8; 12];
    transaction_id.copy_from_slice(&Uuid::new_v4().as_bytes()[..12]);

    let mut request = [0_u8; HEADER_LEN];
    request[..2].copy_from_slice(&BINDING_REQUEST.to_be_bytes());
    request[4..8].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
    request[8..20].copy_from_slice(&transaction_id);
    (request, transaction_id)
}

/// 根据合法 Binding Request 和服务端观察到的来源地址生成 XOR-MAPPED-ADDRESS 响应。
#[cfg(test)]
pub fn binding_response(request: &[u8], observed_addr: SocketAddr) -> Option<Vec<u8>> {
    if request.len() < HEADER_LEN
        || u16::from_be_bytes([request[0], request[1]]) != BINDING_REQUEST
        || u32::from_be_bytes([request[4], request[5], request[6], request[7]]) != MAGIC_COOKIE
    {
        return None;
    }
    let message_len = u16::from_be_bytes([request[2], request[3]]) as usize;
    if HEADER_LEN.checked_add(message_len)? > request.len() {
        return None;
    }

    let transaction_id: [u8; 12] = request[8..20].try_into().ok()?;
    let value_len = if observed_addr.is_ipv4() { 8 } else { 20 };
    let attribute_len = 4 + value_len;
    let mut response = vec![0_u8; HEADER_LEN + attribute_len];
    response[..2].copy_from_slice(&BINDING_SUCCESS_RESPONSE.to_be_bytes());
    response[2..4].copy_from_slice(&(attribute_len as u16).to_be_bytes());
    response[4..8].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
    response[8..20].copy_from_slice(&transaction_id);
    response[20..22].copy_from_slice(&XOR_MAPPED_ADDRESS.to_be_bytes());
    response[22..24].copy_from_slice(&(value_len as u16).to_be_bytes());
    response[25] = if observed_addr.is_ipv4() { 0x01 } else { 0x02 };
    response[26..28]
        .copy_from_slice(&(observed_addr.port() ^ (MAGIC_COOKIE >> 16) as u16).to_be_bytes());

    match observed_addr.ip() {
        IpAddr::V4(ip) => {
            let encoded = u32::from(ip) ^ MAGIC_COOKIE;
            response[28..32].copy_from_slice(&encoded.to_be_bytes());
        }
        IpAddr::V6(ip) => {
            let mut mask = [0_u8; 16];
            mask[..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
            mask[4..].copy_from_slice(&transaction_id);
            for (encoded, (octet, mask_octet)) in response[28..44]
                .iter_mut()
                .zip(ip.octets().into_iter().zip(mask))
            {
                *encoded = octet ^ mask_octet;
            }
        }
    }
    Some(response)
}

/// 解析并校验 STUN Binding Success Response 中的 XOR-MAPPED-ADDRESS。
pub fn parse_binding_response(
    response: &[u8],
    expected_transaction_id: &[u8; 12],
) -> Option<SocketAddr> {
    if response.len() < HEADER_LEN
        || u16::from_be_bytes([response[0], response[1]]) != BINDING_SUCCESS_RESPONSE
        || u32::from_be_bytes([response[4], response[5], response[6], response[7]]) != MAGIC_COOKIE
        || response[8..20] != expected_transaction_id[..]
    {
        return None;
    }
    let message_len = u16::from_be_bytes([response[2], response[3]]) as usize;
    let end = HEADER_LEN.checked_add(message_len)?;
    if end > response.len() {
        return None;
    }

    let mut offset = HEADER_LEN;
    while offset.checked_add(4)? <= end {
        let attribute_type = u16::from_be_bytes([response[offset], response[offset + 1]]);
        let value_len = u16::from_be_bytes([response[offset + 2], response[offset + 3]]) as usize;
        let value_start = offset + 4;
        let value_end = value_start.checked_add(value_len)?;
        if value_end > end {
            return None;
        }
        if attribute_type == XOR_MAPPED_ADDRESS {
            return parse_xor_mapped_address(
                &response[value_start..value_end],
                expected_transaction_id,
            );
        }
        offset = value_end.checked_add((4 - value_len % 4) % 4)?;
    }
    None
}

/// 解码 XOR-MAPPED-ADDRESS 属性值，支持 IPv4 和 IPv6 地址。
fn parse_xor_mapped_address(value: &[u8], transaction_id: &[u8; 12]) -> Option<SocketAddr> {
    if value.len() < 8 {
        return None;
    }
    let port = u16::from_be_bytes([value[2], value[3]]) ^ (MAGIC_COOKIE >> 16) as u16;
    match value[1] {
        0x01 if value.len() == 8 => {
            let encoded = u32::from_be_bytes(value[4..8].try_into().ok()?);
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(encoded ^ MAGIC_COOKIE)),
                port,
            ))
        }
        0x02 if value.len() == 20 => {
            let mut mask = [0_u8; 16];
            mask[..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
            mask[4..].copy_from_slice(transaction_id);
            let mut decoded = [0_u8; 16];
            for (decoded_octet, (encoded_octet, mask_octet)) in decoded
                .iter_mut()
                .zip(value[4..20].iter().copied().zip(mask))
            {
                *decoded_octet = encoded_octet ^ mask_octet;
            }
            Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(decoded)), port))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证 IPv4 Binding Request/Response 可往返还原服务端观察地址。
    #[test]
    fn binding_response_round_trips_ipv4_observed_addr() {
        let (request, transaction_id) = new_binding_request();
        let observed_addr = "198.51.100.8:42001".parse().unwrap();
        let response = binding_response(&request, observed_addr).unwrap();

        assert_eq!(
            parse_binding_response(&response, &transaction_id),
            Some(observed_addr)
        );
    }

    /// 验证 IPv6 XOR mask 同时使用 magic cookie 与事务 ID。
    #[test]
    fn binding_response_round_trips_ipv6_observed_addr() {
        let (request, transaction_id) = new_binding_request();
        let observed_addr = "[2001:db8::8]:42002".parse().unwrap();
        let response = binding_response(&request, observed_addr).unwrap();

        assert_eq!(
            parse_binding_response(&response, &transaction_id),
            Some(observed_addr)
        );
    }

    /// 验证响应事务 ID 不匹配时不会接受其他请求的映射地址。
    #[test]
    fn binding_response_rejects_wrong_transaction_id() {
        let (request, transaction_id) = new_binding_request();
        let response = binding_response(&request, "127.0.0.1:42003".parse().unwrap()).unwrap();
        let mut wrong_transaction_id = transaction_id;
        wrong_transaction_id[0] ^= 0xff;

        assert!(parse_binding_response(&response, &wrong_transaction_id).is_none());
    }
}
