//! Topic name and filter tests for MQTT 5.0
//!
//! Tests normative statements from Section 4.7 of the MQTT 5.0 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "topics".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-4.7.0-1 (carried over from v3.1.1)
        make_test(
            "MQTT-4.7.0-1",
            "Topic Names and Filters MUST be at least one character",
            test_mqtt_4_7_0_1,
        ),
    ]
}

/// MQTT-4.7.0-1: Topic Names and Filters MUST be at least one character
fn test_mqtt_4_7_0_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test4701v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with empty topic filter
        let mut subscribe = Vec::new();
        subscribe.push(0x82);
        subscribe.push(0x06); // Remaining length
        subscribe.push(0x00); // Packet ID MSB
        subscribe.push(0x01); // Packet ID LSB
        subscribe.push(0x00); // Properties
        subscribe.push(0x00); // Topic length MSB
        subscribe.push(0x00); // Topic length LSB (empty)
        subscribe.push(0x00); // Options

        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Ok(n)) if n > 0 && buf[0] == 0x90 => {
                // SUBACK - check for error reason code
                // In v5, 0x80+ are error codes
                if n >= 5 && buf[4] >= 0x80 {
                    Ok(()) // Error reason code
                } else {
                    Err(ConformanceError::ProtocolViolation(
                        "Server accepted empty topic filter".to_string()
                    ))
                }
            }
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not reject empty topic filter".to_string()
            )),
            _ => Ok(()),
        }
    })
}

fn build_connect_packet_v5(client_id: &str, keep_alive: u16) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();

    let mut var_header_payload = Vec::new();
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");
    var_header_payload.push(5);
    var_header_payload.push(0x02);
    var_header_payload.push((keep_alive >> 8) as u8);
    var_header_payload.push((keep_alive & 0xFF) as u8);
    var_header_payload.push(0x00);
    var_header_payload.push((client_id_bytes.len() >> 8) as u8);
    var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(client_id_bytes);

    let mut packet = Vec::new();
    packet.push(0x10);
    encode_remaining_length(&mut packet, var_header_payload.len());
    packet.extend_from_slice(&var_header_payload);

    packet
}

fn encode_remaining_length(packet: &mut Vec<u8>, mut length: usize) {
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if length == 0 {
            break;
        }
    }
}
