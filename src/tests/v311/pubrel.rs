//! PUBREL packet tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 3.6 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "pubrel".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.6.1-1
        make_test(
            "MQTT-3.6.1-1",
            "PUBREL fixed header flags MUST be 0,0,1,0",
            test_mqtt_3_6_1_1,
        ),
    ]
}

/// MQTT-3.6.1-1: PUBREL fixed header flags MUST be 0,0,1,0
/// The Server MUST treat any other value as malformed and close the Network Connection
fn test_mqtt_3_6_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3611{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if buf[0] != 0x20 || buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse("Expected successful CONNACK".to_string()));
        }

        // First, we need to initiate a QoS 2 flow to have a valid packet ID
        // But to test PUBREL flags, we can just send a malformed PUBREL directly
        // and see if the server closes the connection

        // Send PUBREL with INVALID flags (0x60 instead of 0x62)
        // PUBREL should be 0x62 (0110 0010)
        let pubrel_invalid = [
            0x60, // PUBREL with INVALID flags (should be 0x62)
            0x02, // Remaining length
            0x00, 0x01, // Packet ID
        ];
        stream.write_all(&pubrel_invalid).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let mut buf = [0u8; 16];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for invalid PUBREL flags".to_string()
            )),
            Ok(Ok(_)) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for invalid PUBREL flags".to_string()
            )),
        }
    })
}

fn build_connect_packet(client_id: &str, keep_alive: u16) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let client_id_len = client_id_bytes.len();
    let remaining_length = 6 + 1 + 1 + 2 + 2 + client_id_len;

    let mut packet = Vec::with_capacity(2 + remaining_length);
    packet.push(0x10);
    packet.push(remaining_length as u8);
    packet.push(0x00);
    packet.push(0x04);
    packet.extend_from_slice(b"MQTT");
    packet.push(4);
    packet.push(0x02);
    packet.push((keep_alive >> 8) as u8);
    packet.push((keep_alive & 0xFF) as u8);
    packet.push((client_id_len >> 8) as u8);
    packet.push((client_id_len & 0xFF) as u8);
    packet.extend_from_slice(client_id_bytes);

    packet
}
