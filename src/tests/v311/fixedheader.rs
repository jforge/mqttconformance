//! Fixed Header tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 2.2 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "fixedheader".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-2.2.2-1
        make_test(
            "MQTT-2.2.2-1",
            "Reserved flag bits MUST be set to specified values",
            test_mqtt_2_2_2_1,
        ),
        // MQTT-2.2.2-2
        make_test(
            "MQTT-2.2.2-2",
            "Server MUST close connection on invalid flags",
            test_mqtt_2_2_2_2,
        ),
    ]
}

/// MQTT-2.2.2-1: Reserved flag bits MUST be set to specified values
/// For PUBLISH, flags encode DUP, QoS, RETAIN
/// For other packets, flags must be specific values (e.g., SUBSCRIBE must be 0x02)
fn test_mqtt_2_2_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // First establish a valid connection
        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test2221{}", &uuid::Uuid::new_v4().to_string()[..8]);
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

        // Send SUBSCRIBE with correct reserved flags (0x82 = 0x80 | 0x02)
        // The lower 4 bits MUST be 0010 for SUBSCRIBE
        let subscribe_correct = [
            0x82, // SUBSCRIBE with correct flags
            0x09, // Remaining length (2 + 2 + 4 + 1 = 9)
            0x00, 0x01, // Packet ID
            0x00, 0x04, // Topic length
            b't', b'e', b's', b't', // Topic
            0x00, // QoS 0
        ];
        stream.write_all(&subscribe_correct).await
            .map_err(ConformanceError::Io)?;

        // Should get SUBACK
        let mut buf = [0u8; 5];
        match tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await {
            Ok(Ok(n)) if n >= 4 && buf[0] == 0x90 => Ok(()), // SUBACK received
            Ok(Ok(0)) => Err(ConformanceError::ProtocolViolation(
                "Connection closed unexpectedly".to_string()
            )),
            Ok(Err(e)) => Err(ConformanceError::Io(e)),
            Err(_) => Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            _ => Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string())),
        }
    })
}

/// MQTT-2.2.2-2: Server MUST close connection on invalid flags
fn test_mqtt_2_2_2_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test2222{}", &uuid::Uuid::new_v4().to_string()[..8]);
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

        // Send SUBSCRIBE with INVALID reserved flags (0x83 instead of 0x82)
        // The lower 4 bits should be 0010 but we're sending 0011
        let subscribe_invalid = [
            0x83, // SUBSCRIBE with INVALID flags (should be 0x82)
            0x09, // Remaining length (2 + 2 + 4 + 1 = 9)
            0x00, 0x01, // Packet ID
            0x00, 0x04, // Topic length
            b't', b'e', b's', b't', // Topic
            0x00, // QoS 0
        ];
        stream.write_all(&subscribe_invalid).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let mut buf = [0u8; 16];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for invalid flags".to_string()
            )),
            Ok(Ok(_)) => {
                // Check if it's a disconnect or just ignored
                Err(ConformanceError::ProtocolViolation(
                    "Server did not close connection for invalid flags".to_string()
                ))
            }
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
