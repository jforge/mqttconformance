//! PINGREQ/PINGRESP tests for MQTT 5.0
//!
//! Tests normative statements from Section 3.12 of the MQTT 5.0 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "ping".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.12.4-1
        make_test(
            "MQTT-3.12.4-1",
            "Server MUST send PINGRESP in response to PINGREQ",
            test_mqtt_3_12_4_1,
        ),
    ]
}

/// MQTT-3.12.4-1: Server MUST send PINGRESP in response to PINGREQ
fn test_mqtt_3_12_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test31241v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send PINGREQ
        let pingreq = [0xC0, 0x00];
        stream.write_all(&pingreq).await
            .map_err(ConformanceError::Io)?;

        // Expect PINGRESP
        let mut pingresp = [0u8; 2];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut pingresp)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PINGRESP".to_string()))?
            .map_err(ConformanceError::Io)?;

        if pingresp[0] == 0xD0 && pingresp[1] == 0x00 {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(format!(
                "Expected PINGRESP (0xD0 0x00), got {:02X} {:02X}",
                pingresp[0], pingresp[1]
            )))
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
