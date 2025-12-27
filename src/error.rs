use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum ConformanceError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("MQTT client error: {0}")]
    MqttClient(#[from] rumqttc::ClientError),

    #[error("MQTT connection error: {0}")]
    MqttConnection(#[from] rumqttc::ConnectionError),

    #[error("Test timeout: {0}")]
    Timeout(String),

    #[error("Protocol violation: {0}")]
    ProtocolViolation(String),

    #[error("Unexpected response: {0}")]
    UnexpectedResponse(String),

    #[error("Test assertion failed: {0}")]
    AssertionFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, ConformanceError>;
