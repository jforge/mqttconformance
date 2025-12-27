mod v311;
mod v5;

use crate::cli::MqttVersion;
use crate::error::ConformanceError;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub type TestFn = fn(TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>>;

#[derive(Clone)]
#[allow(dead_code)]
pub struct TestContext {
    pub host: String,
    pub port: u16,
    pub version: MqttVersion,
    pub username: Option<String>,
    pub password: Option<String>,
    pub timeout: Duration,
    pub verbose: bool,
}

pub struct NormativeTest {
    pub id: String,
    pub section: String,
    pub description: String,
    pub runner: TestFn,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct TestResult {
    pub id: String,
    pub description: String,
    pub passed: bool,
    pub skipped: bool,
    pub error: Option<String>,
    pub duration: Duration,
}

pub fn get_test_registry(version: MqttVersion) -> Vec<NormativeTest> {
    match version {
        MqttVersion::V311 => v311::get_tests(),
        MqttVersion::V5 => v5::get_tests(),
    }
}
