use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "mqtt-conformance")]
#[command(author = "MQTT Conformance Team")]
#[command(version = "0.1.0")]
#[command(about = "MQTT Broker Conformance Testing Tool", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run conformance tests against an MQTT broker
    Run {
        /// MQTT broker hostname
        #[arg(short = 'H', long, default_value = "localhost")]
        host: String,

        /// MQTT broker port
        #[arg(short, long, default_value_t = 1883)]
        port: u16,

        /// MQTT protocol version to test
        #[arg(short, long, value_enum, default_value_t = MqttVersion::V311)]
        version: MqttVersion,

        /// Filter tests by section name (snake_case, e.g., "connect", "publish")
        #[arg(short, long)]
        section: Option<String>,

        /// Filter by specific normative ID (e.g., "MQTT-3.1.0-1")
        #[arg(short, long)]
        normative: Option<String>,

        /// Username for broker authentication
        #[arg(short, long)]
        username: Option<String>,

        /// Password for broker authentication
        #[arg(short = 'P', long)]
        password: Option<String>,

        /// Test timeout in seconds
        #[arg(short, long, default_value_t = 10)]
        timeout: u64,

        /// Enable verbose output
        #[arg(short = 'V', long)]
        verbose: bool,

        /// Run tests in parallel
        #[arg(short = 'j', long)]
        parallel: bool,
    },

    /// List available tests for a given MQTT version
    List {
        /// MQTT protocol version
        #[arg(short, long, value_enum, default_value_t = MqttVersion::V311)]
        version: MqttVersion,

        /// Filter by section name
        #[arg(short, long)]
        section: Option<String>,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum MqttVersion {
    /// MQTT 3.1.1
    #[value(name = "3.1.1", aliases = ["3", "311"])]
    V311,
    /// MQTT 5.0
    #[value(name = "5", alias = "5.0")]
    V5,
}
