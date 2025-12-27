# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Rust-based conformance testing tool that validates MQTT brokers against OASIS MQTT 3.1.1 and 5.0 specifications. Tests map directly to normative statements from the RFC (e.g., MQTT-3.1.0-1).

## Build Commands

```bash
# Build release binary (outputs to target/release/mqtt-conformance)
cargo build --release

# Run tests against local broker
cargo run --release -- run -H localhost -p 1883

# Run tests for MQTT 5.0
cargo run --release -- run -v 5

# Run specific section
cargo run --release -- run --section connect

# Run specific normative test
cargo run --release -- run --normative MQTT-3.1.0-1

# List all available tests
cargo run --release -- list

# List tests for specific section
cargo run --release -- list --section publish
```

## Local Testing Setup

Start a test broker with Docker:
```bash
docker compose up -d
```
This runs Mosquitto on localhost:1883 with anonymous connections enabled.

## Architecture

- **src/main.rs** - Async entry point (Tokio runtime), CLI dispatch, result formatting
- **src/cli.rs** - Clap-based argument parsing with `run` and `list` subcommands
- **src/runner.rs** - Test execution orchestration with timeout handling
- **src/error.rs** - Error types using thiserror
- **src/tests/mod.rs** - Test registry, `TestContext`, `NormativeTest`, and `TestResult` types
- **src/tests/v311/** - MQTT 3.1.1 tests (137 tests across 13 modules)
- **src/tests/v5/** - MQTT 5.0 tests (123 tests across 16 modules)

## Test Structure

Tests are organized by packet type/feature (connect, publish, subscribe, etc.). Each test module follows this pattern:

```rust
fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "section_name".to_string(),
        description: description.to_string(),
        runner,
    }
}

// Test function signature
async fn test_mqtt_x_x_x(ctx: TestContext) -> Result<(), ConformanceError>
```

## Key Dependencies

- **rumqttc** - MQTT client library for broker connections
- **tokio** - Async runtime
- **clap** - CLI argument parsing with derive macros
- **thiserror** - Error handling

## RFC Documentation

Complete MQTT specifications in `docs/mqtt-rfc/v3.1.1/` and `docs/mqtt-rfc/v5/`. Reference these when implementing new normative tests.
