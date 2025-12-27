#![allow(clippy::vec_init_then_push)]
#![allow(clippy::needless_range_loop)]

mod cli;
mod error;
mod runner;
mod tests;

use clap::Parser;
use cli::{Cli, Commands};
use colored::Colorize;
use runner::TestRunner;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            host,
            port,
            version,
            section,
            normative,
            username,
            password,
            timeout,
            verbose,
            parallel,
        } => {
            let mut runner = TestRunner::new(
                &host,
                port,
                version,
                username,
                password,
                timeout,
                verbose,
                parallel,
            );

            runner.filter_section(section.as_deref());
            runner.filter_normative(normative.as_deref());

            println!();
            println!(
                "{}",
                "╔══════════════════════════════════════════════════════════════╗"
                    .bright_blue()
            );
            println!(
                "{}",
                "║           MQTT Broker Conformance Test Suite                 ║"
                    .bright_blue()
            );
            println!(
                "{}",
                "╚══════════════════════════════════════════════════════════════╝"
                    .bright_blue()
            );
            println!();

            println!("Target: {}:{}", host.bright_cyan(), port.to_string().bright_cyan());
            println!("Version: MQTT {}", runner.version_str().bright_yellow());
            println!();

            let results = runner.run().await?;

            let passed = results.iter().filter(|r| r.passed).count();
            let failed_results: Vec<_> = results.iter().filter(|r| !r.passed && !r.skipped).collect();
            let skipped = results.iter().filter(|r| r.skipped).count();
            let total = results.len();

            // Show failure details if any
            if !failed_results.is_empty() {
                println!("\n{}", "═".repeat(66).bright_red());
                println!("{}", "                        FAILURES".bright_red().bold());
                println!("{}\n", "═".repeat(66).bright_red());

                for result in &failed_results {
                    println!(
                        "  {} {}",
                        "✗".bright_red(),
                        result.id.bright_white().bold()
                    );
                    println!("    {}", result.description.dimmed());
                    if let Some(ref error) = result.error {
                        println!("    {} {}", "Error:".bright_red(), error);
                    }
                    println!();
                }
            }

            println!("{}", "═".repeat(66).bright_blue());
            println!("{}", "                        SUMMARY".bright_white().bold());
            println!("{}\n", "═".repeat(66).bright_blue());

            println!(
                "  {} {} passed",
                "✓".bright_green(),
                passed.to_string().bright_green()
            );
            println!(
                "  {} {} failed",
                "✗".bright_red(),
                failed_results.len().to_string().bright_red()
            );
            println!(
                "  {} {} skipped",
                "○".bright_yellow(),
                skipped.to_string().bright_yellow()
            );
            println!(
                "  {} {} total\n",
                "•".bright_white(),
                total.to_string().bright_white()
            );

            if !failed_results.is_empty() {
                std::process::exit(1);
            }
        }
        Commands::List { version, section } => {
            let registry = tests::get_test_registry(version);

            println!(
                "\n{} MQTT {} Normative Tests:\n",
                "Available".bright_cyan(),
                match version {
                    cli::MqttVersion::V311 => "3.1.1",
                    cli::MqttVersion::V5 => "5.0",
                }
                .bright_yellow()
            );

            let mut current_section = String::new();
            for test in registry.iter() {
                if let Some(ref filter) = section {
                    if test.section != *filter {
                        continue;
                    }
                }

                if test.section != current_section {
                    current_section = test.section.clone();
                    println!("  {} {}:", "Section:".bright_blue(), current_section.bright_white());
                }

                println!(
                    "    {} - {}",
                    test.id.bright_green(),
                    test.description.dimmed()
                );
            }
            println!();
        }
    }

    Ok(())
}
