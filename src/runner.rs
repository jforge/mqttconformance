use crate::cli::MqttVersion;
use crate::tests::{get_test_registry, NormativeTest, TestContext, TestResult};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub struct TestRunner {
    host: String,
    port: u16,
    version: MqttVersion,
    username: Option<String>,
    password: Option<String>,
    timeout: Duration,
    verbose: bool,
    parallel: bool,
    section_filter: Option<String>,
    normative_filter: Option<String>,
}

impl TestRunner {
    pub fn new(
        host: &str,
        port: u16,
        version: MqttVersion,
        username: Option<String>,
        password: Option<String>,
        timeout: u64,
        verbose: bool,
        parallel: bool,
    ) -> Self {
        Self {
            host: host.to_string(),
            port,
            version,
            username,
            password,
            timeout: Duration::from_secs(timeout),
            verbose,
            parallel,
            section_filter: None,
            normative_filter: None,
        }
    }

    pub fn filter_section(&mut self, section: Option<&str>) {
        self.section_filter = section.map(|s| s.to_string());
    }

    pub fn filter_normative(&mut self, normative: Option<&str>) {
        self.normative_filter = normative.map(|s| s.to_string());
    }

    pub fn version_str(&self) -> &str {
        match self.version {
            MqttVersion::V311 => "3.1.1",
            MqttVersion::V5 => "5.0",
        }
    }

    pub async fn run(&self) -> Result<Vec<TestResult>, Box<dyn std::error::Error>> {
        let registry = get_test_registry(self.version);

        let tests_to_run: Vec<&NormativeTest> = registry
            .iter()
            .filter(|t| {
                if let Some(ref section) = self.section_filter {
                    if t.section != *section {
                        return false;
                    }
                }
                if let Some(ref normative) = self.normative_filter {
                    if t.id != *normative {
                        return false;
                    }
                }
                true
            })
            .collect();

        if tests_to_run.is_empty() {
            println!(
                "{}",
                "No tests match the specified filters.".bright_yellow()
            );
            return Ok(Vec::new());
        }

        println!(
            "Running {} tests...\n",
            tests_to_run.len().to_string().bright_cyan()
        );

        if self.parallel {
            self.run_parallel(tests_to_run).await
        } else {
            self.run_sequential(tests_to_run).await
        }
    }

    async fn run_sequential(&self, tests_to_run: Vec<&NormativeTest>) -> Result<Vec<TestResult>, Box<dyn std::error::Error>> {
        let mut results = Vec::new();

        if self.verbose {
            // Verbose: show each test as it runs
            let mut current_section = String::new();

            for test in tests_to_run {
                if test.section != current_section {
                    current_section = test.section.clone();
                    println!(
                        "\n{} {}\n{}",
                        "▶".bright_blue(),
                        current_section.to_uppercase().bright_white().bold(),
                        "─".repeat(50).dimmed()
                    );
                }

                let ctx = self.create_context();
                let result = self.run_single_test(test, &ctx).await;
                self.print_result(test, &result);
                results.push(result);
            }
        } else {
            // Non-verbose: show progress bar
            let pb = ProgressBar::new(tests_to_run.len() as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} tests")
                    .unwrap()
                    .progress_chars("=> "),
            );

            for test in tests_to_run {
                let ctx = self.create_context();
                let result = self.run_single_test(test, &ctx).await;
                pb.inc(1);
                results.push(result);
            }

            pb.finish_and_clear();
        }

        Ok(results)
    }

    async fn run_parallel(&self, tests_to_run: Vec<&NormativeTest>) -> Result<Vec<TestResult>, Box<dyn std::error::Error>> {
        let pb = ProgressBar::new(tests_to_run.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} tests")
                .unwrap()
                .progress_chars("=> "),
        );

        let futures: Vec<_> = tests_to_run
            .iter()
            .map(|test| {
                let ctx = self.create_context();
                let timeout = self.timeout;
                let test_id = test.id.clone();
                let test_desc = test.description.clone();
                let test_section = test.section.clone();
                let runner = test.runner;
                let pb = pb.clone();

                async move {
                    let start = std::time::Instant::now();
                    let result = tokio::time::timeout(timeout, runner(ctx)).await;
                    let duration = start.elapsed();
                    pb.inc(1);

                    let test_result = match result {
                        Ok(Ok(())) => TestResult {
                            id: test_id,
                            description: test_desc,
                            passed: true,
                            skipped: false,
                            error: None,
                            duration,
                        },
                        Ok(Err(e)) => TestResult {
                            id: test_id,
                            description: test_desc,
                            passed: false,
                            skipped: false,
                            error: Some(e.to_string()),
                            duration,
                        },
                        Err(_) => TestResult {
                            id: test_id,
                            description: test_desc,
                            passed: false,
                            skipped: false,
                            error: Some("Test timed out".to_string()),
                            duration,
                        },
                    };

                    (test_section, test_result)
                }
            })
            .collect();

        let results: Vec<(String, TestResult)> = futures::future::join_all(futures).await;
        pb.finish_and_clear();

        // Print results grouped by section (only in verbose mode)
        if self.verbose {
            self.print_results_grouped(&results);
        }

        Ok(results.into_iter().map(|(_, r)| r).collect())
    }

    fn create_context(&self) -> TestContext {
        TestContext {
            host: self.host.clone(),
            port: self.port,
            version: self.version,
            username: self.username.clone(),
            password: self.password.clone(),
            timeout: self.timeout,
            verbose: self.verbose,
        }
    }

    fn print_results_grouped(&self, results: &[(String, TestResult)]) {
        // Group by section while preserving original order
        let mut current_section = String::new();

        for (section, result) in results {
            if *section != current_section {
                current_section = section.clone();
                println!(
                    "\n{} {}\n{}",
                    "▶".bright_blue(),
                    current_section.to_uppercase().bright_white().bold(),
                    "─".repeat(50).dimmed()
                );
            }

            let status = if result.passed {
                "PASS".bright_green()
            } else if result.skipped {
                "SKIP".bright_yellow()
            } else {
                "FAIL".bright_red()
            };

            let icon = if result.passed {
                "✓".bright_green()
            } else if result.skipped {
                "○".bright_yellow()
            } else {
                "✗".bright_red()
            };

            println!(
                "  {} [{}] {} ({:.2?})",
                icon,
                status,
                result.id.bright_white(),
                result.duration
            );

            if self.verbose {
                println!("      {}", result.description.dimmed());
            }

            if let Some(ref error) = result.error {
                println!("      {} {}", "Error:".bright_red(), error.dimmed());
            }
        }
    }

    async fn run_single_test(&self, test: &NormativeTest, ctx: &TestContext) -> TestResult {
        let start = std::time::Instant::now();

        let result = tokio::time::timeout(self.timeout, (test.runner)(ctx.clone())).await;

        let duration = start.elapsed();

        match result {
            Ok(Ok(())) => TestResult {
                id: test.id.clone(),
                description: test.description.clone(),
                passed: true,
                skipped: false,
                error: None,
                duration,
            },
            Ok(Err(e)) => TestResult {
                id: test.id.clone(),
                description: test.description.clone(),
                passed: false,
                skipped: false,
                error: Some(e.to_string()),
                duration,
            },
            Err(_) => TestResult {
                id: test.id.clone(),
                description: test.description.clone(),
                passed: false,
                skipped: false,
                error: Some("Test timed out".to_string()),
                duration,
            },
        }
    }

    fn print_result(&self, test: &NormativeTest, result: &TestResult) {
        let status = if result.passed {
            "PASS".bright_green()
        } else if result.skipped {
            "SKIP".bright_yellow()
        } else {
            "FAIL".bright_red()
        };

        let icon = if result.passed {
            "✓".bright_green()
        } else if result.skipped {
            "○".bright_yellow()
        } else {
            "✗".bright_red()
        };

        println!(
            "  {} [{}] {} ({:.2?})",
            icon,
            status,
            test.id.bright_white(),
            result.duration
        );

        if self.verbose {
            println!("      {}", test.description.dimmed());
        }

        if let Some(ref error) = result.error {
            println!("      {} {}", "Error:".bright_red(), error.dimmed());
        }
    }
}
