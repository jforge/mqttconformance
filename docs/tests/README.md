# MQTT Conformance Test Coverage

This directory contains documentation mapping normative statements from the MQTT specifications to test coverage in this project.

## Documents

- [MQTT 3.1.1 Coverage](v3.1.1.md) - 117/117 normative statements covered and verified (100%)
- [MQTT 5.0 Coverage](v5.md) - 88/88 normative statements covered and verified (100%)

## Overview

Each document lists all normative statements (MUST/MUST NOT requirements) from the respective MQTT specification and indicates whether the test suite includes coverage for that requirement.

### Coverage Summary

| Version | Total Statements | Covered | Verified | Coverage |
|---------|-----------------|---------|----------|----------|
| MQTT 3.1.1 | 117 | 117 | 117 | 100% |
| MQTT 5.0 | 88 | 88 | 88 | 100% |

**Last Audit:** 2025-12-26

All tests have been manually audited to confirm they correctly implement their corresponding normative requirements.

**Note:** WebSocket transport (Section 6/10) is optional and not included in the core normative count.

## Reference

The normative statements are sourced from:
- [MQTT 3.1.1 Specification](../mqtt-rfc/v3.1.1/)
- [MQTT 5.0 Specification](../mqtt-rfc/v5/)
