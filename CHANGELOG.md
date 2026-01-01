# Changelog

All notable changes to this project are documented in this file.

## Unreleased
- TBD

## 1.0.4
- Refreshed `uv.lock` after the 1.0.3 version bump to keep locked installs green.

## 1.0.3
- Added README badges for PyPI, downloads, and CI/documentation status.

## 1.0.2
- Updated project URLs and documentation link in package metadata.

## 1.0.1
- Updated README for PyPI and documentation links.

## 1.0.0
- Fixed dedupe index filtering to ignore null dedupe keys.
- Ensured rate-limited tasks reschedule with a delay.
- Improved AsyncTaskQueue export typing.

## 0.3.1
- Removed license classifier for PEP 639 compatibility.

## 0.3.0
- Added delayed enqueue, leases/heartbeats, backoff, and rate limiting.
- Added dead-letter support and async queue (optional extra).
- Expanded CLI commands and docs.

## 0.2.7
- Fixed license metadata for setuptools/CI builds.

## 0.2.6
- Added CLI for common queue operations.
- Added context manager support and client options.
- Added Docker and GitHub Actions test harness.
- Added release workflow, examples, and contributor docs.
- Added optional developer tooling configs and typing marker.

## 0.2.5
- Updated dependency floor and uv tooling.
- Fixed task queue edge cases.
