#!/bin/bash
set -euo pipefail

rm -rf dist
uv build
uv publish -u SHi-ON dist/*
