# Contributing

Thanks for helping improve Mongo-TaskQueue.

## Development
- Use `uv` for environments and tooling.
- Run tests in Docker: `docker compose up --build --abort-on-container-exit --exit-code-from tests`.

## Pull requests
- Keep changes focused and add tests for new behavior.
- Update `CHANGELOG.md` when behavior changes.

## Releases
- Tag releases as `vX.Y.Z`.
- GitHub Actions publishes to PyPI using the `PYPI_TOKEN` secret.
