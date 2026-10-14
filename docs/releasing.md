# Releasing

The release workflow publishes the Python package to PyPI and the Rust crate to crates.io when a semantic version tag such as `v0.2.0` is pushed. It runs the shared checks first and verifies that the tag matches the versions in `pyproject.toml` and `rust/Cargo.toml`.

## Published release

Version **0.2.0** was released from the [`v0.2.0` tag](https://github.com/eddiethedean/spark-ddl-parser/tree/v0.2.0). The Python package is available on [PyPI](https://pypi.org/project/spark-ddl-parser/0.2.0/), and the Rust crate is available on [crates.io](https://crates.io/crates/spark-ddl-parser/0.2.0/). [Release CI](https://github.com/eddiethedean/spark-ddl-parser/actions/runs/38623091999) passed the shared checks and published both packages.

## One-time publishing setup

Configure a PyPI trusted publisher for the existing `spark-ddl-parser` project with these values:

| Setting | Value |
| --- | --- |
| Owner | `eddiethedean` |
| Repository | `spark-ddl-parser` |
| Workflow filename | `release-ci.yml` |
| GitHub environment | `pypi` |

The workflow requests the `id-token: write` permission and uses the `pypi` environment. The trusted publisher must match those values exactly. See the [PyPI trusted publisher setup guide](https://docs.pypi.org/trusted-publishers/adding-a-publisher/).

Add a repository Actions secret named `CARGO_REGISTRY_TOKEN` with a crates.io token that can publish this crate. A secret with this name is already configured; verify that its token remains valid and has permission to publish this crate.

## Publish a version

1. Set the same release version in `pyproject.toml` and `rust/Cargo.toml`.
2. Run the shared Python and Rust checks and confirm the commit is on `main`.
3. Push a matching version tag, for example `git tag -a v0.2.0 -m "Release 0.2.0"` followed by `git push origin v0.2.0`.
4. Confirm the **Release CI** run completes successfully and both registries show the new version.

The tag push starts publication automatically; no manual GitHub Release is created by this workflow. PyPI and crates.io versions cannot be overwritten, so correct the source and version before pushing a release tag.
