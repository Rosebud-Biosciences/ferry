# Contributing to ferry

Thank you for helping. Bug reports, fixes, documentation and features are all
welcome; for anything bigger than a small fix, open an issue first, so we can
agree on the approach before you spend time on it.

ferry moves other people's only copies of their data, so changes are held to
one rule above the rest: **ferry must never report, register or delete
something it has not verified.** A change that weakens a check needs a strong
reason and a test that shows the reason.

Taking part means following the [Code of Conduct](CODE_OF_CONDUCT.md). Report
security problems privately, as [SECURITY.md](SECURITY.md) describes.

## Scope

ferry is for write-once data -- acquisitions that land, settle and are left
alone -- going to S3-compatible stores. It is not a backup, sync or mirror
tool, and it never deletes from a store. Features that need either are out of
scope; features that let another tool do that job (hooks, events, manifests)
are in.

## Building and testing

You need stable Rust; the minimum supported version is `rust-version` in
`Cargo.toml`. Before you push:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

CI runs the tests on Linux, Windows and macOS, so a test that needs one
platform says so with `#[cfg(...)]`, and anything that differs on Windows --
paths, file locking, permissions, the service -- deserves a test that runs
there.

The S3 tests run against a real S3 API when `FERRY_TEST_S3_ENDPOINT`,
`FERRY_TEST_S3_KEY`, `FERRY_TEST_S3_SECRET` and an existing
`FERRY_TEST_S3_BUCKET` are set (`cargo test --test s3 -- --include-ignored`);
the `s3-seaweedfs` job in [`ci.yml`](.github/workflows/ci.yml) shows how to
start SeaweedFS for them in Docker.

## Writing a change

- Test behavior, not internals. `tests/engine.rs` drives whole passes over a
  temporary folder with a fake clock; `tests/cli.rs` runs the binary. A change
  to when ferry uploads, registers or finalizes needs a test there that fails
  without it.
- What hooks receive is a contract. A change to an event or a manifest must be
  additive -- new fields, new event kinds -- and comes with its schemas
  regenerated (`FERRY_BLESS=1 cargo test --lib schemas`) and `docs/hooks.md`
  updated. Removing or changing a field is a breaking change.
- The journal upgrades in place. A schema change bumps `SCHEMA_VERSION` in
  `src/journal.rs`, adds a step to `migrate`, and tests upgrading a journal
  from the version before.
- Config keys are documented in [`examples/ferry.example.yaml`](examples/ferry.example.yaml)
  (a test loads it) and, where they matter, the README.
- Note user-visible changes under `## [Unreleased]` in
  [CHANGELOG.md](CHANGELOG.md).

Keep pull requests to one change, with a subject line in the imperative ("Add
...", "Fix ...") and, where it is not obvious, a body that says why.

## License

ferry is Apache-2.0. Contributions are accepted under the same license
(section 5 of the [license](LICENSE)); there is no CLA.

## Releasing (maintainers)

1. Set `version` in `Cargo.toml` (`cargo check` updates `Cargo.lock`).
2. In CHANGELOG.md, move `Unreleased` to `## [X.Y.Z] - YYYY-MM-DD`.
3. Merge that, then tag the merge commit `vX.Y.Z` and push the tag.

[`release.yml`](.github/workflows/release.yml) then runs CI on the tag,
builds and smoke-tests the wheels, publishes them to PyPI, and makes a GitHub
release with standalone binaries, `SHA256SUMS` and provenance attestations; a
version with a `-` (`0.2.0-alpha.1`) is marked a pre-release. PyPI publishing
is trusted publishing from the `pypi` environment, configured once on PyPI.
