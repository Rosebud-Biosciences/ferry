## What and why

<!-- What this changes, and why. Link the issue it closes, if any. -->

## Checklist

- [ ] `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings` and `cargo test --locked` pass.
- [ ] A change to what ferry uploads, registers or finalizes has a test that fails without it.
- [ ] A change to events or manifests is additive, with `schemas/` regenerated (`FERRY_BLESS=1 cargo test --lib schemas`) and `docs/hooks.md` updated.
- [ ] User-visible changes are under `## [Unreleased]` in CHANGELOG.md, and documented.
