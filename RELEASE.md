# Release flow

To create a new release for the project:

1. Make sure the version in `cargo.toml` is set correctly (so version info is correctly
embedded in the binary once we have such a feature).
1. Create a tag with the same version as specified in the `cargo.toml`.
1. Build binaries in `release` mode for all supported targets.
    - Currently supported targets:
      - x86_64-unknown-linux-gnu
      - x86_64-unknown-linux-musl (static build)
1. Rename the resulting binaries (or copy them and then rename them), so the name
is `zstor_v2-${arch}-{os}`, i.e. add the `target` spec, but drop the `vendor` segment
1. Push the tag to github if this hasn't happened yet, and create a full release
from this tag.
1. Add release notes, and upload the binaries as assets.
1. Optionally but preferably bump the project version in cargo.toml in preparation
for the next release.
