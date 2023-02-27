# Integration tests

These tests test zdbfs+zdb+zstor setup.

It requires having:

- `/usr/local/bin/hook.sh` with the hook file
- zstor, zdb, zdbfs binaries
- [wondershaper](https://github.com/magnific0/wondershaper/blob/master/wondershaper)
- redis-cli

It runs as root.

## Running tests

`cargo test --test integration_test -- --nocapture`
