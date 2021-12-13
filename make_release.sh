#!/usr/bin/env bash

set -e
set -x

features=("+aes" "+avx" "+sse" "+sse2" "+sse3" "+sse4.1" "+sse4.2" "+ssse3" "+xsave" "+xsavec" "+xsaveopt" "+xsaves")
for f in ${features[@]}; do
	RUSTFLAGS="-C target-feature=$f $RUSTFLAGS"
done
mkdir -p release
cargo build --target x86_64-unknown-linux-musl --release --features vendored
cp ./target/x86_64-unknown-linux-musl/release/zstor_v2 release
pushd release
strip zstor_v2
mv zstor_v2 zstor_v2-x86_64-linux-musl
popd
