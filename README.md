# 0-stor_v2

`zstor` is an object encoding storage system. It can be run in either a
daemon - client setup, or it can perform single actions without an
associated daemon, which is mainly useful for uploading/retrieving
single items. The daemon is part of the same binary, and will run other
useful features, such as a repair queue which periodically verifies the
integrity of objects, and automatic expansion of the storage expansion.
To this end, `zstor` is self contained: after a full setup, the system
can reserve capacity to keep itself going indefinitely.

## Expected setup

Currently, `zstor` expects a stable system to start from, which is user
provided. In order for the system to work optimally, the following
should be considered:

- The system relies on the explorer to automatically manage capacity
	pools and 0-db reservations. In order to do this, an *already
	registered* identity must be provided in the config, along with a
	stellar wallet secret. This wallet is used to fund all capacity
	reservations. `zstor` will attempt to keep _all_ capacity pools
	registered to this identity funded. It will *not create capacity
	pools* currently. This means that all pools to be used should be set
	up by the user.
- `zstor` has a redundancy configuration which introduces the notion of
	`groups`: a list of one or more 0-db backends which are physically
	together. In practice, it is expected that:
	- Every group represents a farm
	- All 0-db's in a group are managed by a single capacity pool.
	The above will make sure that `zstor` can correctly expand and
	replace 0-dbs while keeping the group redundancy profile in tact.
- `zstor` aims to replace backends on the same farm / capacity pool. To
	do this, it aims to extract reservation ID's from the backends it
	starts with in the config. The best results will be gained by making
	sure all 0-db backends are actually running on the grid.
- Because of the above, it is recommended to run a dedicated identity
	for the `zstor`, so it only manages pools used by the setup.
	Optionally, a dedicated wallet can be used (this is probably best
	security wise).
- Due to the nature of `TFT`, the wallet must have both `TFT`, and `XLM`
	to fund the transaction (the latter only being used for the small
	transaction fee). This is necessary as we don't rely on external
	services for the payment.

## Daemon - client usage vs standalone usage

The daemon, or monitor, can be started by invoking `zstor` with the
`monitor` subcommand. This starts a long running process, and opens up a
unix socket on the path specified in the config. Regular command
invocations (example "store") of `zstor` will then read the path to the
unix socket from the config, connect to it, send the command, and wait
until the monitor daemon returns a response after executing the command.
This setup is recommended as:

- It actually allows for the automatic expansion / replacement of failed
	0-dbs.
- It exposes optional metrics for prometheus to scrape.
- Only a single upload/download of a file happens at once, meaning you
	won't burn out your whole cpu by sending multiple upload commands in
	quick succession.

If the socket path is not specified, `zstor` will fall back to its
single command flow, where it executes the command in process, and then
exits. Invoking `zstor` multiple times in quick succession might cause
multiple uploads to be performed at the same time, causing multiple cpu
cores to be used for the encryption/compression.

## Current features

### Supported commands

- `Store` data in multiple chunks on zdb backends, according to a given policy
- `Retrieve` said data, using just the path and the metadata store. Zdbs can be
removed, as long as sufficient are left to recover the data.
- `Rebuild` the data, loading existing data (as long as sufficient zdbs are left),
reencoding it, and storing it in (new) zdbs according to the current config
- `Check` a file, returning a 16 byte `blake2b` checksum (in hex) if it
	is present in the backend (by fetching it from the metastore).

### Other features

- Monitoring of active 0-db backends. An active backend is considered a
backend that is tracked in the config, which has sufficient space
	left to write new blocks. If a backend is full, it will be rotated
	out by reserving a new one, but the existing backend is not
	decommissioned.
- Repair queue: periodically, all 0-db's used are checked, to see if the
	are still online. If a 0-db is unreachable, all objects which have a
	chunk stored on that 0-db will be rebuild on fully healthy 0-db's.
- Explorer client, allowing for full self contained operation (after the
	initial bootstrap currently).
- Prometheus metrics. The metrics server is bound to all interfaces, on
	the port specified in the config. The path is `/metrics`. If no port
	is set in the config, the metrics server won't be enabled.

## Building

Make sure you have the latest Rust stable installed. Clone the repository:

```shell
git clone https://github.com/threefoldtech/0-stor_v2
cd 0-stor_v2
```

Then build with the standard toolchain through cargo:

```shell
cargo build
```

This will produce the executable in `./target/debug/zstor_v2`.

### Static binary

On linux, a fully static binary can be compiled by using the `x86_64-unknown-linux-musl`
target, as follows:

```rust
cargo build --target x86_64-unknown-linux-musl --release
```

## Config file

Running `zstor` requires a config file. An example config, and
explanation of the parameters is found below.

### Example config file

```toml
data_shards = 10
parity_shards = 5
redundant_groups = 1
redundant_nodes = 1
root = "/virtualroot"
socket = "/tmp/zstor.sock"
network = "Main"
wallet_secret = "Definitely not a secret"
identity_name = "testid"
identity_email = "test@example.com"
identity_id = 25
identity_mnemonic = "an unexisting mnemonic"
prometheus_port = 9100
zdb_data_dir_path = "/tmp/0-db/data"
max_zdb_data_dir_size = 25600

[encryption]
algorithm = "AES"
key = "0000000000000000000000000000000000000000000000000000000000000000"

[compression]
algorithm = "snappy"

[meta]
type = "zdb"

[meta.config]
prefix = "someprefix"

[meta.config.encryption]
algorithm = "AES"
key = "0101010101010101010101010101010101010101010101010101010101010101"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9900"
namespace = "test2"
password = "supersecretpass"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9901"
namespace = "test2"
password = "supersecretpass"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9902"
namespace = "test2"
password = "supersecretpass"

[[meta.config.backends]]
address = "[2a02:1802:5e::dead:beef]:9903"
namespace = "test2"
password = "supersecretpass"

[[groups]]
[[groups.backends]]
address = "[fe80::1]:9900"

[[groups.backends]]
address = "[fe80::1]:9900"
namespace = "test"

[[groups]]
[[groups.backends]]
address = "[2a02:1802:5e::dead:babe]:9900"

[[groups.backends]]
address = "[2a02:1802:5e::dead:beef]:9900"
namespace = "test2"
password = "supersecretpass"
```

### Config file explanation

- `data_shards`: The minimum amount of shards which are needed to recover
    the original data.
- `parity_shards`: The amount of redundant data shards which are generated
    when the data is encoded. Essentially, this many shards can be lost 
	while still being able to recover the original data.
- `redundant_groups`: The amount of groups which one should be able to
    loose while still being able to recover the original data.
- `redundant_nodes`: The amount of nodes that can be lost in every group
    while still being able to recover the original data.
- `root`: virtual root on the filesystem to use, this path will be removed
    from all files saved. If a file path is loaded, the path will be
	interpreted as relative to this directory
- `socket`: Optional path to a unix socket. This socket is required in
    case zstor needs to run in daemon mode. If this is present, zstor
	invocations will first try to connect to the socket. If it is not found,
	the command is run in-process, else it is encoded and send to the socket
	so the daemon can process it.
- `zdb_data_dir_path`: Optional path to the local 0-db data file directory.
    If set, it will be monitored and kept within the size limits.
- `max_zdb_data_dir_size`: Maximum size of the data dir in MiB, if this
    is set and the sum of the file sizes in the data dir gets higher than
	this value, the least used, already encoded file will be removed.
- `prometheus_port`: An optional port on which prometheus metrics will be
    exposed. If this is not set, the metrics will not get exposed.
- `network`: The grid network to manage 0-dbs on, one of {Main, Test, Dev}.
- `wallet_secret`: The stellar secret of the wallet used to fund capacity
    pools. This wallet must have TFT, and a small amount of XLM to fund
	the transactions.
- `identity_name`: The name of the identity registered on the explorer to
    use for pools / reservations.
- `identity_email`: The email associated with the identity on the explorer.
- `identity_id`: The id of the identity.
- `identity_mnemonic`: The mnemonic of the secret used by the identity.
- `encryption`: configuration to use for the encryption stage. Currently
	only `AES` is supported.
- `compression`: configuration to use for the compression stage.
	Currently only `snappy` is supported
- `meta`: configuration for the metadata store to use, currently only
	`zdb` is supported
- `groups`: The backend groups to write the data to.

Explanation:

## Metadata

When data is encoded, metadata is generated to later retrieve this data.
The metadata is stored in 4 0-dbs, with a given prefix.

For every file, we get the full path of the file on the system, generate a 16 byte
blake2b hash, and hex encode the bytes. We then append this to the prefix to
generate the final key.

The key structure is: `/{prefix}/meta/{hashed_path_hex}`

The metadata itself is encrypted, binary encoded, and then dispersed in
the metadata 0-dbs.
