# 0-stor_v2

## Current features

- `Store` data in multiple chunks on zdb backends, according to a given policy
- `Retrieve` said data, using just the path and the metadata store. Zdbs can be
removed, as long as sufficient are left to recover the data.
- `Rebuild` the data, loading existing data (as long as sufficient zdbs are left),
reencoding it, and storing it in (new) zdbs according to the current config

NOTE: currently all backends in the config are assumed to be healthy: they are
reachable, and the namespace has enough space to hold the data shard which will
be written

## Building

Make sure you have the lastest Rust stable installed. Clone the repository:

```shell
git clone https://github.com/threefoldtech/0-stor_v2
cd 0-stor_v2
```

Then build with the standard toolchain through cargo:

```shell
cargo build
```

This will produce the executable in `./target/debug/zstor_v2`.

## Config file

Storing data and rebuilding existing data on new backends requires a config file.
The config file is expected to be in `toml` format. An example config is:

```toml
data_shards = 2
parity_shards = 1
redundant_groups = 0
redundant_nodes = 0
root = "/some/root"

[encryption]
algorithm = "AES"
key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[compression]
algorithm = "snappy"

[meta]
type = "etcd"

[meta.config]
endpoints = ["http://127.0.0.1:2379", "http://127.0.0.1:22379", "http://127.0.0.1:32379"]
prefix = "someprefix"

[[groups]]
[[groups.backends]]
address = "[::1]:19900"

[[groups.backends]]
address = "[::1]:19901"
namespace = "some_ns"
password = "supersecretnamespacepass"

[[groups]]
[[groups.backends]]
address = "[::1]:29901"
```

Explanation:

- `data_shards`: the minimum amount of shards needed to later recover the data
- `parity_shards`: the amount of redundant shards that will be written
- `redundant_groups`: the maximum amount of groups that can be lost completely,
while still retaining the ability to recover the data
- `redundant_nodes`: the maximim amount of nodes that can be lost in _every_ group
while still retraining the ability to recover the data

Note that you can lose the complete groups and also the individual nodes in the
remaining groups, and you should still be able to recover your data

The backends are automatically selected when writing data to guarantee data recovery
according to these options. If no vaible backend distribution can be generated,
the program will exit.

- `root`: Optional directory to use as a virtual root for all the files. If set,
this prefix will be stripped from the full path of the files uploaded or downloaded.
Example: assume we upload file `/mnt/somedir/somesubdir/file`, and set the `root`
to `/mnt/somedir`. The key in the metastorage (if supported) will now be built
from `somesubdir/file`, since the `/mnt/somedir` directories are stripped. Now,
if you download the file again (possibly on a different machine), with `/mnt/someotherdir`
as root, the file can be downloaded by given `/mnt/someotherdir/somesubdir/file`
as argument to the `retrieve` command.

- `encryption`: encryption configuration
  - `algorithm`: the encryption algorithm to use, currently only `AES` is supported
  - `key`: hex encoded symmetric key to use for encryption, must be 32 bytes (64
  hex chars)

- `compression`: compression configuration
  - `algorithm`: compression algorithm to use, currently only `snappy` is supported

- `groups`: list of backend groups. A group is a list of zdb backends. These are
intended to represent grouped backends. The setup here will influence the generated
backend distributions (if any) in accordance with the redundancy parameters

- `backends`: A zdb backend, identified by an IP address and port. Both IPv4 and
IPv6 are supported. Optionally, a `namespace` can be specified, in which case this
namespace will be used to write the data. If a namespace is given, you can also
optionally specify a `password`. In this case, the namespace will be opened via
means of `SELECT SECURE` (old zdbs might not support this)

- `meta`: The metadata system ot use. Currently `etcd` is the only one supported

- `meta.config`: Configuration for the metadata backend that is used, if required.
Since only etcd is supported right now, it always needs to be present.
For `etcd`, there are 2 fields (both required):
`endpoints`: A list of http listening endpoints for the cluster nodes
`prefix`: The prefix to use for the keys in etcd. See the `Metadata` section for
more info.

## Metadata

When deta is encoded, metadata is generated to later retrieve this data. The metadata
is stored in etcd, with a given prefix. Both the etcd cluster endpoints and the
prefix to use must be provided for every action.

For every file, we get the full path of the file on the system, generate a 16 byte
blake2b hash, and hex encode the bytes. We then append this to the prefix to
generate the final key.

The key structure is: `/{prefix}/{hashed_path_hex}`

The metadata itself is also stored in `TOML` format in etcd.

## Example usage

Note, if the config file is not passed explicitly, it is assumed to be `config.toml`
in the working directory.

- Store file:

`./target/debug/zstor_v2 -e "http://127.0.0.1:2379,http://127.0.0.1:22379,http://127.0.0.1:32379" -p me store -f file.txt`

- Retrieve file:

`./target/debug/zstor_v2 -e "http://127.0.0.1:2379,http://127.0.0.1:22379,http://127.0.0.1:32379" -p me retrieve -f file.txt`

- Rebuild file (with possibly new configuration)

`./target/debug/zstor_v2 -e "http://127.0.0.1:2379,http://127.0.0.1:22379,http://127.0.0.1:32379" -p me rebuild -f file.txt`

