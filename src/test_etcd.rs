use simple_logger::SimpleLogger;
use std::path::PathBuf;
use tokio::runtime::Builder;
use tokio_compat_02::FutureExt;
use zstor_v2::{
    config::{Compression, Encryption},
    encryption::SymmetricKey,
    etcd,
    meta::{MetaData, ShardInfo},
    zdb::ZdbConnectionInfo,
};

fn main() {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();

    rt.block_on(async {
        SimpleLogger::new()
            .with_level(log::LevelFilter::Info)
            .init()
            .unwrap();

        let nodes = vec![
            "http://127.0.0.1:2379".to_owned(),
            "http://127.0.0.1:22379".to_owned(),
            "http://127.0.0.1:32379".to_owned(),
        ];

        let cluster = etcd::Etcd::new(&etcd::EtcdConfig::new(
            nodes,
            "prefix".to_string(),
            None,
            None,
        ))
        .compat()
        .await
        .unwrap();

        let mut path = PathBuf::new();
        path.push("./here");
        let mut data = MetaData::new(
            1,
            2,
            Encryption::new(
                "AES",
                &SymmetricKey::new([
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5,
                    6, 7, 8, 9, 0, 1,
                ]),
            ),
            Compression::new("snappy"),
        );
        data.add_shard(ShardInfo::new(
            0,
            vec![0],
            ZdbConnectionInfo::new("[::1]:9900".parse().unwrap(), None, None),
        ));

        cluster.save_meta(&path, &data).compat().await.unwrap();
        let rec = cluster.load_meta(&path).compat().await.unwrap();

        log::info!("comparing data");
        assert_eq!(&rec, &data);
        log::info!("compared data");
    });
}
