use simple_logger::SimpleLogger;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use tokio::runtime::Builder;
use zstor_v2::config::{Compression, Encryption};
use zstor_v2::encryption::{AesGcm, SymmetricKey};
use zstor_v2::erasure::Encoder;
use zstor_v2::meta::{MetaData, MetaStore};
use zstor_v2::zdb;
use zstor_v2::zdb_meta::ZdbMetaStore;

const ENCRYPTIONKEY: SymmetricKey = SymmetricKey::new([
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
]);

fn main() {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();

    rt.block_on(async {
        SimpleLogger::new()
            .with_level(log::LevelFilter::Info)
            .with_module_level("zstor_v2", log::LevelFilter::Trace)
            .init()
            .unwrap();
        let addr0 = SocketAddr::new(IpAddr::V6(Ipv6Addr::from_str("::").unwrap()), 9990);
        let addr1 = SocketAddr::new(IpAddr::V6(Ipv6Addr::from_str("::").unwrap()), 9991);
        let addr2 = SocketAddr::new(IpAddr::V6(Ipv6Addr::from_str("::").unwrap()), 9992);
        let addr3 = SocketAddr::new(IpAddr::V6(Ipv6Addr::from_str("::").unwrap()), 9993);
        #[allow(clippy::eval_order_dependence)]
        let backends = vec![
            zdb::UserKeyZdb::new(zdb::ZdbConnectionInfo::new(addr0, None, None))
                .await
                .unwrap(),
            zdb::UserKeyZdb::new(zdb::ZdbConnectionInfo::new(addr1, None, None))
                .await
                .unwrap(),
            zdb::UserKeyZdb::new(zdb::ZdbConnectionInfo::new(addr2, None, None))
                .await
                .unwrap(),
            zdb::UserKeyZdb::new(zdb::ZdbConnectionInfo::new(addr3, None, None))
                .await
                .unwrap(),
        ];

        let encoder = Encoder::new(2, 2);
        let encryptor = AesGcm::new(ENCRYPTIONKEY);

        let metastore = ZdbMetaStore::new(
            backends,
            encoder,
            encryptor,
            "some_prefix".to_string(),
            None,
        );

        let enc = Encryption::Aes(ENCRYPTIONKEY);
        let compression = Compression::Snappy;

        // write
        for i in 0..1000 {
            let checksum = (i as u128).to_be_bytes();
            let meta = MetaData::new(
                2,
                2,
                checksum,
                enc.clone().into(),
                compression.clone().into(),
            );
            metastore
                .save_meta_by_key(&format!("/some_prefix/meta/{}", i), &meta)
                .await
                .unwrap();
        }

        // recreate metastore with only 2 shards in different order
        #[allow(clippy::eval_order_dependence)]
        let backends = vec![
            zdb::UserKeyZdb::new(zdb::ZdbConnectionInfo::new(addr3, None, None))
                .await
                .unwrap(),
            zdb::UserKeyZdb::new(zdb::ZdbConnectionInfo::new(addr0, None, None))
                .await
                .unwrap(),
        ];
        let encoder = Encoder::new(2, 2);
        let encryptor = AesGcm::new(ENCRYPTIONKEY);

        let metastore = ZdbMetaStore::new(
            backends,
            encoder,
            encryptor,
            "some_prefix".to_string(),
            None,
        );

        // read & verify
        for i in 0..1000 {
            let checksum = (i as u128).to_be_bytes();
            let meta = metastore
                .load_meta_by_key(&format!("/some_prefix/meta/{}", i))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(meta.checksum(), &checksum);
        }

        // check key iterator
        let mut count = 0;
        for (i, (_, meta)) in metastore
            .object_metas()
            .await
            .unwrap()
            .into_iter()
            .enumerate()
        {
            let checksum = (i as u128).to_be_bytes();
            assert_eq!(meta.checksum(), &checksum);
            count += 1;
        }

        assert_eq!(count, 1000);
    });
}
