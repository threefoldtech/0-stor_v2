use simple_logger::SimpleLogger;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use tokio::runtime::Builder;
use zstor_v2::zdb;

fn main() {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();

    rt.block_on(async {
        SimpleLogger::new().init().unwrap();
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::from_str("::").unwrap()), 9900);
        println!("{}", addr);
        let mut db = zdb::SequentialZdb::new(zdb::ZdbConnectionInfo::new(addr, None, None))
            .await
            .unwrap();

        let data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let key = db.set(&data).await.unwrap();

        let retrieved = db.get(&key).await.unwrap().unwrap();

        assert_eq!(&retrieved, &data);
    });
}
