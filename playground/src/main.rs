use grid_explorer_client;
use std::env;
use tokio::runtime::Runtime;

fn main() {
    // Create the runtime
    let rt = Runtime::new().unwrap();

    let args: Vec<String> = env::args().collect();

    let stellar_secret = args[1].clone();
    let user_id: i64 = args[2].parse().unwrap();
    let mnemonic = args[3].clone();

    // Execute the future, blocking the current thread until completion
    rt.block_on(async {
        // node_get(stellar_secret, user_id, mnemonic).await;
        // nodes_get(stellar_secret, user_id, mnemonic).await;
        // farms_get(stellar_secret, user_id, mnemonic).await;
        // farm_get().await;
        workload_get(stellar_secret, user_id, mnemonic).await;
        // pool_get(stellar_secret, user_id, mnemonic).await;
        // pool_create(stellar_secret, user_id, mnemonic).await;
        // pools_by_owner(stellar_secret, user_id, mnemonic).await;
        // nodes_filter(stellar_secret, user_id, mnemonic).await;
        // let res = zdb_create(stellar_secret.clone(), user_id, mnemonic.clone()).await;
        // if let Ok(wid) = res {
        //     workload_poll(stellar_secret.clone(), user_id, mnemonic.clone(), wid).await;
        //     workload_decommission(stellar_secret, user_id, mnemonic, wid).await;
        // }
    });
}

const NETWORK: &str = "devnet";

async fn node_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client
        .node_get_by_id(&"2anfZwrzskXiUHPLTqH1veJAia8G6rW2eFLy5YFMa1MP".to_string())
        .await;
    match result {
        Ok(node) => {
            println!("{:?}", node);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn nodes_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client.nodes_get().await;
    match result {
        Ok(nodes) => {
            println!("{:?}", nodes);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn nodes_filter(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client.nodes_filter(Some(1), 0, 0, 0, 0, Some(false)).await;
    match result {
        Ok(nodes) => {
            println!("{:?}", nodes);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}
async fn workload_decommission(stellar_secret: String, user_id: i64, mnemonic: String, wid: i64) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client.workload_decommission(wid).await;
    match result {
        Ok(decommissioned) => {
            println!("{:?}", decommissioned);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}
async fn workload_poll(stellar_secret: String, user_id: i64, mnemonic: String, id: i64) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client.workload_poll(id, 1 * 60).await;
    match result {
        Ok(v) => {
            println!("workload deployed: {:?}", v);
            let result = &v.result.unwrap();
            if let serde_json::Value::Array(ips) = &result.data_json["IPs"] {
                println!("first ip is {}", ips[0])
            }
        }
        Err(err) => match err {
            grid_explorer_client::ExplorerError::WorkloadFailedError(v) => {
                println!("couldn't deploy workload: {}", v);
            }
            grid_explorer_client::ExplorerError::ExplorerClientError(v) => {
                println!("client error: {}", v);
            }
            grid_explorer_client::ExplorerError::WorkloadTimeoutError(v) => {
                println!("workload timeout: {}", v);
            }
            _ => {
                println!("another error: {:?}", err);
            }
        },
    }
}

async fn farm_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client.farm_get_by_id(1).await;
    match result {
        Ok(node) => {
            println!("{:?}", node);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn farms_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client.farms_get().await;
    match result {
        Ok(nodes) => {
            println!("{:?}", nodes);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn workload_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client.workload_get_by_id(49975).await;
    match result {
        Ok(workload) => {
            println!("{:?}", workload);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn pool_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client.pool_get_by_id(2).await;
    match result {
        Ok(pool) => {
            println!("{:?}", pool);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn pool_create(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);

    let data = grid_explorer_client::reservation::ReservationData {
        pool_id: 0,
        cus: 1,
        sus: 1,
        ipv4us: 0,
        node_ids: vec![String::from("26ZATmd3K1fjeQKQsi8Dr7bm9iSRa3ePsV8ubMcbZEuY")],
        currencies: vec![String::from("TFT")],
    };

    let result = client.create_capacity_pool(data).await;
    match result {
        Ok(pool) => {
            println!("{:?}", pool);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn pools_by_owner(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let result = client.pools_by_owner().await;
    match result {
        Ok(pool) => {
            println!("{:?}", pool);
        }
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn zdb_create(
    stellar_secret: String,
    user_id: i64,
    mnemonic: String,
) -> Result<i64, grid_explorer_client::ExplorerError> {
    let user = grid_explorer_client::identity::Identity::new(
        String::from(""),
        String::from(""),
        user_id,
        mnemonic.as_str(),
    )
    .unwrap();

    let client = grid_explorer_client::ExplorerClient::new(NETWORK, stellar_secret.as_str(), user);
    let zdb = grid_explorer_client::workload::ZdbInformationBuilder::new()
        .size(5)
        .mode(grid_explorer_client::workload::ZdbMode::ZdbModeUser)
        .password(String::from(""))
        .disk_type(grid_explorer_client::workload::DiskType::Ssd)
        .public(true)
        .build()?;

    let pool_id = 11124;
    let node_id = String::from("3NAkUYqm5iPRmNAnmLfjwdreqDssvsebj4uPUt9BxFPm");

    let result = client.create_zdb_reservation(node_id, pool_id, zdb).await;
    match result {
        Ok(wid) => {
            println!("{:?}", wid);
            return Ok(wid);
        }
        Err(err) => {
            println!("{:?}", err);
            return Err(err);
        }
    }
}
