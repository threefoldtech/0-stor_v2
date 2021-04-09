use grid_explorer_client;
use tokio::runtime::Runtime;
use std::env;

fn main() {
    // Create the runtime
    let rt = Runtime::new().unwrap();

    let args: Vec<String> = env::args().collect();

    let stellar_secret = args[1].clone();
    let user_id: i64 = args[2].parse().unwrap();
    let mnemonic = args[3].clone();

    // Execute the future, blocking the current thread until completion
    rt.block_on(async {
        // node_get().await;
        // nodes_get().await;
        // farms_get().await;
        // farm_get().await;
        // workload_get().await;
        // pool_get(stellar_secret, user_id, mnemonic).await;
        pool_create(stellar_secret, user_id, mnemonic).await;
    });
}

const NETWORK: &str = "testnet";

async fn node_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::new(String::from(""), String::from(""), user_id, mnemonic.as_str()).unwrap();

    let client = grid_explorer_client::new_explorer_client(NETWORK, stellar_secret.as_str(), user);
    let result = client.node_get_by_id("2anfZwrzskXiUHPLTqH1veJAia8G6rW2eFLy5YFMa1MP".to_string()).await;
    match result {
        Ok(node) => {
            println!("{:?}", node);
        },
        Err(err) => {
            println!("{:?}", err)
        }
    }
} 

async fn nodes_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::new(String::from(""), String::from(""), user_id, mnemonic.as_str()).unwrap();

    let client = grid_explorer_client::new_explorer_client(NETWORK, stellar_secret.as_str(), user);
    let result = client.nodes_get().await;
    match result {
        Ok(nodes) => {
            println!("{:?}", nodes);
        },
        Err(err) => {
            println!("{:?}", err)
        }
    }
} 

async fn farm_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::new(String::from(""), String::from(""), user_id, mnemonic.as_str()).unwrap();

    let client = grid_explorer_client::new_explorer_client(NETWORK, stellar_secret.as_str(), user);
    let result = client.farm_get_by_id(1).await;
    match result {
        Ok(node) => {
            println!("{:?}", node);
        },
        Err(err) => {
            println!("{:?}", err)
        }
    }
} 

async fn farms_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::new(String::from(""), String::from(""), user_id, mnemonic.as_str()).unwrap();

    let client = grid_explorer_client::new_explorer_client(NETWORK, stellar_secret.as_str(), user);
    let result = client.farms_get().await;
    match result {
        Ok(nodes) => {
            println!("{:?}", nodes);
        },
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn workload_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::new(String::from(""), String::from(""), user_id, mnemonic.as_str()).unwrap();

    let client = grid_explorer_client::new_explorer_client(NETWORK, stellar_secret.as_str(), user);
    let result = client.workload_get_by_id(28338).await;
    match result {
        Ok(workload) => {
            println!("{:?}", workload);
        },
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn pool_get(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::new(String::from(""), String::from(""), user_id, mnemonic.as_str()).unwrap();

    let client = grid_explorer_client::new_explorer_client(NETWORK, stellar_secret.as_str(), user);
    let result = client.pool_get_by_id(2).await;
    match result {
        Ok(pool) => {
            println!("{:?}", pool);
        },
        Err(err) => {
            println!("{:?}", err)
        }
    }
}

async fn pool_create(stellar_secret: String, user_id: i64, mnemonic: String) {
    let user = grid_explorer_client::identity::new(String::from(""), String::from(""), user_id, mnemonic.as_str()).unwrap();

    let client = grid_explorer_client::new_explorer_client(NETWORK, stellar_secret.as_str(), user);

    let data = grid_explorer_client::reservation::ReservationData{
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
        },
        Err(err) => {
            println!("{:?}", err)
        }
    }
}