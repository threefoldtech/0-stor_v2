use grid_explorer_client;
use tokio::runtime::Runtime;

fn main() {
    // Create the runtime
    let rt = Runtime::new().unwrap();

    // Execute the future, blocking the current thread until completion
    rt.block_on(async {
        // node_get().await;
        // nodes_get().await;
        // farms_get().await;
        // farm_get().await;
        workload_get().await;
    });
}

async fn node_get() {
    let client = grid_explorer_client::new_explorer_client("https://explorer.devnet.grid.tf".to_string());
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

async fn nodes_get() {
    let client = grid_explorer_client::new_explorer_client("https://explorer.devnet.grid.tf".to_string());
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

async fn farm_get() {
    let client = grid_explorer_client::new_explorer_client("https://explorer.devnet.grid.tf".to_string());
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

async fn farms_get() {
    let client = grid_explorer_client::new_explorer_client("https://explorer.devnet.grid.tf".to_string());
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

async fn workload_get() {
    let client = grid_explorer_client::new_explorer_client("https://explorer.testnet.grid.tf".to_string());
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