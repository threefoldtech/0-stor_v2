mod types;
mod workload;

pub struct ExplorerClient {
    pub url: String
}

pub fn new_explorer_client(url: String) -> ExplorerClient {
    ExplorerClient{
        url: url
    }
}

impl ExplorerClient {
    pub async fn nodes_get(&self) -> Result<Vec<types::Node>, reqwest::Error> {
        let url = format!("{url}/api/v1/nodes", url=self.url); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<Vec<types::Node>>()
            .await?)
    }

    pub async fn node_get_by_id(&self, id: String) -> Result<types::Node, reqwest::Error> {
        let url = format!("{url}/api/v1/nodes/{id}", url=self.url, id=id); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<types::Node>()
            .await?)
    }

    pub async fn farms_get(&self) -> Result<Vec<types::Farm>, reqwest::Error> {
        let url = format!("{url}/api/v1/farms", url=self.url); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<Vec<types::Farm>>()
            .await?)
    }

    pub async fn farm_get_by_id(&self, id: i64) -> Result<types::Farm, reqwest::Error> {
        let url = format!("{url}/api/v1/farms/{id}", url=self.url, id=id); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<types::Farm>()
            .await?)
    }

    pub async fn workload_get_by_id(&self, id: i64) -> Result<workload::Workload, reqwest::Error> {
        let url = format!("{url}/api/v1/reservations/workloads/{id}", url=self.url, id=id); 
        Ok(reqwest::get(url.as_str())
            .await?
            .json::<workload::Workload>()
            .await?)
    }
}