use super::reservation;
use crate::types::GridNetwork;
use std::str::FromStr;
use stellar_base::amount::{Amount, Stroops};
use stellar_base::asset::Asset;
use stellar_base::crypto::{KeyPair, PublicKey};
use stellar_base::memo::Memo;
use stellar_base::network::Network;
use stellar_base::operations::Operation;
use stellar_base::transaction::{Transaction, MIN_BASE_FEE};
use stellar_horizon::api;
use stellar_horizon::client::{HorizonClient, HorizonHttpClient};

impl From<stellar_base::error::Error> for super::ExplorerError {
    fn from(err: stellar_base::error::Error) -> super::ExplorerError {
        super::ExplorerError::StellarError(err)
    }
}

impl From<stellar_horizon::error::Error> for super::ExplorerError {
    fn from(err: stellar_horizon::error::Error) -> super::ExplorerError {
        super::ExplorerError::HorizonError(err)
    }
}

pub struct StellarClient {
    pub network: GridNetwork,
    pub keypair: KeyPair,
}

impl StellarClient {
    pub async fn pay_capacity_pool(
        &self,
        capacity_pool_information: reservation::CapacityPoolCreateResponse,
    ) -> Result<bool, super::ExplorerError> {
        let destination: PublicKey = PublicKey::from_account_id(
            capacity_pool_information
                .escrow_information
                .address
                .as_str(),
        )?;

        let amount_in_stroops = Stroops::new(capacity_pool_information.escrow_information.amount);
        let payment_amount = Amount::from_stroops(&amount_in_stroops)?;

        let tft_asset_string: Vec<&str> = capacity_pool_information
            .escrow_information
            .asset
            .split(':')
            .collect();
        let issuer = PublicKey::from_str(tft_asset_string[1])?;
        let tft_asset = Asset::new_credit(tft_asset_string[0], issuer)?;

        let payment = Operation::new_payment()
            .with_destination(destination.clone())
            .with_amount(payment_amount)?
            .with_asset(tft_asset)
            .build()?;

        let client = HorizonHttpClient::new_from_str(self.get_horizon_url())?;
        let request = api::accounts::single(&self.keypair.public_key().clone());
        let (_headers, response) = client.request(request).await?;

        // get sequence number and increment
        let sequence = response.sequence.parse::<i64>().unwrap() + 1;

        // memo should be "p-reservation_id"
        let memo = Memo::new_text(format!("p-{}", capacity_pool_information.reservation_id))?;
        let mut tx =
            Transaction::builder(self.keypair.public_key().clone(), sequence, MIN_BASE_FEE)
                .with_memo(memo)
                .add_operation(payment)
                .into_transaction()?;

        tx.sign(&self.keypair, &self.get_network())?;
        let tx_envelope = tx.into_envelope();

        let res = api::transactions::submit(&tx_envelope)?;

        let (_, response) = client.request(res).await?;

        Ok(response.successful)
    }

    fn get_network(&self) -> Network {
        Network::new_public()
    }

    fn get_horizon_url(&self) -> &str {
        "https://horizon.stellar.org"
    }
}
