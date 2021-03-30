use stellar_base::amount::{Amount, Stroops};
use stellar_base::asset::Asset;
use stellar_base::crypto::{KeyPair, PublicKey};
use stellar_base::memo::Memo;
use stellar_base::network::Network;
use stellar_base::operations::Operation;
use stellar_base::transaction::{Transaction, MIN_BASE_FEE};
use stellar_base::xdr::XDRSerialize;
use stellar_horizon::api;
use stellar_horizon::client::{HorizonClient, HorizonHttpClient};
use std::str::FromStr;
use super::reservation;

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

pub async fn pay_capacity_pool(keypair: KeyPair, capacity_pool_information: reservation::CapacityPoolCreateResponse) -> Result<String, super::ExplorerError> {
    let destination: PublicKey = PublicKey::from_account_id(capacity_pool_information.escrow_information.address.as_str())?;

    let amount_in_stroops = Stroops::new(capacity_pool_information.escrow_information.amount);
    let payment_amount = Amount::from_stroops(&amount_in_stroops)?;    

    let tft_asset_string: Vec<&str> = capacity_pool_information.escrow_information.asset.split(":").collect();
    let issuer = PublicKey::from_str(tft_asset_string[1])?;
    let tft_asset = Asset::new_credit(tft_asset_string[0], issuer)?;

    let payment = Operation::new_payment()
        .with_destination(destination.clone())
        .with_amount(payment_amount)?
        .with_asset(tft_asset)
        .build()?;

    let client = HorizonHttpClient::new_from_str("https://horizon.stellar.org")?;
    let request = api::accounts::single(&keypair.public_key().clone());
    let (_headers, response) = client.request(request).await?;

    let sequence = response.sequence.parse::<i64>().unwrap();

    // memo should be "p-reservation_id"
    let memo = Memo::new_text(format!("p-{:?}", capacity_pool_information.id))?;
    let mut tx = Transaction::builder(keypair.public_key().clone(), sequence, MIN_BASE_FEE)
        .with_memo(memo)
        .add_operation(payment)
        .into_transaction()?;

    tx.sign(&keypair, &Network::new_test())?;
    let xdr = tx.into_envelope().xdr_base64()?;

    Ok(xdr)
}