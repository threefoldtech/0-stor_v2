use stellar_base::amount::{Amount, Stroops};
use stellar_base::asset::Asset;
use stellar_base::crypto::{KeyPair, PublicKey};
use stellar_base::memo::Memo;
use stellar_base::network::Network;
use stellar_base::operations::Operation;
use stellar_base::transaction::{Transaction, MIN_BASE_FEE};
use stellar_base::xdr::XDRSerialize;
use stellar_base::error::Error;
use super::reservation;

pub fn pay_capacity_pool(keypair: KeyPair, escrow_information: reservation::EscrowInformation) -> Result<String, Error> {
    let destination: PublicKey = PublicKey::from_account_id(escrow_information.address.as_str())?;

    let amount_in_stroops = Stroops::new(escrow_information.amount);
    let payment_amount = Amount::from_stroops(&amount_in_stroops)?;

    let payment = Operation::new_payment()
        .with_destination(destination.clone())
        .with_amount(payment_amount)?
        .with_asset(Asset::new_native())
        .build()?;

    let mut tx = Transaction::builder(keypair.public_key().clone(), 1234, MIN_BASE_FEE)
        .with_memo(Memo::new_id(7483792))
        .add_operation(payment)
        .into_transaction()?;

    tx.sign(&keypair, &Network::new_test())?;
    let xdr = tx.into_envelope().xdr_base64()?;

    Ok(xdr)
}