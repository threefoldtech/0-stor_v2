use super::identity;

pub fn create_header(
    id: &identity::Identity,
    date: &chrono::DateTime<chrono::Utc>,
    date_str: &String,
) -> String {
    let created = date.timestamp();

    let sig_str = format!(
        "(created): {}\ndate: {}\nthreebot-id: {}",
        created,
        date_str,
        id.get_id(),
    );

    let sig = id.sign(sig_str.as_ref());

    format!(
        r#"Signature keyId="{}",algorithm="ed25519",created="{}",headers="(created) date threebot-id",signature="{}""#,
        id.get_id(),
        created,
        base64::encode(&sig)
    )
}
