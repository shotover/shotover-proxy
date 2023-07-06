use aws_config::SdkConfig;

pub async fn user_name(config: &SdkConfig) -> String {
    let client = aws_sdk_iam::Client::new(config);
    client
        .get_user()
        .send()
        .await
        .map_err(|e| e.into_service_error())
        .unwrap()
        .user()
        .unwrap()
        .user_name()
        .unwrap()
        .to_string()
}
