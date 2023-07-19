use opensearch::{
    auth::Credentials,
    cert::CertificateValidation,
    http::Url,
    http::{
        response::Response,
        transport::{SingleNodeConnectionPool, TransportBuilder},
        StatusCode,
    },
    indices::IndicesExistsParts,
    params::Refresh,
    BulkOperation, BulkParts, OpenSearch, SearchParts,
};
use serde_json::{json, Value};
use test_helpers::docker_compose::docker_compose;
use test_helpers::run_command;

pub async fn index_documents(client: &OpenSearch) -> Response {
    let index = "posts";
    let exists_response = client
        .indices()
        .exists(IndicesExistsParts::Index(&[index]))
        .send()
        .await
        .unwrap();

    if exists_response.status_code() == StatusCode::NOT_FOUND {
        let mut body: Vec<BulkOperation<_>> = vec![];
        for i in 1..=10 {
            let op = BulkOperation::index(json!({"title":"OpenSearch"}))
                .id(i.to_string())
                .into();
            body.push(op);
        }

        client
            .bulk(BulkParts::Index(index))
            .body(body)
            .refresh(Refresh::WaitFor)
            .send()
            .await
            .unwrap()
    } else {
        exists_response
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn passthrough_standard() {
    // This setting changes the maximum number of memory map areas a process may have.
    // OpenSearch requires vm.max_map_count to be set higher to run.
    // This command sets it temporarily higher (for the length of the user session), it is reset after rebooting.
    if &run_command("sysctl", &["vm.max_map_count"]).unwrap() != "vm.max_map_count = 262144\n" {
        run_command("sudo", &["sysctl", "-w", "vm.max_map_count=262144"]).unwrap();
    };

    let _compose = docker_compose("tests/test-configs/opensearch-passthrough/docker-compose.yaml");

    let url = Url::parse("https://localhost:9200").unwrap();
    let credentials = Credentials::Basic("admin".into(), "admin".into());
    let transport = TransportBuilder::new(SingleNodeConnectionPool::new(url))
        .cert_validation(CertificateValidation::None)
        .auth(credentials)
        .build()
        .unwrap();
    let client = OpenSearch::new(transport);

    let _ = index_documents(&client).await;
    let response = client
        .search(SearchParts::None)
        .body(json!({
            "query": {
                "match_all": {}
            }
        }))
        .allow_no_indices(true)
        .send()
        .await
        .unwrap();

    let expected_url = {
        let addr = "https://localhost:9200/";
        let mut url = Url::parse(addr).unwrap();
        url.set_username("").unwrap();
        url.set_password(None).unwrap();
        url.join("_search?allow_no_indices=true").unwrap()
    };

    if let Some(c) = response.content_length() {
        assert!(c > 0)
    };

    assert_eq!(response.url(), &expected_url);
    assert_eq!(response.status_code(), StatusCode::OK);
    assert_eq!(response.method(), opensearch::http::Method::Post);
    let debug = format!("{:?}", &response);
    assert!(debug.contains("method"));
    assert!(debug.contains("status_code"));
    assert!(debug.contains("headers"));
    let response_body = response.json::<Value>().await.unwrap();
    assert!(response_body["took"].as_i64().is_some());
}
