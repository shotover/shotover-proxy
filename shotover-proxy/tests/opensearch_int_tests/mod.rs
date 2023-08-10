use crate::shotover_process;
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

pub async fn index_documents(client: &OpenSearch) -> Response {
    let index = "posts";
    let exists_response = client
        .indices()
        .exists(IndicesExistsParts::Index(&[index]))
        .send()
        .await
        .unwrap();

    if exists_response.status_code() != StatusCode::NOT_FOUND {
        return exists_response;
    }

    assert_eq!(exists_response.status_code(), StatusCode::NOT_FOUND);

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
}

#[tokio::test(flavor = "multi_thread")]
async fn passthrough_standard() {
    let _compose = docker_compose("tests/test-configs/opensearch-passthrough/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/opensearch-passthrough/topology.yaml")
        .start()
        .await;

    let addr = "http://localhost:9201";

    let url = Url::parse(addr).unwrap();
    let credentials = Credentials::Basic("admin".into(), "admin".into());
    let transport = TransportBuilder::new(SingleNodeConnectionPool::new(url))
        .cert_validation(CertificateValidation::None)
        .auth(credentials)
        .build()
        .unwrap();
    let client = OpenSearch::new(transport);

    index_documents(&client).await;

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

    assert_eq!(
        response.url(),
        &Url::parse(format!("{}/_search?allow_no_indices=true", addr).as_str()).unwrap()
    );
    assert_eq!(response.status_code(), StatusCode::OK);
    assert_eq!(response.method(), opensearch::http::Method::Post);

    let response_body = response.json::<Value>().await.unwrap();
    println!("{:?}", response_body);
    assert!(response_body["took"].as_i64().is_some());
    assert_eq!(
        response_body["hits"].as_object().unwrap()["hits"]
            .as_array()
            .unwrap()
            .len(),
        10
    );

    shotover.shutdown_and_then_consume_events(&[]).await;
}
