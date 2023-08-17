use crate::shotover_process;
use opensearch::{
    auth::Credentials,
    cert::CertificateValidation,
    cluster::ClusterHealthParts,
    http::Url,
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        StatusCode,
    },
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts},
    params::Refresh,
    BulkOperation, BulkParts, DeleteParts, IndexParts, OpenSearch, SearchParts,
};
use serde_json::{json, Value};
use test_helpers::docker_compose::docker_compose;

pub async fn test_bulk(client: &OpenSearch) {
    client
        .indices()
        .create(IndicesCreateParts::Index("posts"))
        .send()
        .await
        .unwrap();

    let mut body: Vec<BulkOperation<_>> = vec![];
    for i in 1..=10 {
        let op = BulkOperation::index(json!({"title":"OpenSearch"}))
            .id(i.to_string())
            .into();
        body.push(op);
    }

    client
        .bulk(BulkParts::Index("posts"))
        .body(body)
        .refresh(Refresh::WaitFor)
        .send()
        .await
        .unwrap();

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

    assert_eq!(response.status_code(), StatusCode::OK);
    assert_eq!(response.method(), opensearch::http::Method::Post);

    let response_body = response.json::<Value>().await.unwrap();
    assert!(response_body["took"].as_i64().is_some());
    assert_eq!(
        response_body["hits"].as_object().unwrap()["hits"]
            .as_array()
            .unwrap()
            .len(),
        10
    );
}

async fn test_health(client: &OpenSearch) {
    client
        .cluster()
        .health(ClusterHealthParts::None)
        .wait_for_status(opensearch::params::WaitForStatus::Green);

    let response = client
        .cat()
        .health()
        .format("json")
        .pretty(true)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status_code(), StatusCode::OK);
    assert!(response
        .headers()
        .get(opensearch::http::headers::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("application/json"));

    let response_body = response.json::<Value>().await.unwrap();
    assert_eq!(response_body[0]["status"], String::from("green"));
}

async fn test_create_index(client: &OpenSearch) {
    client
        .indices()
        .create(IndicesCreateParts::Index("test-index"))
        .send()
        .await
        .unwrap();

    let exists_response = client
        .indices()
        .exists(IndicesExistsParts::Index(&["test-index"]))
        .send()
        .await
        .unwrap();

    assert_eq!(exists_response.status_code(), StatusCode::OK);
}

async fn test_index_and_search_document(client: &OpenSearch) -> String {
    client
        .index(IndexParts::Index("test-index"))
        .body(json!({
            "name": "John",
            "age": 30
        }))
        .refresh(Refresh::WaitFor)
        .send()
        .await
        .unwrap();

    let response = client
        .search(SearchParts::Index(&["test-index"]))
        .from(0)
        .size(10)
        .body(json!({
            "query": {
                "match": {
                    "name": "John",
                }
            }
        }))
        .send()
        .await
        .unwrap();

    let results = response.json::<Value>().await.unwrap();
    assert!(results["took"].as_i64().is_some());
    assert_eq!(
        results["hits"].as_object().unwrap()["hits"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    results["hits"].as_object().unwrap()["hits"]
        .as_array()
        .unwrap()[0]["_id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn test_delete_and_search_document(client: &OpenSearch, id: String) {
    client
        .delete(DeleteParts::IndexId("test-index", &id))
        .refresh(Refresh::WaitFor)
        .send()
        .await
        .unwrap();

    let response = client
        .search(SearchParts::Index(&["test-index"]))
        .from(0)
        .size(10)
        .body(json!({
            "query": {
                "match": {
                    "name": "John",
                }
            }
        }))
        .allow_no_indices(true)
        .send()
        .await
        .unwrap();

    let results = response.json::<Value>().await.unwrap();
    assert!(results["took"].as_i64().is_some());
    assert_eq!(
        results["hits"].as_object().unwrap()["hits"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

async fn test_delete_index(client: &OpenSearch) {
    client
        .indices()
        .delete(IndicesDeleteParts::Index(&["test-index"]))
        .send()
        .await
        .unwrap();

    let exists_response = client
        .indices()
        .exists(IndicesExistsParts::Index(&["test-index"]))
        .send()
        .await
        .unwrap();

    assert_eq!(exists_response.status_code(), StatusCode::NOT_FOUND);
}

async fn opensearch_test_suite(client: &OpenSearch) {
    test_health(client).await;
    test_create_index(client).await;

    let doc_id = test_index_and_search_document(client).await;
    test_delete_and_search_document(client, doc_id).await;

    test_bulk(client).await;
    test_delete_index(client).await;
    // test_authentication(&client).await;
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

    opensearch_test_suite(&client).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}
