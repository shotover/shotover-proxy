use crate::shotover_process;
use opensearch::{
    auth::Credentials,
    cert::CertificateValidation,
    http::{
        headers::{HeaderName, HeaderValue},
        response::Response,
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Method, StatusCode, Url,
    },
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts},
    nodes::NodesInfoParts,
    params::Refresh,
    BulkOperation, BulkParts, DeleteParts, Error, IndexParts, OpenSearch, SearchParts,
};
use pretty_assertions::assert_eq;
use serde_json::{json, Value};
use test_helpers::docker_compose::docker_compose;

async fn assert_ok_and_get_json(response: Result<Response, Error>) -> Value {
    let response = response.unwrap();
    let status = response.status_code();

    if response.method() == Method::Head {
        if status != StatusCode::OK {
            panic!("Opensearch HEAD query returned status code {status}");
        }
        Value::Null
    } else {
        let json = response.json().await.unwrap();
        if status != StatusCode::OK && status != StatusCode::CREATED {
            panic!("Opensearch query failed: {status:#?}\n{json:#?}");
        }
        json
    }
}

pub async fn test_bulk(client: &OpenSearch) {
    assert_ok_and_get_json(
        client
            .indices()
            .create(IndicesCreateParts::Index("posts"))
            .send()
            .await,
    )
    .await;

    let mut body: Vec<BulkOperation<_>> = vec![];
    for i in 0..10 {
        let op = BulkOperation::index(json!({"title": "OpenSearch", "i": i}))
            .id(i.to_string())
            .into();
        body.push(op);
    }

    assert_ok_and_get_json(
        client
            .bulk(BulkParts::Index("posts"))
            .body(body)
            .refresh(Refresh::WaitFor)
            .send()
            .await,
    )
    .await;

    let results = assert_ok_and_get_json(
        client
            .search(SearchParts::None)
            .body(json!({
                "query": {
                    "match_all": {}
                },
                "sort": [
                    {
                        "i": {
                            "order": "asc"
                        }
                    }
                ]
            }))
            .allow_no_indices(true)
            .send()
            .await,
    )
    .await;

    assert!(results["took"].is_i64());
    let hits = results["hits"]["hits"].as_array().unwrap();
    assert_eq!(
        hits.iter().map(|x| &x["_source"]).collect::<Vec<_>>(),
        vec!(
            &json!({ "title": "OpenSearch", "i": 0 }),
            &json!({ "title": "OpenSearch", "i": 1 }),
            &json!({ "title": "OpenSearch", "i": 2 }),
            &json!({ "title": "OpenSearch", "i": 3 }),
            &json!({ "title": "OpenSearch", "i": 4 }),
            &json!({ "title": "OpenSearch", "i": 5 }),
            &json!({ "title": "OpenSearch", "i": 6 }),
            &json!({ "title": "OpenSearch", "i": 7 }),
            &json!({ "title": "OpenSearch", "i": 8 }),
            &json!({ "title": "OpenSearch", "i": 9 }),
        )
    );
}

async fn test_create_index(client: &OpenSearch) {
    assert_ok_and_get_json(
        client
            .indices()
            .create(IndicesCreateParts::Index("test-index"))
            .send()
            .await,
    )
    .await;

    assert_ok_and_get_json(
        client
            .indices()
            .exists(IndicesExistsParts::Index(&["test-index"]))
            .send()
            .await,
    )
    .await;
}

async fn test_index_and_search_document(client: &OpenSearch) -> String {
    assert_ok_and_get_json(
        client
            .index(IndexParts::Index("test-index"))
            .body(json!({
                "name": "John",
                "age": 30
            }))
            .refresh(Refresh::WaitFor)
            .send()
            .await,
    )
    .await;

    let response = assert_ok_and_get_json(
        client
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
            .await,
    )
    .await;

    assert!(response["took"].is_i64());
    let hits = response["hits"]["hits"].as_array().unwrap();
    assert_eq!(
        hits.iter().map(|x| &x["_source"]).collect::<Vec<_>>(),
        vec!(&json!({
            "name": "John",
            "age": 30,
        }))
    );
    hits[0]["_id"].as_str().unwrap().to_owned()
}

async fn test_delete_and_search_document(client: &OpenSearch, id: String) {
    assert_ok_and_get_json(
        client
            .delete(DeleteParts::IndexId("test-index", &id))
            .refresh(Refresh::WaitFor)
            .send()
            .await,
    )
    .await;

    let response = assert_ok_and_get_json(
        client
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
            .await,
    )
    .await;

    // let results = response.json::<Value>().await.unwrap();
    assert!(response["took"].is_i64());
    assert_eq!(response["hits"]["hits"].as_array().unwrap().len(), 0);
}

async fn test_delete_index(client: &OpenSearch) {
    assert_ok_and_get_json(
        client
            .indices()
            .delete(IndicesDeleteParts::Index(&["test-index"]))
            .send()
            .await,
    )
    .await;

    let exists_response = client
        .indices()
        .exists(IndicesExistsParts::Index(&["test-index"]))
        .send()
        .await
        .unwrap();

    assert_eq!(exists_response.status_code(), StatusCode::NOT_FOUND);
}

async fn opensearch_test_suite(client: &OpenSearch) {
    test_create_index(client).await;

    let doc_id = test_index_and_search_document(client).await;
    test_delete_and_search_document(client, doc_id).await;

    test_bulk(client).await;
    test_delete_index(client).await;

    // request a large message without compression that has to be processed in multiple batches on the codec side
    let _ = assert_ok_and_get_json(
        client
            .nodes()
            .info(NodesInfoParts::None)
            .header(
                HeaderName::from_lowercase(b"accept-encoding").unwrap(),
                HeaderValue::from_str("").unwrap(),
            )
            .send()
            .await,
    )
    .await;
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
