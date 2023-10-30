use crate::shotover_process;
use hyper::{Body as HyperBody, Client, Method as HyperMethod, Request, Response as HyperResponse};
use opensearch::{
    auth::Credentials,
    cert::CertificateValidation,
    cluster::ClusterHealthParts,
    http::{
        headers::{HeaderName, HeaderValue},
        response::Response,
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Method, StatusCode, Url,
    },
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts},
    nodes::NodesInfoParts,
    params::{Refresh, WaitForStatus},
    BulkOperation, BulkParts, DeleteParts, Error, IndexParts, OpenSearch, SearchParts,
};
use serde_json::{json, Value};
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::{EventMatcher, Level};
use tokio::time::Duration;

async fn hyper_request(
    uri: String,
    method: HyperMethod,
    body: HyperBody,
) -> HyperResponse<HyperBody> {
    let client = Client::new();

    let req = Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .expect("request builder");

    client.request(req).await.unwrap()
}

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

async fn get_cluster_name(client: &OpenSearch) -> String {
    let response = assert_ok_and_get_json(
        client
            .cluster()
            .stats(opensearch::cluster::ClusterStatsParts::None)
            .send()
            .await,
    )
    .await;

    response["cluster_name"].as_str().unwrap().to_owned()
}

async fn assert_ok_and_same_data(
    response_a: Result<Response, Error>,
    response_b: Result<Response, Error>,
    warn: bool,
) {
    let mut response_a = assert_ok_and_get_json(response_a).await["hits"]["hits"]
        .as_array()
        .unwrap()
        .clone();
    let mut response_b = assert_ok_and_get_json(response_b).await["hits"]["hits"]
        .as_array()
        .unwrap()
        .clone();

    if !warn {
        assert_eq!(response_a.len(), response_b.len());
    } else if response_a.len() != response_b.len() {
        println!("Response A len: {:#?}", response_a.len());
        println!("Response B len: {:#?}", response_b.len());
    }

    response_a.sort_by(|a, b| {
        let a_age = a["_source"]["age"].as_i64().unwrap();
        let b_age = b["_source"]["age"].as_i64().unwrap();
        a_age.cmp(&b_age)
    });

    response_b.sort_by(|a, b| {
        let a_age = a["_source"]["age"].as_i64().unwrap();
        let b_age = b["_source"]["age"].as_i64().unwrap();
        a_age.cmp(&b_age)
    });

    if !warn {
        assert_eq!(response_a, response_b);
    } else if response_a != response_b {
        println!("Response A: {:#?}", response_a);
        println!("Response B: {:#?}", response_b);
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

fn create_client(addr: &str) -> OpenSearch {
    let url = Url::parse(addr).unwrap();
    let credentials = Credentials::Basic("admin".into(), "admin".into());
    let transport = TransportBuilder::new(SingleNodeConnectionPool::new(url))
        .cert_validation(CertificateValidation::None)
        .auth(credentials)
        .build()
        .unwrap();
    let client = OpenSearch::new(transport);

    client
        .cluster()
        .health(ClusterHealthParts::None)
        .wait_for_status(WaitForStatus::Green);

    client
}

#[tokio::test(flavor = "multi_thread")]
async fn passthrough_standard() {
    let _compose = docker_compose("tests/test-configs/opensearch-passthrough/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/opensearch-passthrough/topology.yaml")
        .start()
        .await;

    let addr = "http://localhost:9201";
    let client = create_client(addr);

    let res = client
        .nodes()
        .info(NodesInfoParts::None)
        .header(
            HeaderName::from_lowercase(b"accept-encoding").unwrap(),
            HeaderValue::from_str("").unwrap(),
        )
        .send()
        .await
        .unwrap();

    println!("{:#?}", res);

    opensearch_test_suite(&client).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dual_write_basic() {
    let _compose = docker_compose("tests/test-configs/opensearch-dual-write/docker-compose.yaml");

    let addr1 = "http://172.16.1.2:9200";
    let client1 = create_client(addr1);
    let addr2 = "http://172.16.1.3:9200";
    let client2 = create_client(addr2);

    let shotover = shotover_process("tests/test-configs/opensearch-dual-write/topology.yaml")
        .start()
        .await;

    let shotover_client = create_client("http://localhost:9200");

    shotover_client
        .indices()
        .create(IndicesCreateParts::Index("test-index"))
        .send()
        .await
        .unwrap();

    let exists_response = shotover_client
        .indices()
        .exists(IndicesExistsParts::Index(&["test-index"]))
        .send()
        .await
        .unwrap();

    assert_eq!(exists_response.status_code(), StatusCode::OK);

    shotover_client
        .index(IndexParts::Index("test-index"))
        .body(json!({
            "name": "John",
            "age": 30
        }))
        .refresh(Refresh::WaitFor)
        .send()
        .await
        .unwrap();

    for client in &[shotover_client, client1, client2] {
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
    }

    shotover.shutdown_and_then_consume_events(&[]).await;
}

async fn index_100_documents(client: &OpenSearch) {
    let mut body: Vec<BulkOperation<_>> = vec![];
    for i in 0..100 {
        let op = BulkOperation::index(json!({
            "name": "John",
            "age": i
        }))
        .id(i.to_string())
        .into();
        body.push(op);
    }

    assert_ok_and_get_json(
        client
            .bulk(BulkParts::Index("test-index"))
            .body(body)
            .refresh(Refresh::WaitFor)
            .send()
            .await,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dual_write_reindex() {
    let shotover_addr = "http://localhost:9200";
    let source_addr = "http://172.16.1.2:9200";
    let target_addr = "http://172.16.1.3:9200";

    let _compose = docker_compose("tests/test-configs/opensearch-dual-write/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/opensearch-dual-write/topology.yaml")
        .start()
        .await;

    let source_client = create_client(source_addr);
    let target_client = create_client(target_addr);
    let shotover_client = create_client(shotover_addr);

    // verify that shotover is returning responses from the source clusters
    let cluster_name = get_cluster_name(&shotover_client).await;
    println!("Shotover is returning responses from {:?}", cluster_name);
    assert_eq!(cluster_name, "source-cluster");

    // Create indexes in source cluster
    assert_ok_and_get_json(
        source_client
            .indices()
            .create(IndicesCreateParts::Index("test-index"))
            .send()
            .await,
    )
    .await;

    // Create in target cluster
    assert_ok_and_get_json(
        target_client
            .indices()
            .create(IndicesCreateParts::Index("test-index"))
            .send()
            .await,
    )
    .await;

    index_100_documents(&source_client).await;

    let shotover_client_c = shotover_client.clone();
    let dual_write_jh = tokio::spawn(async move {
        for _ in 0..20 {
            // get a random number in between 0 and 2000
            let i = rand::random::<u32>() % 100;

            let response = assert_ok_and_get_json(
                shotover_client_c
                    .search(SearchParts::Index(&["test-index"]))
                    .from(0)
                    .size(200)
                    .body(json!({
                        "query": {
                            "match": {
                                "age": i,
                            }
                        }
                    }))
                    .send()
                    .await,
            )
            .await;

            assert_eq!(response["hits"]["hits"].as_array().unwrap().len(), 1);

            let document = &response["hits"]["hits"][0];

            assert_ok_and_get_json(
                shotover_client_c
                    .update(opensearch::UpdateParts::IndexId(
                        "test-index",
                        document["_id"].as_str().unwrap(),
                    ))
                    .body(json!({
                        "doc": { "name" : Value::String("Smith".into())}
                    }))
                    .refresh(Refresh::WaitFor)
                    .send()
                    .await,
            )
            .await;

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    let target_client_c = target_client.clone();
    let reindex_jh = tokio::spawn(async move {
        target_client_c
            .reindex()
            .body(json!(
                {
                    "source":{
                        "remote":{
                            "host": source_addr,
                            "username":"admin",
                            "password":"admin"
                        },
                        "index": "test-index"
                   },
                   "dest":{
                        "index": "test-index",
                   }
                }
            ))
            .requests_per_second(1)
            .send()
            .await
            .unwrap();
    });

    // Begin dual writes
    // Begin reindex operations
    let _ = tokio::join!(reindex_jh, dual_write_jh);

    // verify both clusters end up in the same state
    let target = target_client
        .search(SearchParts::Index(&["test-index"]))
        .from(0)
        .size(200)
        .body(json!({
            "query": {
                "match": {
                    "name": "Smith",
                }
            }
        }))
        .send();

    let source = source_client
        .search(SearchParts::Index(&["test-index"]))
        .from(0)
        .size(200)
        .body(json!({
            "query": {
                "match": {
                    "name": "Smith",
                }
            }
        }))
        .send();

    assert_ok_and_same_data(target.await, source.await, true).await;

    // verify both clusters end up in the same state
    let target = target_client
        .search(SearchParts::Index(&["test-index"]))
        .from(0)
        .size(200)
        .body(json!({
            "query": {
                "match": {
                    "name": "John",
                }
            }
        }))
        .send();

    let source = source_client
        .search(SearchParts::Index(&["test-index"]))
        .from(0)
        .size(200)
        .body(json!({
            "query": {
                "match": {
                    "name": "John",
                }
            }
        }))
        .send();

    assert_ok_and_same_data(target.await, source.await, true).await;

    // switch shotover to the target cluster
    let _ = hyper_request(
        format!("http://localhost:{}/transform/tee/result-source", 1234),
        HyperMethod::PUT,
        HyperBody::from("tee-chain"),
    )
    .await;
    let cluster_name = get_cluster_name(&shotover_client).await;
    println!("Shotover is returning responses from {:?}", cluster_name);
    assert_eq!(cluster_name, "target-cluster");

    shotover
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::tee")])
        .await;
}
