use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

mod test_cases;

#[tokio::test]
#[serial]
async fn basic() {
    let _docker_compose = DockerCompose::new("tests/test-configs/kafka-simple/docker-compose.yaml");
    test_cases::basic().await;
}
