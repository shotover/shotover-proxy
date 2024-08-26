use crate::run_command;
use rcgen::{BasicConstraints, Certificate, CertificateParams, DnType, IsCa, KeyPair, SanType};
use std::path::Path;

pub fn generate_test_certs(path: &Path) {
    generate_test_certs_with_sans(
        path,
        vec![
            // Just dump every address we could ever need in here.
            // Usually you would want unique certs per instance but this works just fine for integration testing.
            SanType::DnsName("localhost".try_into().unwrap()),
            SanType::DnsName("kafka0".try_into().unwrap()),
            SanType::DnsName("kafka1".try_into().unwrap()),
            SanType::DnsName("kafka2".try_into().unwrap()),
            SanType::DnsName("kafka3".try_into().unwrap()),
            SanType::DnsName("kafka4".try_into().unwrap()),
            SanType::DnsName("kafka5".try_into().unwrap()),
            SanType::IpAddress("127.0.0.1".parse().unwrap()),
            SanType::IpAddress("172.16.1.2".parse().unwrap()),
            SanType::IpAddress("172.16.1.3".parse().unwrap()),
            SanType::IpAddress("172.16.1.4".parse().unwrap()),
            SanType::IpAddress("172.16.1.5".parse().unwrap()),
            SanType::IpAddress("172.16.1.6".parse().unwrap()),
            SanType::IpAddress("172.16.1.7".parse().unwrap()),
        ],
    );
}

/// used for testing `verify_hostname: false` config
pub fn generate_test_certs_with_bad_san(path: &Path) {
    generate_test_certs_with_sans(path, vec![]);
}

pub fn generate_test_certs_with_sans(path: &Path, sans: Vec<SanType>) {
    let (ca_cert, ca_key) = new_ca();
    let (cert, cert_key) = new_cert(sans, &ca_cert, &ca_key);

    std::fs::create_dir_all(path).unwrap();
    std::fs::write(path.join("localhost_CA.crt"), ca_cert.pem()).unwrap();
    std::fs::write(path.join("localhost.crt"), cert.pem()).unwrap();
    std::fs::write(path.join("localhost.key"), cert_key.serialize_pem()).unwrap();
}

fn new_ca() -> (Certificate, KeyPair) {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    // This must be "Certificate Authority"
    params
        .distinguished_name
        .push(DnType::CommonName, "Certificate Authority");
    // This can be whatever
    params
        .distinguished_name
        .push(DnType::OrganizationName, "ShotoverTestCertificate");

    let key_pair = KeyPair::generate().unwrap();
    let ca_cert = params.self_signed(&key_pair).unwrap();
    (ca_cert, key_pair)
}

fn new_cert(sans: Vec<SanType>, ca_cert: &Certificate, ca_key: &KeyPair) -> (Certificate, KeyPair) {
    let mut params = CertificateParams::default();

    // This needs to refer to the hosts that certificate will be used by
    params.subject_alt_names = sans;
    // This can be whatever
    params
        .distinguished_name
        .push(DnType::CommonName, "Generic-Cert");
    // This can be whatever
    params
        .distinguished_name
        .push(DnType::OrganizationName, "ShotoverTestCertificate");
    let cert_key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&cert_key, ca_cert, ca_key).unwrap();
    (cert, cert_key)
}

pub fn generate_cassandra_test_certs() {
    let path = Path::new("tests/test-configs/cassandra/tls/certs");
    generate_test_certs(path);
    std::fs::remove_file(path.join("keystore.p12")).ok();
    run_command(
        "openssl",
        &[
            "pkcs12",
            "-export",
            "-out",
            path.join("keystore.p12").to_str().unwrap(),
            "-inkey",
            path.join("localhost.key").to_str().unwrap(),
            "-in",
            path.join("localhost.crt").to_str().unwrap(),
            "-passout",
            "pass:password",
        ],
    )
    .unwrap();
}

pub fn generate_redis_test_certs() {
    generate_test_certs(Path::new("tests/test-configs/redis/tls/certs"));
}

pub fn generate_kafka_test_certs() {
    let path = Path::new("tests/test-configs/kafka/tls/certs");
    generate_test_certs(path);

    // create keystore
    std::fs::remove_file(path.join("kafka.keystore.p12")).ok();
    std::fs::remove_file(path.join("kafka.keystore.jks")).ok();
    run_command(
        "openssl",
        &[
            "pkcs12",
            "-export",
            "-out",
            path.join("kafka.keystore.p12").to_str().unwrap(),
            "-inkey",
            path.join("localhost.key").to_str().unwrap(),
            "-in",
            path.join("localhost.crt").to_str().unwrap(),
            "-passout",
            "pass:password",
        ],
    )
    .unwrap();
    run_command(
        "keytool",
        &[
            "-importkeystore",
            "-srckeystore",
            path.join("kafka.keystore.p12").to_str().unwrap(),
            "-srcstoretype",
            "pkcs12",
            "-destkeystore",
            path.join("kafka.keystore.jks").to_str().unwrap(),
            "-deststoretype",
            "JKS",
            "-storepass",
            "password",
            "-srcstorepass",
            "password",
        ],
    )
    .unwrap();

    // create truststore
    std::fs::remove_file(path.join("kafka.truststore.jks")).ok();
    run_command(
        "keytool",
        &[
            "-import",
            "-noprompt",
            "-file",
            path.join("localhost_CA.crt").to_str().unwrap(),
            "-keystore",
            path.join("kafka.truststore.jks").to_str().unwrap(),
            "-storepass",
            "password",
        ],
    )
    .unwrap();
}
