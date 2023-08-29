use crate::run_command;
use rcgen::{BasicConstraints, Certificate, CertificateParams, DnType, IsCa, SanType};
use std::path::Path;

pub fn generate_test_certs(path: &Path) {
    generate_test_certs_with_sans(
        path,
        vec![
            // Just dump every address we could ever need in here.
            // Usually you would want unique certs per instance but this works just fine for integration testing.
            SanType::DnsName("localhost".into()),
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
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    // This must be "Certificate Authority"
    params
        .distinguished_name
        .push(DnType::CommonName, "Certificate Authority");
    // This can be whatever
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Shotover test certificate");

    let ca_cert = Certificate::from_params(params).unwrap();

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
        .push(DnType::OrganizationName, "Shotover test certificate");
    let cert = Certificate::from_params(params).unwrap();

    std::fs::create_dir_all(path).unwrap();
    std::fs::write(
        path.join("localhost_CA.crt"),
        ca_cert.serialize_pem().unwrap(),
    )
    .unwrap();
    std::fs::write(
        path.join("localhost.crt"),
        cert.serialize_pem_with_signer(&ca_cert).unwrap(),
    )
    .unwrap();
    std::fs::write(path.join("localhost.key"), cert.serialize_private_key_pem()).unwrap();
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
