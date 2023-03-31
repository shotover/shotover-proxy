use crate::docker_compose::run_command;
use rcgen::{BasicConstraints, Certificate, CertificateParams, DnType, IsCa};
use std::path::Path;

pub fn generate_redis_test_certs(path: &Path) {
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

    let mut params = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]);
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
    let path = Path::new("example-configs/docker-images/cassandra-tls-4.0.6/certs");
    generate_redis_test_certs(path);
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
