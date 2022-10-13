use rcgen::{BasicConstraints, Certificate, CertificateParams, DnType, IsCa};
use std::path::Path;
use std::process::Command;

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
    std::fs::write(path.join("ca.crt"), ca_cert.serialize_pem().unwrap()).unwrap();
    std::fs::write(
        path.join("redis.crt"),
        cert.serialize_pem_with_signer(&ca_cert).unwrap(),
    )
    .unwrap();
    std::fs::write(path.join("redis.key"), cert.serialize_private_key_pem()).unwrap();
}

pub fn generate_cassandra_test_certs() {
    Command::new("example-configs/docker-images/cassandra-tls-4.0.6/certs/gen_certs.sh")
        .output()
        .unwrap();
}
