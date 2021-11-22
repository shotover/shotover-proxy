use cassandra_cpp::{Cluster, Session};

mod basic_driver_tests;

pub fn cassandra_connection(contact_points: &str, port: u16) -> Session {
    for contact_point in contact_points.split(',') {
        crate::helpers::wait_for_socket_to_open(contact_point, port);
    }
    let mut cluster = Cluster::default();
    cluster.set_contact_points(contact_points).unwrap();
    cluster.set_port(port).ok();
    cluster.set_load_balance_round_robin();
    cluster.connect().unwrap()
}
