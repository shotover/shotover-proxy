use cassandra_cpp::*;
use std::thread::sleep;
use std::time::Duration;
use tracing::info;

mod basic_driver_tests;

pub struct CassandraTestContext {
    pub session: Session,
}

impl CassandraTestContext {
    pub fn new() -> Self {
        CassandraTestContext::new_with_points("127.0.0.1")
    }

    pub fn new_with_points(contact_points: &str) -> Self{ CassandraTestContext::new_with_points_and_port( contact_points, 9042 )}

    pub fn new_with_points_and_port(contact_points: &str,  port : u16) -> Self
    {

        let mut cluster = Cluster::default();
        cluster.set_contact_points(contact_points).unwrap();
        cluster.set_port(port);
        cluster.set_load_balance_round_robin();
        let mut session;
        //let query = stmt!("SELECT keyspace_name FROM system_schema.keyspaces;");
        let query = stmt!("SELECT release_version FROM system.local");

        let attempts = 30;
        let mut current_attempt = 0;

        loop {
            current_attempt += 1;
            info!("attempt {}", current_attempt);
            if current_attempt > attempts {
                panic!("Could not connect!")
            }
            let millisecond = Duration::from_millis(100 * current_attempt);

            match cluster.connect() {
                Ok(conn) => {
                    session = conn;
                    match session.execute(&query).wait() {
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            info!(
                                "Could not execute dummy query {}, retrying again - retries {}",
                                e, current_attempt
                            );
                            sleep(millisecond);
                        }
                    }
                }
                Err(e) => {
                    info!(
                        "Could not connect {}, retrying again - retries {}",
                        e, current_attempt
                    );
                    sleep(millisecond);
                }
            }
        }

        CassandraTestContext { session }
    }
}
