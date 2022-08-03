use crate::helpers::cassandra::{assert_query_result, execute_query, ResultValue};
use cassandra_cpp::Session;

pub fn test_rewrite_port(normal_connection: &Session, rewrite_port_connection: &Session) {
    {
        assert_query_result(
            normal_connection,
            "SELECT data_center, native_port, rack FROM system.peers_v2;",
            &[&[
                ResultValue::Varchar("dc1".into()),
                ResultValue::Int(9042),
                ResultValue::Varchar("West".into()),
            ]],
        );
        assert_query_result(
            normal_connection,
            "SELECT native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9042)]],
        );

        assert_query_result(
            normal_connection,
            "SELECT native_port as foo FROM system.peers_v2;",
            &[&[ResultValue::Int(9042)]],
        );
    }

    {
        assert_query_result(
            rewrite_port_connection,
            "SELECT data_center, native_port, rack FROM system.peers_v2;",
            &[&[
                ResultValue::Varchar("dc1".into()),
                ResultValue::Int(9044),
                ResultValue::Varchar("West".into()),
            ]],
        );

        assert_query_result(
            rewrite_port_connection,
            "SELECT native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044)]],
        );

        assert_query_result(
            rewrite_port_connection,
            "SELECT native_port as foo FROM system.peers_v2;",
            &[&[ResultValue::Int(9044)]],
        );

        assert_query_result(
            rewrite_port_connection,
            "SELECT native_port, native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044), ResultValue::Int(9044)]],
        );

        assert_query_result(
            rewrite_port_connection,
            "SELECT native_port, native_port as some_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044), ResultValue::Int(9044)]],
        );

        let result = execute_query(rewrite_port_connection, "SELECT * FROM system.peers_v2;");
        assert_eq!(result[0][5], ResultValue::Int(9044));
    }
}

pub fn test_assign_token_range(normal_connection: &Session, rewrite_connection: &Session) {
    assert_query_result(rewrite_connection, "SELECT * FROM system.peers", &[]);
    assert_query_result(rewrite_connection, "SELECT * FROM system.peers_v2", &[]);

    assert_query_result(
        normal_connection,
        "SELECT data_center, rack FROM system.peers",
        &[&[
            ResultValue::Varchar("dc1".into()),
            ResultValue::Varchar("West".into()),
        ]],
    );
    assert_query_result(
        normal_connection,
        "SELECT data_center, native_port, rack FROM system.peers_v2;",
        &[&[
            ResultValue::Varchar("dc1".into()),
            ResultValue::Int(9042),
            ResultValue::Varchar("West".into()),
        ]],
    );
}
