FROM library/cassandra:4.0.6

COPY ./cassandra.yaml /etc/cassandra/cassandra.yaml

CMD ["cassandra", "-f", "-Dcassandra.initial_token=\"$CASSANDRA_INITIAL_TOKENS\"", "-Dcassandra.native_transport_port=$CASSANDRA_CQL_PORT_NUMBER"]
