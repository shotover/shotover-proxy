FROM library/cassandra:4.0.6

COPY ./cassandra.yaml /etc/cassandra/cassandra.yaml

CMD ["cassandra", "-f", "-Dcassandra.skip_wait_for_gossip_to_settle=0", "-Dcassandra.initial_token=0"]
