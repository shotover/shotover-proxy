package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	fmt.Println("Starting go smoke test")

	dc := "datacenter1"
	cluster := gocql.NewCluster("127.0.0.1:9042")
	cluster.Keyspace = "system"
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 5}
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(dc))
	cluster.HostFilter = gocql.DataCentreHostFilter(dc)
	cluster.WriteCoalesceWaitTime = 0
	cluster.NumConns = 4
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	session.Close()

	fmt.Println("Success!")
}
