# Cache Design Documentation

## Overview
The redis cache design is intended to cache Cassandra queries into
a Redis based cache.  It intercepts calls to Cassandra determines 
if it can respond and if so intercepts the entire set of Messages.
If not, the messages are sent on for processing and any cacheable 
results are processed on the return.

## Cassandra mapping

There are 38 Cassandra query commands of which we are concerned with 
6:

 * Select
 * Insert
 * Update 
 * Delete
 * Truncate
 * Drop Table

We initially process Select statements to see if we can respond to them. 
We also process Select statements on their return from Cassandra to
populate the cache.

Insert, Update, Delete methods clear the cache for specific rows or
potentially cause the entire table to be removed from the cache.

Truncate and Drop Table calls cause all the table entries to be 
removed from the cache.

## Redis 

### Cache configuration

The cache configuration is specified in the topology yaml file.
entries in the `caching_schema` define which tables are caching 
candidates and defines the partition and range key.  Combined the
partition and range key define the primary key.  The partition key
should match the Cassandra partition key, and the range key should 
match the Cassandra clustering key.  This is not checked during 
operation and is not strictly required, however, unexpected results 
may occur if they do not match.

In the following extract, `test_cache_keyspace_batch_insert.test_table` 
is a fully qualified table name.  That table has a single partition
key segment: the `id` column from the table.  There is no range key 
defined.

```yaml
      caching_schema:
          test_cache_keyspace_batch_insert.test_table:
            partition_key: [id]
            range_key: []
          test_cache_keyspace_simple.test_table:
            partition_key: [id]
            range_key: []
```

### Cache structure

The redis cache is a single Redis source.  All keys into the redis
source are defined by the fully qualified table name, the primary
key segments, and the range segments. 

Each key identifies a Hashset.  The keys within the hash set 
comprise the base column names in the select (not tha aliases)
followed by " WHERE " and then the filtering statements other than
the Cassandra key fields.

for example `SELECT a, b, c as g FROM keyspace1.table2 WHERE e='foo' a[2]=3`
on where table2 is defined with a primary key of `e` will yield the redis key
`keyspace1.table2:'foo'` and a hash key of `a b c WHERE a[2]=3`.  The entry
in the redis cache is a serialized version of the row metadata as well as the
row values as returned from the Cassandra call.

### Instrumentation

The cache uses the metrics crate to provide a counter called "cache_miss" that records
every attempt to read the cache where the cache did not have the requested value.  It 
does not count any statement that was rejected because it need not meet the requirements
for a cacheable statement.

## Cacheable statements

The cache system defines a CacheableState.  The possible cacheable states are 
 * Read( table ) - Indicates that the statement reads a table and so is may be served from the cache or alternatively the cache shoud be updated.
 * Update( table ) - Indicates that the statement performs an update on the table.  Only effected rows should be removed.
 * Delete( table ) - Indicates that the statement deletes a row or part of a row from the table.  Only effected rows should be removed.
 * Drop( table ) - Indicates tha the table should  be dropped from the cache.
 * Skip( reason ) - Indicates that the cache should not be queried/updated.  The reason is a human readable statement of why that may appear in the logs.
 * Err( reason ) - Indicates that an error has occurred in the caching system.  The reason is a human readable statement that will appear in the logs.

### Table of CacheableState results

| Cassandra<br/>Query | Read     | Update  | Delete                                                                                                                                     | Skip                                       | Error |
|---------------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|-------|
| Select              | default  |         | * has parameters                                                                                                                           | * allows filtering<br/>* no `where` clause |       |
| Insert              |          | default | * has parameters<br/>* has `if not exists` clause                                                                                          |                                            |       |
| Update              |          | default | * has parameters<br/>* has `if exists` clause<br/>* if there are calculations in the assignment<br/>* if the assigned colum uses an index. |                                            |       |
| Delete              |          |         | not implemented                                                                                                                            | default                                    |       |
| Truncate            |          |         | not implemented                                                                                                                            | default                                    |       |
| Drop Table          |          |         | default - delete all                                                                                                                       |                                            |       |
| All others          |          |         |                                                                                                                                            | default                                    |       |


Once a statement is determined to be cacheable it is processed accordingly.  During
processing the state may be reset.  Specifically it may be reset to Skip or Error. 

## Process Flow

The cache system sits on both the outbound and return data flows.  On the outbound flow,
toward the data source, the cache determines if the message wrapper contains only
cacheable Cassandra statements and if so attempts to answer from the cache.  If during 
the read from the cache any statements fail to be answered the entire message wrapper
is forwarded down the chain and the results are processed.

If the cache can not answer _ALL_ the original Cassandra queries the answers
from upstream are processed.  If the upstream commands were successfull, the 
results may modify the cache.  Any command with a cacheable state of
Read, Update or Delete is processed again on return and the cache updated 
appropriately.

