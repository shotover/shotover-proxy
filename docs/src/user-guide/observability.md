# Metrics

This interface will serve Prometheus metrics from `/metrics`. The following metrics are included by default, others are transform specific.

| Name                                       | Labels      | Data type               | Description                                                               |
|--------------------------------------------|-------------|-------------------------|---------------------------------------------------------------------------|
| `shotover_transform_total_count`           | `transform` | [counter](#counter)     | Counts the amount of times the `transform` is used                        |
| `shotover_transform_failures_count`        | `transform` | [counter](#counter)     | Counts the amount of times the `transform` fails                          |
| `shotover_transform_latency_seconds`       | `transform` | [histogram](#histogram) | The latency for a message batch to go through the `transform`             |
| `shotover_transform_pushed_latency_seconds`| `transform` | [histogram](#histogram) | The latency for a pushed message from the DB to go through the `transform`|
| `shotover_chain_total_count`               | `chain`     | [counter](#counter)     | Counts the amount of times `chain` is used                                |
| `shotover_chain_failures_count`            | `chain`     | [counter](#counter)     | Counts the amount of times `chain` fails                                  |
| `shotover_chain_latency_seconds`           | `chain`     | [histogram](#histogram) | The latency for running `chain`                                           |
| `shotover_chain_messages_per_batch_count`  | `chain`     | [histogram](#histogram) | The number of messages in each batch passing through `chain`.             |
| `shotover_available_connections_count`     | `source`    | [gauge](#gauge)         | The number of connections currently connected to `source`                 |
| `source_to_sink_latency`                   | `sink`      | [histogram](#histogram) | The milliseconds between reading a request from a source TCP connection and writing it to a sink TCP connection  |
| `sink_to_source_latency`                   | `source`    | [histogram](#histogram) | The milliseconds between reading a response from a sink TCP connection and writing it to a source TCP connection |

## Metric data types

### Counter

A single value, which can only be incremented, not decremented. Starts out with an initial value of zero.

### Histogram

Measures the distribution of values for a set of measurements and starts with no initial values.

Every 20 seconds one of the 3 chunks of historical values are cleared.
This means that values are held onto for around 60 seconds.

### Gauge

A single value that can increment or decrement over time. Starts out with an initial value of zero.

## Log levels and filters

You can configure log levels and filters at `/filter`. This can be done by a POST HTTP request to the `/filter` endpoint with the `env_filter` string set as the POST data. For example:

```shell
curl -X PUT -d 'info,shotover_proxy=info' http://127.0.0.1:9001/filter
```
