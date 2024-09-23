# Metrics

This optional interface will serve Prometheus metrics from `/metrics`. It will be disabled if the field `observability_interface` is not provided in `configuration.yaml`. The following metrics are included by default, others are transform specific.

| Name                                       | Labels      | Data type               | Description                                                               |
|--------------------------------------------|-------------|-------------------------|---------------------------------------------------------------------------|
| `shotover_transform_total_count`           | `transform` | [counter](#counter)     | Counts the amount of times the `transform` is used                        |
| `shotover_transform_failures_count`        | `transform` | [counter](#counter)     | Counts the amount of times the `transform` fails                          |
| `shotover_transform_latency_seconds`       | `transform` | [histogram](#histogram) | The latency for a message batch to go through the `transform`             |
| `shotover_chain_total_count`               | `chain`     | [counter](#counter)     | Counts the amount of times `chain` is used                                |
| `shotover_chain_failures_count`            | `chain`     | [counter](#counter)     | Counts the amount of times `chain` fails                                  |
| `shotover_chain_latency_seconds`           | `chain`     | [histogram](#histogram) | The latency for running `chain`                                           |
| `shotover_chain_requests_batch_size`       | `chain`     | [histogram](#histogram) | The number of requests in each request batch passing through `chain`.     |
| `shotover_chain_responses_batch_size`      | `chain`     | [histogram](#histogram) | The number of responses in each response batch passing through `chain`.   |
| `shotover_available_connections_count`     | `source`    | [gauge](#gauge)         | How many more connections can be opened to `source` before new connections will be rejected. |
| `connections_opened`                       | `source`    | [counter](#counter)     | Counts the total number of connections that clients have opened against this source.         |
| `shotover_source_to_sink_latency_seconds`  | `sink`      | [histogram](#histogram) | The milliseconds between reading a request from a source TCP connection and writing it to a sink TCP connection  |
| `shotover_sink_to_source_latency_seconds`  | `source`    | [histogram](#histogram) | The milliseconds between reading a response from a sink TCP connection and writing it to a source TCP connection |

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
curl -X PUT -d 'info, shotover_proxy=info, shotover::connection_span::info` http://127.0.0.1:9001/filter
```

Some examples of how you can tweak this filter:

* configure the first `info` to set the log level for dependencies
* configure `shotover=info` to set the log level for shotover itself
* set `shotover::connection_span=info` to `shotover::connection_span=debug` to attach connection info to most log events, this is disabled by default due to a minor performance hit.

For more control over filtering you should understand [The tracing filter format](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives).
