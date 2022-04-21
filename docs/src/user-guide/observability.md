# Metrics

This interface will serve Prometheus metrics from `/metrics`. The following metrics are included by default, others are transform specific.

| Name                             | Labels      | Data type               | Description                                               |
|----------------------------------|-------------|-------------------------|-----------------------------------------------------------|
| `shotover_transform_total`       | `transform` | [counter](#counter)     | Counts the amount of times the `transform` is used        |
| `shotover_transform_failures`    | `transform` | [counter](#counter)     | Counts the amount of times the `transform` fails          |
| `shotover_transform_latency`     | `transform` | [histogram](#histogram) | The latency for running `transform`                       |
| `shotover_chain_total`           | `chain`     | [counter](#counter)     | Counts the amount of times `chain` is used                |
| `shotover_chain_failures`        | `chain`     | [counter](#counter)     | Counts the amount of times `chain` fails                  |
| `shotover_chain_latency`         | `chain`     | [histogram](#histogram) | The latency for running `chain`                           |
| `shotover_available_connections` | `source`    | [gauge](#gauge)         | The number of connections currently connected to `source` |

## Metric data types

### Counter

A single value, which can only be incremented, not decremented. Starts out with an initial value of zero.

### Histogram

Measures the distribution of values for a set of measurements and starts with no initial values.

### Gauge

A single value that can increment or decrement over time. Starts out with an initial value of zero.

# Log levels and filters

You can configure log levels and filters at `/filter`. This can be done by a POST HTTP request to the `/filter` endpoint with the `env_filter` string set as the POST data. For example:

```shell
curl -X PUT -d 'info,shotover_proxy=info' http://127.0.0.1:9001/filter
```
