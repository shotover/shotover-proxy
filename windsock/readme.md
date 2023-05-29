# Windsock - A DB benchmarking framework

Windsock is a generic DB benchmarking framework.

What you do:

* Bring your own rust async compatible DB driver
* Define your benchmark logic which reports some simple stats back to windsock
* Define your pool of benchmarks

What windsock does:

* Provides a CLI from which you can:
  * Query available benchmarks
  * Selectively run benchmarks
  * Process benchmark results into readable tables
* Generates a webpage from which you can explore graphed results

Windsock is suitable for:

* Iteratively testing performance during development of a database or service (for microbenchmarks you will need a different tool though)
* Investigating performance of different workloads on a database you intend to use.
* Generating a webpage of graphs to show off the performance of your released database. (not yet implemented)

## Define benches

To use windsock create a rust crate that imports windsock:

```toml
windsock = { git = "https://github.com/shotover/shotover-proxy" }
```

And then implement the crate like this (simplified):

```rust
fn main() {
    // Define our benchmarks and give them to windsock
    Windsock::new(vec![
        Box::new(CassandraBench { topology: Topology::Cluster3 }),
        Box::new(CassandraBench { topology: Topology::Single })
    ])
    // Hand control of the app over to windsock
    // Windsock processes CLI args, possibly running benchmarks and then terminates.
    .run();
}

pub struct CassandraBench { topology: Topology }

#[async_trait]
impl Bench for CassandraBench {
    // define tags that windsock will use to filter and name the benchmark instance
    fn tags(&self) -> HashMap<String, String> {
        [
            ("name".to_owned(), "cassandra".to_owned()),
            (
                "topology".to_owned(),
                match &self.topology {
                    Topology::Single => "single".to_owned(),
                    Topology::Cluster3 => "cluster3".to_owned(),
                },
            ),
        ]
        .into_iter()
        .collect()
    }

    // the benchmark logic for this benchmark instance
    async fn run(&self, runtime_seconds: usize, operations_per_second: Option<u64>, reporter: UnboundedSender<Report>) {
        // bring up the DB
        let _handle = init_cassandra();

        // create the DB driver session
        let session = init_session().await;

        // spawn tokio tasks to concurrently hit the database
        // The exact query is defined in `run_one_operation` below
        BenchTaskCassandra { session }.spawn_tasks(reporter.clone(), operations_per_second).await;

        // tell windsock to begin benchmarking
        reporter.send(Report::Start).unwrap();
        let start = Instant::now();

        // run the bench for the time requested by the user on the CLI (defaults to 15s)
        tokio::time::sleep(Duration::from_secs(runtime_seconds)).await;

        // tell windsock to finalize the benchmark
        reporter.send(Report::FinishedIn(start.elapsed())).unwrap();
    }
}

// This struct is cloned once for each tokio task it will be run in.
#[derive(Clone)]
struct BenchTaskCassandra {
    session: Arc<Session>,
}

#[async_trait]
impl BenchTask for BenchTaskCassandra {
    async fn run_one_operation(&self) {
        self.session.query("SELECT * FROM table").await.unwrap();
    }
}
```

This example is simplified for demonstration purposes, refer to `examples/cassandra.rs` for a full working example.

## Running benches

Then we run our crate to run the benchmarks and view results like:

```none
> cargo run
... benchmark running logs
> cargo run -- --results-by-name "cassandra,topology=single cassandra,topology=cluster3"
Results for cassandra
           topology   ──single ─cluster3
Measurements ═══════════════════════════
   Operations Total     750762    372624
 Operations Per Sec      83418     41403
             Min       0.255ms   0.229ms
               1       0.389ms   0.495ms
               2       0.411ms   0.571ms
               5       0.460ms   0.714ms
              10       0.567ms   0.876ms
              25       1.131ms   1.210ms
              50       1.306ms   1.687ms
              75       1.519ms   2.600ms
              90       1.763ms   4.881ms
              95       2.132ms   7.542ms
              98       2.588ms  14.008ms
              99       2.951ms  19.297ms
              99.9     7.952ms  40.896ms
              99.99   25.559ms  80.692ms
```

TODO: make this into a comparison to make it more flashy and use an image to include the coloring

and graphs: TODO

## How to perform various tasks in windsock

### Just run every bench

```shell
> cargo run
```

### Run benches with matching tags and view all the results in one table

```shell
> cargo run -- db=kafka OPS=1000 topology=single # run benchmarks matching some tags
> cargo run -- --results-by-tag db=kafka OPS=1000 topology=single # view the results of the benchmarks with the same tags in a single table
```

### Iteratively compare results against a previous implementation

```shell
> git checkout main # checkout original implementation
> cargo run # run all benchmarks
> cargo run -- --set-baseline # set the last benchmark run as the baseline
> vim src/main.rs # modify implementation
> cargo run # run all benchmarks, every result is compared against the baseline
> vim src/main.rs # modify implementation again
> cargo run # run all benchmarks, every result is compared against the baseline
```

### Generate graph webpage

TODO: not yet implemented

```shell
> cargo run # run all benches
> cargo run -- --generate_webpage # generate a webpage from the results
```
