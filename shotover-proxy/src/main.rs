use shotover::runner::Shotover;

fn main() {
    // Disable anyhow from taking backtraces, which makes for very verbose logs and is possibly a performance issue.
    if std::env::var("RUST_LIB_BACKTRACE").is_err() {
        // Safety: Safe because this is the first thing in main, we know that we havent launched any other threads which may access set_var.
        // TODO: Avoid usage of unsafe:
        //       Maybe this https://github.com/dtolnay/anyhow/issues/403 ?
        //       Or this https://github.com/rust-lang/rust/issues/93346 ?
        //       Or maybe fork anyhow?
        unsafe { std::env::set_var("RUST_LIB_BACKTRACE", "0") };
    }

    Shotover::new().run_block();
}
