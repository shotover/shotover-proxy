// A helper to run `windsock --cloud` within docker to workaround libc issues
// It is not possible to use this helper to run windsock locally as that would involve running docker within docker

mod container;

use container::{Container, cleanup};
use tokio::signal::unix::{SignalKind, signal};

#[tokio::main]
async fn main() {
    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    let mut terminate = signal(SignalKind::terminate()).unwrap();

    let container = Container::new().await;

    tokio::select!(
        _ = container.run_windsock() => {},
        _ = interrupt.recv() => cleanup().await,
        _ = terminate.recv() => cleanup().await,
    );
}
