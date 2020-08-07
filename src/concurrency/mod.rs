use futures::future::{abortable, AbortHandle, Aborted};
use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::transmute;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::mpsc;

enum Void {}

#[derive(Clone)]
pub struct Scope<'env> {
    handle: tokio::runtime::Handle,
    chan: mpsc::Sender<Void>,
    abort_handles: Arc<Mutex<Vec<AbortHandle>>>,
    _marker: PhantomData<&'env mut &'env ()>,
}

#[pin_project]
pub struct ScopedJoinHandle<'scope, R> {
    #[pin]
    handle: tokio::task::JoinHandle<Result<R, Aborted>>,
    _marker: PhantomData<&'scope ()>,
}

impl<'env, R> Future for ScopedJoinHandle<'env, R> {
    type Output = Result<R, Box<dyn std::error::Error + Send + 'static>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.project().handle.poll(cx) {
            Poll::Ready(Ok(Ok(x))) => Poll::Ready(Ok(x)),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(Box::new(e))),
            Poll::Ready(Err(e)) => Poll::Ready(Err(Box::new(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'env> Scope<'env> {
    pub fn spawn<'scope, T, R>(&'scope self, task: T) -> ScopedJoinHandle<'scope, R>
    where
        T: Future<Output = R> + Send + 'env,
        R: Send + 'static, // TODO: weaken to 'env
    {
        let chan = self.chan.clone();
        let (abortable, abort_handle) = abortable(task);
        self.abort_handles.lock().unwrap().push(abort_handle);

        let task_env: Pin<Box<dyn Future<Output = Result<R, Aborted>> + Send + 'env>> =
            Box::pin(async move {
                // the cloned channel gets dropped at the end of the task
                let _chan = chan;

                abortable.await
            });

        // SAFETY: scoped API ensures the spawned tasks will not outlive the parent scope
        let task_static: Pin<Box<dyn Future<Output = Result<R, Aborted>> + Send + 'static>> =
            unsafe { transmute(task_env) };

        let handle = self.handle.spawn(task_static);

        ScopedJoinHandle {
            handle,
            _marker: PhantomData,
        }
    }
}

#[derive(Default)]
struct AbortOnDrop {
    abort_handles: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.abort_handles
            .lock()
            .unwrap()
            .drain(..)
            .for_each(|h| h.abort());

        // TODO: we must wait for spawned tasks to abort synchronously here
    }
}

// TODO: if `F` takes a reference to the scope, `scope.spawn` will generate a cryptic error
#[doc(hidden)]
pub async unsafe fn scope_impl<'env, F, T, R>(handle: tokio::runtime::Handle, f: F) -> R
where
    F: FnOnce(Scope<'env>) -> T,
    T: Future<Output = R> + Send,
    R: Send,
{
    let mut _abort_on_drop = AbortOnDrop::default();

    // we won't send data through this channel, so reserve the minimal buffer (buffer size must be
    // greater than 0).
    let (tx, mut rx) = mpsc::channel(1);
    let scope = Scope::<'env> {
        handle,
        chan: tx,
        abort_handles: _abort_on_drop.abort_handles.clone(),
        _marker: PhantomData,
    };

    // TODO: verify that `AbortOnDrop` correctly handles drop and panic.
    let result = f(scope).await;

    // yield the control until all spawned tasks finish(drop).
    assert!(rx.recv().await.is_none());

    // no need to abort tasks anymore
    _abort_on_drop.abort_handles.lock().unwrap().clear();

    result
}

#[macro_export]
macro_rules! scope {
    ($handle:expr, $func:expr) => {{
        unsafe { crate::concurrency::scope_impl($handle, $func) }.await
    }};
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use tokio::time::delay_for;

    #[test]
    fn test_scoped() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();

        rt.block_on(async {
            {
                let local = String::from("hello world");
                let local = &local;

                scope!(handle, |scope| {
                    // TODO: without this `move`, we get a compilation error. why?
                    async move {
                        // this spawned subtask will continue running after the scoped task
                        // finished, but `scope!` will wait until this task completes.
                        scope.spawn(async move {
                            delay_for(Duration::from_millis(500)).await;
                            println!("spanwed task is done: {}", local);
                        });

                        // since spawned tasks can outlive the scoped task, they cannot have
                        // references to the scoped task's stack
                        /*
                        let evil = String::from("may dangle");
                        scope.spawn(async {
                            delay_for(Duration::from_millis(200)).await;
                            println!("spanwed task cannot access evil: {}", evil);
                        });
                        */

                        let handle = scope.spawn(async {
                            println!("another spawned task");
                        });
                        handle.await.unwrap(); // you can await the returned handle

                        delay_for(Duration::from_millis(100)).await;
                        println!("scoped task is done: {}", local);
                    }
                });

                delay_for(Duration::from_millis(110)).await;
                println!("local can be used here: {}", local);
            }

            println!("local is freed");
            delay_for(Duration::from_millis(600)).await;
        });
    }

    #[test]
    #[should_panic]
    fn test_panic() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();

        rt.block_on(async {
            {
                let local = String::from("hello world");
                let local = &local;

                scope!(handle, |scope| {
                    async move {
                        scope.spawn(async move {
                            println!("spanwed task started: {}", local);
                            delay_for(Duration::from_millis(500)).await;
                            println!("spanwed task is done: {}", local);
                        });

                        delay_for(Duration::from_millis(100)).await;
                        panic!("scoped task panicked: {}", local);
                    }
                });

                delay_for(Duration::from_millis(110)).await;
                println!("local can be used here: {}", local);
            }

            println!("local is freed");
            delay_for(Duration::from_millis(600)).await;
        });
    }
}
