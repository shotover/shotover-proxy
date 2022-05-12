use std::cell::RefCell;
use std::sync::Arc;

/// The returned Fn will return the result of the provided Fn Wrapped in an `Arc<RefCell<Option<T>>>`
/// The reason for this complicated return type is simply for implementation reasons.
/// Refcell::borrow_mut can always be called on the RefCell (assuming the bench creation logic is single threaded) and the Option will always be Some.
///
/// The returned Fn may be called any amount of times but the provided Fn will only be called 0 or 1 times.
/// The provided Fn is not called until the first time the returned Fn is called.
///
/// This function can be easily pulled out and reused by other benchmarks if needed.
pub fn new_lazy_shared<T>(create: impl Fn() -> T) -> impl Fn() -> Arc<RefCell<Option<T>>> {
    let resources = Arc::new(RefCell::new(None));
    move || {
        if resources.borrow().is_none() {
            resources.replace(Some(create()));
        }
        resources.clone()
    }
}
