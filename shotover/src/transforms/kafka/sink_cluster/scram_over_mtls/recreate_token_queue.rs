use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Keeps track of when tokens need to be recreated.
pub(crate) struct RecreateTokenQueue {
    queue: VecDeque<TokenToRecreate>,
    token_lifetime: Duration,
}

impl RecreateTokenQueue {
    /// token_lifetime must be the lifetime that all tokens are created with.
    pub(crate) fn new(token_lifetime: Duration) -> Self {
        RecreateTokenQueue {
            queue: VecDeque::new(),
            token_lifetime,
        }
    }

    /// Returns the username of a token that needs to be recreated now.
    /// It will wait asynchronously until there is a token ready for recreation.
    /// If there are no pending token recreations this method will never return.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    /// If it is cancelled, it is guaranteed that no element was removed from the queue.
    pub(crate) async fn next(&mut self) -> String {
        if let Some(token) = self.queue.front() {
            tokio::time::sleep_until(token.recreate_at.into()).await;
            self.queue.pop_front().unwrap().username
        } else {
            futures::future::pending::<String>().await
        }
    }

    /// Adds a token to the queue with the provided username
    /// token_lifetime is the lifetime that the existing token was created with.
    pub(crate) fn push(&mut self, username: String) {
        self.queue.push_back(TokenToRecreate {
            // recreate the token when it is halfway through its lifetime
            recreate_at: Instant::now() + self.token_lifetime / 2,
            username,
        })
    }
}

struct TokenToRecreate {
    recreate_at: Instant,
    username: String,
}
