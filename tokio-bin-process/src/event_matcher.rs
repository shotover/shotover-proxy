//! Structures for matching an [`Event`]

use crate::event::{Event, Level};
use itertools::Itertools;

/// Use to check for any matching [`Event`]'s among a list of events.
#[derive(Debug)]
pub struct Events {
    pub events: Vec<Event>,
}

impl Events {
    pub fn contains(&self, matcher: &EventMatcher) {
        if !self.events.iter().any(|e| matcher.matches(e)) {
            panic!(
                "An event with {matcher:?} was not found in the list of events:\n{}",
                self.events.iter().join("\n")
            )
        }
    }

    #[allow(dead_code)]
    fn contains_in_order(&self, _matchers: &[EventMatcher]) {
        todo!()
    }
}

/// Use to check if an [`Event`] matches certain criteria.
///
/// Construct by chaining methods, e.g:
/// ```rust,no_run
/// # use tokio_bin_process::event_matcher::EventMatcher;
/// # use tokio_bin_process::event::Level;
/// # let event = todo!();
///
/// assert!(
///     EventMatcher::new()
///         .with_level(Level::Info)
///         .with_target("module::internal_module")
///         .with_message("Some message")
///         .matches(event)
/// );
/// ```
#[derive(Default, Debug)]
pub struct EventMatcher {
    level: Matcher<Level>,
    message: Matcher<String>,
    target: Matcher<String>,
    pub(crate) count: Count,
}

impl EventMatcher {
    /// Creates a new [`EventMatcher`] that by default matches any Event.
    pub fn new() -> EventMatcher {
        EventMatcher::default()
    }

    /// Sets the matcher to only match an [`Event`] when it has the exact provided level
    pub fn with_level(mut self, level: Level) -> EventMatcher {
        self.level = Matcher::Matches(level);
        self
    }

    /// Sets the matcher to only match an [`Event`] when it has the exact provided target
    pub fn with_target(mut self, target: &str) -> EventMatcher {
        self.target = Matcher::Matches(target.to_owned());
        self
    }

    /// Sets the matcher to only match an [`Event`] when it has the exact provided message
    pub fn with_message(mut self, message: &str) -> EventMatcher {
        self.message = Matcher::Matches(message.to_owned());
        self
    }

    /// Defines how many times the matcher must match to pass an assertion
    ///
    /// This is not used internally i.e. it has no effect on [`EventMatcher::matches`]
    /// Instead its only used by higher level assertion logic.
    pub fn with_count(mut self, count: Count) -> EventMatcher {
        self.count = count;
        self
    }

    /// Returns true only if this matcher matches the passed [`Event`]
    pub fn matches(&self, event: &Event) -> bool {
        self.level.matches(&event.level)
            && self.message.matches(&event.fields.message)
            && self.target.matches(&event.target)
    }
}

/// Defines how many times the [`EventMatcher`] must match to pass an assertion.
#[derive(Debug)]
pub enum Count {
    /// This matcher must match this many times to pass an assertion.
    Times(usize),
    /// This matcher may match 0 or more times and will still pass an assertion.
    /// Use sparingly but useful for ignoring a warning or error that is not appearing deterministically.
    Any,
}

impl Default for Count {
    fn default() -> Self {
        Count::Times(1)
    }
}

#[derive(Debug)]
enum Matcher<T: PartialEq> {
    Matches(T),
    Any,
}

impl<T: PartialEq> Default for Matcher<T> {
    fn default() -> Self {
        Matcher::Any
    }
}

impl<T: PartialEq> Matcher<T> {
    fn matches(&self, value: &T) -> bool {
        match self {
            Matcher::Matches(x) => value == x,
            Matcher::Any => true,
        }
    }
}
