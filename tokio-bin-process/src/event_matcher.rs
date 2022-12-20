use crate::event::{Event, Level};
use itertools::Itertools;

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

    pub fn contains_in_order(&self, _matchers: &[EventMatcher]) {
        todo!()
    }
}

#[derive(Default, Debug)]
pub struct EventMatcher {
    pub level: Matcher<Level>,
    pub message: Matcher<String>,
    pub target: Matcher<String>,
    pub count: Count,
}

impl EventMatcher {
    pub fn new() -> EventMatcher {
        EventMatcher::default()
    }

    pub fn with_level(mut self, level: Level) -> EventMatcher {
        self.level = Matcher::Matches(level);
        self
    }

    pub fn with_target(mut self, target: &str) -> EventMatcher {
        self.target = Matcher::Matches(target.to_owned());
        self
    }

    pub fn with_message(mut self, message: &str) -> EventMatcher {
        self.message = Matcher::Matches(message.to_owned());
        self
    }

    /// This is not used internally i.e. it has no effect on EventMatcher::matches
    /// Instead its used by higher level matching logic
    pub fn with_count(mut self, count: Count) -> EventMatcher {
        self.count = count;
        self
    }

    pub fn matches(&self, event: &Event) -> bool {
        self.level.matches(&event.level)
            && self.message.matches(&event.fields.message)
            && self.target.matches(&event.target)
    }
}

#[derive(Debug)]
pub enum Count {
    Times(usize),
    Any,
}

impl Default for Count {
    fn default() -> Self {
        Count::Times(1)
    }
}

#[derive(Debug)]
pub enum Matcher<T: PartialEq> {
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
