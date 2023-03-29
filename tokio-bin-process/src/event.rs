//! Structures to represent an event created by tokio tracing

use anyhow::Context;
use anyhow::Result;
use itertools::Itertools;
use nu_ansi_term::Color;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Represents an event created by tokio tracing.
///
/// It is not possible to construct one directly from a tracing event.
/// Instead its expected that they are returned by one of the methods on tokio-bin-process which retrives them by parsing tracings json output.
#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub struct Event {
    /// The timestamp of the event
    pub timestamp: String,
    /// The level of the event
    pub level: Level,
    /// The target of the event, this is usually the module name of the code that triggered the event.
    pub target: String,
    /// Contains the message and other fields included in the event.
    pub fields: Fields,
    /// The last span that was entered before the event was triggered.
    #[serde(default)]
    pub span: HashMap<String, JsonValue>,
    /// Every span that was active while the event was triggered.
    #[serde(default)]
    pub spans: Vec<HashMap<String, JsonValue>>,
}

/// The level of the event
#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub enum Level {
    #[serde(rename = "ERROR")]
    Error,
    #[serde(rename = "WARN")]
    Warn,
    #[serde(rename = "INFO")]
    Info,
    #[serde(rename = "DEBUG")]
    Debug,
    #[serde(rename = "TRACE")]
    Trace,
}

impl Display for Level {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Level::Error => write!(f, "{}", Color::Red.paint("ERROR")),
            Level::Warn => write!(f, " {}", Color::Yellow.paint("WARN")),
            Level::Info => write!(f, " {}", Color::Green.paint("INFO")),
            Level::Debug => write!(f, " {}", Color::Blue.paint("DEBUG")),
            Level::Trace => write!(f, " {}", Color::Purple.paint("TRACE")),
        }
    }
}

/// Contains the message and other fields included in the event.
#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub struct Fields {
    /// The message of the event.
    /// Some events dont have a message in which case this is an empty String.
    #[serde(default)]
    pub message: String,
    /// All fields other than the message.
    /// For an event created by: `tracing::info!("message", some_field=4)`
    /// This would contain the HashMap: `{"some_field", JsonValue::Number(4)}`
    #[serde(flatten)]
    pub fields: HashMap<String, JsonValue>,
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{}",
            // Strip the date from the datetime.
            // The date portion isnt useful in integration tests
            Color::Default.dimmed().paint(&self.timestamp[11..])
        )?;

        if let Some(backtrace) = self.fields.fields.get("panic.backtrace") {
            // Special case panics.
            // Panics usually get reasonable formatting locally and we dont want to regress on that just because we had to stuff them through tracing
            writeln!(
                f,
                " {} {}",
                Color::Red.reverse().paint("PANIC"),
                self.fields.message,
            )?;
            match backtrace {
                JsonValue::String(backtrace) => {
                    for line in backtrace.lines() {
                        // we can have a little color as a treat
                        if line.trim().starts_with("at ") {
                            writeln!(f, "{}", Color::Default.dimmed().paint(line))?;
                        } else {
                            writeln!(f, "{}", line)?;
                        }
                    }
                }
                backtrace => write!(f, "{backtrace}")?,
            }
        } else {
            // Regular old formatting, matches the formatting used by the default tracing subscriber
            write!(f, " {} ", self.level)?;
            if !self.spans.is_empty() {
                for span in &self.spans {
                    let name = span.get("name").unwrap().as_str().unwrap();
                    write!(f, "{}", Color::Default.bold().paint(name))?;
                    write!(f, "{}", Color::Default.bold().paint("{"))?;
                    let mut first = true;
                    for (key, value) in span.iter().sorted_by_key(|(key, _)| <&String>::clone(key))
                    {
                        if key != "name" {
                            if !first {
                                write!(f, " ")?;
                            }
                            first = false;

                            write!(f, "{}", key)?;
                            write!(f, "{}", Color::Default.dimmed().paint("="))?;
                            write!(f, "{}", value)?;
                        }
                    }
                    write!(f, "{}", Color::Default.bold().paint("}"))?;
                    write!(f, "{}", Color::Default.dimmed().paint(":"))?;
                }
                write!(f, " ")?;
            }

            write!(
                f,
                "{}{}",
                Color::Default.dimmed().paint(&self.target),
                Color::Default.dimmed().paint(":"),
            )?;

            if !self.fields.message.is_empty() {
                write!(f, " {}", self.fields.message)?;
            }

            for (key, value) in self
                .fields
                .fields
                .iter()
                .sorted_by_key(|(key, _)| <&String>::clone(key))
            {
                write!(f, " {}", Color::Default.italic().paint(key))?;
                write!(f, "{}", Color::Default.dimmed().paint("="))?;
                write!(f, "{}", QuotelessDisplay(value))?;
            }
        }

        Ok(())
    }
}

struct QuotelessDisplay<'a>(&'a JsonValue);

impl<'a> Display for QuotelessDisplay<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            QuotelessDisplay(JsonValue::String(str)) => write!(f, "{str}"),
            QuotelessDisplay(value) => write!(f, "{value}"),
        }
    }
}

impl Event {
    /// Constructs an Event by parsing a single event from tokio tracing's json output
    pub fn from_json_str(s: &str) -> Result<Self> {
        serde_json::from_str(s).context(format!("Failed to parse json: {s}"))
    }
}
