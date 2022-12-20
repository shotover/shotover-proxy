use anyhow::Context;
use anyhow::Result;
use itertools::Itertools;
use nu_ansi_term::Color;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub struct Event {
    pub timestamp: String,
    pub level: Level,
    pub target: String,
    pub fields: Fields,
    #[serde(default)]
    pub span: HashMap<String, JsonValue>,
    #[serde(default)]
    pub spans: Vec<HashMap<String, JsonValue>>,
}

impl Event {
    pub fn new(level: Level, target: &str, message: &str) -> Event {
        Event {
            timestamp: "".to_owned(),
            level,
            target: target.to_owned(),
            fields: Fields {
                message: message.to_owned(),
                fields: HashMap::new(),
            },
            span: HashMap::new(),
            spans: vec![],
        }
    }
}

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

#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub struct Fields {
    #[serde(default)]
    pub message: String,
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
    pub fn from_json_str(s: &str) -> Result<Self> {
        serde_json::from_str(s).context(format!("Failed to parse json: {s}"))
    }
}
