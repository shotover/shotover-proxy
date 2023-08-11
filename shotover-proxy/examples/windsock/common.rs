use anyhow::Context;
use std::path::Path;

#[derive(Clone, Copy)]
pub enum Shotover {
    None,
    Standard,
    ForcedMessageParsed,
}

impl Shotover {
    pub fn to_tag(self) -> (String, String) {
        (
            "shotover".to_owned(),
            match self {
                Shotover::None => "none".to_owned(),
                Shotover::Standard => "standard".to_owned(),
                Shotover::ForcedMessageParsed => "message-parsed".to_owned(),
            },
        )
    }
}

pub async fn rewritten_file(path: &Path, find_replace: &[(&str, &str)]) -> String {
    let mut text = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("Failed to read from {path:?}"))
        .unwrap();
    for (find, replace) in find_replace {
        text = text.replace(find, replace);
    }
    text
}
