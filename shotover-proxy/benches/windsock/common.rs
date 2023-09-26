use shotover::{config::topology::Topology, sources::SourceConfig};

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

pub fn generate_topology(source: SourceConfig) -> String {
    Topology {
        sources: vec![source],
    }
    .serialize()
    .unwrap()
}
