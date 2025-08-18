use crate::hot_reload::protocol::HotReloadListenerRequest;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// Manages hot reload channels for all TcpCodecListener instances
#[derive(Default)]
pub struct HotReloadChannelManager {
    senders: HashMap<String, mpsc::UnboundedSender<HotReloadListenerRequest>>,
}

impl HotReloadChannelManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new channel for a source and return the receiver
    pub fn create_channel_for_source(
        &mut self,
        source_name: String,
    ) -> mpsc::UnboundedReceiver<HotReloadListenerRequest> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.senders.insert(source_name, tx);
        rx
    }

    /// Get all channel senders for the hot reload server to use
    pub fn get_all_senders(
        &self,
    ) -> &HashMap<String, mpsc::UnboundedSender<HotReloadListenerRequest>> {
        &self.senders
    }

    pub fn get_sender(
        &self,
        source_name: &str,
    ) -> Option<&mpsc::UnboundedSender<HotReloadListenerRequest>> {
        self.senders.get(source_name)
    }
}
