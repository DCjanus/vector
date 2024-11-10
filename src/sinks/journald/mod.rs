use crate::config::{AcknowledgementsConfig, Input, SinkConfig, SinkContext, SourceContext};
use crate::sinks::prelude::*;
use crate::sinks::{Healthcheck, VectorSink};
use async_trait::async_trait;
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::net::UnixDatagram;
use vector_lib::configurable::configurable_component;
use crate::sinks::util::unix::UnixSinkConfig;
// reference: https://systemd.io/JOURNAL_NATIVE_PROTOCOL/

/// Configuration for the `JournalD` sink.
#[configurable_component(sink("journald", "Deliver logs into JournalD via the native protocol."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct JournaldSinkConfig {
    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    /// The Unix socket path.
    ///
    /// This should be an absolute path.
    #[serde(default = "default_journald_socket_path")]
    pub path: PathBuf,
}

fn default_journald_socket_path() -> PathBuf {
    PathBuf::from("/run/systemd/journal/socket")
}

#[async_trait::async_trait]
#[typetag::serde(name = "journald")]
impl SinkConfig for JournaldSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let healthcheck = async move { Ok(()) }.boxed(); // TODO: implement healthcheck
        let target = UnixDatagram::bind()
        let sink = JournalSink {}; // TODO: implement JournalSink
        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl_generate_config_from_default!(JournaldSinkConfig);

impl Default for JournaldSinkConfig {
    fn default() -> Self {
        Self {
            acknowledgements: Default::default(),
            path: default_journald_socket_path(),
        }
    }
}

struct JournalSink {
    socket: UnixDatagram,
}

#[async_trait::async_trait]
impl StreamSink<Event> for JournalSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

impl JournalSink {
    async fn run_inner(&self, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        while let Some(event) = input.next().await {
            let data = toml::to_string_pretty(&event).unwrap();
            println!("{}", data);
        }
        Ok(())
    }
}

struct JournaldEncoder {}

impl encoding::Encoder<Event> for JournaldEncoder {
    fn encode_input(&self, input: Event, writer: &mut dyn Write) -> std::io::Result<(usize, GroupedCountByteSize)> {
        let log = input.into_log();
        todo!()
    }
}