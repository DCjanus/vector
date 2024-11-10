use crate::config::{AcknowledgementsConfig, Input, SinkConfig, SinkContext};
use crate::sinks::prelude::*;
use crate::sinks::{Healthcheck, VectorSink};
use bytes::BufMut;
use encoding::Encoder;
use snafu::ResultExt;
use std::path::PathBuf;
use tokio::net::UnixDatagram;
use vector_lib::configurable::configurable_component;
use vrl::core::encode_key_value::EncodingError;
use vrl::value::KeyString;
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
        let socket = UnixDatagram::unbound()?;
        let target = self.path.clone();

        let healthcheck = async move { Ok(()) }.boxed(); // TODO: implement healthcheck
        let sink = JournalSink { socket, target }; // TODO: implement JournalSink
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
    target: PathBuf,
}

#[async_trait::async_trait]
impl StreamSink<Event> for JournalSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

impl JournalSink {
    async fn run_inner(&self, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let mut buffer = Vec::new();
        while let Some(event) = input.next().await {
            let event = event.into_log();
            let fields = event.convert_to_fields();

            for (k, v) in fields {
                write_field_name(&k, &mut buffer);
                write_field_value(v, &mut buffer);
            }
            // TODO: error handle for send_to
            self.socket
                .send_to(&buffer, &self.target)
                .await
                .expect("Failed to send data to JournalD");
            buffer.clear();
        }
        Ok(())
    }
}

/// Convert a field name to a valid field name in JournalD.
/// This logger mangles the keys of additional key-values on records and names of custom fields according to the following rules, to turn them into valid journal fields:
/// This function magles the field name to a valid field name in JournalD according to the following rules:
///
/// - If the field name is empty, replace it with "EMPTY".
/// - Transform all characters to ASII uppercase.
/// - Replace all characters that are not allowed in field names with an underscore, which means that only 'A'-'Z', '0'-'9', and '_' are allowed.
/// - If the field name starts with a digit or an underscore, prefix it with `ESC_`.
/// - Only the first 64 characters of the field name are used.
///
/// # Reference
///
/// + [Upstream Validations](https://github.com/systemd/systemd/blob/cf8fd7148cd8fbdb79381202ce8686eed1de09d2/src/libsystemd/sd-journal/journal-file.c#L1703-L1739)
fn write_field_name(name: &str, output: &mut Vec<u8>) {
    let name = name.as_bytes();
    if name.is_empty() {
        output.extend_from_slice(b"EMPTY");
        return;
    }

    let mut wrote = 0;
    if !name[0].is_ascii_alphabetic() {
        output.extend_from_slice(b"ESC_");
        wrote += 4;
    }

    for byte in name.iter().take(64) {
        if byte.is_ascii_alphanumeric() {
            output.push(byte.to_ascii_uppercase());
        } else {
            output.push(b'_');
        }
        wrote += 1;

        if wrote >= 64 {
            break;
        }
    }
}

fn write_field_value(value: &Value, output: &mut Vec<u8>) {
    fn write_bytes(bytes: impl AsRef<[u8]>, output: &mut Vec<u8>) {
        let bytes = bytes.as_ref();
        if !output.contains(&b'\n') {
            output.push(b'=');
        } else {
            output.push(b'\n');
            output.put_u64_le(bytes.len() as u64);
        }
        output.extend_from_slice(bytes);
        output.push(b'\n');
    }
    match value {
        Value::Bytes(bytes) => write_bytes(bytes, output),
        Value::Regex(regex) => write_bytes(regex.as_bytes_slice(), output),
        Value::Integer(integer) => write_bytes(integer.to_string(), output),
        Value::Float(float) => write_bytes(float.to_string(), output),
        Value::Boolean(boolean) => write_bytes(boolean.to_string(), output),
        Value::Timestamp(timestamp) => write_bytes(timestamp.to_string(), output),
        Value::Object(_) | Value::Array(_) => {
            unreachable!("Value should be flattened before calling this function")
        }
        Value::Null => write_bytes("<NULL>", output),
    }
}
