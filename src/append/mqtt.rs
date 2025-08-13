//! The MQTT appender.
//!
//! Requires the `mqtt_appender` feature.

use derive_more::Debug;
use log::Record;
use parking_lot::Mutex;
use rumqttc::{Client, Event, MqttOptions, Packet, QoS};
use std::{
    io::{self, Write},
    sync::Arc,
    thread,
    time::Duration,
};

#[cfg(feature = "config_parsing")]
use crate::config::{Deserialize, Deserializers};
#[cfg(feature = "config_parsing")]
use crate::encode::EncoderConfig;

use crate::{
    append::Append,
    encode::{pattern::PatternEncoder, Encode},
};

/// The MQTT appender's configuration.
#[cfg(feature = "config_parsing")]
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttAppenderConfig {
    broker: String,
    client_id: String,
    topic: String,
    qos: Option<u8>,
    username: Option<String>,
    password: Option<String>,
    encoder: Option<EncoderConfig>,
}

/// An appender which logs to an MQTT broker.
#[derive(Debug)]
pub struct MqttAppender {
    #[debug(skip)]
    client: Arc<Mutex<Client>>,
    topic_template: String,
    qos: QoS,
    encoder: Box<dyn Encode>,
}

impl Append for MqttAppender {
    fn append(&self, record: &Record) -> anyhow::Result<()> {
        // Format the log message using the encoder
        let mut buffer = MqttBuffer::new();
        self.encoder.encode(&mut buffer, record)?;
        
        // Replace {level} in topic if present
        let topic = self.topic_template
            .replace("{level}", &record.level().to_string().to_lowercase());
        // Send the message
        let client = self.client.lock();
        match client.publish(&topic, self.qos, false, buffer.0) {
            Ok(_) => {
                Ok(())
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    fn flush(&self) {}
}

impl MqttAppender {
    /// Creates a new `MqttAppender` builder.
    pub fn builder() -> MqttAppenderBuilder {
        MqttAppenderBuilder {
            broker: "mqtt://localhost:1883".to_string(),
            client_id: "log4rs_client".to_string(),
            topic: "logs".to_string(),
            qos: QoS::AtMostOnce,
            username: None,
            password: None,
            encoder: None,
        }
    }
}

/// A builder for `MqttAppender`s.
pub struct MqttAppenderBuilder {
    broker: String,
    client_id: String,
    topic: String,
    qos: QoS,
    username: Option<String>,
    password: Option<String>,
    encoder: Option<Box<dyn Encode>>,
}

impl MqttAppenderBuilder {
    /// Sets the MQTT broker URL.
    pub fn broker(mut self, broker: String) -> MqttAppenderBuilder {
        self.broker = broker;
        self
    }

    /// Sets the MQTT client ID.
    pub fn client_id(mut self, client_id: String) -> MqttAppenderBuilder {
        self.client_id = client_id;
        self
    }

    /// Sets the MQTT topic template.
    /// Can include {level} placeholder which will be replaced with the log level.
    pub fn topic(mut self, topic: String) -> MqttAppenderBuilder {
        self.topic = topic;
        self
    }

    /// Sets the MQTT QoS level (0, 1, or 2).
    pub fn qos(mut self, qos: u8) -> MqttAppenderBuilder {
        self.qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtMostOnce,
        };
        self
    }

    /// Sets the username for MQTT authentication.
    pub fn username(mut self, username: Option<String>) -> MqttAppenderBuilder {
        self.username = username;
        self
    }

    /// Sets the password for MQTT authentication.
    pub fn password(mut self, password: Option<String>) -> MqttAppenderBuilder {
        self.password = password;
        self
    }

    /// Sets the output encoder for the `MqttAppender`.
    pub fn encoder(mut self, encoder: Box<dyn Encode>) -> MqttAppenderBuilder {
        self.encoder = Some(encoder);
        self
    }

    /// Consumes the `MqttAppenderBuilder`, producing a `MqttAppender`.
    pub fn build(self) -> io::Result<MqttAppender> {
        // Parse broker URL to extract host and port
        let broker_url = self.broker.clone();
        let (host, port) = parse_broker_url(&broker_url)?;

        // Create MQTT options
        let mut mqtt_options = MqttOptions::new(self.client_id.clone(), host, port);
        mqtt_options.set_keep_alive(Duration::from_secs(30));
        
        // Set credentials if provided
        if let (Some(ref username), Some(ref password)) = (self.username, self.password) {
            mqtt_options.set_credentials(username.clone(), password.clone());
        }
        
        // Create sync client and connection
        let (client, mut connection) = Client::new(mqtt_options, 10);
        let client = Arc::new(Mutex::new(client));
                
        // Start connection handler in background thread
        thread::spawn(move || {
            // Handle connection events
            for notification in connection.iter() {
                match notification {
                    Ok(Event::Incoming(Packet::ConnAck(connack))) => {
                    }
                    Ok(Event::Incoming(packet)) => {
                    }
                    Ok(Event::Outgoing(packet)) => {
                    }
                    Err(e) => {
                        // Connection will automatically retry
                    }
                }
            }
        });
        
        Ok(MqttAppender {
            client,
            topic_template: self.topic,
            qos: self.qos,
            encoder: self
                .encoder
                .unwrap_or_else(|| Box::<PatternEncoder>::default()),
        })
    }
}

fn parse_broker_url(url: &str) -> io::Result<(String, u16)> {
    // Remove protocol prefix if present
    let url = url.trim_start_matches("mqtt://")
        .trim_start_matches("mqtts://")
        .trim_start_matches("tcp://");
    
    // Split host and port
    if let Some(colon_pos) = url.rfind(':') {
        let host = url[..colon_pos].to_string();
        let port_str = &url[colon_pos + 1..];
        let port = port_str.parse::<u16>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid port number"))?;
        Ok((host, port))
    } else {
        // Default port for MQTT
        Ok((url.to_string(), 1883))
    }
}

// Wrapper type for MQTT message buffer
struct MqttBuffer(Vec<u8>);

impl MqttBuffer {
    fn new() -> Self {
        MqttBuffer(Vec::new())
    }
}

impl Write for MqttBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl crate::encode::Write for MqttBuffer {
    fn set_style(&mut self, _style: &crate::encode::Style) -> io::Result<()> {
        // MQTT messages don't support styling
        Ok(())
    }
}

/// A deserializer for the `MqttAppender`.
///
/// # Configuration
///
/// ```yaml
/// kind: mqtt
/// broker: mqtt://localhost:1883
/// client_id: log4rs_client
/// topic: logs/{level}
/// qos: 1  # Optional, defaults to 0
/// username: user  # Optional
/// password: pass  # Optional
/// encoder:  # Optional
///   pattern: "{d} {l} {t} - {m}{n}"
/// ```
#[cfg(feature = "config_parsing")]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct MqttAppenderDeserializer;

#[cfg(feature = "config_parsing")]
impl Deserialize for MqttAppenderDeserializer {
    type Trait = dyn Append;
    type Config = MqttAppenderConfig;

    fn deserialize(
        &self,
        config: MqttAppenderConfig,
        deserializers: &Deserializers,
    ) -> anyhow::Result<Box<dyn Append>> {
        let mut builder = MqttAppender::builder()
            .broker(config.broker)
            .client_id(config.client_id)
            .topic(config.topic)
            .username(config.username)
            .password(config.password);
        
        if let Some(qos) = config.qos {
            builder = builder.qos(qos);
        }
        
        if let Some(encoder) = config.encoder {
            builder = builder.encoder(deserializers.deserialize(&encoder.kind, encoder.config)?);
        }
        
        Ok(Box::new(builder.build()?))
    }
}