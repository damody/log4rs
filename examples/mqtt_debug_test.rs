//! Simple MQTT debug test to verify connection and message sending

use log::{error, info, warn};
use log4rs::{
    config::Deserializers,
    append::mqtt::MqttAppenderDeserializer,
};

fn main() {
    println!("Starting MQTT debug test...");
    
    // Setup deserializers with MQTT appender support
    let mut deserializers = Deserializers::default();
    deserializers.insert("mqtt", MqttAppenderDeserializer);

    // Create a simple config for testing
    let config_yaml = r#"
refresh_rate: 10 seconds

appenders:
  stdout:
    kind: console
    encoder:
      pattern: "{d(%H:%M:%S)} [{l}] - {m}{n}"
  
  mqtt:
    kind: mqtt
    broker: "mqtt://localhost:1883"
    client_id: "debug_test_client"
    topic: "test/logs/{level}"
    qos: 0
    encoder:
      pattern: "{d(%H:%M:%S)} [{l}] - {m}{n}"

root:
  level: info
  appenders:
    - stdout
    - mqtt
"#;

    // Write config to temporary file
    std::fs::write("temp_mqtt_debug.yml", config_yaml).expect("Failed to write config");
    
    // Initialize log4rs
    match log4rs::init_file("temp_mqtt_debug.yml", deserializers) {
        Ok(_) => println!("Log4rs initialized successfully"),
        Err(e) => {
            eprintln!("Failed to initialize log4rs: {:?}", e);
            return;
        }
    }
    
    println!("Waiting 2 seconds for MQTT connection...");
    std::thread::sleep(std::time::Duration::from_secs(2));
    
    // Test logging
    info!("Test info message");
    warn!("Test warning message");
    error!("Test error message");
    
    println!("Waiting 3 seconds for messages to be sent...");
    std::thread::sleep(std::time::Duration::from_secs(3));
    
    // Cleanup
    std::fs::remove_file("temp_mqtt_debug.yml").ok();
    
    println!("Debug test completed. Check the debug output above for MQTT connection status.");
}