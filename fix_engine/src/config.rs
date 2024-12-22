use log::info;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Error, ErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::orderstore::OrderStore;
use crate::sequence::SequenceNumberStore;
use crate::{HEART_BT_INT, IS_INITIATOR, RECONNECT_INTERVAL};

/// Check if the configuration file exists in the specified directory.
/// Returns the path to the configuration file if it exists, otherwise returns an error.
pub fn check_config_file_existence(cwd: &PathBuf) -> io::Result<PathBuf> {
    let config_file_path = cwd.join("config").join("setting.conf");
    if !fs::metadata(&config_file_path).is_ok() {
        return Err(Error::new(
            ErrorKind::NotFound,
            "config/setting.conf file not found.",
        ));
    }
    Ok(config_file_path)
}

/// Load the configuration from the specified file path into a nested HashMap.
/// The outer HashMap's keys are section names, and the inner HashMap's keys are property names.
pub fn load_config(
    config_file_path: &PathBuf,
) -> Result<HashMap<String, HashMap<String, String>>, Error> {
    // Check if the configuration file exists
    if !config_file_path.exists() {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!(
                "Couldn't open {}: No such file or directory",
                config_file_path.display()
            ),
        ));
    }

    // Attempt to load the config file
    let conf = ini::macro_load(config_file_path.to_str().unwrap());

    // Create a HashMap to store the config data
    let mut config_map: HashMap<String, HashMap<String, String>> = HashMap::new();

    for (section, prop) in conf.iter() {
        let mut section_map: HashMap<String, String> = HashMap::new();
        for (key, value) in prop.iter() {
            if let Some(value) = value {
                section_map.insert(key.clone(), value.clone());
            }
        }
        config_map.insert(section.to_owned(), section_map);
    }
    Ok(config_map)
}

/// Parse and update a specified interval from the configuration map.
/// Uses a default value if the interval is not found or cannot be parsed.
fn parse_and_update_interval(
    config_map: &HashMap<String, HashMap<String, String>>,
    key: &str,
    default_value: u64,
    interval: &AtomicU64,
) -> io::Result<()> {
    let interval_str = config_map
        .get("session")
        .and_then(|session| session.get(key));

    let interval_value: u64 = match interval_str {
        Some(value) => value.parse().map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Failed to parse {}: {}", key, e),
            )
        })?,
        None => default_value,
    };

    interval.store(interval_value, Ordering::SeqCst);
    info!(">>>>>> Updated {}: {}", key, interval_value);
    Ok(())
}

/// Update the reconnect interval from the configuration map.
pub fn update_reconnect_interval(
    config_map: &HashMap<String, HashMap<String, String>>,
) -> io::Result<()> {
    parse_and_update_interval(config_map, "reconnect_interval", 30, &RECONNECT_INTERVAL)
}

/// Update the heartbeat interval from the configuration map.
pub fn update_heart_bt_int(
    config_map: &HashMap<String, HashMap<String, String>>,
) -> io::Result<()> {
    parse_and_update_interval(config_map, "heart_bt_int", 15, &HEART_BT_INT)
}

pub fn get_sequence_store(
    config_map: &HashMap<String, HashMap<String, String>>,
) -> Arc<SequenceNumberStore> {
    let sequence_file = config_map
        .get("session")
        .and_then(|session| session.get("sequence_store"))
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Other,
                "sequence_store not found in configuration.",
            )
        });
    Arc::new(SequenceNumberStore::new(sequence_file.unwrap()))
}

pub fn get_order_store(
    config_map: &HashMap<String, HashMap<String, String>>,
) -> Result<Arc<OrderStore>, Error> {
    let order_store_file = config_map
        .get("session")
        .and_then(|session| session.get("order_store"))
        .ok_or_else(|| Error::new(ErrorKind::Other, "order_store not found in configuration."))?;

    let order_store = OrderStore::new(order_store_file, 1024)?;
    Ok(Arc::new(order_store))
}

/// Get connection details (host and port) from the configuration map.
/// Determines the connection type (initiator or acceptor) and retrieves the corresponding host and port.
pub fn get_connection_details(
    config_map: &HashMap<String, HashMap<String, String>>,
) -> io::Result<(&str, u16)> {
    let (host, port): (&str, u16) = if IS_INITIATOR.load(Ordering::SeqCst) {
        let host_str = config_map
            .get("session")
            .and_then(|session| session.get("socket_connect_host"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "Host not found in configuration."))?;

        let port_str = config_map
            .get("session")
            .and_then(|session| session.get("socket_connect_port"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "Port not found in configuration."))?;

        (
            host_str,
            port_str
                .parse()
                .map_err(|e| Error::new(ErrorKind::Other, e))?,
        )
    } else {
        let host_str = config_map
            .get("session")
            .and_then(|session| session.get("socket_accept_address"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "Host not found in configuration."))?;

        let port_str = config_map
            .get("session")
            .and_then(|session| session.get("socket_accept_port"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "Port not found in configuration."))?;

        (
            host_str,
            port_str
                .parse()
                .map_err(|e| Error::new(ErrorKind::Other, e))?,
        )
    };
    Ok((host, port))
}

/// Determine if the connection type specified in the configuration map is "initiator".
/// Returns true if it is "initiator", otherwise returns false.
pub fn is_initiator(config_map: &HashMap<String, HashMap<String, String>>) -> bool {
    config_map
        .get("default")
        .and_then(|default| default.get("connection_type"))
        .map(|conn_type| conn_type == "initiator")
        .unwrap_or(false)
}

/// Determine if the enable command line specified in the configuration map is "enable_cmd_line".
pub fn enable_cmd_line(config_map: &HashMap<String, HashMap<String, String>>) -> bool {
    config_map
        .get("default")
        .and_then(|default| default.get("enable_cmd_line"))
        .map(|enable_flag| enable_flag == "true")
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use tempfile::tempdir;

    #[test]
    fn test_check_config_file_existence_file_exists() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("config").join("setting.conf");
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        std::fs::File::create(&file_path).unwrap();

        let result = check_config_file_existence(&PathBuf::from(dir.path()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), file_path);
    }

    #[test]
    fn test_check_config_file_existence_file_not_found() {
        let dir = tempdir().unwrap();
        let result = check_config_file_existence(&PathBuf::from(dir.path()));
        assert!(result.is_err());
    }

    #[test]
    fn test_load_config_success() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("setting.conf");
        let mut file = std::fs::File::create(&file_path).unwrap();
        write!(
            file,
            "[session]\nkey1=value1\nkey2=value2\n\n[default]\nkey3=value3\n"
        )
            .unwrap();

        let result = load_config(&file_path);
        assert!(result.is_ok());
        let config = result.unwrap();

        assert_eq!(config.get("session").unwrap().get("key1").unwrap(), "value1");
        assert_eq!(config.get("default").unwrap().get("key3").unwrap(), "value3");
    }

    #[test]
    fn test_load_config_file_not_found() {
        let result = load_config(&PathBuf::from("non_existent.conf"));
        assert!(result.is_err());
    }


    #[test]
    fn test_update_reconnect_interval() {
        let config = HashMap::from([(
            String::from("session"),
            HashMap::from([(
                String::from("reconnect_interval"),
                String::from("45"),
            )]),
        )]);
        let interval = AtomicU64::new(0);
        let result = parse_and_update_interval(&config, "reconnect_interval", 30, &interval);
        assert!(result.is_ok());
        assert_eq!(interval.load(Ordering::SeqCst), 45);
    }

    #[test]
    fn test_update_reconnect_interval_default() {
        let config = HashMap::new();
        let interval = AtomicU64::new(0);
        let result = parse_and_update_interval(&config, "reconnect_interval", 30, &interval);
        assert!(result.is_ok());
        assert_eq!(interval.load(Ordering::SeqCst), 30);
    }

    #[test]
    fn test_get_sequence_store() {
        let config = HashMap::from([(
            String::from("session"),
            HashMap::from([(
                String::from("sequence_store"),
                String::from("sequence.txt"),
            )]),
        )]);
        let store = get_sequence_store(&config);
        assert!(Arc::strong_count(&store) > 0);
    }

    #[test]
    fn test_get_order_store() {
        let config = HashMap::from([(
            String::from("session"),
            HashMap::from([(
                String::from("order_store"),
                String::from("order.txt"),
            )]),
        )]);
        let result = get_order_store(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_connection_details_initiator() {
        IS_INITIATOR.store(true, Ordering::SeqCst);
        let config = HashMap::from([(
            String::from("session"),
            HashMap::from([
                (String::from("socket_connect_host"), String::from("127.0.0.1")),
                (String::from("socket_connect_port"), String::from("8080")),
            ]),
        )]);

        let result = get_connection_details(&config);
        assert!(result.is_ok());
        let (host, port) = result.unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 8080);
    }

    #[test]
    fn test_get_connection_details_acceptor() {
        IS_INITIATOR.store(false, Ordering::SeqCst);
        let config = HashMap::from([(
            String::from("session"),
            HashMap::from([
                (String::from("socket_accept_address"), String::from("192.168.0.1")),
                (String::from("socket_accept_port"), String::from("9090")),
            ]),
        )]);

        let result = get_connection_details(&config);
        assert!(result.is_ok());
        let (host, port) = result.unwrap();
        assert_eq!(host, "192.168.0.1");
        assert_eq!(port, 9090);
    }

    #[test]
    fn test_is_initiator_true() {
        let config = HashMap::from([(
            String::from("default"),
            HashMap::from([(String::from("connection_type"), String::from("initiator"))]),
        )]);

        let result = is_initiator(&config);
        assert!(result);
    }

    #[test]
    fn test_is_initiator_false() {
        let config = HashMap::from([(
            String::from("default"),
            HashMap::from([(String::from("connection_type"), String::from("acceptor"))]),
        )]);

        let result = is_initiator(&config);
        assert!(!result);
    }

    #[test]
    fn test_enable_cmd_line_true() {
        let config = HashMap::from([(
            String::from("default"),
            HashMap::from([(String::from("enable_cmd_line"), String::from("true"))]),
        )]);

        let result = enable_cmd_line(&config);
        assert!(result);
    }

    #[test]
    fn test_enable_cmd_line_false() {
        let config = HashMap::from([(
            String::from("default"),
            HashMap::from([(String::from("enable_cmd_line"), String::from("false"))]),
        )]);

        let result = enable_cmd_line(&config);
        assert!(!result);
    }
}