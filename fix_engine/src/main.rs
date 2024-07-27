#[macro_use]
extern crate lazy_static;
extern crate log;

use std::{
    collections::HashMap,
    env,
    io::{self, Error, ErrorKind},
    path::PathBuf,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64},
};
use std::sync::atomic::Ordering;

use chrono::Utc;
use flexi_logger::{Duplicate, FileSpec, Logger};
use indexmap::IndexMap;
use log::{error, info};

pub use macros::*;

use crate::{
    config::{check_config_file_existence, get_connection_details, is_initiator,
             load_config, update_heart_bt_int, update_reconnect_interval, enable_cmd_line,
             get_sequence_store, get_order_store},
    connection::{establish_connection, handle_stream, send_logon_message, start_listener},
    message_converter::read_json_file,
    parse_payload_xml::{FixMsgTag, parse_fix_payload_xml},
    parse_xml::{FixTag, parse_fix_xml},
    sequence::SequenceNumberStore,
};
use crate::orderstore::OrderStore;

mod config;
mod parse_xml;
mod connection;
mod message_handling;
mod parse_payload_xml;
mod message_converter;
mod macros;
mod message_validator;
mod sequence;
mod orderstore;

// Define global variables wrapped in Arc<Mutex<>> using custom macros
initialize_flag!(ENABLE_CMD_LINE, false);
initialize_flag!(SENT_LOGON, false);
initialize_flag!(RECEIVED_LOGON, false);
initialize_flag!(IS_LOGGED_ON, false);
initialize_flag!(IS_INITIATOR, false);
initialize_atomic_datetime!(LAST_SENT_TIME);
initialize_value!(HEART_BT_INT, 15);
initialize_value!(RECONNECT_INTERVAL, 30);

#[derive(Clone)]
pub struct MessageMap {
    fix_header: IndexMap<String, String>,
    fix_tag_number_map: HashMap<u32, FixTag>,
    admin_msg_list: Vec<String>,
    admin_msg: HashMap<String, IndexMap<String, String>>,
    app_msg: HashMap<String, IndexMap<String, String>>,
    fix_tag_name_map: HashMap<String, FixTag>,
    msgname_fields_map: HashMap<String, FixMsgTag>,
    msgnumber_fields_map: HashMap<String, FixMsgTag>,
    valid_msg_types: Vec<String>,
    required_fields: Vec<String>
}

fn main() -> io::Result<()> {
    let _ = configure_logger();

    let cwd = env::current_dir()?;
    info!("Current working directory: {}", cwd.display());

    let config_file_path = check_config_file_existence(&cwd)?;
    info!("Config file path: {}", config_file_path.display());

    let config_map = load_config(&config_file_path)?;

    // Update the ENABLE_CMD_LINE flag
    ENABLE_CMD_LINE.store(enable_cmd_line(&config_map), Ordering::SeqCst);
    IS_INITIATOR.store(is_initiator(&config_map), Ordering::SeqCst);
    update_reconnect_interval(&config_map)?;
    update_heart_bt_int(&config_map)?;

    let sequence_store: Arc<SequenceNumberStore> = get_sequence_store(&config_map);

    let order_store : Arc<OrderStore>= get_order_store(&config_map)?;

    let (host, port) = get_connection_details(&config_map)?;
    let all_msg_map_collection = initialize_message_maps(&cwd, &config_map)?;

    info!("Application started successfully");

    if IS_INITIATOR.load(Ordering::SeqCst)  {
        let mut stream = establish_connection(&host, port)?;

        let seq_store_clone = Arc::clone(&sequence_store);
        send_logon_message(&mut stream, &all_msg_map_collection, seq_store_clone)?;

        let order_store_clone = Arc::clone(&order_store);

        let seq_store_clone = Arc::clone(&sequence_store);
        if let Err(e) = handle_stream(stream, &all_msg_map_collection, seq_store_clone, order_store_clone) {
            error!("Error handling client: {}", e);
        }
    } else {
        start_listener(host, port, all_msg_map_collection, sequence_store, order_store)?;
    }
    Ok(())
}

fn configure_logger() -> Result<(), flexi_logger::FlexiLoggerError> {
    Logger::try_with_str("info")
        .unwrap()
        .format(|write, now, record| {
            writeln!(
                write,
                "[{}] [{}] [{:?}] {}",
                now.now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                std::thread::current().id(),
                record.args()
            )
        })
        .duplicate_to_stdout(Duplicate::All)
        .log_to_file(FileSpec::default().directory("logs"))
        .start()?;
    info!("Logger initialized.");
    Ok(())
}

fn initialize_message_maps(
    cwd: &PathBuf,
    config_map: &HashMap<String, HashMap<String, String>>,
) -> io::Result<Arc<MessageMap>> {
    let mut payload_xml_path = cwd.join("reference").join("FIX4_2_Payload.xml");
    let mut fix_tag_xml_path = cwd.join("reference").join("FIX4_2.xml");

    let use_data_dictionary = config_map
        .get("session")
        .and_then(|session| session.get("use_data_dictionary"))
        .ok_or_else(|| Error::new(ErrorKind::Other, "use_data_dictionary not found in configuration."))?;

    info!("config_map:session:use_data_dictionary - [{}]", use_data_dictionary);

    if use_data_dictionary == "Y" {
        let use_data_dictionary_path = config_map
            .get("session")
            .and_then(|session| session.get("data_dictionary"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "data_dictionary not found in configuration."))?;

        fix_tag_xml_path = cwd.join(use_data_dictionary_path);
        info!("config_map:session:data_dictionary - [{}]", fix_tag_xml_path.display());

        let data_payload_dictionary_path = config_map
            .get("session")
            .and_then(|session| session.get("data_payload_dictionary"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "data_payload_dictionary not found in configuration."))?;

        payload_xml_path = cwd.join(data_payload_dictionary_path);
        info!("config_map:session:data_payload_dictionary - [{}]", payload_xml_path.display());
    }

    let admin_messages_list = config_map
        .get("session")
        .and_then(|session| session.get("admin_messages"))
        .ok_or_else(|| Error::new(ErrorKind::Other, "admin_messages not found in configuration."))?;

    info!("config_map:session:admin_messages - [{}]", admin_messages_list);

    let admin_msg_list: Vec<String> = admin_messages_list
        .split(',')
        .map(|s| s.trim().to_string().to_uppercase())
        .collect();

    let (fix_tagname_number_map, fix_number_tagname_map, msgtype_name_map, _msgname_type_map) = parse_fix_xml(fix_tag_xml_path.to_str().unwrap()).unwrap();
    let (msgname_fields_map, msgnumber_fields_map) = parse_fix_payload_xml(payload_xml_path.to_str().unwrap(), &msgtype_name_map, &fix_number_tagname_map).unwrap();

    // Read predefined messages from JSON file
    let (fix_header, admin_msg, app_msg) = match read_json_file("reference/predefined_msg.json") {
        Ok(result) => result,
        Err(e) => return Err(Error::new(ErrorKind::Other, e.to_string())),
    };

    // Predefined valid message types for validation
    let valid_msg_types: Vec<String> = msgtype_name_map.keys().cloned().collect();

    // Extract the header field information safely
    let required_fields: Vec<String> = match msgnumber_fields_map.get(&"<".to_string()) {
        Some(header_fld_info) => match &header_fld_info.field {
            Some(field_map) => field_map.keys().cloned().collect(),
            None=> {
                error!("Header field information is empty");
                Vec::new() // or you could return a default Vec if needed
            }
        },
        None => {
            error!("Header field information not found");
            Vec::new() // or you could return a default Vec if needed
        }
    };

    Ok(Arc::new(MessageMap {
        fix_header,
        fix_tag_number_map: fix_tagname_number_map,
        admin_msg_list,
        admin_msg,
        app_msg,
        fix_tag_name_map: fix_number_tagname_map,
        msgname_fields_map,
        msgnumber_fields_map,
        valid_msg_types,
        required_fields
    }))
}