use std::{io, process, thread};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::thread::sleep;
use std::time::Duration;

use chrono::Utc;
use log::{error, info};

use crate::{
    ENABLE_CMD_LINE, HEART_BT_INT, LAST_SENT_TIME, MessageMap, RECEIVED_LOGON, SENT_LOGON,
    message_converter::{fixmsg2msgtype, msgtype2fixmsg, fixmap2fixmsg},
    message_handling::{client_session_thread, venue_session_thread, read_and_route_messages, send_message},
    orderstore::OrderStore,
    parse_xml::print_fix_message,
    sequence::SequenceNumberStore,
};

type TcpStreamArcMutex = Arc<Mutex<TcpStream>>;

/// Establishes a connection to the target IP and port.
pub fn establish_connection(target_ip: &str, port: u16) -> Result<TcpStream, io::Error> {
    let stream = TcpStream::connect((target_ip, port)).map_err(|e| {
        error!("Failed to connect to server: {}", e);
        e
    })?;
    let address = format!("{}:{}", target_ip, port);
    info!("Connected to {}", address);
    Ok(stream)
}

pub fn handle_stream(
    mut stream: TcpStream,
    all_msg_map_collection: &MessageMap,
    seq_store: Arc<SequenceNumberStore>,
    order_store: Arc<OrderStore>,
) -> io::Result<()> {
    let client_session_stream = stream.try_clone()?;
    let venue_session_stream = stream.try_clone()?;
    let input_stream = Arc::new(Mutex::new(stream.try_clone()?));
    let tick_stream = Arc::new(Mutex::new(stream.try_clone()?));

    let client_session_handle = thread::spawn(move || {
        client_session_thread(client_session_stream);
    });

    let venue_session_handle = thread::spawn(move || {
        venue_session_thread(venue_session_stream);
    });

    let all_msg_map_collection_clone = all_msg_map_collection.clone();
    let seq_store_clone = Arc::clone(&seq_store);
    let order_store_clone = Arc::clone(&order_store);
    let read_and_route_handle = thread::spawn(move || {
        let _ = read_and_route_messages(&mut stream, &all_msg_map_collection_clone, seq_store_clone, order_store_clone);
    });

    let all_msg_map_collection_clone2 = all_msg_map_collection.clone();
    let seq_store_clone = Arc::clone(&seq_store);
    let tick_handle = thread::spawn(move || {
        run_periodic_task(tick_stream, all_msg_map_collection_clone2, seq_store_clone);
    });

    if ENABLE_CMD_LINE.load(Ordering::SeqCst) {
        handle_cmd_line(input_stream, all_msg_map_collection, seq_store)?;
    }

    tick_handle.join().unwrap();
    read_and_route_handle.join().unwrap();
    client_session_handle.join().unwrap();
    venue_session_handle.join().unwrap();

    Ok(())
}

fn run_periodic_task(stream: TcpStreamArcMutex, all_msg_map_collection: MessageMap, seq_store: Arc<SequenceNumberStore>) {
    let interval = Duration::from_secs(1);
    loop {
        sleep(interval);
        if let Err(e) = check_interval(stream.clone(), &all_msg_map_collection, &seq_store) {
            error!("Failed to perform periodic task: {}", e);
            process::exit(1);
        }
    }
}

fn check_interval(stream: TcpStreamArcMutex, all_msg_map_collection: &MessageMap, seq_store: &Arc<SequenceNumberStore>) -> Result<(), io::Error> {
    let now = Utc::now();
    let elapsed = now.signed_duration_since(LAST_SENT_TIME.load(Ordering::SeqCst)).num_seconds();
    let heart_bt_int = HEART_BT_INT.load(Ordering::SeqCst) as i64;

    if elapsed >= heart_bt_int {
        perform_task(stream.clone(), all_msg_map_collection.clone(), seq_store)?;
    }

    Ok(())
}

fn perform_task(stream: TcpStreamArcMutex, all_msg_map_collection: MessageMap, seq_store: &Arc<SequenceNumberStore>) -> Result<(), io::Error> {
    let msgtype = if !RECEIVED_LOGON.load(Ordering::SeqCst) {
        "Logon"
    } else {
        "Heartbeat"
    };

    let response = msgtype2fixmsg(
        msgtype.to_string(),
        &all_msg_map_collection.admin_msg,
        &all_msg_map_collection.fix_tag_name_map,
        None,
        seq_store.get_outgoing(),
    );

    let modified_response = response.replace("|", "\x01");
    send_message(&stream, modified_response)?;
    seq_store.increment_outgoing();

    LAST_SENT_TIME.store(Utc::now(), Ordering::SeqCst);
    info!("{} message sent, updated last sent time", msgtype);

    Ok(())
}

/// Starts the TCP listener on the specified host and port, accepting incoming connections.
pub fn start_listener(
    host: &str,
    port: u16,
    all_msg_map_collection: Arc<MessageMap>,
    seq_store: Arc<SequenceNumberStore>,
    order_store: Arc<OrderStore>,
) -> io::Result<()> {
    let address = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&address)?;
    info!("Listening on {}", address);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                info!("New connection: {}", stream.peer_addr()?);
                let all_msg_map_collection_clone = Arc::clone(&all_msg_map_collection);
                let seq_store_clone = Arc::clone(&seq_store);
                let order_store_clone = Arc::clone(&order_store);
                thread::spawn(move || {
                    if let Err(e) = handle_stream(stream, &all_msg_map_collection_clone, seq_store_clone, order_store_clone) {
                        error!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}

pub fn send_logon_message(
    stream: &mut TcpStream,
    all_msg_map_collection: &Arc<MessageMap>,
    seq_store: Arc<SequenceNumberStore>
) -> io::Result<()> {
    let logon_message = build_logon_message(all_msg_map_collection, seq_store.clone());
    stream.write_all(logon_message.as_bytes())?;
    stream.flush()?;
    info!("Logon message sent");
    seq_store.increment_outgoing();

    SENT_LOGON.store(true, Ordering::SeqCst);
    Ok(())
}

/// Builds the logon message.
fn build_logon_message(
    all_msg_map_collection: &Arc<MessageMap>,
    seq_store: Arc<SequenceNumberStore>
) -> String {
    let fix_msg = msgtype2fixmsg(
        "Logon".to_string(),
        &all_msg_map_collection.admin_msg,
        &all_msg_map_collection.fix_tag_name_map,
        None,
        seq_store.get_outgoing(),
    );
    fix_msg.replace("|", "\x01")
}

fn handle_cmd_line(
    input_stream: TcpStreamArcMutex,
    all_msg_map_collection: &MessageMap,
    seq_store: Arc<SequenceNumberStore>
) -> io::Result<()> {
    let mut input = String::new();
    loop {
        io::stdin().read_line(&mut input)?;
        if input.trim() == "exit" {
            break;
        } else {
            handle_input_message(input.trim(), input_stream.clone(), all_msg_map_collection, seq_store.clone())?;
        }
        input.clear();
    }

    Ok(())
}

fn handle_input_message(
    input: &str,
    input_stream: TcpStreamArcMutex,
    all_msg_map_collection: &MessageMap,
    seq_store: Arc<SequenceNumberStore>
) -> io::Result<()> {
    if input.starts_with("8=FIX") {
        if let Ok(fix_details) = print_fix_message(input, &all_msg_map_collection.fix_tag_number_map) {
            println!("{}", fix_details);
        }

        if let Ok(fix_message) = crate::message_validator::FixMessage::parse(input) {
            if fix_message.validate(&all_msg_map_collection.required_fields, &all_msg_map_collection.valid_msg_types, &all_msg_map_collection.msgnumber_fields_map.clone()) {
                let (msgtype, msg_map) = fixmsg2msgtype(input, &all_msg_map_collection.fix_tag_number_map).unwrap();
                info!("Parsed message type: {}, map: {:?}", msgtype, msg_map);

                let mut merged_msg_map = all_msg_map_collection.fix_header.clone();
                merged_msg_map.extend(msg_map);
                info!("Merged message map: {:?}", merged_msg_map);

                let mut msg = fixmap2fixmsg(&merged_msg_map, &all_msg_map_collection.fix_tag_name_map, seq_store.get_outgoing());
                msg = msg.replace("|", "\x01");

                send_message(&input_stream, msg.clone())?;

                seq_store.increment_outgoing();
                LAST_SENT_TIME.store(Utc::now(), Ordering::SeqCst);
                info!("Message sent, updated last sent time");
            } else {
                error!("Message validation failed");
            }
        }
    }

    Ok(())
}
