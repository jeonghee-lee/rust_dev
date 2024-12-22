use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::{io, process, thread};

use chrono::Utc;
use log::{error, info};

use crate::{
    message_converter::{fixmap2fixmsg, fixmsg2msgtype, msgtype2fixmsg},
    message_handling::{
        client_session_thread, read_and_route_messages, send_message, venue_session_thread,
    },
    orderstore::OrderStore,
    parse_xml::print_fix_message,
    sequence::SequenceNumberStore,
    MessageMap, ENABLE_CMD_LINE, HEART_BT_INT, LAST_SENT_TIME, RECEIVED_LOGON, SENT_LOGON,
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
        let _ = read_and_route_messages(
            &mut stream,
            &all_msg_map_collection_clone,
            seq_store_clone,
            order_store_clone,
        );
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

fn run_periodic_task(
    stream: TcpStreamArcMutex,
    all_msg_map_collection: MessageMap,
    seq_store: Arc<SequenceNumberStore>,
) {
    let interval = Duration::from_secs(1);
    loop {
        sleep(interval);
        if let Err(e) = check_interval(stream.clone(), &all_msg_map_collection, &seq_store) {
            error!("Failed to perform periodic task: {}", e);
            process::exit(1);
        }
    }
}

fn check_interval(
    stream: TcpStreamArcMutex,
    all_msg_map_collection: &MessageMap,
    seq_store: &Arc<SequenceNumberStore>,
) -> Result<(), io::Error> {
    let now = Utc::now();
    let elapsed = now
        .signed_duration_since(LAST_SENT_TIME.load(Ordering::SeqCst))
        .num_seconds();
    let heart_bt_int = HEART_BT_INT.load(Ordering::SeqCst) as i64;

    if elapsed >= heart_bt_int {
        perform_task(stream.clone(), all_msg_map_collection.clone(), seq_store)?;
    }

    Ok(())
}

fn perform_task(
    stream: TcpStreamArcMutex,
    all_msg_map_collection: MessageMap,
    seq_store: &Arc<SequenceNumberStore>,
) -> Result<(), io::Error> {
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
    let listener = TcpListener::bind(&address).map_err(|e| {
        eprintln!("Failed to start listener at {address}: {e}");
        e
    })?;
    info!("Listening on {}", address);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                info!("New connection: {}", stream.peer_addr()?);
                let all_msg_map_collection_clone = Arc::clone(&all_msg_map_collection);
                let seq_store_clone = Arc::clone(&seq_store);
                let order_store_clone = Arc::clone(&order_store);
                thread::spawn(move || {
                    if let Err(e) = handle_stream(
                        stream,
                        &all_msg_map_collection_clone,
                        seq_store_clone,
                        order_store_clone,
                    ) {
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
    seq_store: Arc<SequenceNumberStore>,
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
    seq_store: Arc<SequenceNumberStore>,
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
    seq_store: Arc<SequenceNumberStore>,
) -> io::Result<()> {
    let mut input = String::new();
    loop {
        io::stdin().read_line(&mut input)?;
        if input.trim() == "exit" {
            break;
        } else {
            handle_input_message(
                input.trim(),
                input_stream.clone(),
                all_msg_map_collection,
                seq_store.clone(),
            )?;
        }
        input.clear();
    }

    Ok(())
}

fn handle_input_message(
    input: &str,
    input_stream: TcpStreamArcMutex,
    all_msg_map_collection: &MessageMap,
    seq_store: Arc<SequenceNumberStore>,
) -> io::Result<()> {
    if input.starts_with("8=FIX") {
        if let Ok(fix_details) =
            print_fix_message(input, &all_msg_map_collection.fix_tag_number_map)
        {
            println!("{}", fix_details);
        }

        if let Ok(fix_message) = crate::message_validator::FixMessage::parse(input) {
            if fix_message.validate(
                &all_msg_map_collection.required_fields,
                &all_msg_map_collection.valid_msg_types,
                &all_msg_map_collection.msgnumber_fields_map.clone(),
            ) {
                let (msgtype, msg_map) =
                    fixmsg2msgtype(input, &all_msg_map_collection.fix_tag_number_map).unwrap();
                info!("Parsed message type: {}, map: {:?}", msgtype, msg_map);

                let mut merged_msg_map = all_msg_map_collection.fix_header.clone();
                merged_msg_map.extend(msg_map);
                info!("Merged message map: {:?}", merged_msg_map);

                let mut msg = fixmap2fixmsg(
                    &merged_msg_map,
                    &all_msg_map_collection.fix_tag_name_map,
                    seq_store.get_outgoing(),
                );
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::net::TcpListener;
    use std::io::Read;
    use std::thread;

    use crate::sequence::SequenceNumberStore;
    use crate::orderstore::OrderStore;
    use crate::MessageMap;

    fn setup_dummy_msg_map() -> Arc<MessageMap> {
        // Assuming MessageMap implements Default or a similar scaffold
        Arc::new(MessageMap {
            admin_msg: Default::default(),
            admin_msg_list: Default::default(),
            app_msg: Default::default(),
            fix_tag_name_map: Default::default(),
            fix_tag_number_map: Default::default(),
            required_fields: Default::default(),
            valid_msg_types: Default::default(),
            msgnumber_fields_map: Default::default(),
            msgname_fields_map: Default::default(),
            fix_header: Default::default(),
        })
    }

    fn setup_dummy_sequence_store() -> Arc<SequenceNumberStore> {
        Arc::new(SequenceNumberStore::new("dummy_sequence.txt"))
    }

    fn setup_dummy_order_store() -> Arc<OrderStore> {
        Arc::new(OrderStore::new("dummy_order.txt", 1024).unwrap())
    }

    #[test]
    fn test_establish_connection_success() {
        // Set up a dummy server to allow connection testing
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server_address = listener.local_addr().unwrap();

        // Spawn a thread to accept connections
        thread::spawn(move || {
            for stream in listener.incoming() {
                if let Ok(_) = stream {
                    break;
                }
            }
        });

        // Attempt to establish connection
        let result = establish_connection(&server_address.ip().to_string(), server_address.port());
        assert!(result.is_ok());
        assert!(result.unwrap().peer_addr().is_ok());
    }

    #[test]
    fn test_establish_connection_failure() {
        // Attempt to connect to an invalid address
        let result = establish_connection("256.256.256.256", 8080);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_logon_message() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server_address = listener.local_addr().unwrap();

        // Spawn server thread
        let _server_thread = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = Vec::new();
                stream.read_to_end(&mut buffer).unwrap();
                assert!(buffer.starts_with(b"8=FIX"));
            }
        });

        // Client-side test
        let mut stream = establish_connection(&server_address.ip().to_string(), server_address.port()).unwrap();
        let all_msg_map_collection = setup_dummy_msg_map();
        let seq_store = setup_dummy_sequence_store();

        // Send the logon message
        let result = send_logon_message(&mut stream, &all_msg_map_collection, seq_store);
        assert!(result.is_ok());
    }
}