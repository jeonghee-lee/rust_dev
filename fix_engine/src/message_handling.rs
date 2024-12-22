use chrono::Utc;
use indexmap::IndexMap;
use log::{error, info};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::process;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::message_converter::{fixmsg2msgtype, msgtype2fixmsg};
use crate::orderstore::{add_order_to_store, update_order_in_store, OrderStore};
use crate::parse_xml::{print_fix_message, FixTag};
use crate::sequence::SequenceNumberStore;
use crate::{MessageMap, IS_INITIATOR, LAST_SENT_TIME, RECEIVED_LOGON, SENT_LOGON};

pub fn read_and_route_messages(
    stream: &mut TcpStream,
    all_msg_map_collection: &MessageMap,
    seq_store: Arc<SequenceNumberStore>,
    order_store: Arc<OrderStore>,
) -> Result<(), io::Error> {
    let mut buf = [0; 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => {
                info!("Got disconnected, exiting!!");
                process::exit(1);
            }
            Ok(bytes_read) => {
                handle_incoming_message(
                    &buf[..bytes_read],
                    stream,
                    all_msg_map_collection,
                    Arc::clone(&seq_store),
                    Arc::clone(&order_store),
                )?;
            }
            Err(e) => {
                error!("Error reading from stream: {}", e);
                break;
            }
        }
        buf = [0; 1024];
    }
    Ok(())
}

fn handle_incoming_message(
    buf: &[u8],
    stream: &mut TcpStream,
    all_msg_map_collection: &MessageMap,
    seq_store: Arc<SequenceNumberStore>,
    order_store: Arc<OrderStore>,
) -> Result<(), io::Error> {
    if let Ok(message) = std::str::from_utf8(buf) {
        info!("Received message: {}", message);

        if is_fix_message(message) {
            process_fix_message(
                message,
                stream,
                all_msg_map_collection,
                Arc::clone(&seq_store),
                Arc::clone(&order_store),
            )?;
        }
    } else {
        info!("Received invalid UTF-8");
    }
    Ok(())
}

fn process_fix_message(
    message: &str,
    stream: &mut TcpStream,
    all_msg_map_collection: &MessageMap,
    seq_store: Arc<SequenceNumberStore>,
    order_store: Arc<OrderStore>,
) -> Result<(), io::Error> {
    if let Ok(fix_details) = print_fix_message(&message, &all_msg_map_collection.fix_tag_number_map)
    {
        println!("{}", fix_details);
    }

    let modified_message = message.replace('\x01', "|");
    if let Ok(fix_message) = crate::message_validator::FixMessage::parse(&modified_message) {
        if fix_message.validate(
            &all_msg_map_collection.required_fields,
            &all_msg_map_collection.valid_msg_types,
            &all_msg_map_collection.msgnumber_fields_map.clone(),
        ) {
            if let Ok((msgtype, msg_map)) =
                fixmsg2msgtype(&message, &all_msg_map_collection.fix_tag_number_map)
            {
                info!("Parsed message type: {}, map: {:?}", msgtype, msg_map);

                let expected_incoming_seq_num = seq_store.get_incoming();
                if let Some(incoming_seq_num) =
                    msg_map.get("MsgSeqNum").and_then(|s| s.parse::<u64>().ok())
                {
                    if expected_incoming_seq_num == incoming_seq_num {
                        println!(
                            "Expected incoming seq num: {} vs msg.MsgSeqNum: {}",
                            expected_incoming_seq_num, incoming_seq_num
                        );
                        seq_store.increment_incoming();

                        if is_admin_message(&msgtype, all_msg_map_collection.admin_msg_list.clone())
                        {
                            handle_admin_message(
                                stream.try_clone().expect("Failed to clone stream"),
                                &msgtype,
                                &msg_map,
                                &all_msg_map_collection.admin_msg,
                                &all_msg_map_collection.fix_tag_name_map,
                                message,
                                Arc::clone(&seq_store),
                            );
                        } else {
                            handle_business_message(
                                stream.try_clone().expect("Failed to clone stream"),
                                &msgtype,
                                &msg_map,
                                &all_msg_map_collection.app_msg,
                                &all_msg_map_collection.fix_tag_name_map,
                                message,
                                Arc::clone(&seq_store),
                                Arc::clone(&order_store),
                            );
                        }
                    } else if expected_incoming_seq_num < incoming_seq_num {
                        if msgtype == "SEQUENCE_RESET" {
                            handle_admin_message(
                                stream.try_clone().expect("Failed to clone stream"),
                                &msgtype,
                                &msg_map,
                                &all_msg_map_collection.admin_msg,
                                &all_msg_map_collection.fix_tag_name_map,
                                message,
                                Arc::clone(&seq_store),
                            );
                        } else {
                            println!("Resend Request, MsgSeqNum too high, expecting {} but received {}!!", expected_incoming_seq_num, incoming_seq_num);
                            handle_resend_request(
                                expected_incoming_seq_num,
                                &msgtype,
                                &all_msg_map_collection,
                                Arc::clone(&seq_store),
                                stream,
                            )?;
                        }
                    } else {
                        let err_text: String = format!(
                            "MsgSeqNum too low, expecting {} but received {}!!",
                            expected_incoming_seq_num, incoming_seq_num
                        );
                        handle_logout(
                            &err_text,
                            &msgtype,
                            &all_msg_map_collection,
                            Arc::clone(&seq_store),
                            stream,
                        )?;
                        process::exit(1);
                    }
                }
            } else {
                error!("fixmsg2msgtype parse error: {}", modified_message);
            }
        } else {
            error!(
                "Dropping the message due to validation failure!!! - {}",
                modified_message
            );
        }
    }
    Ok(())
}

fn handle_resend_request(
    expected_incoming_seq_num: u64,
    msgtype: &str,
    all_msg_map_collection: &MessageMap,
    seq_store: Arc<SequenceNumberStore>,
    stream: &mut TcpStream,
) -> Result<(), io::Error> {
    println!("Resend Request!!!");
    let mut override_map: HashMap<String, String> = HashMap::new();
    override_map.insert(
        "BeginSeqNo".to_string(),
        expected_incoming_seq_num.to_string(),
    );
    let fix_msg: String = msgtype2fixmsg(
        "Resend_Request".to_string(),
        &all_msg_map_collection.admin_msg,
        &all_msg_map_collection.fix_tag_name_map,
        Some(&override_map),
        seq_store.get_outgoing(),
    );
    println!("{}", fix_msg);
    let modified_response = fix_msg.replace("|", "\x01");
    let new_stream = stream.try_clone()?;
    let stream = Arc::new(Mutex::new(new_stream));
    if let Err(err) = send_message(&stream, modified_response) {
        error!("Failed to send resend request response: {}", err);
    }
    seq_store.increment_outgoing();
    Ok(())
}

fn handle_logout(
    err_text: &str,
    msgtype: &str,
    all_msg_map_collection: &MessageMap,
    seq_store: Arc<SequenceNumberStore>,
    stream: &mut TcpStream,
) -> Result<(), io::Error> {
    let mut override_map: HashMap<String, String> = HashMap::new();
    override_map.insert("Text".to_string(), err_text.to_string());
    let fix_msg: String = msgtype2fixmsg(
        "Logout".to_string(),
        &all_msg_map_collection.admin_msg,
        &all_msg_map_collection.fix_tag_name_map,
        Some(&override_map),
        seq_store.get_outgoing(),
    );
    println!("{}", fix_msg);
    let modified_response = fix_msg.replace("|", "\x01");
    let new_stream = stream.try_clone()?;
    let stream = Arc::new(Mutex::new(new_stream));
    if let Err(err) = send_message(&stream, modified_response) {
        error!("Failed to send logout response: {}", err);
    }
    seq_store.increment_outgoing();
    Ok(())
}

pub fn handle_admin_message(
    stream: TcpStream,
    msgtype: &str,
    msg_map: &IndexMap<String, String>,
    admin_msg: &HashMap<String, IndexMap<String, String>>,
    fix_tag_name_map: &HashMap<String, FixTag>,
    message: &str,
    seq_store: Arc<SequenceNumberStore>,
) {
    info!("Handling admin message {}: {}", msgtype, message);

    if SENT_LOGON.load(Ordering::SeqCst) && msgtype == "LOGON" {
        if IS_INITIATOR.load(Ordering::SeqCst) {
            RECEIVED_LOGON.store(true, Ordering::SeqCst);
            info!(
                "Initiator received the Logon message: RECEIVED_LOGON - {}",
                RECEIVED_LOGON.load(Ordering::SeqCst)
            );
        }
        info!(
            "No message sent: SENT_LOGON - {}",
            SENT_LOGON.load(Ordering::SeqCst)
        );
        return;
    }
    let response = match msgtype {
        "LOGON" => {
            // Set the RECEIVED_LOGON and SENT_LOGON flags to true
            RECEIVED_LOGON.store(true, Ordering::SeqCst);
            SENT_LOGON.store(true, Ordering::SeqCst);

            // Generate the FIX message for Logon
            msgtype2fixmsg(
                "Logon".to_string(),      // The type of message
                admin_msg,                // The admin message
                fix_tag_name_map,         // The FIX tag name map
                None,                     // No overrides
                seq_store.get_outgoing(), // The current outgoing sequence number
            )
        }

        "HEARTBEAT" | "TEST_REQUEST" => {
            // Generate the FIX message for Heartbeat
            msgtype2fixmsg(
                "Heartbeat".to_string(),  // The type of message
                admin_msg,                // The admin message
                fix_tag_name_map,         // The FIX tag name map
                None,                     // No overrides
                seq_store.get_outgoing(), // The current outgoing sequence number
            )
        }

        "RESEND_REQUEST" => {
            // Create a new HashMap to hold the override mappings
            let mut override_map: HashMap<String, String> = HashMap::new();
            // Insert the current incoming sequence number into the override map
            override_map.insert("NewSeqNo".to_string(), seq_store.get_incoming().to_string());
            // Generate the FIX message for Sequence_Reset
            msgtype2fixmsg(
                "Sequence_Reset".to_string(), // The type of message
                admin_msg,                    // The admin message
                fix_tag_name_map,             // The FIX tag name map
                Some(&override_map),          // The override map with the new sequence number
                seq_store.get_outgoing(),     // The current outgoing sequence number
            )
        }

        "SEQUENCE_RESET" => {
            // Retrieve the value associated with "NewSeqNo" and attempt to parse it as an u64
            let new_seqno: u64 = msg_map
                .get("NewSeqNo")
                .expect("NewSeqNo key missing in msg_map")
                .parse::<u64>()
                .expect("Failed to parse NewSeqNo as u64");

            // Log the reset of the outgoing sequence number
            info!(
                "Resetting Outgoing Sequence number! {} -> {}",
                seq_store.get_outgoing(),
                new_seqno
            );

            // Update the outgoing sequence number
            seq_store.set_outgoing(new_seqno);

            // Return an empty string
            "".to_string()
        }
        _ => "".to_string(),
    };

    if !response.is_empty() {
        let modified_response = response.replace("|", "\x01");
        let stream = Arc::new(Mutex::new(stream));
        if let Err(err) = send_message(&stream, modified_response) {
            error!("Failed to send admin response: {}", err);
        }
        seq_store.increment_outgoing();

        LAST_SENT_TIME.store(Utc::now(), Ordering::SeqCst);
        info!(
            "Updated last sent time: {:?}",
            LAST_SENT_TIME.load(Ordering::SeqCst)
        );
    } else {
        info!("Nothing to send out!");
    }
}

pub fn handle_business_message(
    stream: TcpStream,
    msgtype: &str,
    msg_map: &IndexMap<String, String>,
    app_msg: &HashMap<String, IndexMap<String, String>>,
    fix_tag_name_map: &HashMap<String, FixTag>,
    message: &str,
    seq_store: Arc<SequenceNumberStore>,
    order_store: Arc<OrderStore>,
) {
    info!("Handling business message {}: {}", msgtype, message);

    let response = match msgtype {
        "NEW_ORDER_SINGLE" => handle_new_order_single(
            msg_map,
            app_msg,
            fix_tag_name_map,
            seq_store.clone(),
            order_store.clone(),
        ),
        "ORDER_CANCEL_REPLACE_REQUEST" => handle_order_cancel_replace_request(
            msg_map,
            app_msg,
            fix_tag_name_map,
            seq_store.clone(),
            order_store.clone(),
        ),
        "ORDER_CANCEL_REQUEST" => handle_order_cancel_request(
            msg_map,
            app_msg,
            fix_tag_name_map,
            seq_store.clone(),
            order_store.clone(),
        ),
        "EXECUTION_REPORT" => "".to_string(), // TODO
        // "BUSINESS_MESSAGE_REJECT" => msgtype2fixmsg("Business_Message_Reject".to_string(), app_msg, fix_tag_name_map, None, seq_store.get_outgoing()),
        _ => msgtype2fixmsg(
            "Business_Message_Reject".to_string(),
            app_msg,
            fix_tag_name_map,
            None,
            seq_store.get_outgoing(),
        ),
    };

    if !response.is_empty() {
        let modified_response = response.replace("|", "\x01");
        let stream = Arc::new(Mutex::new(stream));
        if let Err(err) = send_message(&stream, modified_response) {
            error!("Failed to send business response: {}", err);
        }
        seq_store.increment_outgoing();
    } else {
        info!(" >>>> No message to send out");
    }
}

fn is_fix_message(message: &str) -> bool {
    message.contains("8=FIX")
}

fn is_admin_message(msgtype: &str, admin_msg_list: Vec<String>) -> bool {
    admin_msg_list.contains(&msgtype.to_string())
}

fn handle_new_order_single(
    msg_map: &IndexMap<String, String>,
    app_msg: &HashMap<String, IndexMap<String, String>>,
    fix_tag_name_map: &HashMap<String, FixTag>,
    seq_store: Arc<SequenceNumberStore>,
    order_store: Arc<OrderStore>,
) -> String {
    // Add an order
    if let (
        Some(clordid),
        Some(symbol),
        Some(side),
        Some(orderqty),
        Some(price),
        Some(ordtype),
        Some(transacttime),
    ) = (
        msg_map.get("ClOrdID"),
        msg_map.get("Symbol"),
        msg_map.get("Side"),
        msg_map.get("OrderQty"),
        msg_map.get("Price"),
        msg_map.get("OrdType"),
        msg_map.get("TransactTime"),
    ) {
        let mut msg_map_clone = msg_map.clone();
        msg_map_clone.insert("OrdStatus".to_string(), "New".to_string());
        add_order_to_store(order_store.clone(), &msg_map_clone).expect("Failed to add order");

        match order_store.print_orders() {
            Ok(fix_details) => println!("{}", fix_details),
            Err(err) => error!("Failed to print orders: {:?}", err),
        }

        if IS_INITIATOR.load(Ordering::SeqCst) {
            info!("Oops, got a new order single message from server!");
            "".to_string() // if client(initiator) get new order single nessage, it will be ignored!
        } else {
            info!("Preparing Execution_Report message for New Order Single Request");
            let override_map = prepare_execution_report(
                Some(clordid),                                           // orderid
                Some("XYZ123"),                                          // execid
                Some(msg_map.get("Account").unwrap_or(&"".to_string())), // account
                Some(symbol),                                            // symbol
                Some(side),                                              // side
                Some(ordtype),                                           // ordtype
                Some(transacttime),                                      // transacttime
                Some(orderqty),                                          // orderqty
                Some("0"),                                               // lastshares
                Some(price),                                             // lastpx
                Some("0"),                                               // leavesqty
                Some("0"),                                               // cumqty
                Some("0"),                                               // avgpx
                Some("0"),                                               // exectranstype
                Some("0"),                                               // exectype
                Some("0"),                                               // ordstatus
            );

            msgtype2fixmsg(
                "Execution_Report".to_string(),
                app_msg,
                fix_tag_name_map,
                Some(&override_map),
                seq_store.get_outgoing(),
            )
        }
    } else {
        if IS_INITIATOR.load(Ordering::SeqCst) {
            info!(
                "Oops, got a new order single message which has some missing fields from server!"
            );
            "".to_string() // if client(initiator) get new order single nessage, it will be ignored!
        } else {
            error!("Missing fields in NEW_ORDER_SINGLE message");

            let override_map = prepare_execution_report(
                Some(msg_map.get("ClOrdID").unwrap_or(&"".to_string())), // orderid
                Some("XYZ123"),                                          // execid
                Some(msg_map.get("Account").unwrap_or(&"".to_string())), // account
                Some(msg_map.get("Symbol").unwrap_or(&"".to_string())),  // symbol
                Some(msg_map.get("Side").unwrap_or(&"".to_string())),    // side
                Some(msg_map.get("OrdType").unwrap_or(&"".to_string())), // ordtype
                Some(msg_map.get("TransactTime").unwrap_or(&"".to_string())), // transacttime
                Some("0"),                                               // orderqty
                Some("0"),                                               // lastshares
                Some(msg_map.get("Price").unwrap_or(&"".to_string())),   // lastpx
                Some("0"),                                               // leavesqty
                Some("0"),                                               // cumqty
                Some("0"),                                               // avgpx
                Some("0"),                                               // exectranstype
                Some("8"),                                               // exectype
                Some("8"),                                               // ordstatus
            );

            msgtype2fixmsg(
                "Execution_Report".to_string(),
                app_msg,
                fix_tag_name_map,
                Some(&override_map),
                seq_store.get_outgoing(),
            )
        }
    }
}

fn handle_order_cancel_replace_request(
    msg_map: &IndexMap<String, String>,
    app_msg: &HashMap<String, IndexMap<String, String>>,
    fix_tag_name_map: &HashMap<String, FixTag>,
    seq_store: Arc<SequenceNumberStore>,
    order_store: Arc<OrderStore>,
) -> String {
    if let (
        Some(origclordid),
        Some(clordid),
        Some(symbol),
        Some(side),
        Some(orderqty),
        Some(price),
        Some(ordtype),
        Some(transacttime),
    ) = (
        msg_map.get("OrigClOrdID"),
        msg_map.get("ClOrdID"),
        msg_map.get("Symbol"),
        msg_map.get("Side"),
        msg_map.get("OrderQty"),
        msg_map.get("Price"),
        msg_map.get("OrdType"),
        msg_map.get("TransactTime"),
    ) {
        let mut msg_map_clone = msg_map.clone();
        msg_map_clone.insert("OrdStatus".to_string(), "Replaced".to_string());
        update_order_in_store(order_store.clone(), &msg_map_clone).expect("Failed to add order");

        match order_store.print_orders() {
            Ok(fix_details) => println!("{}", fix_details),
            Err(err) => error!("Failed to print orders: {:?}", err),
        };
        if IS_INITIATOR.load(Ordering::SeqCst) {
            info!("Oops, got a order cancel replace message from server!");
            "".to_string() // if client(initiator) get new order single nessage, it will be ignored!
        } else {
            info!("Preparing Execution_Report message for Cancel Replace Request");

            let override_map = prepare_execution_report(
                Some(clordid),                                           // orderid
                Some("XYZ123"),                                          // execid
                Some(msg_map.get("Account").unwrap_or(&"".to_string())), // account
                Some(symbol),                                            // symbol
                Some(side),                                              // side
                Some(ordtype),                                           // ordtype
                Some(transacttime),                                      // transacttime
                Some(orderqty),                                          // orderqty
                Some("0"),                                               // lastshares
                Some(price),                                             // lastpx
                Some("0"),                                               // leavesqty
                Some("0"),                                               // cumqty
                Some("0"),                                               // avgpx
                Some("2"),                                               // exectranstype
                Some("5"),                                               // exectype
                Some("5"),                                               // ordstatus
            );

            msgtype2fixmsg(
                "Execution_Report".to_string(),
                app_msg,
                fix_tag_name_map,
                Some(&override_map),
                seq_store.get_outgoing(),
            )
        }
    } else {
        if IS_INITIATOR.load(Ordering::SeqCst) {
            info!("Oops, got a order cancel replace message which has some missing fields from server!");
            "".to_string() // if client(initiator) get new order single nessage, it will be ignored!
        } else {
            error!("Missing fields in ORDER_CANCEL_REPLACE_REQUEST message");
            msgtype2fixmsg(
                "Order_Cancel_Reject".to_string(),
                app_msg,
                fix_tag_name_map,
                None,
                seq_store.get_outgoing(),
            )
        }
    }
}

fn handle_order_cancel_request(
    msg_map: &IndexMap<String, String>,
    app_msg: &HashMap<String, IndexMap<String, String>>,
    fix_tag_name_map: &HashMap<String, FixTag>,
    seq_store: Arc<SequenceNumberStore>,
    order_store: Arc<OrderStore>,
) -> String {
    if let (
        Some(origclordid),
        Some(clordid),
        Some(symbol),
        Some(side),
        Some(orderqty),
        Some(price),
        Some(ordtype),
        Some(transacttime),
    ) = (
        msg_map.get("OrigClOrdID"),
        msg_map.get("ClOrdID"),
        msg_map.get("Symbol"),
        msg_map.get("Side"),
        msg_map.get("OrderQty"),
        msg_map.get("Price"),
        msg_map.get("OrdType"),
        msg_map.get("TransactTime"),
    ) {
        let mut msg_map_clone = msg_map.clone();
        msg_map_clone.insert("OrdStatus".to_string(), "Canceled".to_string());
        update_order_in_store(order_store.clone(), &msg_map_clone).expect("Failed to add order");

        match order_store.print_orders() {
            Ok(fix_details) => println!("{}", fix_details),
            Err(err) => error!("Failed to print orders: {:?}", err),
        };

        if IS_INITIATOR.load(Ordering::SeqCst) {
            info!("Oops, got a order cancel message from server!");
            "".to_string() // if client(initiator) get new order single message, it will be ignored!
        } else {
            info!("Preparing Execution_Report message for Cancel Request");

            let override_map = prepare_execution_report(
                Some(clordid),      // orderid
                Some("XYZ123"),     // execid
                None,               // account
                Some(symbol),       // symbol
                Some(side),         // side
                None,               // ordtype
                Some(transacttime), // transacttime
                None,               // orderqty
                None,               // lastshares
                None,               // lastpx
                None,               // leavesqty
                None,               // cumqty
                None,               // avgpx
                Some("1"),          // exectranstype
                Some("4"),          // exectype
                Some("4"),          // ordstatus
            );
            msgtype2fixmsg(
                "Execution_Report".to_string(),
                app_msg,
                fix_tag_name_map,
                Some(&override_map),
                seq_store.get_outgoing(),
            )
        }
    } else {
        if IS_INITIATOR.load(Ordering::SeqCst) {
            info!("Oops, got a order cancel message which has some missing fields from server!");
            "".to_string() // if client(initiator) get new order single message, it will be ignored!
        } else {
            error!("Missing fields in ORDER_CANCEL_REQUEST message");
            msgtype2fixmsg(
                "Order_Cancel_Reject".to_string(),
                app_msg,
                fix_tag_name_map,
                None,
                seq_store.get_outgoing(),
            )
        }
    }
}

fn insert_if_some_and_not_empty(map: &mut HashMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        if !value.is_empty() {
            map.insert(key.to_string(), value.to_string());
        }
    }
}

fn prepare_execution_report(
    orderid: Option<&str>,
    execid: Option<&str>,
    account: Option<&str>,
    symbol: Option<&str>,
    side: Option<&str>,
    ordtype: Option<&str>,
    transactiontime: Option<&str>,
    orderqty: Option<&str>,
    lastshares: Option<&str>,
    lastpx: Option<&str>,
    leavesqty: Option<&str>,
    cumqty: Option<&str>,
    avgpx: Option<&str>,
    exectranstype: Option<&str>,
    exectype: Option<&str>,
    ordstatus: Option<&str>,
) -> HashMap<String, String> {
    let mut override_map = HashMap::new();

    insert_if_some_and_not_empty(&mut override_map, "OrderID", orderid);
    insert_if_some_and_not_empty(&mut override_map, "ExecID", execid);
    insert_if_some_and_not_empty(&mut override_map, "Account", account);
    insert_if_some_and_not_empty(&mut override_map, "Symbol", symbol);
    insert_if_some_and_not_empty(&mut override_map, "Side", side);
    insert_if_some_and_not_empty(&mut override_map, "OrdType", ordtype);
    insert_if_some_and_not_empty(&mut override_map, "TransactionTime", transactiontime);
    insert_if_some_and_not_empty(&mut override_map, "OrderQty", orderqty);
    insert_if_some_and_not_empty(&mut override_map, "LastShares", lastshares);
    insert_if_some_and_not_empty(&mut override_map, "LastPx", lastpx);
    insert_if_some_and_not_empty(&mut override_map, "LeavesQty", leavesqty);
    insert_if_some_and_not_empty(&mut override_map, "CumQty", cumqty);
    insert_if_some_and_not_empty(&mut override_map, "AvgPx", avgpx);
    insert_if_some_and_not_empty(&mut override_map, "ExecTransType", exectranstype);
    insert_if_some_and_not_empty(&mut override_map, "ExecType", exectype);
    insert_if_some_and_not_empty(&mut override_map, "OrdStatus", ordstatus);

    override_map
}

pub fn send_message(stream: &Arc<Mutex<TcpStream>>, message: String) -> Result<(), io::Error> {
    let mut stream = stream.lock().unwrap();
    stream.write_all(message.as_bytes())?;
    stream.flush()?;
    info!("sent out message: {}", message);
    Ok(())
}

pub fn client_session_thread(_stream: TcpStream) {
    // let ten_millis = time::Duration::from_millis(1000);
    // sleep(ten_millis);
    info!("Client session thread started.");
}

pub fn venue_session_thread(_stream: TcpStream) {
    info!("Venue session thread started.");
}
