use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

use chrono::Utc;
use indexmap::IndexMap;
use json::JsonValue;
use log::{error, info};

use crate::parse_xml::{FixError, FixTag};

/// Reads and parses a JSON file containing FIX message definitions.
pub fn read_json_file(
    file_path: &str
) -> Result<(IndexMap<String, String>, HashMap<String, IndexMap<String, String>>, HashMap<String, IndexMap<String, String>>), Box<dyn std::error::Error>> {
    // Open the JSON file
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);

    // Read the JSON content into a string
    let mut contents = String::new();
    reader.read_to_string(&mut contents)?;

    // Parse JSON string into JsonValue
    let json_value = json::parse(&contents)?;

    // Extract FIX message sections from JSON
    let (fix_header, admin_msg, app_msg) = extract_fix_sections(&json_value)?;

    Ok((fix_header, admin_msg, app_msg))
}

/// Extracts FIX message sections (header, admin, app) from JSON value.
fn extract_fix_sections(
    json_value: &JsonValue
) -> Result<(IndexMap<String, String>, HashMap<String, IndexMap<String, String>>, HashMap<String, IndexMap<String, String>>), Box<dyn std::error::Error>> {
    let fix_header = extract_section(json_value, "header")?;
    let admin_msg = extract_msg_map(json_value, "admin", &fix_header)?;
    let app_msg = extract_msg_map(json_value, "app", &fix_header)?;

    Ok((fix_header, admin_msg, app_msg))
}

/// Extracts a single section (header, admin, app) from JSON value.
fn extract_section(
    json_value: &JsonValue,
    section_name: &str
) -> Result<IndexMap<String, String>, Box<dyn std::error::Error>> {
    let mut section_map = IndexMap::new();

    if let JsonValue::Object(obj) = json_value {
        if let Some(section) = obj.get(section_name) {
            if let JsonValue::Object(section_obj) = section {
                for (key, value) in section_obj.iter() {
                    section_map.insert(key.to_string(), value.as_str().unwrap_or("").to_string());
                }
            }
        }
    }

    Ok(section_map)
}

/// Extracts and constructs message maps (admin, app) from JSON value.
fn extract_msg_map(
    json_value: &JsonValue,
    msg_type: &str,
    fix_header: &IndexMap<String, String>
) -> Result<HashMap<String, IndexMap<String, String>>, Box<dyn std::error::Error>> {
    let mut msg_map = HashMap::new();

    if let JsonValue::Object(obj) = json_value {
        if let Some(msg_section) = obj.get(msg_type) {
            if let JsonValue::Object(msg_obj) = msg_section {
                for (key, value) in msg_obj.iter() {
                    let mut msg_tags = IndexMap::new();

                    // Populate with fix_header tags
                    for (f_k, f_v) in fix_header.iter() {
                        if f_k == "MsgType" {
                            msg_tags.insert(f_k.clone(), key.to_string().clone());
                        } else {
                            msg_tags.insert(f_k.clone(), f_v.clone());
                        }
                    }

                    // Populate with current msg_obj tags
                    for (k, v) in value.entries() {
                        msg_tags.insert(k.to_string(), v.as_str().unwrap_or("").to_string());
                    }

                    msg_map.insert(key.to_string(), msg_tags);
                }
            }
        }
    }

    Ok(msg_map)
}

pub fn fixmsg2msgtype(
    fixmsg: &str,
    fix_tag_number_map: &HashMap<u32, FixTag>,
) -> Result<(String, IndexMap<String, String>), FixError> {
    let modified_message = fixmsg.replace('\x01', "|");
    info!("{}", modified_message);

    let fields: Vec<&str> = modified_message.split('|').collect();
    let mut msgtype = String::new();
    let mut msg_map = IndexMap::new();

    for field in fields {
        let parts: Vec<&str> = field.split('=').collect();
        if parts.len() == 2 {
            if let Ok(tag) = parts[0].parse::<u32>() {
                if let Some(tag_definition) = fix_tag_number_map.get(&tag) {
                    let tag_value = parts[1];
                    if let Some(enum_values) = &tag_definition.enum_values {
                        let enum_description = match enum_values.get(tag_value) {
                            Some(desc) => desc.clone(),
                            None => {
                                println!("{} - Enum value not found for tag {}: {}", tag_definition.name, tag, tag_value);
                                // "".to_string() // You can return an empty string or handle this case as needed
                                tag_value.to_string()
                            }
                        };
                        if tag_definition.name == "MsgType" {
                            msgtype = enum_description.clone();
                        }
                        msg_map
                            .entry(tag_definition.name.clone())
                            .or_insert_with(|| enum_description.clone());
                    } else {
                        msg_map
                            .entry(tag_definition.name.clone())
                            .or_insert_with(|| tag_value.to_string());
                    }
                } else {
                    msgtype = "UnknownTag".to_string();
                    msg_map.insert("Unknown tag".to_string(), parts[1].to_string());
                }
            } else {
                msgtype = "InvalidTagNumber".to_string();
                msg_map.insert("Invalid tag number".to_string(), parts[1].to_string());
            }
        }
    }
    Ok((msgtype, msg_map))
}

//          1         2         3         4         5         6         7         8
// 12345678901234567890123456789012345678901234567890123456789012345678901234567890
// 8=FIX.4.2|9=57|35=A|49=FIX_Engine|56=XYZExchange|34=5|98=N|108=10|141=N|10=070|
// 35=A|49=FIX_Engine|56=XYZExchange|34=5|98=N|108=10|141=N|\
// Converts a FIX message type to a FIX message string.
pub fn msgtype2fixmsg(
    msgtype: String,
    msg_map: &HashMap<String, IndexMap<String, String>>,
    fix_tagname_number_map: &HashMap<String, FixTag>,
    override_map: Option<&HashMap<String, String>>,
    msg_seq_num: u64,
) -> String {
    let mut fix_msg = String::new();
    let mut body_length: u32 = 0;
    let mut checksum: u32 = 0;

    // Formats the current timestamp for the FIX message.
    fn format_timestamp() -> String {
        let now = Utc::now();
        now.format("%Y%m%d-%H:%M:%S%.3f").to_string()
    }

    // Retrieve and modify the predefined message based on msgtype
    if let Some(mut predefined_msg) = msg_map.get(&msgtype).cloned() {
        // Merge override_map into predefined_msg if it's Some
        if let Some(override_map) = override_map {
            for (key, value) in override_map {
                predefined_msg.insert(key.clone(), value.clone());
            }
        }
        // Construct FIX message
        for (key, value) in predefined_msg.iter() {
            let new_tag = if let Some(tags_info) = fix_tagname_number_map.get(key) {
                let tag_value = match &tags_info.enum_values {
                    Some(enum_values) => enum_values.get(&value.to_uppercase()).unwrap_or(value),
                    None => if key == "BodyLength" { "#" } else { value },
                };

                match key.as_str() {
                    "SendingTime" => format!("{}={}", tags_info.number, format_timestamp()),
                    "MsgSeqNum" => format!("{}={}", tags_info.number, msg_seq_num.to_string()),
                    "CheckSum" => continue, // CheckSum is handled separately
                    _ => format!("{}={}", tags_info.number, tag_value),
                }
            } else {
                error!("Field {}={} is not in FIX definition.", key, value);
                continue;
            };

            if fix_msg.is_empty() {
                fix_msg.push_str(&new_tag);
            } else {
                fix_msg.push('|');
                fix_msg.push_str(&new_tag);
            }

            // Update body length excluding BeginString and BodyLength fields
            if key != "BeginString" && key != "BodyLength" {
                // body_length += new_tag.len() as u32 + 1; // +1 for the '|' separator
                // Add 1 octet for SOH separator, ensuring no overflow occurs
                body_length = body_length.saturating_add(new_tag.len() as u32 + 1);
            }
        }
    }

    // Replace placeholder with body length
    fix_msg = fix_msg.replace('#', &body_length.to_string());

    // Calculate checksum
    let chksum_fix_msg = fix_msg.replace("|", "\x01");
    for &byte in chksum_fix_msg.as_bytes() {
        checksum = checksum.wrapping_add(byte as u32);
    }
    let checksum_value = ((checksum +1) % 256) as u8;

    // Append the checksum to the message
    fix_msg.push_str(&format!("|10={:03}|", checksum_value));
    fix_msg
}


/// Converts a FIX message type to a FIX message string.
pub fn fixmap2fixmsg(
    msg_map: &IndexMap<String, String>,
    fix_tag_name_map: &HashMap<String, FixTag>,
    msg_seq_num: u64,
) -> String {
    let mut fix_msg = String::new();
    let mut body_length: u32 = 0;
    let mut checksum: u32 = 0;

    /// Formats the current timestamp for the FIX message.
    fn format_timestamp() -> String {
        let now = Utc::now();
        now.format("%Y%m%d-%H:%M:%S%.3f").to_string()
    }

    for (key, value) in msg_map.iter() {
        let new_tag = if let Some(tags_info) = fix_tag_name_map.get(key) {
            let tag_value = if let Some(enum_values) = &tags_info.enum_values {
                enum_values.get(&value.to_uppercase()).unwrap_or(value)
            } else {
                if key == "BodyLength" {
                    "#"
                } else {
                    value
                }
            };
            if key == "SendingTime" {
                format!("{}={}", tags_info.number, format_timestamp())
            } else if key == "MsgSeqNum" {
                format!("{}={}", tags_info.number, msg_seq_num.to_string())
            } else if key == "CheckSum" {
                continue;
            } else {
                format!("{}={}", tags_info.number, tag_value)
            }
        } else {
            format!("{}={}", key, value)
        };

        if fix_msg.is_empty() {
            fix_msg = new_tag.to_string();
        } else {
            fix_msg = format!("{}|{}", fix_msg, new_tag);
        }

        if key != "BeginString" && key != "BodyLength" {
            // Add 1 octet for SOH separator, ensuring no overflow occurs
            body_length = body_length.saturating_add(new_tag.len() as u32 + 1);
        }
    }


    // Replace placeholder with body length
    let body_len = body_length.to_string();
    fix_msg = fix_msg.replace('#', &body_len);

    // Calculate checksum over tag value bytes
    let chksum_fix_msg = fix_msg.replace("|", "\x01");
    let bytes = chksum_fix_msg.as_bytes();
    for &byte in bytes {
        checksum = checksum.wrapping_add(byte as u32);
    }

    // Take the modulo 256 to get the 8-bit checksum
    fix_msg = format!("{}|10={:03}|", fix_msg, (checksum % 256) as u8 + 1);
    fix_msg
}
