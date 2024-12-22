use std::fs::File;
use std::io::{BufReader, Error as IOError};
use std::{collections::HashMap, fs, io};

use crate::parse_xml::FixTag;
use log::error;
use quick_xml::{events::Event, Error as XmlError, Reader};

#[derive(Debug)]
pub enum FixError {
    XmlError(XmlError),
    IoError(IOError),
    ParseError(String),
}

impl From<io::Error> for FixError {
    fn from(error: io::Error) -> Self {
        FixError::IoError(error)
    }
}

impl From<quick_xml::Error> for FixError {
    fn from(error: quick_xml::Error) -> Self {
        FixError::XmlError(error)
    }
}

impl Clone for FixError {
    fn clone(&self) -> Self {
        match self {
            FixError::XmlError(e) => FixError::XmlError(e.clone()),
            FixError::IoError(e) => FixError::IoError(io::Error::new(e.kind(), e.to_string())),
            FixError::ParseError(e) => FixError::ParseError(e.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FixMsgTag {
    pub(crate) msgcat: String,
    pub(crate) msgname: String,
    pub(crate) field: Option<HashMap<String, String>>,
}

const FIX_MESSAGE_TAG: &[u8] = b"message";
const HEADER_TAG: &[u8] = b"header";
const TRAILER_TAG: &[u8] = b"trailer";
const FIELD_TAG: &[u8] = b"field";

pub fn parse_fix_payload_xml(
    xml_path: &str,
    msgtype_name_map: &HashMap<String, String>,
    fix_tagname_number_map: &HashMap<String, FixTag>,
) -> Result<(HashMap<String, FixMsgTag>, HashMap<String, FixMsgTag>), FixError> {
    if !fs::metadata(xml_path).is_ok() {
        error!("XML Payload definition file not found. - {}", xml_path);
        return Ok((HashMap::new(), HashMap::new()));
    }
    let file = File::open(xml_path).map_err(FixError::IoError)?;
    let file = BufReader::new(file);

    let mut reader = Reader::from_reader(file);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut fixname_map = HashMap::new();
    let mut fixnumber_map = HashMap::new();

    let mut current_msg_name = String::new();
    let mut current_msg_type = String::new();
    let mut current_fieldname_map = HashMap::new();
    let mut current_fieldtag_map = HashMap::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Empty(e)) => {
                if e.name() == quick_xml::name::QName(FIELD_TAG) {
                    let (field_name, required) = parse_field(&e)?;
                    if required == "Y" {
                        current_fieldname_map.insert(field_name.clone(), required.clone());
                        if let Some(tags_info) = fix_tagname_number_map.get(&field_name) {
                            current_fieldtag_map.insert(tags_info.number.clone(), required.clone());
                        } else {
                            current_fieldtag_map.insert(field_name.clone(), required.clone());
                        }
                    }
                }
            }
            Ok(Event::Start(e)) => match e.name() {
                quick_xml::name::QName(FIX_MESSAGE_TAG) => {
                    let (_msg_name, msg_type, msg_cat) = parse_message(&e)?;
                    if let Some(mapped_msg_name) = msgtype_name_map.get(&msg_type) {
                        let fix_msg_tag = FixMsgTag {
                            msgcat: msg_cat.clone(),
                            msgname: mapped_msg_name.clone(),
                            field: None,
                        };
                        fixname_map.insert(mapped_msg_name.clone(), fix_msg_tag.clone());
                        fixnumber_map.insert(msg_type.clone(), fix_msg_tag);

                        current_msg_name = mapped_msg_name.clone();
                        current_msg_type = msg_type.clone();
                    }
                }
                quick_xml::name::QName(HEADER_TAG) => {
                    handle_special_tag(
                        "HEADER".to_string(),
                        "<".to_string(),
                        "header".to_string(),
                        &mut fixname_map,
                        &mut fixnumber_map,
                        &mut current_msg_name,
                        &mut current_msg_type,
                    );
                }
                quick_xml::name::QName(TRAILER_TAG) => {
                    handle_special_tag(
                        "TRAILER".to_string(),
                        ">".to_string(),
                        "trailer".to_string(),
                        &mut fixname_map,
                        &mut fixnumber_map,
                        &mut current_msg_name,
                        &mut current_msg_type,
                    );
                }
                _ => {}
            },
            Ok(Event::End(ref e)) => {
                if [FIX_MESSAGE_TAG, HEADER_TAG, TRAILER_TAG].contains(&e.name().as_ref()) {
                    if let Some(tag) = fixname_map.get_mut(&current_msg_name) {
                        tag.field = Some(current_fieldname_map.clone());
                    }
                    if let Some(tag) = fixnumber_map.get_mut(&current_msg_type) {
                        tag.field = Some(current_fieldtag_map.clone());
                    }
                    current_msg_name.clear();
                    current_fieldname_map.clear();
                    current_msg_type.clear();
                    current_fieldtag_map.clear();
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(FixError::XmlError(e)),
            _ => {}
        }
        buf.clear();
    }
    Ok((fixname_map, fixnumber_map))
}

fn parse_message(
    event: &quick_xml::events::BytesStart,
) -> Result<(String, String, String), FixError> {
    let mut msgname = None;
    let mut msgtype = None;
    let mut msgcat = None;

    for attr in event.attributes() {
        let attr = attr.map_err(|e| FixError::XmlError(XmlError::from(e)))?;
        match attr.key {
            quick_xml::name::QName(b"name") => msgname = Some(attr.unescape_value()?.into_owned()),
            quick_xml::name::QName(b"msgtype") => {
                msgtype = Some(attr.unescape_value()?.into_owned())
            }
            quick_xml::name::QName(b"msgcat") => msgcat = Some(attr.unescape_value()?.into_owned()),
            _ => {}
        }
    }
    if let (Some(msg_name), Some(msg_type), Some(msg_cat)) = (msgname, msgtype, msgcat) {
        Ok((msg_name, msg_type, msg_cat))
    } else {
        Err(FixError::ParseError(
            "Incomplete message attributes".to_string(),
        ))
    }
}

fn parse_field(event: &quick_xml::events::BytesStart) -> Result<(String, String), FixError> {
    let mut field_name = None;
    let mut required = None;

    for attr in event.attributes() {
        let attr = attr.map_err(|e| FixError::XmlError(XmlError::from(e)))?;
        match attr.key {
            quick_xml::name::QName(b"name") => {
                field_name = Some(attr.unescape_value()?.into_owned())
            }
            quick_xml::name::QName(b"required") => {
                required = Some(attr.unescape_value()?.into_owned())
            }
            _ => {}
        }
    }
    if let (Some(field_name), Some(required)) = (field_name, required) {
        Ok((field_name, required))
    } else {
        Err(FixError::ParseError(
            "Incomplete field attributes".to_string(),
        ))
    }
}

fn handle_special_tag(
    msg_name: String,
    msg_type: String,
    msg_cat: String,
    fixname_map: &mut HashMap<String, FixMsgTag>,
    fixnumber_map: &mut HashMap<String, FixMsgTag>,
    current_msg_name: &mut String,
    current_msg_type: &mut String,
) {
    let fix_msg_tag = FixMsgTag {
        msgcat: msg_cat.clone(),
        msgname: msg_name.clone(),
        field: None,
    };

    fixname_map.insert(msg_name.clone(), fix_msg_tag.clone());
    fixnumber_map.insert(msg_type.clone(), fix_msg_tag);

    *current_msg_name = msg_name;
    *current_msg_type = msg_type;
}

#[cfg(test)]
mod tests {
    use super::*;
    use quick_xml::events::BytesStart;
    use std::collections::HashMap;

    #[test]
    fn test_parse_message_success() {
        let mut event = BytesStart::new("message");
        event.push_attribute(("name", "Order"));
        event.push_attribute(("msgtype", "D"));
        event.push_attribute(("msgcat", "app"));

        let result = parse_message(&event);
        assert!(result.is_ok());

        let (msgname, msgtype, msgcat) = result.unwrap();
        assert_eq!(msgname, "Order");
        assert_eq!(msgtype, "D");
        assert_eq!(msgcat, "app");
    }

    #[test]
    fn test_parse_message_missing_attributes() {
        let event = BytesStart::new("message"); // Missing attributes

        let result = parse_message(&event);
        assert!(result.is_err());

        if let FixError::ParseError(err) = result.unwrap_err() {
            assert_eq!(err, "Incomplete message attributes".to_string());
        } else {
            panic!("Expected FixError::ParseError");
        }
    }

    #[test]
    fn test_parse_field_success() {
        let mut event = BytesStart::new("field");
        event.push_attribute(("name", "ClOrdID"));
        event.push_attribute(("required", "Y"));

        let result = parse_field(&event);
        assert!(result.is_ok());

        let (field_name, required) = result.unwrap();
        assert_eq!(field_name, "ClOrdID");
        assert_eq!(required, "Y");
    }

    #[test]
    fn test_parse_field_missing_attributes() {
        let event = BytesStart::new("field"); // Missing attributes

        let result = parse_field(&event);
        assert!(result.is_err());

        if let FixError::ParseError(err) = result.unwrap_err() {
            assert_eq!(err, "Incomplete field attributes".to_string());
        } else {
            panic!("Expected FixError::ParseError");
        }
    }

    #[test]
    fn test_handle_special_tag() {
        let mut fixname_map: HashMap<String, FixMsgTag> = HashMap::new();
        let mut fixnumber_map: HashMap<String, FixMsgTag> = HashMap::new();
        let mut current_msg_name = String::new();
        let mut current_msg_type = String::new();

        handle_special_tag(
            "HEADER".to_string(),
            "<".to_string(),
            "header".to_string(),
            &mut fixname_map,
            &mut fixnumber_map,
            &mut current_msg_name,
            &mut current_msg_type,
        );

        assert!(fixname_map.contains_key("HEADER"));
        let fix_msg_tag = fixname_map.get("HEADER").unwrap();
        assert_eq!(fix_msg_tag.msgname, "HEADER");
        assert_eq!(fix_msg_tag.msgcat, "header");
        assert!(fix_msg_tag.field.is_none());

        assert!(fixnumber_map.contains_key("<"));
        let fix_msg_tag = fixnumber_map.get("<").unwrap();
        assert_eq!(fix_msg_tag.msgname, "HEADER");
        assert_eq!(fix_msg_tag.msgcat, "header");
        assert!(fix_msg_tag.field.is_none());

        assert_eq!(current_msg_name, "HEADER");
        assert_eq!(current_msg_type, "<");
    }

    #[test]
    fn test_parse_fix_payload_xml_file_not_found() {
        let msgtype_name_map: HashMap<String, String> = HashMap::new();
        let fix_tagname_number_map: HashMap<String, FixTag> = HashMap::new();

        let result = parse_fix_payload_xml(
            "nonexistent_file.xml",
            &msgtype_name_map,
            &fix_tagname_number_map,
        );

        assert!(result.is_ok());
        let (fixname_map, fixnumber_map) = result.unwrap();
        assert!(fixname_map.is_empty());
        assert!(fixnumber_map.is_empty());
    }

    #[test]
    fn test_parse_fix_payload_xml_success() {
        let xml_data = r#"
            <fix>
                <message name="TestMessage" msgtype="T" msgcat="app">
                    <field name="Field1" required="Y" />
                    <field name="Field2" required="N" />
                </message>
            </fix>
        "#;

        let file_path = "test_payload.xml";
        std::fs::write(file_path, xml_data).unwrap();

        let mut msgtype_name_map: HashMap<String, String> = HashMap::new();
        msgtype_name_map.insert("T".to_string(), "TestMessage".to_string());

        let fix_tagname_number_map: HashMap<String, FixTag> = HashMap::new();

        let result =
            parse_fix_payload_xml(file_path, &msgtype_name_map, &fix_tagname_number_map);

        // Delete the file after test
        std::fs::remove_file(file_path).unwrap();

        assert!(result.is_ok());

        let (fixname_map, fixnumber_map) = result.unwrap();
        assert!(fixname_map.contains_key("TestMessage"));
        let tag = fixname_map.get("TestMessage").unwrap();
        assert_eq!(tag.msgname, "TestMessage");
        assert_eq!(tag.msgcat, "app");
        assert!(tag.field.is_some());

        let fields = tag.field.as_ref().unwrap();
        assert!(fields.contains_key("Field1"));
        assert_eq!(fields.get("Field1").unwrap(), "Y");
        assert!(!fields.contains_key("Field2"));

        assert!(fixnumber_map.contains_key("T"));
    }
}