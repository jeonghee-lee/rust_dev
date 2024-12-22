use std::{fs, io};
// parse_xml.rs
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Error as IOError};

use log::{error, info};
use prettytable::{format, Cell, Row, Table};
use quick_xml::{events::Event, Error as XmlError, Reader};

// Custom error type for FIX related errors
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

// Data structure representing FIX tag
#[derive(Debug, Clone)]
pub struct FixTag {
    pub number: String, // Public for tests
    pub name: String,   // Public for tests
    data_type: DataType, // Kept private, use a getter if needed
    pub enum_values: Option<HashMap<String, String>>, // Public for tests
}

impl FixTag {
    pub fn new(
        number: String,
        name: String,
        data_type: DataType,
        enum_values: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            number,
            name,
            data_type,
            enum_values,
        }
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

// Data type enum for FIX tag
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DataType {
    String,
    Int,
    Float,
    Char,
    Bool,
}

// Constants for XML parsing
const FIX_FIELD_TAG: &[u8] = b"field";
const ENUM_VALUE_TAG: &[u8] = b"value";

// Parse FIX XML definitions
pub fn parse_fix_xml(
    xml_path: &str,
) -> Result<
    (
        HashMap<u32, FixTag>,
        HashMap<String, FixTag>,
        HashMap<String, String>,
        HashMap<String, String>,
    ),
    FixError,
> {
    // Check if the file exists
    if !fs::metadata(xml_path).is_ok() {
        error!("XML definition file not found. - {}", xml_path);
        return Ok((
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        ));
    }
    let file = File::open(xml_path).map_err(FixError::IoError)?;
    let file = BufReader::new(file);

    let mut reader = Reader::from_reader(file);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut data_tag_map = HashMap::new();
    let mut data_name_map = HashMap::new();
    let mut msgtype_name_map = HashMap::new();
    let mut msgname_type_map = HashMap::new();

    let mut current_tag_number = "0".to_string();
    let mut current_tag_name = "_".to_string();
    let mut current_enum_tag_map = HashMap::new();
    let mut current_enum_name_map = HashMap::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Empty(e)) => match e.name() {
                quick_xml::name::QName(FIX_FIELD_TAG) => {
                    let (field_number, field_name, data_type) = parse_field_number(&e)?;
                    let parsed_number = field_number.parse::<u32>().map_err(|e| {
                        FixError::ParseError(format!("Error parsing tag number: {}", e))
                    })?;
                    data_tag_map.insert(
                        parsed_number,
                        FixTag {
                            number: field_number.clone(),
                            name: field_name.clone(),
                            data_type: data_type.clone(),
                            enum_values: None,
                        },
                    );
                    data_name_map.insert(
                        field_name.clone(),
                        FixTag {
                            number: field_number.clone(),
                            name: field_name.clone(),
                            data_type: data_type.clone(),
                            enum_values: None,
                        },
                    );
                }
                quick_xml::name::QName(ENUM_VALUE_TAG) => {
                    let (enum_data, description) = parse_value_enum(&e)?;
                    current_enum_tag_map.insert(enum_data.clone(), description.clone());
                    current_enum_name_map.insert(description.clone(), enum_data.clone());
                    if current_tag_number == "35" {
                        msgtype_name_map.insert(enum_data.clone(), description.clone());
                        msgname_type_map.insert(description.clone(), enum_data.clone());
                    }
                }
                _ => {}
            },
            Ok(Event::Start(e)) => {
                if e.name() == quick_xml::name::QName(FIX_FIELD_TAG) {
                    let (e_field_number, e_field_name, e_data_type) = parse_field_number(&e)?;
                    let parsed_number = e_field_number.parse::<u32>().map_err(|e| {
                        FixError::ParseError(format!("Error parsing tag number: {}", e))
                    })?;
                    data_tag_map.insert(
                        parsed_number,
                        FixTag {
                            number: e_field_number.clone(),
                            name: e_field_name.clone(),
                            data_type: e_data_type.clone(),
                            enum_values: None,
                        },
                    );
                    data_name_map.insert(
                        e_field_name.clone(),
                        FixTag {
                            number: e_field_number.clone(),
                            name: e_field_name.clone(),
                            data_type: e_data_type.clone(),
                            enum_values: None,
                        },
                    );
                    current_tag_number = e_field_number.clone();
                    current_tag_name = e_field_name.clone();
                }
            }
            Ok(Event::End(ref e)) => {
                if e.name() == quick_xml::name::QName(FIX_FIELD_TAG) {
                    let key_no: u32 = current_tag_number.parse().unwrap();
                    if let Some(tag) = data_tag_map.get_mut(&key_no) {
                        tag.enum_values = Some(current_enum_tag_map.clone());
                    }
                    let key_name: String = current_tag_name.to_string();
                    if let Some(tag) = data_name_map.get_mut(&key_name) {
                        tag.enum_values = Some(current_enum_name_map.clone());
                    }
                    current_tag_number = "0".to_string();
                    current_tag_name = "_".to_string();
                    current_enum_tag_map.clear();
                    current_enum_name_map.clear();
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(FixError::XmlError(e)),
            _ => {}
        }
        buf.clear();
    }
    Ok((
        data_tag_map,
        data_name_map,
        msgtype_name_map,
        msgname_type_map,
    ))
}

// Parse attributes of FIX field or enum value
fn parse_field_number(
    event: &quick_xml::events::BytesStart,
) -> Result<(String, String, DataType), FixError> {
    let mut field_number = None;
    let mut field_name = None;
    let mut data_type = None;
    for attr in event.attributes() {
        if let Ok(attr) = attr {
            match attr.key {
                quick_xml::name::QName(b"number") => {
                    field_number = Some(String::from_utf8_lossy(&attr.value).into_owned())
                }
                quick_xml::name::QName(b"name") => {
                    field_name = Some(String::from_utf8_lossy(&attr.value).into_owned())
                }
                quick_xml::name::QName(b"type") => {
                    let type_str = std::str::from_utf8(&attr.value).map_err(|_| {
                        FixError::ParseError("Error parsing UTF-8 string".to_string())
                    })?;
                    data_type = Some(match type_str {
                        "STRING"
                        | "MULTIPLEVALUESTRING"
                        | "CURRENCY"
                        | "EXCHANGE"
                        | "UTCTIMESTAMP"
                        | "LOCALMKTDATE"
                        | "DATA"
                        | "UTCDATE"
                        | "UTCTIMEONLY" => DataType::String,
                        "INT" | "PRICE" | "AMT" | "QTY" | "LENGTH" | "PRICEOFFSET"
                        | "MONTHYEAR" | "DAYOFMONTH" => DataType::Int,
                        "FLOAT" => DataType::Float,
                        "CHAR" => DataType::Char,
                        "BOOLEAN" => DataType::Bool,
                        _ => {
                            return Err(FixError::ParseError(format!(
                                "Unknown data type: {}",
                                type_str
                            )));
                        }
                    });
                }
                _ => {}
            }
        }
    }
    if let (Some(field_number), Some(field_name), Some(data_type)) =
        (field_number, field_name, data_type)
    {
        Ok((field_number, field_name, data_type))
    } else {
        Err(FixError::ParseError(
            "Incomplete field attributes".to_string(),
        ))
    }
}

// Parse attributes of FIX enum value

fn parse_value_enum(event: &quick_xml::events::BytesStart) -> Result<(String, String), FixError> {
    let mut enum_data = None;
    let mut description = None;

    for attr in event.attributes() {
        match attr {
            Ok(attr) => match attr.key {
                quick_xml::name::QName(b"enum") => {
                    // Ensure detection of invalid UTF-8
                    let enum_value = std::str::from_utf8(&attr.value)
                        .map_err(|_| FixError::ParseError("Error parsing UTF-8 string".to_string()))?;
                    enum_data = Some(enum_value.to_owned());
                }
                quick_xml::name::QName(b"description") => {
                    // Ensure detection of invalid UTF-8
                    let desc_value = std::str::from_utf8(&attr.value)
                        .map_err(|_| FixError::ParseError("Error parsing UTF-8 string".to_string()))?;
                    description = Some(desc_value.to_owned());
                }
                _ => {}
            },
            Err(e) => {
                return Err(FixError::XmlError(e.into()));
            }
        }
    }

    if let (Some(enum_data), Some(description)) = (enum_data, description) {
        Ok((enum_data, description))
    } else {
        Err(FixError::ParseError(
            "Incomplete enum attributes".to_string(),
        ))
    }
}

// Print FIX message with tag definitions
pub fn print_fix_message(
    message: &str,
    tags_map: &HashMap<u32, FixTag>,
) -> Result<String, FixError> {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    // Add header row
    table.set_titles(Row::new(vec![
        Cell::new("Tag Name"),
        Cell::new("Tag Number"),
        Cell::new("Value"),
        Cell::new("Description"),
    ]));
    let modified_message = message.replace('\x01', "|");
    info!("{}", modified_message);
    let fields: Vec<&str> = modified_message.split('|').collect();
    for field in fields {
        let parts: Vec<&str> = field.split('=').collect();
        if parts.len() == 2 {
            if let Ok(tag) = parts[0].parse::<u32>() {
                if let Some(tag_definition) = tags_map.get(&tag) {
                    let mut row = Row::empty();
                    row.add_cell(Cell::new(&tag_definition.name));
                    row.add_cell(Cell::new(&tag_definition.number));
                    row.add_cell(Cell::new(parts[1]));
                    if let Some(enum_values) = &tag_definition.enum_values {
                        if let Some(enum_description) = enum_values.get(parts[1]) {
                            row.add_cell(Cell::new(enum_description));
                        } else {
                            row.add_cell(Cell::new(""));
                        }
                    } else {
                        row.add_cell(Cell::new(""));
                    }
                    table.add_row(row);
                } else {
                    let mut row = Row::empty();
                    row.add_cell(Cell::new("Unknown tag"));
                    row.add_cell(Cell::new(parts[0]));
                    row.add_cell(Cell::new(parts[1]));
                    row.add_cell(Cell::new(""));
                    table.add_row(row);
                }
            } else {
                let mut row = Row::empty();
                row.add_cell(Cell::new("Invalid tag number"));
                row.add_cell(Cell::new(parts[0]));
                row.add_cell(Cell::new(parts[1]));
                row.add_cell(Cell::new(""));
                table.add_row(row);
            }
        }
    }

    // table.printstd();
    // Convert the table to a string
    let table_string = format!("{}", table);
    Ok(table_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quick_xml::events::{BytesStart, BytesText, Event};
    use quick_xml::Reader;

    #[test]
    fn test_parse_value_enum_valid() {
        // Creating a valid XML element for testing
        let xml_data = r#"<value enum="1" description="Test Description"/>"#;

        // Parse the XML into a BytesStart event
        let mut reader = Reader::from_str(xml_data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        match reader.read_event_into(&mut buf).unwrap() {
            Event::Empty(event) => {
                // Use the `parse_value_enum` function and check the result
                let result = parse_value_enum(&event);
                assert!(result.is_ok());
                let (enum_data, description) = result.unwrap();
                assert_eq!(enum_data, "1");
                assert_eq!(description, "Test Description");
            }
            _ => panic!("Expected an Empty event"),
        }
    }

    #[test]
    fn test_parse_value_enum_missing_attributes() {
        // Creating an XML element missing the `description` attribute
        let xml_data = r#"<value enum="1"/>"#;

        // Parse the XML into a BytesStart event
        let mut reader = Reader::from_str(xml_data);
        reader.trim_text(true);
        let mut buf = Vec::new();

        match reader.read_event_into(&mut buf).unwrap() {
            Event::Empty(event) => {
                // Use the `parse_value_enum` function and expect an error
                let result = parse_value_enum(&event);
                assert!(result.is_err());
                let err = result.unwrap_err();
                match err {
                    FixError::ParseError(msg) => {
                        assert_eq!(msg, "Incomplete enum attributes");
                    }
                    _ => panic!("Unexpected error type"),
                }
            }
            _ => panic!("Expected an Empty event"),
        }
    }

    #[test]
    fn test_parse_value_enum_invalid_utf8() {
        // Creating an XML element with invalid UTF-8 in attributes
        let xml_data = b"<value enum=\"\x80\" description=\"Test Description\"/>";

        // Parse the XML into a BytesStart event
        let mut reader = Reader::from_reader(&xml_data[..]);
        reader.trim_text(true);
        let mut buf = Vec::new();

        match reader.read_event_into(&mut buf).unwrap() {
            Event::Empty(event) => {
                // Use the `parse_value_enum` function and expect an error
                let result = parse_value_enum(&event);
                assert!(result.is_err());
                let err = result.unwrap_err();
                match err {
                    FixError::ParseError(msg) => assert_eq!(msg, "Error parsing UTF-8 string"),
                    _ => panic!("Unexpected error type"),
                }
            }
            _ => panic!("Expected an Empty event"),
        }
    }
}