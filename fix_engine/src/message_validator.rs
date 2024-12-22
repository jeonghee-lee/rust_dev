use crate::parse_payload_xml::FixMsgTag;
use log::error;
use std::collections::HashMap;

type FixFieldMap = HashMap<String, String>;
type StrVec = Vec<String>;
type MsgTypeMap = HashMap<String, FixMsgTag>;

#[derive(Debug)]
pub struct FixMessage {
    fields: FixFieldMap,
}

impl FixMessage {
    pub fn parse(raw_message: &str) -> Result<Self, &'static str> {
        let mut fields = FixFieldMap::new();
        for part in raw_message.split('|') {
            if !part.is_empty() {
                let mut iter = part.splitn(2, '=');
                if let (Some(key), Some(value)) = (iter.next(), iter.next()) {
                    fields.insert(key.to_string(), value.to_string());
                } else {
                    return Err("Invalid field format");
                }
            }
        }
        Ok(FixMessage { fields })
    }

    pub fn validate(
        &self,
        required_fields: &StrVec,
        valid_msg_types: &StrVec,
        msgnumber_fields_map: &MsgTypeMap,
    ) -> bool {
        for field in required_fields {
            match self.fields.get(field) {
                Some(value) if !value.is_empty() => (),
                _ => {
                    error!("Required field is missing or empty: {}", field);
                    return false;
                }
            }
        }

        // Check BodyLength field
        if let Some(body_length) = self.fields.get("9") {
            if body_length.parse::<usize>().is_err() || body_length.is_empty() {
                error!("Invalid or empty BodyLength field: {}", body_length);
                return false;
            }
        }

        // Check MsgType field
        if let Some(msg_type) = self.fields.get("35") {
            if !valid_msg_types.contains(msg_type) || msg_type.is_empty() {
                error!("Invalid or empty MsgType field: {}", msg_type);
                return false;
            }

            // Retrieve required fields for this MsgType
            let msgtype_required_fields: StrVec = match msgnumber_fields_map.get(msg_type) {
                Some(msgtype_fld_info) => match &msgtype_fld_info.field {
                    Some(field_map) => field_map.keys().cloned().collect(),
                    None => {
                        error!("MsgType field information is empty");
                        return false;
                    }
                },
                None => {
                    error!(
                        "MsgType field information not found for MsgType: {}",
                        msg_type
                    );
                    return false;
                }
            };

            for field in msgtype_required_fields {
                match self.fields.get(&field) {
                    Some(value) if !value.is_empty() => (),
                    _ => {
                        error!(
                            "MsgType {} required field is missing or empty: {}",
                            msg_type, field
                        );
                        return false;
                    }
                }
            }
        } else {
            error!("Missing MsgType field");
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_msgtype_map() -> MsgTypeMap {
        let mut msgtype_fields_map = MsgTypeMap::new();

        // Define required fields for MsgType "D" (for example purposes)
        let mut order_msg_fields = HashMap::new();
        order_msg_fields.insert("11".to_string(), "ClOrdID".to_string()); // Client Order ID
        order_msg_fields.insert("55".to_string(), "Symbol".to_string()); // Symbol
        let fix_msg_tag = FixMsgTag {
            msgname: "Order".to_string(),
            msgcat: "app".to_string(),
            field: Some(order_msg_fields),
        };

        msgtype_fields_map.insert("D".to_string(), fix_msg_tag);

        msgtype_fields_map
    }

    #[test]
    fn test_parse_valid_fix_message() {
        let raw_message = "8=FIX.4.4|9=65|35=D|11=12345|55=ABC|10=123|";
        let parsed = FixMessage::parse(raw_message);

        assert!(parsed.is_ok());
        let message = parsed.unwrap();

        // Validate fields in message
        assert_eq!(message.fields.get("8").unwrap(), "FIX.4.4");
        assert_eq!(message.fields.get("9").unwrap(), "65");
        assert_eq!(message.fields.get("35").unwrap(), "D");
        assert_eq!(message.fields.get("11").unwrap(), "12345");
        assert_eq!(message.fields.get("55").unwrap(), "ABC");
        assert_eq!(message.fields.get("10").unwrap(), "123");
    }

    #[test]
    fn test_parse_invalid_field_format() {
        let raw_message = "8=FIX.4.4|9=65|35D|11=12345|";
        let parsed = FixMessage::parse(raw_message);

        assert!(parsed.is_err());
        assert_eq!(parsed.unwrap_err(), "Invalid field format");
    }

    #[test]
    fn test_parse_empty_message() {
        let raw_message = "";
        let parsed = FixMessage::parse(raw_message);

        assert!(parsed.is_ok()); // Empty message is allowed, will be an empty `FixFieldMap`
        let message = parsed.unwrap();

        assert!(message.fields.is_empty());
    }

    #[test]
    fn test_validate_fix_message_success() {
        let raw_message = "8=FIX.4.4|9=65|35=D|11=12345|55=ABC|10=123|";
        let message = FixMessage::parse(raw_message).unwrap();

        // Define required and valid MsgTypes
        let required_fields = vec!["8".to_string(), "9".to_string(), "35".to_string()];
        let valid_msg_types = vec!["D".to_string()];
        let msgtype_map = create_test_msgtype_map();

        let is_valid = message.validate(&required_fields, &valid_msg_types, &msgtype_map);
        assert!(is_valid);
    }

    #[test]
    fn test_validate_missing_required_field() {
        let raw_message = "8=FIX.4.4|9=65|35=D|55=ABC|10=123|"; // Missing ClOrdID (11)
        let message = FixMessage::parse(raw_message).unwrap();

        // Define required and valid MsgTypes
        let required_fields = vec!["8".to_string(), "9".to_string(), "35".to_string()];
        let msgtype_map = create_test_msgtype_map();
        let valid_msg_types = vec!["D".to_string()];

        let is_valid = message.validate(&required_fields, &valid_msg_types, &msgtype_map);
        assert!(!is_valid);
    }

    #[test]
    fn test_validate_invalid_msg_type() {
        let raw_message = "8=FIX.4.4|9=65|35=Z|11=12345|55=ABC|10=123|"; // MsgType is not "D"
        let message = FixMessage::parse(raw_message).unwrap();

        // Define required and valid MsgTypes
        let required_fields = vec!["8".to_string(), "9".to_string(), "35".to_string()];
        let msgtype_map = create_test_msgtype_map();
        let valid_msg_types = vec!["D".to_string()];

        let is_valid = message.validate(&required_fields, &valid_msg_types, &msgtype_map);
        assert!(!is_valid);
    }

    #[test]
    fn test_validate_missing_msgtype_definition() {
        let raw_message = "8=FIX.4.4|9=65|35=C|11=12345|55=ABC|10=123|"; // MsgType "C" not in map
        let message = FixMessage::parse(raw_message).unwrap();

        // Define required and valid MsgTypes
        let required_fields = vec!["8".to_string(), "9".to_string(), "35".to_string()];
        let msgtype_map = create_test_msgtype_map();
        let valid_msg_types = vec!["C".to_string()];

        let is_valid = message.validate(&required_fields, &valid_msg_types, &msgtype_map);
        assert!(!is_valid);
    }

    #[test]
    fn test_validate_invalid_body_length() {
        let raw_message = "8=FIX.4.4|9=abc|35=D|11=12345|55=ABC|10=123|"; // BodyLength (9) is invalid
        let message = FixMessage::parse(raw_message).unwrap();

        // Define required and valid MsgTypes
        let required_fields = vec!["8".to_string(), "9".to_string(), "35".to_string()];
        let msgtype_map = create_test_msgtype_map();
        let valid_msg_types = vec!["D".to_string()];

        let is_valid = message.validate(&required_fields, &valid_msg_types, &msgtype_map);
        assert!(!is_valid);
    }

    #[test]
    fn test_validate_missing_msgtype_field() {
        let raw_message = "8=FIX.4.4|9=65|11=12345|55=ABC|10=123|"; // Missing MsgType field (35)
        let message = FixMessage::parse(raw_message).unwrap();

        // Define required fields and valid MsgTypes
        let required_fields = vec!["8".to_string(), "9".to_string(), "35".to_string()];
        let msgtype_map = create_test_msgtype_map();
        let valid_msg_types = vec!["D".to_string()];

        let is_valid = message.validate(&required_fields, &valid_msg_types, &msgtype_map);
        assert!(!is_valid);
    }
}