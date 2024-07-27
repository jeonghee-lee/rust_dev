use std::collections::HashMap;
use log::error;
use crate::parse_payload_xml::FixMsgTag;

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

    pub fn validate(&self, required_fields: &StrVec, valid_msg_types: &StrVec, msgnumber_fields_map: &MsgTypeMap) -> bool {
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
                    error!("MsgType field information not found for MsgType: {}", msg_type);
                    return false;
                }
            };

            for field in msgtype_required_fields {
                match self.fields.get(&field) {
                    Some(value) if !value.is_empty() => (),
                    _ => {
                        error!("MsgType {} required field is missing or empty: {}", msg_type, field);
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
