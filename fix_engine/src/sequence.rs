use std::fs::{File, OpenOptions};
use std::io::Read;
use std::sync::{Mutex, Arc};
use fs2::FileExt;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct SequenceNumber {
    incoming: u64,
    outgoing: u64,
}

pub struct SequenceNumberStore {
    file_path: String,
    sequence_numbers: Arc<Mutex<SequenceNumber>>,
}

impl SequenceNumberStore {
    pub fn new(file_path: &str) -> Self {
        let sequence_numbers = if let Ok(mut file) = File::open(file_path) {
            let mut content = String::new();
            if file.read_to_string(&mut content).is_ok() {
                serde_json::from_str(&content).unwrap_or_else(|_| SequenceNumber { incoming: 1, outgoing: 1 })
            } else {
                SequenceNumber { incoming: 1, outgoing: 1 }
            }
        } else {
            SequenceNumber { incoming: 1, outgoing: 1 }
        };

        SequenceNumberStore {
            file_path: file_path.to_string(),
            sequence_numbers: Arc::new(Mutex::new(sequence_numbers)),
        }
    }

    pub fn get_incoming(&self) -> u64 {
        let seq = self.sequence_numbers.lock().unwrap();
        seq.incoming
    }

    pub fn get_outgoing(&self) -> u64 {
        let seq = self.sequence_numbers.lock().unwrap();
        seq.outgoing
    }

    pub fn increment_incoming(&self) {
        let mut seq = self.sequence_numbers.lock().unwrap();
        seq.incoming += 1;
        self.persist(&seq);
    }

    pub fn increment_outgoing(&self) {
        let mut seq = self.sequence_numbers.lock().unwrap();
        seq.outgoing += 1;
        self.persist(&seq);
    }

    pub fn set_incoming(&self, new_seq: u64) {
        let mut seq = self.sequence_numbers.lock().unwrap();
        seq.incoming = new_seq;
        self.persist(&seq);
    }

    pub fn set_outgoing(&self, new_seq: u64) {
        let mut seq = self.sequence_numbers.lock().unwrap();
        seq.outgoing = new_seq;
        self.persist(&seq);
    }

    fn persist(&self, seq: &SequenceNumber) {
        let file = OpenOptions::new().write(true).create(true).open(&self.file_path).unwrap();
        file.lock_exclusive().unwrap();
        let content = serde_json::to_string(seq).unwrap();
        std::fs::write(&self.file_path, content).unwrap();
        file.unlock().unwrap();
    }
}
