use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::sync::{Arc, Mutex};

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
                serde_json::from_str(&content).unwrap_or_else(|_| SequenceNumber {
                    incoming: 1,
                    outgoing: 1,
                })
            } else {
                SequenceNumber {
                    incoming: 1,
                    outgoing: 1,
                }
            }
        } else {
            SequenceNumber {
                incoming: 1,
                outgoing: 1,
            }
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
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.file_path)
            .unwrap();
        file.lock_exclusive().unwrap();
        let content = serde_json::to_string(seq).unwrap();
        std::fs::write(&self.file_path, content).unwrap();
        file.unlock().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_new_creates_default_sequence_numbers() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = SequenceNumberStore::new(temp_file.path().to_str().unwrap());

        assert_eq!(store.get_incoming(), 1);
        assert_eq!(store.get_outgoing(), 1);
    }

    #[test]
    fn test_new_loads_existing_sequence_numbers() {
        let temp_file = NamedTempFile::new().unwrap();
        let existing_data = r#"{"incoming": 42, "outgoing": 100}"#;
        std::fs::write(temp_file.path(), existing_data).unwrap();

        let store = SequenceNumberStore::new(temp_file.path().to_str().unwrap());

        assert_eq!(store.get_incoming(), 42);
        assert_eq!(store.get_outgoing(), 100);
    }

    #[test]
    fn test_increment_incoming() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = SequenceNumberStore::new(temp_file.path().to_str().unwrap());

        store.increment_incoming();
        assert_eq!(store.get_incoming(), 2);
    }

    #[test]
    fn test_increment_outgoing() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = SequenceNumberStore::new(temp_file.path().to_str().unwrap());

        store.increment_outgoing();
        assert_eq!(store.get_outgoing(), 2);
    }

    #[test]
    fn test_set_incoming() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = SequenceNumberStore::new(temp_file.path().to_str().unwrap());

        store.set_incoming(10);
        assert_eq!(store.get_incoming(), 10);
    }

    #[test]
    fn test_set_outgoing() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = SequenceNumberStore::new(temp_file.path().to_str().unwrap());

        store.set_outgoing(20);
        assert_eq!(store.get_outgoing(), 20);
    }

    #[test]
    fn test_persist_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = SequenceNumberStore::new(temp_file.path().to_str().unwrap());

        store.set_incoming(99);
        store.set_outgoing(88);

        // Reload the sequence number store to verify persisted data
        let reloaded_store = SequenceNumberStore::new(temp_file.path().to_str().unwrap());
        assert_eq!(reloaded_store.get_incoming(), 99);
        assert_eq!(reloaded_store.get_outgoing(), 88);
    }

    #[test]
    fn test_handles_corrupt_file() {
        let temp_file = NamedTempFile::new().unwrap();
        // Write invalid JSON to the file
        std::fs::write(temp_file.path(), "invalid_json").unwrap();

        let store = SequenceNumberStore::new(temp_file.path().to_str().unwrap());

        // Should fall back to default sequence numbers
        assert_eq!(store.get_incoming(), 1);
        assert_eq!(store.get_outgoing(), 1);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let temp_file = NamedTempFile::new().unwrap();
        let store = Arc::new(SequenceNumberStore::new(temp_file.path().to_str().unwrap()));

        let store_clone1 = Arc::clone(&store);
        let handle1 = thread::spawn(move || {
            for _ in 0..50 {
                store_clone1.increment_incoming();
            }
        });

        let store_clone2 = Arc::clone(&store);
        let handle2 = thread::spawn(move || {
            for _ in 0..50 {
                store_clone2.increment_outgoing();
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        assert_eq!(store.get_incoming(), 51);
        assert_eq!(store.get_outgoing(), 51);
    }
}