use chrono::{DateTime, TimeZone, Utc};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct AtomicDateTime {
    inner: AtomicU64,
}

impl AtomicDateTime {
    pub fn new(time: DateTime<Utc>) -> Self {
        let timestamp = time.timestamp();
        Self {
            inner: AtomicU64::new(timestamp as u64),
        }
    }

    pub fn load(&self, order: Ordering) -> DateTime<Utc> {
        let timestamp = self.inner.load(order) as i64;
        Utc.timestamp_opt(timestamp, 0).unwrap()
    }

    pub fn store(&self, time: DateTime<Utc>, order: Ordering) {
        let timestamp = time.timestamp() as u64;
        self.inner.store(timestamp, order);
    }
}

#[macro_export]
macro_rules! clone_and_load {
    ($atomic:expr) => {{
        // Load the value atomically
        $atomic.load(Ordering::SeqCst)
    }};
}

#[macro_export]
macro_rules! increment_and_drop {
    ($atomic:expr) => {{
        // Check for overflow
        if $atomic.load(Ordering::SeqCst) == u64::MAX {
            panic!("Overflow error: Cannot increment as the value is already at its maximum.");
        }
        // Atomically increment the value
        $atomic.fetch_add(1, Ordering::SeqCst);
    }};
}

#[macro_export]
macro_rules! initialize_value {
    ($name:ident, $value:expr) => {
        lazy_static! {
            pub static ref $name: AtomicU64 = AtomicU64::new($value);
        }
    };
}

#[macro_export]
macro_rules! initialize_flag {
    ($name:ident, $value:expr) => {
        lazy_static! {
            pub static ref $name: AtomicBool = AtomicBool::new($value);
        }
    };
}

#[macro_export]
macro_rules! initialize_atomic_datetime {
    ($name:ident) => {
        lazy_static! {
            pub static ref $name: AtomicDateTime = AtomicDateTime::new(Utc::now());
        }
    };
}
