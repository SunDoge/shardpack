#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use shardpack_format::{EncryptionKey, ReaderLimits, ShardReader};

fuzz_target!(|data: &[u8]| {
    let limits = ReaderLimits {
        max_records_per_block: 4096,
        max_stored_body_bytes: 1024 * 1024,
        max_raw_payload_bytes: 1024 * 1024,
    };
    let key = EncryptionKey::new([0xa5; 32]);
    let Ok(mut reader) =
        ShardReader::with_limits_and_key(Cursor::new(data), limits, Some(key))
    else {
        return;
    };
    loop {
        match reader.next_record() {
            Ok(Some(record)) => {
                std::hint::black_box(record);
            }
            Ok(None) | Err(_) => return,
        }
    }
});
