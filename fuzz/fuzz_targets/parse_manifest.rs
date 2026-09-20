#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;
use shardpack_manifest::{proto, validate_layout, validate_snapshot};

fuzz_target!(|data: &[u8]| {
    let Some((&kind, protobuf)) = data.split_first() else {
        return;
    };
    if kind & 1 == 0 {
        if let Ok(manifest) = proto::SnapshotManifest::decode(protobuf) {
            let _ = std::hint::black_box(validate_snapshot(&manifest));
        }
    } else if let Ok(manifest) = proto::LayoutManifest::decode(protobuf) {
        let _ = std::hint::black_box(validate_layout(&manifest));
    }
});
