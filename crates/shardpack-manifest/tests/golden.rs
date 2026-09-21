use prost::Message;
use shardpack_manifest::{layout_id, proto, snapshot_id, validate_layout, validate_snapshot};

const SNAPSHOT_HEX: &str = include_str!("data/v1_snapshot.hex");
const LAYOUT_HEX: &str = include_str!("data/v1_layout.hex");

#[test]
fn reads_and_reproduces_v1_snapshot_without_new_optional_fields() {
    let expected = snapshot_fixture();
    assert_eq!(hex::encode(expected.encode_to_vec()), SNAPSHOT_HEX.trim());
    let bytes = hex::decode(SNAPSHOT_HEX.trim()).unwrap();
    let decoded = proto::SnapshotManifest::decode(bytes.as_slice()).unwrap();
    validate_snapshot(&decoded).unwrap();
    assert_eq!(decoded, expected);
}

#[test]
fn reads_and_reproduces_v1_layout_without_materialization_stats() {
    let expected = layout_fixture();
    assert_eq!(hex::encode(expected.encode_to_vec()), LAYOUT_HEX.trim());
    let bytes = hex::decode(LAYOUT_HEX.trim()).unwrap();
    let decoded = proto::LayoutManifest::decode(bytes.as_slice()).unwrap();
    validate_layout(&decoded).unwrap();
    assert_eq!(decoded, expected);
}

fn snapshot_fixture() -> proto::SnapshotManifest {
    let mut value = proto::SnapshotManifest {
        dataset_id: "golden-v1".into(),
        source: Some(proto::SnapshotSource {
            source: Some(proto::snapshot_source::Source::Imported(
                proto::ImportedSource {
                    description: Some("pre-stats fixture".into()),
                },
            )),
        }),
        record_schema: Some(proto::RecordSchemaDescriptor {
            codec_name: "opaque.v1".into(),
            codec_version: 1,
            schema_id: vec![0x11; 32],
            embedded_schema: None,
        }),
        portability: proto::Portability::SelfContained as i32,
        durability: proto::Durability::Durable as i32,
        logical_record_count: 1,
        created_at: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 123,
        }),
        ..Default::default()
    };
    value.snapshot_id = snapshot_id(&value).0.to_vec();
    value
}

fn layout_fixture() -> proto::LayoutManifest {
    let fingerprint = proto::ContentFingerprint {
        record_count: 1,
        accumulator_a: vec![0x22; 32],
        accumulator_b: vec![0x33; 32],
    };
    let mut value = proto::LayoutManifest {
        snapshot_id: snapshot_fixture().snapshot_id,
        builder_version: "0.1.0".into(),
        recipe: Some(proto::LayoutRecipe {
            target_shard_bytes: 1_073_741_824,
            target_block_bytes: 16_777_216,
            block_codec: "none".into(),
            shuffle_seed: None,
            max_shard_bytes: None,
        }),
        portability: proto::Portability::SelfContained as i32,
        durability: proto::Durability::Durable as i32,
        total_records: 1,
        total_bytes: 89,
        content_fingerprint: Some(fingerprint.clone()),
        shards: vec![proto::ShardMeta {
            shard_id: vec![0x44; 32],
            location: Some(proto::ObjectLocation {
                store_id: "default".into(),
                object_key: "shards/44/example.spk".into(),
            }),
            byte_size: 89,
            record_count: 1,
            block_count: 1,
            content_fingerprint: Some(fingerprint),
        }],
        created_at: Some(prost_types::Timestamp {
            seconds: 1_700_000_001,
            nanos: 456,
        }),
        state: proto::LayoutState::Complete as i32,
        ..Default::default()
    };
    value.layout_id = layout_id(&value).0.to_vec();
    value
}
