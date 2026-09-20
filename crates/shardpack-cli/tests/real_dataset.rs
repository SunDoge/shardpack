use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use bytes::{BufMut, BytesMut};
use shardpack_core::{
    EncodedRecord, MaterializeOptions, Result as CoreResult, SourceReader, materialize_layout,
};
use shardpack_format::BlockCodec;
use shardpack_runtime::DatasetReader;
use shardpack_storage::Store;

struct OcrRecord {
    image: PathBuf,
    relative_image: String,
    annotation: String,
}

struct OcrSource {
    records: VecDeque<OcrRecord>,
}

struct ImageRecord {
    image: PathBuf,
    relative_image: String,
    split_flag: u32,
}

struct ImageDirectorySource {
    records: VecDeque<ImageRecord>,
}

impl ImageDirectorySource {
    fn open(root: &Path, trees: &[&str]) -> Self {
        let mut records = Vec::new();
        for (split_flag, tree) in trees.iter().enumerate() {
            collect_images(
                root,
                &root.join(tree),
                u32::try_from(split_flag).unwrap(),
                &mut records,
            );
        }
        records.sort_unstable_by(|left, right| left.relative_image.cmp(&right.relative_image));
        Self {
            records: records.into(),
        }
    }
}

fn collect_images(root: &Path, directory: &Path, split_flag: u32, output: &mut Vec<ImageRecord>) {
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_images(root, &path, split_flag, output);
        } else if path.extension().is_some_and(|extension| {
            extension.eq_ignore_ascii_case("jpeg") || extension.eq_ignore_ascii_case("jpg")
        }) {
            output.push(ImageRecord {
                relative_image: path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                image: path,
                split_flag,
            });
        }
    }
}

impl OcrSource {
    fn open(root: &Path) -> Self {
        let mut records = VecDeque::new();
        for split in ["train.txt", "val.txt"] {
            let contents = fs::read_to_string(root.join(split)).unwrap();
            for line in contents.lines().filter(|line| !line.is_empty()) {
                let (relative_image, annotation) = line
                    .split_once('\t')
                    .unwrap_or_else(|| panic!("invalid OCR row in {split}: {line}"));
                records.push_back(OcrRecord {
                    image: root.join(relative_image),
                    relative_image: relative_image.to_owned(),
                    annotation: annotation.to_owned(),
                });
            }
        }
        Self { records }
    }
}

impl SourceReader for OcrSource {
    fn next_record(&mut self) -> CoreResult<Option<EncodedRecord>> {
        let Some(record) = self.records.pop_front() else {
            return Ok(None);
        };
        let image = fs::read(&record.image).unwrap();
        let path = record.relative_image.as_bytes();
        let annotation = record.annotation.as_bytes();
        let mut payload = BytesMut::with_capacity(12 + path.len() + annotation.len() + image.len());
        payload.put_u32_le(u32::try_from(path.len()).unwrap());
        payload.put_u32_le(u32::try_from(annotation.len()).unwrap());
        payload.put_u32_le(u32::try_from(image.len()).unwrap());
        payload.extend_from_slice(path);
        payload.extend_from_slice(annotation);
        payload.extend_from_slice(&image);

        let digest = blake3::hash(record.relative_image.as_bytes());
        let mut sample_id = [0_u8; 16];
        sample_id.copy_from_slice(&digest.as_bytes()[..16]);
        Ok(Some(EncodedRecord {
            sample_id,
            flags: 0,
            payload: payload.freeze(),
        }))
    }
}

impl SourceReader for ImageDirectorySource {
    fn next_record(&mut self) -> CoreResult<Option<EncodedRecord>> {
        let Some(record) = self.records.pop_front() else {
            return Ok(None);
        };
        let image = fs::read(&record.image).unwrap();
        let path = record.relative_image.as_bytes();
        let mut payload = BytesMut::with_capacity(8 + path.len() + image.len());
        payload.put_u32_le(u32::try_from(path.len()).unwrap());
        payload.put_u32_le(u32::try_from(image.len()).unwrap());
        payload.extend_from_slice(path);
        payload.extend_from_slice(&image);

        let digest = blake3::hash(record.relative_image.as_bytes());
        let mut sample_id = [0_u8; 16];
        sample_id.copy_from_slice(&digest.as_bytes()[..16]);
        Ok(Some(EncodedRecord {
            sample_id,
            flags: record.split_flag,
            payload: payload.freeze(),
        }))
    }
}

#[tokio::test]
#[ignore = "requires SHARDPACK_TEST_OCR_DATASET pointing at the OCR detection fixture"]
async fn packs_real_ocr_detection_dataset_and_reads_every_byte_back() {
    let dataset = PathBuf::from(
        std::env::var_os("SHARDPACK_TEST_OCR_DATASET").expect("set SHARDPACK_TEST_OCR_DATASET"),
    );
    let expected_records = OcrSource::open(&dataset).records.len() as u64;
    assert!(expected_records > 0);

    let output = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    let store = Store::filesystem(output.path()).unwrap();
    let mut source = OcrSource::open(&dataset);
    let materialized = materialize_layout(
        &mut source,
        *blake3::hash(b"ocr-det-real-dataset-test").as_bytes(),
        &store,
        staging.path(),
        &MaterializeOptions {
            target_shard_bytes: 8 * 1024 * 1024,
            target_block_bytes: 4 * 1024 * 1024,
            block_codec: BlockCodec::None,
            object_prefix: "real-ocr/shards".into(),
            layout_prefix: "real-ocr/layouts".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(materialized.layout.total_records, expected_records);
    assert!(materialized.layout.shards.len() > 1);

    let mut expected = OcrSource::open(&dataset);
    let mut reader = DatasetReader::new(store, materialized.layout.clone()).unwrap();
    let mut observed_records = 0_u64;
    while let Some(observed) = reader.next_record().await.unwrap() {
        let expected = expected.next_record().unwrap().unwrap();
        assert_eq!(observed.sample_id, expected.sample_id);
        assert_eq!(observed.flags, expected.flags);
        assert_eq!(observed.payload, expected.payload);
        observed_records += 1;
    }
    assert!(expected.next_record().unwrap().is_none());
    assert_eq!(observed_records, expected_records);

    eprintln!(
        "packed {observed_records} OCR records into {} shards ({} stored bytes)",
        materialized.layout.shards.len(),
        materialized.layout.total_bytes
    );
}

#[tokio::test]
#[ignore = "requires SHARDPACK_TEST_IMAGE_DATASET with train/ and val/ directories"]
async fn packs_real_image_dataset_and_reports_sequential_throughput() {
    let dataset = PathBuf::from(
        std::env::var_os("SHARDPACK_TEST_IMAGE_DATASET").expect("set SHARDPACK_TEST_IMAGE_DATASET"),
    );
    let tree_names =
        std::env::var("SHARDPACK_TEST_IMAGE_TREES").unwrap_or_else(|_| "train,val".into());
    let trees = tree_names.split(',').collect::<Vec<_>>();
    assert!(trees.iter().all(|tree| !tree.is_empty()));
    let expected_records = ImageDirectorySource::open(&dataset, &trees).records.len() as u64;
    assert!(expected_records > 0);

    let output = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    let store = Store::filesystem(output.path()).unwrap();
    let mut source = ImageDirectorySource::open(&dataset, &trees);
    let write_started = Instant::now();
    let materialized = materialize_layout(
        &mut source,
        *blake3::hash(b"real-image-dataset-test").as_bytes(),
        &store,
        staging.path(),
        &MaterializeOptions {
            target_shard_bytes: 256 * 1024 * 1024,
            target_block_bytes: 16 * 1024 * 1024,
            block_codec: BlockCodec::None,
            object_prefix: "real-images/shards".into(),
            layout_prefix: "real-images/layouts".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let write_elapsed = write_started.elapsed();

    assert_eq!(materialized.layout.total_records, expected_records);
    assert!(materialized.layout.shards.len() > 1);

    let mut expected = ImageDirectorySource::open(&dataset, &trees);
    let mut reader = DatasetReader::new(store.clone(), materialized.layout.clone()).unwrap();
    let read_started = Instant::now();
    let mut observed_records = 0_u64;
    while let Some(observed) = reader.next_record().await.unwrap() {
        let expected = expected.next_record().unwrap().unwrap();
        assert_eq!(observed.sample_id, expected.sample_id);
        assert_eq!(observed.flags, expected.flags);
        assert_eq!(observed.payload, expected.payload);
        observed_records += 1;
    }
    let read_elapsed = read_started.elapsed();
    assert!(expected.next_record().unwrap().is_none());
    assert_eq!(observed_records, expected_records);

    let runtime_read_started = Instant::now();
    let mut runtime_reader = DatasetReader::new(store, materialized.layout.clone()).unwrap();
    let mut runtime_records = 0_u64;
    let mut runtime_payload_bytes = 0_u64;
    while let Some(record) = runtime_reader.next_record().await.unwrap() {
        runtime_records += 1;
        runtime_payload_bytes += record.payload.len() as u64;
    }
    let runtime_read_elapsed = runtime_read_started.elapsed();
    assert_eq!(runtime_records, expected_records);

    let mib = materialized.layout.total_bytes as f64 / (1024.0 * 1024.0);
    let payload_mib = runtime_payload_bytes as f64 / (1024.0 * 1024.0);
    eprintln!(
        "packed {observed_records} image records into {} shards ({mib:.1} MiB): write {:.1} MiB/s, source-compared read {:.1} MiB/s, runtime-only payload read {:.1} MiB/s",
        materialized.layout.shards.len(),
        mib / write_elapsed.as_secs_f64(),
        mib / read_elapsed.as_secs_f64(),
        payload_mib / runtime_read_elapsed.as_secs_f64(),
    );
}
