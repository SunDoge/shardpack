//! Portable multi-file schemas encoded as the existing embedded schema bytes.
use crate::{Error, Result, proto};
use prost::Message;
use std::collections::BTreeSet;

pub const MAGIC: &[u8] = b"SPKSCHEMA\x01";
pub const MAX_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_FILES: usize = 4096;

fn invalid(reason: &'static str) -> Error {
    Error::Invalid {
        field: "schema_bundle",
        reason,
    }
}

/// Paths use a portable slash-separated relative syntax, independent of the host OS.
pub fn validate_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > 1024
        || path.split('/').any(|c| {
            let stem = c.split('.').next().unwrap_or("").to_ascii_uppercase();
            let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
                || ((stem.starts_with("COM") || stem.starts_with("LPT"))
                    && stem.len() == 4
                    && matches!(stem.as_bytes()[3], b'1'..=b'9'));
            c.is_empty()
                || c.len() > 255
                || reserved
                || c == "."
                || c == ".."
                || c.ends_with('.')
                || c.ends_with(' ')
                || !c
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        })
    {
        return Err(invalid("invalid relative path"));
    }
    Ok(())
}

pub fn validate(bundle: &proto::SchemaBundle) -> Result<()> {
    validate_path(&bundle.entrypoint)?;
    if bundle.files.is_empty() || bundle.files.len() > MAX_FILES {
        return Err(invalid("invalid file count"));
    }
    let mut names = BTreeSet::new();
    let mut previous: Option<&str> = None;
    for file in &bundle.files {
        validate_path(&file.path)?;
        if previous.is_some_and(|p| p >= file.path.as_str()) {
            return Err(invalid("files must be sorted and unique"));
        }
        let folded = file.path.to_ascii_lowercase();
        if !names.insert(folded) {
            return Err(invalid("case-insensitive path collision"));
        }
        previous = Some(&file.path);
    }
    if !bundle.files.iter().any(|f| f.path == bundle.entrypoint) {
        return Err(invalid("entrypoint not found"));
    }
    for name in &names {
        let mut path = name.as_str();
        while let Some((parent, _)) = path.rsplit_once('/') {
            if names.contains(parent) {
                return Err(invalid("file conflicts with directory"));
            }
            path = parent;
        }
    }
    Ok(())
}

pub fn encode(mut bundle: proto::SchemaBundle) -> Result<Vec<u8>> {
    bundle.files.sort_by(|a, b| a.path.cmp(&b.path));
    validate(&bundle)?;
    let size = bundle.encoded_len();
    if size > MAX_BYTES - MAGIC.len() {
        return Err(invalid("bundle too large"));
    }
    let mut bytes = MAGIC.to_vec();
    bundle.encode(&mut bytes).expect("Vec encoding cannot fail");
    Ok(bytes)
}

pub fn decode(bytes: &[u8]) -> Result<proto::SchemaBundle> {
    if bytes.len() > MAX_BYTES {
        return Err(invalid("bundle too large"));
    }
    let encoded = bytes
        .strip_prefix(MAGIC)
        .ok_or_else(|| invalid("unknown bundle version or not a bundle"))?;
    let bundle = proto::SchemaBundle::decode(encoded).map_err(|source| Error::Decode { source })?;
    validate(&bundle)?;
    // Reject unknown fields and alternate protobuf representations to make the bytes canonical.
    if bundle.encode_to_vec() != encoded {
        return Err(invalid("non-canonical bundle encoding"));
    }
    Ok(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn file(path: &str) -> proto::SchemaFile {
        proto::SchemaFile {
            path: path.into(),
            content: b"data".to_vec(),
        }
    }
    #[test]
    fn canonical_roundtrip_and_rejection() {
        let b = proto::SchemaBundle {
            entrypoint: "root.fbs".into(),
            files: vec![file("root.fbs"), file("media/image.fbs")],
        };
        let a = encode(b.clone()).unwrap();
        let mut reversed = b;
        reversed.files.reverse();
        assert_eq!(a, encode(reversed).unwrap());
        assert_eq!(decode(&a).unwrap().files[0].path, "media/image.fbs");
        for p in [
            "../x", "/tmp/x", "a\\b", "a//b", "C:/x", "a/./b", "aux.fbs", "COM1",
        ] {
            assert!(validate_path(p).is_err(), "{p}");
        }
        for paths in [vec!["a", "a/b"], vec!["a", "A"]] {
            assert!(
                encode(proto::SchemaBundle {
                    entrypoint: "a".into(),
                    files: paths.into_iter().map(file).collect()
                })
                .is_err()
            );
        }
        let mut tampered = a;
        tampered.extend_from_slice(&[0x18, 0x01]);
        assert!(decode(&tampered).is_err());
    }
}
