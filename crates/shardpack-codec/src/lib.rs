//! Type-safe record decoder registry, independent of the shard container.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bytes::Bytes;
use shardpack_manifest::proto;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("codec {name}@{version} is already registered"))]
    DuplicateCodec { name: String, version: u32 },
    #[snafu(display("codec {name}@{version} is not registered"))]
    UnsupportedCodec { name: String, version: u32 },
    #[snafu(display("schema ID must be exactly 32 bytes"))]
    InvalidSchemaId,
    #[snafu(display("embedded schema is required for codec {name}@{version}"))]
    EmbeddedSchemaRequired { name: String, version: u32 },
    #[snafu(display("embedded schema does not match schema ID"))]
    EmbeddedSchemaMismatch,
    #[snafu(display("schema {schema_id} is unsupported by codec {name}@{version}"))]
    UnsupportedSchema {
        name: String,
        version: u32,
        schema_id: String,
    },
    #[snafu(display("record decode failed for codec {name}@{version}: {message}"))]
    Decode {
        name: String,
        version: u32,
        message: String,
    },
    #[snafu(display("snapshot manifest is invalid: {source}"))]
    Manifest { source: shardpack_manifest::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Decoder<T>: Send + Sync + 'static {
    type Error: std::fmt::Display;
    fn decode(&self, payload: Bytes) -> std::result::Result<T, Self::Error>;
}

trait ErasedDecoder<T>: Send + Sync {
    fn decode(&self, payload: Bytes) -> std::result::Result<T, String>;
}

impl<T, D> ErasedDecoder<T> for D
where
    D: Decoder<T>,
{
    fn decode(&self, payload: Bytes) -> std::result::Result<T, String> {
        Decoder::decode(self, payload).map_err(|error| error.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CodecKey {
    name: String,
    version: u32,
}

enum SchemaSupport {
    /// Portable schema bytes must be embedded and hash to `schema_id`.
    Embedded,
    /// The codec specification defines these schema IDs without embedding.
    BuiltIn(BTreeSet<[u8; 32]>),
}

struct Entry<T> {
    schemas: SchemaSupport,
    decoder: Arc<dyn ErasedDecoder<T>>,
}

pub struct CodecRegistry<T> {
    entries: BTreeMap<CodecKey, Entry<T>>,
}

impl<T: 'static> Default for CodecRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: 'static> CodecRegistry<T> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn register_embedded<D: Decoder<T>>(
        &mut self,
        name: impl Into<String>,
        version: u32,
        decoder: D,
    ) -> Result<()> {
        self.register(name, version, SchemaSupport::Embedded, decoder)
    }

    pub fn register_builtin<D, I>(
        &mut self,
        name: impl Into<String>,
        version: u32,
        schema_ids: I,
        decoder: D,
    ) -> Result<()>
    where
        D: Decoder<T>,
        I: IntoIterator<Item = [u8; 32]>,
    {
        let schemas = schema_ids.into_iter().collect::<BTreeSet<_>>();
        self.register(name, version, SchemaSupport::BuiltIn(schemas), decoder)
    }

    fn register<D: Decoder<T>>(
        &mut self,
        name: impl Into<String>,
        version: u32,
        schemas: SchemaSupport,
        decoder: D,
    ) -> Result<()> {
        let key = CodecKey {
            name: name.into(),
            version,
        };
        if self.entries.contains_key(&key) {
            return Err(Error::DuplicateCodec {
                name: key.name,
                version,
            });
        }
        self.entries.insert(
            key,
            Entry {
                schemas,
                decoder: Arc::new(decoder),
            },
        );
        Ok(())
    }

    pub fn validate_descriptor(&self, descriptor: &proto::RecordSchemaDescriptor) -> Result<()> {
        let (_, entry, schema_id) = self.resolve(descriptor)?;
        match (&entry.schemas, descriptor.embedded_schema.as_deref()) {
            (SchemaSupport::Embedded, Some(schema)) if !schema.is_empty() => {
                if schema_id_for(schema) != schema_id {
                    return Err(Error::EmbeddedSchemaMismatch);
                }
            }
            (SchemaSupport::Embedded, _) => {
                return Err(Error::EmbeddedSchemaRequired {
                    name: descriptor.codec_name.clone(),
                    version: descriptor.codec_version,
                });
            }
            (SchemaSupport::BuiltIn(supported), _) if !supported.contains(&schema_id) => {
                return Err(Error::UnsupportedSchema {
                    name: descriptor.codec_name.clone(),
                    version: descriptor.codec_version,
                    schema_id: hex_string(&schema_id),
                });
            }
            (SchemaSupport::BuiltIn(_), _) => {}
        }
        Ok(())
    }

    pub fn validate_snapshot(&self, snapshot: &proto::SnapshotManifest) -> Result<()> {
        shardpack_manifest::validate_snapshot(snapshot)
            .map_err(|source| Error::Manifest { source })?;
        self.validate_descriptor(snapshot.record_schema.as_ref().expect("validated schema"))
    }

    pub fn decode(&self, descriptor: &proto::RecordSchemaDescriptor, payload: Bytes) -> Result<T> {
        self.validate_descriptor(descriptor)?;
        let (key, entry, _) = self.resolve(descriptor)?;
        entry
            .decoder
            .decode(payload)
            .map_err(|message| Error::Decode {
                name: key.name.clone(),
                version: key.version,
                message,
            })
    }

    fn resolve(
        &self,
        descriptor: &proto::RecordSchemaDescriptor,
    ) -> Result<(&CodecKey, &Entry<T>, [u8; 32])> {
        let schema_id: [u8; 32] = descriptor
            .schema_id
            .as_slice()
            .try_into()
            .map_err(|_| Error::InvalidSchemaId)?;
        let lookup = CodecKey {
            name: descriptor.codec_name.clone(),
            version: descriptor.codec_version,
        };
        let (key, entry) = self
            .entries
            .get_key_value(&lookup)
            .ok_or(Error::UnsupportedCodec {
                name: lookup.name,
                version: lookup.version,
            })?;
        Ok((key, entry, schema_id))
    }
}

#[must_use]
pub fn schema_id_for(embedded_schema: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shardpack-record-schema-v1");
    hasher.update(embedded_schema);
    *hasher.finalize().as_bytes()
}

fn hex_string(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(64);
    for byte in bytes {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    result
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{CodecRegistry, Decoder, Error, schema_id_for};
    use shardpack_manifest::proto;

    struct Identity;
    impl Decoder<Bytes> for Identity {
        type Error = std::convert::Infallible;

        fn decode(&self, payload: Bytes) -> std::result::Result<Bytes, Self::Error> {
            Ok(payload)
        }
    }

    #[test]
    fn validates_embedded_schema_and_dispatches_decoder() {
        let schema = b"portable protobuf descriptor".to_vec();
        let descriptor = proto::RecordSchemaDescriptor {
            codec_name: "protobuf".into(),
            codec_version: 1,
            schema_id: schema_id_for(&schema).to_vec(),
            embedded_schema: Some(schema),
        };
        let mut registry = CodecRegistry::new();
        registry.register_embedded("protobuf", 1, Identity).unwrap();
        assert_eq!(
            registry
                .decode(&descriptor, Bytes::from_static(b"record"))
                .unwrap(),
            "record"
        );
        let mut broken = descriptor;
        broken.embedded_schema = Some(b"another schema".to_vec());
        assert!(matches!(
            registry.validate_descriptor(&broken),
            Err(Error::EmbeddedSchemaMismatch)
        ));
    }

    #[test]
    fn builtin_codec_accepts_only_registered_schema_ids() {
        let supported = [7; 32];
        let mut registry = CodecRegistry::new();
        registry
            .register_builtin("standard", 3, [supported], Identity)
            .unwrap();
        let mut descriptor = proto::RecordSchemaDescriptor {
            codec_name: "standard".into(),
            codec_version: 3,
            schema_id: supported.to_vec(),
            embedded_schema: None,
        };
        registry.validate_descriptor(&descriptor).unwrap();
        descriptor.schema_id = vec![8; 32];
        assert!(matches!(
            registry.validate_descriptor(&descriptor),
            Err(Error::UnsupportedSchema { .. })
        ));
    }
}
