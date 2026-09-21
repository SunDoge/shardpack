//! Typed, layered ShardPack configuration.

use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
};

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use shardpack_storage::{S3Options, Store, StoreRegistry};
use snafu::Snafu;

const DEFAULT_WRITE_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_WRITE_CONCURRENCY: usize = 8;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to load ShardPack configuration: {source}"))]
    Load { source: config::ConfigError },
    #[snafu(display("configuration references unknown store {store_id:?}"))]
    UnknownStore { store_id: String },
    #[snafu(display("configuration must define at least one store"))]
    NoStores,
    #[snafu(display("unsupported configuration version {version}; expected version 1"))]
    UnsupportedVersion { version: u32 },
    #[snafu(display("environment variable {name:?} required by store {store_id:?} is missing"))]
    MissingEnvironment { store_id: String, name: String },
    #[snafu(display("failed to initialize store {store_id:?}: {source}"))]
    Storage {
        store_id: String,
        source: shardpack_storage::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardPackConfig {
    #[serde(default = "config_version")]
    pub version: u32,
    pub default_store: Option<String>,
    pub stores: BTreeMap<String, StoreConfig>,
    #[serde(default)]
    pub catalog: CatalogConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    pub store: Option<String>,
    #[serde(default = "catalog_prefix")]
    pub prefix: String,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            store: None,
            prefix: catalog_prefix(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoreConfig {
    S3(S3Config),
    Filesystem(FilesystemConfig),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemConfig {
    pub root: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    pub bucket: String,
    pub root: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub credentials: Option<CredentialsConfig>,
    #[serde(default = "write_chunk_bytes")]
    pub write_chunk_bytes: usize,
    #[serde(default = "write_concurrency")]
    pub write_concurrency: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CredentialsConfig {
    DefaultChain,
    Profile {
        profile: String,
    },
    Env {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
    Inline {
        access_key_id: SecretString,
        secret_access_key: SecretString,
        session_token: Option<SecretString>,
    },
}

impl ShardPackConfig {
    pub fn load(path: &Path) -> Result<Self> {
        config::Config::builder()
            .add_source(config::File::from(path).required(true))
            .add_source(
                config::Environment::with_prefix("SHARDPACK")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .and_then(config::Config::try_deserialize)
            .map_err(|source| Error::Load { source })
            .and_then(Self::validate)
    }

    /// Finds the conventional user configuration without making it mandatory.
    #[must_use]
    pub fn discover_path() -> Option<PathBuf> {
        if let Some(path) = env::var_os("SHARDPACK_CONFIG") {
            return Some(PathBuf::from(path));
        }
        if let Some(base) = env::var_os("XDG_CONFIG_HOME") {
            let path = PathBuf::from(base).join("shardpack/config.toml");
            return path.is_file().then_some(path);
        }
        env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(".config/shardpack/config.toml"))
            .filter(|path| path.is_file())
    }

    pub fn validate(self) -> Result<Self> {
        if self.version != 1 {
            return Err(Error::UnsupportedVersion {
                version: self.version,
            });
        }
        if self.stores.is_empty() {
            return Err(Error::NoStores);
        }
        for store_id in self.default_store.iter().chain(self.catalog.store.iter()) {
            if !self.stores.contains_key(store_id) {
                return Err(Error::UnknownStore {
                    store_id: store_id.clone(),
                });
            }
        }
        Ok(self)
    }

    pub fn selected_store_id<'a>(&'a self, requested: Option<&'a str>) -> Result<&'a str> {
        requested
            .or(self.default_store.as_deref())
            .or_else(|| {
                (self.stores.len() == 1).then(|| self.stores.keys().next().unwrap().as_str())
            })
            .ok_or_else(|| Error::UnknownStore {
                store_id: "<no --store and no default_store>".into(),
            })
            .and_then(|store_id| {
                self.stores
                    .contains_key(store_id)
                    .then_some(store_id)
                    .ok_or_else(|| Error::UnknownStore {
                        store_id: store_id.to_owned(),
                    })
            })
    }

    pub fn catalog_store_id(&self) -> Result<&str> {
        self.selected_store_id(self.catalog.store.as_deref())
    }

    pub fn build_registry(&self) -> Result<StoreRegistry> {
        let mut registry = StoreRegistry::new();
        for (store_id, definition) in &self.stores {
            let store = definition.open(store_id)?;
            registry
                .insert(store_id, store)
                .map_err(|source| Error::Storage {
                    store_id: store_id.clone(),
                    source,
                })?;
        }
        Ok(registry)
    }
}

impl StoreConfig {
    fn open(&self, store_id: &str) -> Result<Store> {
        let result = match self {
            Self::Filesystem(config) => Store::filesystem(&config.root),
            Self::S3(config) => Store::s3(&config.options(store_id)?),
        };
        result.map_err(|source| Error::Storage {
            store_id: store_id.to_owned(),
            source,
        })
    }
}

impl S3Config {
    fn options(&self, store_id: &str) -> Result<S3Options> {
        let mut options = S3Options {
            bucket: self.bucket.clone(),
            root: self.root.clone(),
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            write_chunk_bytes: self.write_chunk_bytes,
            write_concurrency: self.write_concurrency,
            ..S3Options::default()
        };
        match self.credentials.as_ref() {
            None | Some(CredentialsConfig::DefaultChain) => {}
            Some(CredentialsConfig::Profile { profile }) => options.profile = Some(profile.clone()),
            Some(CredentialsConfig::Env {
                access_key_id,
                secret_access_key,
                session_token,
            }) => {
                options.access_key_id = Some(required_env(store_id, access_key_id)?);
                options.secret_access_key = Some(required_env(store_id, secret_access_key)?);
                options.session_token = session_token
                    .as_ref()
                    .map(|name| required_env(store_id, name))
                    .transpose()?;
            }
            Some(CredentialsConfig::Inline {
                access_key_id,
                secret_access_key,
                session_token,
            }) => {
                options.access_key_id = Some(access_key_id.expose_secret().to_owned());
                options.secret_access_key = Some(secret_access_key.expose_secret().to_owned());
                options.session_token = session_token
                    .as_ref()
                    .map(|value| value.expose_secret().to_owned());
            }
        }
        Ok(options)
    }
}

fn required_env(store_id: &str, name: &str) -> Result<String> {
    env::var(name).map_err(|_| Error::MissingEnvironment {
        store_id: store_id.to_owned(),
        name: name.to_owned(),
    })
}

const fn config_version() -> u32 {
    1
}
fn catalog_prefix() -> String {
    "catalog".into()
}
const fn write_chunk_bytes() -> usize {
    DEFAULT_WRITE_CHUNK_BYTES
}
const fn write_concurrency() -> usize {
    DEFAULT_WRITE_CONCURRENCY
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{CredentialsConfig, ShardPackConfig, StoreConfig};
    use secrecy::ExposeSecret;

    #[test]
    fn loads_multiple_stores_and_redacts_inline_secrets() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        fs::write(
            &config_path,
            format!(
                r#"
version = 1
default_store = "primary"

[catalog]
store = "primary"

[stores.primary]
type = "filesystem"
root = "{}"

[stores.archive]
type = "s3"
bucket = "archive"
endpoint = "http://127.0.0.1:9000"
region = "us-east-1"

[stores.archive.credentials]
type = "inline"
access_key_id = "visible-id"
secret_access_key = "must-not-leak"
"#,
                directory.path().display()
            ),
        )
        .unwrap();

        let config = ShardPackConfig::load(&config_path).unwrap();
        assert_eq!(config.selected_store_id(None).unwrap(), "primary");
        assert_eq!(config.catalog_store_id().unwrap(), "primary");
        assert_eq!(config.stores.len(), 2);
        let StoreConfig::S3(archive) = &config.stores["archive"] else {
            panic!()
        };
        let Some(CredentialsConfig::Inline {
            secret_access_key, ..
        }) = &archive.credentials
        else {
            panic!()
        };
        assert_eq!(secret_access_key.expose_secret(), "must-not-leak");
        assert!(!format!("{config:?}").contains("must-not-leak"));
        let registry = config.build_registry().unwrap();
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn rejects_unknown_selected_store_and_unknown_fields() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        fs::write(
            &config_path,
            r#"
default_store = "missing"
[stores.local]
type = "filesystem"
root = "."
unexpected = true
"#,
        )
        .unwrap();
        assert!(ShardPackConfig::load(&config_path).is_err());
    }

    #[test]
    fn omitted_catalog_uses_the_normalized_default_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        fs::write(
            &config_path,
            r#"
default_store = "local"
[stores.local]
type = "filesystem"
root = "."
"#,
        )
        .unwrap();
        let config = ShardPackConfig::load(&config_path).unwrap();
        assert_eq!(config.catalog.prefix, "catalog");
    }
}
