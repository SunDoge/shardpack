use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use shardpack_catalog::Catalog;
use shardpack_core::{
    ImportOptions, MaterializeOptions, import_shardpack_coordinated_with_registry, load_layout,
    load_snapshot, rebalance_layout_transaction_with_registry,
};
use shardpack_format::BlockCodec;
use shardpack_manifest::{LayoutId, SnapshotId, proto};
use shardpack_runtime::{DatasetReader, IncompleteLayoutPolicy};
use shardpack_storage::{S3Options, Store, StoreRegistry};
use snafu::Snafu;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("configuration failed: {source}"))]
    Config { source: shardpack_config::Error },
    #[snafu(display("storage initialization failed: {source}"))]
    Storage { source: shardpack_storage::Error },
    #[snafu(display("ShardPack operation failed: {source}"))]
    Core { source: shardpack_core::Error },
    #[snafu(display("runtime verification failed: {source}"))]
    Runtime { source: shardpack_runtime::Error },
    #[snafu(display("catalog operation failed: {source}"))]
    Catalog { source: shardpack_catalog::Error },
    #[snafu(display("garbage collection failed: {source}"))]
    Gc { source: shardpack_gc::Error },
    #[snafu(display("manifest ID is invalid: {source}"))]
    Manifest { source: shardpack_manifest::Error },
    #[snafu(display("cannot read {path}: {source}", path = path.display()))]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("invalid hexadecimal schema ID: {source}"))]
    Hex { source: hex::FromHexError },
    #[snafu(display("refusing destructive GC without --yes"))]
    ConfirmationRequired,
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Parser)]
#[command(
    name = "shardpack",
    version,
    about = "Operate immutable ShardPack datasets"
)]
struct Cli {
    /// TOML configuration file. Defaults to SHARDPACK_CONFIG or the XDG path.
    #[arg(long, global = true, env = "SHARDPACK_CONFIG")]
    config: Option<PathBuf>,

    /// Named store from the configuration file.
    #[arg(long, global = true)]
    store: Option<String>,

    /// Local OpenDAL filesystem root. Used when --s3-bucket is absent.
    #[arg(long, global = true, conflicts_with = "s3_bucket")]
    root: Option<PathBuf>,

    /// S3 or S3-compatible bucket.
    #[arg(long, global = true, env = "SHARDPACK_S3_BUCKET")]
    s3_bucket: Option<String>,
    #[arg(long, global = true, env = "SHARDPACK_S3_ROOT")]
    s3_root: Option<String>,
    #[arg(long, global = true, env = "SHARDPACK_S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    #[arg(long, global = true, env = "AWS_REGION")]
    s3_region: Option<String>,
    #[arg(long, global = true, env = "AWS_ACCESS_KEY_ID", hide_env_values = true)]
    s3_access_key_id: Option<String>,
    #[arg(
        long,
        global = true,
        env = "AWS_SECRET_ACCESS_KEY",
        hide_env_values = true
    )]
    s3_secret_access_key: Option<String>,
    #[arg(long, global = true, env = "AWS_SESSION_TOKEN", hide_env_values = true)]
    s3_session_token: Option<String>,
    /// Multipart upload part size in bytes (minimum 5 MiB).
    #[arg(
        long,
        global = true,
        env = "SHARDPACK_S3_WRITE_CHUNK_BYTES",
        default_value_t = 16 * 1024 * 1024
    )]
    s3_write_chunk_bytes: usize,
    /// Maximum concurrent multipart uploads for one object.
    #[arg(
        long,
        global = true,
        env = "SHARDPACK_S3_WRITE_CONCURRENCY",
        default_value_t = 8
    )]
    s3_write_concurrency: usize,

    /// Prefix for mutable catalog records.
    #[arg(long, global = true)]
    catalog_prefix: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the validated summary of one manifest.
    Inspect {
        #[arg(value_enum)]
        kind: ManifestKind,
        key: String,
    },
    /// Stream and verify every shard, block, record and aggregate in a layout.
    Verify { layout_key: String },
    /// Import existing shard objects into a durable standalone snapshot.
    Import {
        #[arg(long)]
        dataset_id: String,
        #[arg(long)]
        codec_name: String,
        #[arg(long)]
        codec_version: u32,
        /// 32-byte schema ID encoded as 64 hexadecimal characters.
        #[arg(long)]
        schema_id: String,
        #[arg(long)]
        embedded_schema_file: Option<PathBuf>,
        #[arg(long)]
        description: Option<String>,
        #[arg(long, default_value = "snapshots")]
        snapshot_prefix: String,
        #[arg(long, default_value = "layouts")]
        layout_prefix: String,
        #[arg(long, default_value_t = 1_073_741_824)]
        target_shard_bytes: u64,
        #[arg(long, default_value_t = 16_777_216)]
        target_block_bytes: u64,
        #[arg(long, value_enum, default_value_t = Codec::None)]
        block_codec: Codec,
        /// Object keys in the selected store, or STORE_ID::OBJECT_KEY.
        #[arg(required = true)]
        shards: Vec<String>,
    },
    /// Copy-on-write repack a layout, validate equivalence and switch preferred.
    Rebalance {
        layout_key: String,
        #[arg(long)]
        staging_directory: PathBuf,
        #[arg(long, default_value = "shards")]
        object_prefix: String,
        #[arg(long, default_value = "layouts")]
        layout_prefix: String,
        #[arg(long, default_value_t = 1_073_741_824)]
        target_shard_bytes: u64,
        #[arg(long, default_value_t = 16_777_216)]
        target_block_bytes: usize,
        #[arg(long, value_enum, default_value_t = Codec::None)]
        block_codec: Codec,
        #[arg(long, default_value_t = 3)]
        zstd_level: i32,
    },
    /// Persistently protect a layout from GC.
    Pin {
        pin_id: String,
        layout_key: String,
        #[arg(long, value_enum, default_value_t = PinKind::Manual)]
        reason: PinKind,
        /// Optional Unix expiry time in seconds.
        #[arg(long)]
        expires_at: Option<i64>,
    },
    /// Remove a persistent layout pin.
    Unpin { pin_id: String },
    /// Inventory objects and print the GC deletion plan without deleting anything.
    GcPlan {
        #[command(flatten)]
        options: GcOptions,
    },
    /// Inventory and delete objects under the shared catalog barrier.
    GcApply {
        #[command(flatten)]
        options: GcOptions,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ManifestKind {
    Snapshot,
    Layout,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum Codec {
    #[default]
    None,
    Zstd,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum PinKind {
    RunningJob,
    #[default]
    Manual,
    Retention,
}

#[derive(Clone, Debug, clap::Args)]
struct GcOptions {
    #[arg(long, default_value = "snapshots")]
    snapshot_prefix: String,
    #[arg(long, default_value = "layouts")]
    layout_prefix: String,
    #[arg(long, default_value = "shards")]
    shard_prefix: String,
    #[arg(long, default_value_t = 604_800)]
    grace_period_seconds: u64,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let storage = open_storage(&cli)?;
    let store = storage.selected_store.clone();
    let catalog = Catalog::new(
        storage.catalog_store.clone(),
        storage.catalog_prefix.clone(),
    )
    .map_err(|source| Error::Catalog { source })?;
    match cli.command {
        Command::Inspect { kind, key } => inspect(&store, kind, &key).await,
        Command::Verify { layout_key } => verify(&storage.registry, &store, &layout_key).await,
        Command::Import {
            dataset_id,
            codec_name,
            codec_version,
            schema_id,
            embedded_schema_file,
            description,
            snapshot_prefix,
            layout_prefix,
            target_shard_bytes,
            target_block_bytes,
            block_codec,
            shards,
        } => {
            let schema_id = hex::decode(schema_id).map_err(|source| Error::Hex { source })?;
            let embedded_schema = read_optional(embedded_schema_file.as_deref()).await?;
            let imported = import_shardpack_coordinated_with_registry(
                &store,
                &storage.registry,
                &catalog,
                &ImportOptions {
                    dataset_id,
                    description,
                    record_schema: proto::RecordSchemaDescriptor {
                        codec_name,
                        codec_version,
                        schema_id,
                        embedded_schema,
                    },
                    shard_locations: shards
                        .into_iter()
                        .map(|value| shard_location_arg(&value, &storage.selected_store_id))
                        .collect(),
                    snapshot_prefix,
                    layout_prefix,
                    layout_recipe: proto::LayoutRecipe {
                        target_shard_bytes,
                        target_block_bytes,
                        block_codec: codec_name_for(block_codec).into(),
                        shuffle_seed: None,
                    },
                    builder_version: env!("CARGO_PKG_VERSION").into(),
                },
            )
            .await
            .map_err(|source| Error::Core { source })?;
            println!("snapshot={}", hex::encode(&imported.snapshot.snapshot_id));
            println!("snapshot_object={}", imported.snapshot_object_key);
            println!("layout={}", hex::encode(&imported.layout.layout_id));
            println!("layout_object={}", imported.layout_object_key);
            Ok(())
        }
        Command::Rebalance {
            layout_key,
            staging_directory,
            object_prefix,
            layout_prefix,
            target_shard_bytes,
            target_block_bytes,
            block_codec,
            zstd_level,
        } => {
            let source = load_layout(&store, &layout_key)
                .await
                .map_err(|source| Error::Core { source })?;
            let options = MaterializeOptions {
                store_id: storage.selected_store_id.clone(),
                target_shard_bytes,
                target_block_bytes,
                block_codec: block_codec.into(),
                zstd_level,
                object_prefix,
                layout_prefix,
                ..MaterializeOptions::default()
            };
            let result = rebalance_layout_transaction_with_registry(
                &storage.registry,
                &store,
                &catalog,
                &source,
                &staging_directory,
                &options,
            )
            .await
            .map_err(|source| Error::Core { source })?;
            println!(
                "layout={}\nlayout_object={}",
                hex::encode(&result.materialization.layout.layout_id),
                result.materialization.layout_object_key
            );
            Ok(())
        }
        Command::Pin {
            pin_id,
            layout_key,
            reason,
            expires_at,
        } => {
            let layout = load_layout(&store, &layout_key)
                .await
                .map_err(|source| Error::Core { source })?;
            catalog
                .pin(proto::LayoutPin {
                    pin_id,
                    snapshot_id: layout.snapshot_id,
                    layout_id: layout.layout_id,
                    reason: reason.into(),
                    created_at: Some(now_timestamp()),
                    expires_at: expires_at.map(timestamp),
                })
                .await
                .map_err(|source| Error::Catalog { source })?;
            println!("pinned");
            Ok(())
        }
        Command::Unpin { pin_id } => {
            catalog
                .unpin(&pin_id)
                .await
                .map_err(|source| Error::Catalog { source })?;
            println!("unpinned");
            Ok(())
        }
        Command::GcPlan { options } => {
            let plan = gc_plan(&store, &catalog, &options).await?;
            print_gc_plan(&plan);
            Ok(())
        }
        Command::GcApply { options, yes } => {
            if !yes {
                return Err(Error::ConfirmationRequired);
            }
            let plan = shardpack_gc::sweep(
                &store,
                &catalog,
                &inventory_options(&options),
                &shardpack_catalog::new_owner_id(),
            )
            .await
            .map_err(|source| Error::Gc { source })?;
            print_gc_plan(&plan);
            Ok(())
        }
    }
}

struct StorageContext {
    selected_store_id: String,
    selected_store: Store,
    catalog_store: Store,
    catalog_prefix: String,
    registry: StoreRegistry,
}

fn open_storage(cli: &Cli) -> Result<StorageContext> {
    let config_path = cli.config.clone().or_else(|| {
        (cli.root.is_none() && cli.s3_bucket.is_none())
            .then(shardpack_config::ShardPackConfig::discover_path)
            .flatten()
    });
    if let Some(config_path) = config_path {
        let config = shardpack_config::ShardPackConfig::load(&config_path)
            .map_err(|source| Error::Config { source })?;
        let selected_store_id = config
            .selected_store_id(cli.store.as_deref())
            .map_err(|source| Error::Config { source })?
            .to_owned();
        let catalog_store_id = config
            .catalog_store_id()
            .map_err(|source| Error::Config { source })?
            .to_owned();
        let registry = config
            .build_registry()
            .map_err(|source| Error::Config { source })?;
        let selected_store = registry
            .get(&selected_store_id)
            .map_err(|source| Error::Storage { source })?
            .clone();
        let catalog_store = registry
            .get(&catalog_store_id)
            .map_err(|source| Error::Storage { source })?
            .clone();
        return Ok(StorageContext {
            selected_store_id,
            selected_store,
            catalog_store,
            catalog_prefix: cli.catalog_prefix.clone().unwrap_or(config.catalog.prefix),
            registry,
        });
    }

    let selected_store = match &cli.s3_bucket {
        Some(bucket) => Store::s3(&S3Options {
            bucket: bucket.clone(),
            root: cli.s3_root.clone(),
            endpoint: cli.s3_endpoint.clone(),
            region: cli.s3_region.clone(),
            profile: None,
            access_key_id: cli.s3_access_key_id.clone(),
            secret_access_key: cli.s3_secret_access_key.clone(),
            session_token: cli.s3_session_token.clone(),
            write_chunk_bytes: cli.s3_write_chunk_bytes,
            write_concurrency: cli.s3_write_concurrency,
        }),
        None => Store::filesystem(cli.root.as_deref().unwrap_or_else(|| Path::new("."))),
    }
    .map_err(|source| Error::Storage { source })?;
    let selected_store_id = cli.store.clone().unwrap_or_else(|| "default".into());
    let mut registry = StoreRegistry::new();
    registry
        .insert(&selected_store_id, selected_store.clone())
        .map_err(|source| Error::Storage { source })?;
    Ok(StorageContext {
        selected_store_id,
        selected_store: selected_store.clone(),
        catalog_store: selected_store,
        catalog_prefix: cli
            .catalog_prefix
            .clone()
            .unwrap_or_else(|| "catalog".into()),
        registry,
    })
}

fn shard_location_arg(value: &str, default_store_id: &str) -> proto::ObjectLocation {
    let (store_id, object_key) = value.split_once("::").unwrap_or((default_store_id, value));
    proto::ObjectLocation {
        store_id: store_id.to_owned(),
        object_key: object_key.to_owned(),
    }
}

async fn inspect(store: &Store, kind: ManifestKind, key: &str) -> Result<()> {
    match kind {
        ManifestKind::Snapshot => {
            let value = load_snapshot(store, key)
                .await
                .map_err(|source| Error::Core { source })?;
            let id = SnapshotId::from_slice(&value.snapshot_id)
                .map_err(|source| Error::Manifest { source })?;
            println!(
                "kind=snapshot\nid={id}\ndataset={}\nrecords={}\nportability={:?}\ndurability={:?}\nsplits={}\nskipped_records={}",
                value.dataset_id,
                value.logical_record_count,
                proto::Portability::try_from(value.portability).ok(),
                proto::Durability::try_from(value.durability).ok(),
                value.splits.len(),
                value
                    .materialization_stats
                    .as_ref()
                    .map_or(0, |stats| stats.skipped_records)
            );
        }
        ManifestKind::Layout => {
            let value = load_layout(store, key)
                .await
                .map_err(|source| Error::Core { source })?;
            let id = LayoutId::from_slice(&value.layout_id)
                .map_err(|source| Error::Manifest { source })?;
            println!(
                "kind=layout\nid={id}\nsnapshot={}\nstate={:?}\nrecords={}\nskipped_records={}\nbytes={}\nshards={}\nsplit={}",
                hex::encode(&value.snapshot_id),
                proto::LayoutState::try_from(value.state).ok(),
                value.total_records,
                value
                    .materialization_stats
                    .as_ref()
                    .map_or(0, |stats| stats.skipped_records),
                value.total_bytes,
                value.shards.len(),
                value.split_name.as_deref().unwrap_or("-")
            );
        }
    }
    Ok(())
}

async fn verify(registry: &StoreRegistry, store: &Store, layout_key: &str) -> Result<()> {
    let layout = load_layout(store, layout_key)
        .await
        .map_err(|source| Error::Core { source })?;
    let expected = layout.total_records;
    let shard_count = layout.shards.len();
    let total_bytes = layout.total_bytes;
    let mut reader = DatasetReader::new_with_registry_and_incomplete_policy(
        registry.clone(),
        layout,
        IncompleteLayoutPolicy::Allow,
    )
    .map_err(|source| Error::Runtime { source })?;
    let mut records = 0_u64;
    while reader
        .next_record()
        .await
        .map_err(|source| Error::Runtime { source })?
        .is_some()
    {
        records += 1;
    }
    println!("verified records={records} shards={shard_count} bytes={total_bytes}");
    debug_assert_eq!(records, expected);
    Ok(())
}

async fn gc_plan(
    store: &Store,
    catalog: &Catalog,
    options: &GcOptions,
) -> Result<shardpack_gc::GcPlan> {
    let input = shardpack_gc::inventory(store, catalog, &inventory_options(options))
        .await
        .map_err(|source| Error::Gc { source })?;
    shardpack_gc::plan(&input).map_err(|source| Error::Gc { source })
}

fn inventory_options(options: &GcOptions) -> shardpack_gc::InventoryOptions {
    shardpack_gc::InventoryOptions {
        snapshot_prefix: options.snapshot_prefix.clone(),
        layout_prefix: options.layout_prefix.clone(),
        shard_prefix: options.shard_prefix.clone(),
        now_unix_seconds: now_seconds(),
        grace_period_seconds: options.grace_period_seconds,
    }
}

fn print_gc_plan(plan: &shardpack_gc::GcPlan) {
    println!(
        "retained_layouts={} delete_layouts={} delete_shards={}",
        plan.retained_layouts.len(),
        plan.delete_layout_manifests.len(),
        plan.delete_shards.len()
    );
    for key in &plan.delete_layout_manifests {
        println!("delete layout {key}");
    }
    for key in &plan.delete_shards {
        println!("delete shard {key}");
    }
}

async fn read_optional(path: Option<&Path>) -> Result<Option<Vec<u8>>> {
    match path {
        Some(path) => tokio::fs::read(path)
            .await
            .map(Some)
            .map_err(|source| Error::ReadFile {
                path: path.to_owned(),
                source,
            }),
        None => Ok(None),
    }
}

fn codec_name_for(codec: Codec) -> &'static str {
    match codec {
        Codec::None => "none",
        Codec::Zstd => "zstd",
    }
}

impl From<Codec> for BlockCodec {
    fn from(value: Codec) -> Self {
        match value {
            Codec::None => Self::None,
            Codec::Zstd => Self::Zstd,
        }
    }
}

impl From<PinKind> for i32 {
    fn from(value: PinKind) -> Self {
        match value {
            PinKind::RunningJob => proto::PinReason::RunningJob as i32,
            PinKind::Manual => proto::PinReason::Manual as i32,
            PinKind::Retention => proto::PinReason::Retention as i32,
        }
    }
}

fn now_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn now_timestamp() -> prost_types::Timestamp {
    timestamp(now_seconds())
}

const fn timestamp(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;
    use shardpack_format::{ShardWriter, WriterOptions};

    use super::{Cli, Codec, Command, run};

    #[test]
    fn parses_safe_gc_and_operational_commands() {
        let cli = Cli::try_parse_from([
            "shardpack",
            "--root",
            "/tmp/data",
            "verify",
            "layouts/x.layout.pb",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::Verify { .. }));

        let cli = Cli::try_parse_from(["shardpack", "gc-apply", "--yes"]).unwrap();
        assert!(matches!(cli.command, Command::GcApply { yes: true, .. }));
    }

    #[tokio::test]
    async fn imports_verifies_and_pins_on_the_filesystem_backend() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("incoming")).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([1; 16], 0, b"first").unwrap();
        writer.add_record([2; 16], 0, b"second").unwrap();
        let (bytes, _) = writer.finish().unwrap();
        std::fs::write(root.path().join("incoming/data.spk"), bytes).unwrap();

        run(local_cli(
            root.path(),
            Command::Import {
                dataset_id: "test-dataset".into(),
                codec_name: "opaque-test".into(),
                codec_version: 1,
                schema_id: "00".repeat(32),
                embedded_schema_file: None,
                description: None,
                snapshot_prefix: "snapshots".into(),
                layout_prefix: "layouts".into(),
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: Codec::None,
                shards: vec!["incoming/data.spk".into()],
            },
        ))
        .await
        .unwrap();

        let store = shardpack_storage::Store::filesystem(root.path()).unwrap();
        let layouts = store.list_keys("layouts/").await.unwrap();
        assert_eq!(layouts.len(), 1);
        run(local_cli(
            root.path(),
            Command::Verify {
                layout_key: layouts[0].clone(),
            },
        ))
        .await
        .unwrap();
        run(local_cli(
            root.path(),
            Command::Pin {
                pin_id: "integration-test".into(),
                layout_key: layouts[0].clone(),
                reason: super::PinKind::Manual,
                expires_at: None,
            },
        ))
        .await
        .unwrap();
        let catalog = shardpack_catalog::Catalog::new(store, "catalog").unwrap();
        assert_eq!(catalog.pins().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn config_selects_data_and_catalog_stores_independently() {
        let directory = tempfile::tempdir().unwrap();
        let data_root = directory.path().join("data");
        let catalog_root = directory.path().join("control");
        std::fs::create_dir_all(data_root.join("incoming")).unwrap();
        std::fs::create_dir_all(&catalog_root).unwrap();
        let mut writer = ShardWriter::new(Vec::new(), WriterOptions::default()).unwrap();
        writer.add_record([3; 16], 0, b"configured").unwrap();
        let (bytes, _) = writer.finish().unwrap();
        std::fs::write(data_root.join("incoming/configured.spk"), bytes).unwrap();
        let config_path = directory.path().join("config.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
default_store = "data"
[catalog]
store = "control"
prefix = "catalog"
[stores.data]
type = "filesystem"
root = "{}"
[stores.control]
type = "filesystem"
root = "{}"
"#,
                data_root.display(),
                catalog_root.display()
            ),
        )
        .unwrap();

        run(config_cli(
            config_path.clone(),
            Command::Import {
                dataset_id: "configured-dataset".into(),
                codec_name: "opaque-test".into(),
                codec_version: 1,
                schema_id: "00".repeat(32),
                embedded_schema_file: None,
                description: None,
                snapshot_prefix: "snapshots".into(),
                layout_prefix: "layouts".into(),
                target_shard_bytes: 1024,
                target_block_bytes: 1024,
                block_codec: Codec::None,
                shards: vec!["incoming/configured.spk".into()],
            },
        ))
        .await
        .unwrap();

        let data = shardpack_storage::Store::filesystem(&data_root).unwrap();
        let layouts = data.list_keys("layouts/").await.unwrap();
        assert_eq!(layouts.len(), 1);
        run(config_cli(
            config_path,
            Command::Verify {
                layout_key: layouts[0].clone(),
            },
        ))
        .await
        .unwrap();
        let catalog_store = shardpack_storage::Store::filesystem(&catalog_root).unwrap();
        assert!(
            catalog_store
                .list_keys("catalog/")
                .await
                .unwrap()
                .is_empty()
        );
    }

    fn local_cli(root: &std::path::Path, command: Command) -> Cli {
        Cli {
            config: None,
            store: None,
            root: Some(root.to_owned()),
            s3_bucket: None,
            s3_root: None,
            s3_endpoint: None,
            s3_region: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_session_token: None,
            s3_write_chunk_bytes: 16 * 1024 * 1024,
            s3_write_concurrency: 8,
            catalog_prefix: Some("catalog".into()),
            command,
        }
    }

    fn config_cli(config: PathBuf, command: Command) -> Cli {
        Cli {
            config: Some(config),
            store: None,
            root: None,
            s3_bucket: None,
            s3_root: None,
            s3_endpoint: None,
            s3_region: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_session_token: None,
            s3_write_chunk_bytes: 16 * 1024 * 1024,
            s3_write_concurrency: 8,
            catalog_prefix: None,
            command,
        }
    }
}
