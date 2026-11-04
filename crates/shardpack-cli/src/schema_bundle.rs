use crate::{Error, Result};
use shardpack_manifest::{proto, schema_bundle};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

fn error(value: impl std::fmt::Display) -> Error {
    Error::SchemaBundle {
        reason: value.to_string(),
    }
}

fn collect(
    root: &Path,
    dir: &Path,
    files: &mut Vec<proto::SchemaFile>,
    total: &mut usize,
) -> Result<()> {
    for item in fs::read_dir(dir).map_err(error)? {
        let path = item.map_err(error)?.path();
        let metadata = fs::symlink_metadata(&path).map_err(error)?;
        if metadata.file_type().is_symlink() {
            return Err(error("symlinks are not allowed"));
        }
        let name = path
            .strip_prefix(root)
            .map_err(error)?
            .to_str()
            .ok_or_else(|| error("paths must be UTF-8"))?
            .replace(std::path::MAIN_SEPARATOR, "/");
        schema_bundle::validate_path(&name).map_err(error)?;
        if metadata.is_dir() {
            collect(root, &path, files, total)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(error("only regular schema files are allowed"));
        }
        if files.len() >= schema_bundle::MAX_FILES
            || metadata.len() > schema_bundle::MAX_BYTES as u64
        {
            return Err(error("schema bundle limit exceeded"));
        }
        let mut content = Vec::new();
        fs::File::open(&path)
            .map_err(error)?
            .take((schema_bundle::MAX_BYTES + 1) as u64)
            .read_to_end(&mut content)
            .map_err(error)?;
        *total = total
            .checked_add(content.len())
            .ok_or_else(|| error("schema size overflow"))?;
        if *total > schema_bundle::MAX_BYTES {
            return Err(error("schema bundle limit exceeded"));
        }
        files.push(proto::SchemaFile {
            path: name,
            content,
        });
    }
    Ok(())
}

pub fn pack(directory: &Path, entrypoint: &str, output: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(directory).map_err(error)?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(error("schema directory must be a real directory"));
    }
    let mut files = Vec::new();
    let mut total = 0;
    collect(directory, directory, &mut files, &mut total)?;
    let bytes = schema_bundle::encode(proto::SchemaBundle {
        entrypoint: entrypoint.into(),
        files,
    })
    .map_err(error)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(error)?;
    file.write_all(&bytes).map_err(error)?;
    println!(
        "schema_id={}",
        hex::encode(shardpack_codec::schema_id_for(&bytes))
    );
    Ok(())
}

pub fn export(descriptor: &proto::RecordSchemaDescriptor, output: &Path) -> Result<()> {
    let bytes = descriptor
        .embedded_schema
        .as_deref()
        .ok_or_else(|| error("snapshot has no embedded schema"))?;
    if descriptor.schema_id.as_slice() != shardpack_codec::schema_id_for(bytes) {
        return Err(error("schema ID does not match embedded bytes"));
    }
    let bundle = schema_bundle::decode(bytes).map_err(error)?;
    fs::create_dir(output).map_err(error)?;
    for file in &bundle.files {
        let path: PathBuf = output.join(&file.path);
        fs::create_dir_all(path.parent().expect("has parent")).map_err(error)?;
        let mut target = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(error)?;
        target.write_all(&file.content).map_err(error)?;
    }
    println!("entrypoint={}", bundle.entrypoint);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_export_preserves_tree_and_rejects_tampering() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("input");
        fs::create_dir_all(source.join("media")).unwrap();
        fs::write(
            source.join("detection.fbs"),
            b"include \"media/image.fbs\";",
        )
        .unwrap();
        fs::write(source.join("media/image.fbs"), b"namespace Media;").unwrap();
        let packed = temp.path().join("schema.bin");
        pack(&source, "detection.fbs", &packed).unwrap();
        let bytes = fs::read(packed).unwrap();
        let mut descriptor = proto::RecordSchemaDescriptor {
            codec_name: "flatbuffers".into(),
            codec_version: 1,
            schema_id: shardpack_codec::schema_id_for(&bytes).to_vec(),
            embedded_schema: Some(bytes),
        };
        let out = temp.path().join("output");
        export(&descriptor, &out).unwrap();
        assert_eq!(
            fs::read(out.join("media/image.fbs")).unwrap(),
            b"namespace Media;"
        );
        assert!(export(&descriptor, &out).is_err());
        descriptor.embedded_schema.as_mut().unwrap().push(1);
        let rejected = temp.path().join("rejected");
        assert!(export(&descriptor, &rejected).is_err());
        assert!(!rejected.exists());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink("/etc/passwd", temp.path().join("link")).unwrap();
        assert!(pack(temp.path(), "link", &temp.path().join("out")).is_err());
    }

    #[test]
    fn exported_flatbuffers_schema_compiles_when_flatc_is_available() {
        let Some(flatc) = std::env::var_os("SHARDPACK_TEST_FLATC") else {
            return;
        };
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../dotnet/benchmarks/RecordCodecs/schemas");
        let temp = tempfile::tempdir().unwrap();
        let packed = temp.path().join("schema.bin");
        pack(&source, "detection.fbs", &packed).unwrap();
        let bytes = fs::read(packed).unwrap();
        let descriptor = proto::RecordSchemaDescriptor {
            codec_name: "flatbuffers".into(),
            codec_version: 1,
            schema_id: shardpack_codec::schema_id_for(&bytes).to_vec(),
            embedded_schema: Some(bytes),
        };
        let out = temp.path().join("schema");
        export(&descriptor, &out).unwrap();
        let generated = temp.path().join("generated");
        fs::create_dir(&generated).unwrap();
        let status = std::process::Command::new(flatc)
            .arg("--csharp")
            .arg("-I")
            .arg(&out)
            .arg("-o")
            .arg(&generated)
            .arg(out.join("detection.fbs"))
            .status()
            .unwrap();
        assert!(status.success());
        assert!(
            generated
                .join("ShardPack/Experiment/DetectionSample.cs")
                .is_file()
        );
    }
}
