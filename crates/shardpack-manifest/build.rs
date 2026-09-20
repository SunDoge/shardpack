fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.btree_map(["."]);
    config
        .compile_protos(&["proto/shardpack/v1/manifest.proto"], &["proto"])
        .expect("compile protobuf manifests");
    println!("cargo:rerun-if-changed=proto/shardpack/v1/manifest.proto");
}
