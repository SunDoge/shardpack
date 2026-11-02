fn main() {
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-env-changed=SHARDPACK_BINDINGS_OUT");
    let output = std::env::var_os("SHARDPACK_BINDINGS_OUT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap()));
    std::fs::create_dir_all(&output).unwrap();
    csbindgen::Builder::default()
        .input_extern_file("src/lib.rs")
        .csharp_namespace("SunDoge.ShardPack.Interop")
        .csharp_dll_name("shardpack_ffi")
        .generate_csharp_file(output.join("NativeMethods.g.cs"))
        .expect("generate C# bindings");
}
