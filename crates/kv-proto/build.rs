fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../proto");
    let protos = [
        format!("{proto_dir}/raft.proto"),
        format!("{proto_dir}/kv.proto"),
        format!("{proto_dir}/admin.proto"),
    ];
    for p in &protos {
        println!("cargo:rerun-if-changed={p}");
    }
    tonic_build::configure().compile_protos(&protos, &[proto_dir])?;
    Ok(())
}
