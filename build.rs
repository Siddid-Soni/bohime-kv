fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/bohime.proto");
    tonic_build::compile_protos("proto/bohime.proto")?;
    Ok(())
}
