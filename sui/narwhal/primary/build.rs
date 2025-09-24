fn main() {
    tonic_build::configure()
        .build_server(false) // we only need the client in primary
        .compile(&["proto/interface.proto"], &["proto"])
        .expect("failed to compile interface.proto");

    println!("cargo:rerun-if-changed=proto/interface.proto");
}
