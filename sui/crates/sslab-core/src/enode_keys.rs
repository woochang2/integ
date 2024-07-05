use anyhow::anyhow;
use secp256k1::KeyPair;

/// Write Base64 encoded `privkey` to file.
pub fn write_enode_key_to_file<P: AsRef<std::path::Path>>(
    keypair: &KeyPair,
    path: P,
) -> anyhow::Result<()> {
    let contents = keypair.secret_key().display_secret().to_string();
    std::fs::write(path.as_ref(), contents)?;

    // let id_file_path = add_suffix_to_path(path, "-id");
    // write_enode_id_to_file(keypair, id_file_path)?;

    Ok(())
}

/// Read from file as Base64 encoded `privkey` and return a KeyPair.
pub fn read_enode_key_from_file<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<KeyPair> {
    let contents = std::fs::read_to_string(path)?;
    KeyPair::from_seckey_str_global(contents.as_str().trim()).map_err(|e| anyhow!(e))
}

pub fn get_enode_id(keypair: &KeyPair) -> reth::rpc::types::PeerId {
    reth::rpc::types::PeerId::from_slice(&keypair.public_key().serialize_uncompressed()[1..])
}

#[allow(dead_code)]
fn write_enode_id_to_file<P: AsRef<std::path::Path>>(
    keypair: &KeyPair,
    path: P,
) -> anyhow::Result<()> {
    let enode_id = get_enode_id(keypair);
    let contents = enode_id.to_string();
    std::fs::write(path, contents)?;
    Ok(())
}

#[allow(dead_code)]
fn add_suffix_to_path<P: AsRef<std::path::Path>>(path: P, suffix: &str) -> std::path::PathBuf {
    let mut path = path.as_ref().to_path_buf();
    let file_name = path.file_name().unwrap().to_str().unwrap();
    let new_file_name = format!("{}{}", file_name, suffix);
    path.set_file_name(new_file_name);
    path
}
