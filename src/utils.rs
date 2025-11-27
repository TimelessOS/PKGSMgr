use std::io::Write;
use xxhash_rust::xxh3;

pub async fn get(url: &str) -> Result<reqwest::Response, reqwest::Error> {
    let req = reqwest::get(url).await?;
    let req = req.error_for_status()?;

    Ok(req)
}

pub enum Hasher {
    Blake3(blake3::Hasher),
    Xxh3_128(xxh3::Xxh3Default),
}

impl Hasher {
    pub fn write(&mut self, data: &[u8]) {
        match self {
            Hasher::Blake3(hash) => {
                hash.write_all(data).expect("could not use blake3");
            }
            Hasher::Xxh3_128(hash) => {
                hash.write_all(data).expect("could not use blake3");
            }
        }
    }

    pub fn digest(self) -> String {
        match self {
            Hasher::Blake3(hash) => hash.finalize().to_hex().to_string(),
            Hasher::Xxh3_128(hash) => hex::encode(hash.digest128().to_le_bytes()),
        }
    }

    pub fn new(hash_method: crate::types::HashType) -> Self {
        match hash_method {
            crate::types::HashType::Blake3 => Hasher::Blake3(blake3::Hasher::new()),
            crate::types::HashType::Xxh3_128 => Hasher::Xxh3_128(xxh3::Xxh3Default::new()),
        }
    }
}
