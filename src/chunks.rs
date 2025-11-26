use async_compression::tokio::bufread::ZstdDecoder;
use futures_util::TryStreamExt;
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tokio::io::{AsyncReadExt, BufReader};

use crate::Compression;
use crate::get;
use crate::parse_manifest;

#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub hash: String,
    pub size: u64,
    pub path: String,
    pub permissions: u32,
}

pub async fn install_chunk(
    chunk: &Chunk,
    repo_url: &str,
    chunk_path: &Path,
    compression: &Compression,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("[INFO] Downloading {}", chunk.path);
    let extension = match compression {
        Compression::None => "",
        Compression::Zstd => ".zstd",
    };
    let chunk_url = format!("{repo_url}/chunks/{}{extension}", chunk.hash);
    let res = get(&chunk_url).await?;

    let mut hasher = blake3::Hasher::new();
    let temp_file_path = chunk_path.join(format!("{}.new", chunk.hash));
    let mut temp_file = fs::File::create(&temp_file_path)?;

    // Turn the response into a stream
    let stream = res.bytes_stream().map_err(std::io::Error::other);
    let stream = tokio_util::io::StreamReader::new(stream);

    // Turn the response into a reader, decompressing if required.
    let mut reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> = match compression {
        Compression::None => Box::new(stream),
        Compression::Zstd => Box::new(ZstdDecoder::new(BufReader::new(stream))),
    };

    let mut buf = [0u8; 8192];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        let chunk = &buf[0..n];

        hasher.update(chunk);
        temp_file.write_all(chunk)?;
    }

    if *hasher.finalize().to_hex() != *chunk.hash {
        panic!("Invalid hash recieved. Corruption?")
    }

    // Set permissions
    let mut perms = temp_file.metadata()?.permissions();
    perms.set_mode(chunk.permissions);
    perms.set_readonly(true);
    temp_file.set_permissions(perms)?;

    fs::rename(&temp_file_path, chunk_path.join(chunk_filename(chunk)))?;

    Ok(())
}

pub fn clean_old_chunks(
    manifests_path: &Path,
    chunkstore_path: &Path,
) -> Result<u64, std::io::Error> {
    let mut freed = 0;
    let mut allowed_chunks = HashSet::new();

    let current_path = manifests_path.join("current");
    let old_path = manifests_path.join("old");

    // Calculate a list of all chunks
    if current_path.exists() {
        let (_, chunklist) = parse_manifest(&fs::read_to_string(current_path)?);
        for chunk in chunklist {
            allowed_chunks.insert(chunk_filename(&chunk));
        }
    }
    if old_path.exists() {
        let (_, chunklist) = parse_manifest(&fs::read_to_string(old_path)?);
        for chunk in chunklist {
            allowed_chunks.insert(chunk_filename(&chunk));
        }
    }

    for entry in fs::read_dir(chunkstore_path)? {
        let entry = entry?;
        let filename = entry
            .file_name()
            .into_string()
            .expect("non utf8 filename in chunkstore.");

        if !allowed_chunks.contains(&filename) {
            freed += fs::metadata(entry.path())?.len();
            fs::remove_file(entry.path())?;
        }
    }

    Ok(freed)
}

pub fn chunk_filename(chunk: &Chunk) -> String {
    format!("{}{}", chunk.hash, chunk.permissions)
}
