use async_compression::tokio::write::ZstdEncoder;
use clap::Parser;
use futures_util::future::try_join_all;
use std::boxed::Box;
use std::collections::HashMap;
use std::hash::Hasher;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use pkgsmgr::types::*;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    hash: HashType,
    #[arg(long)]
    compression: Compression,

    input_path: PathBuf,
    output_path: PathBuf,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let chunks_path = &args.output_path.join("chunks");
    if !chunks_path.exists() {
        std::fs::create_dir_all(chunks_path)?;
    }

    let mut directories = Vec::new();
    let mut files = Vec::new();
    let mut symlinks = Vec::new();

    println!("Discovering files...");
    for entry in walkdir::WalkDir::new(&args.input_path).min_depth(1) {
        let entry = entry?;
        let path = entry.path().to_path_buf();

        if entry.file_type().is_dir() {
            directories.push(path);
        } else if entry.file_type().is_symlink() {
            symlinks.push(path);
        } else if entry.file_type().is_file() {
            files.push(path.clone());
        }
    }

    println!("Beginning hashing and compressing...");
    let mut hashes = HashMap::new();

    for file_group in files.chunks_mut(256) {
        let futures: Vec<_> = file_group
            .iter_mut()
            .map(|file_path| {
                compress_file(
                    file_path.to_path_buf(),
                    args.hash,
                    args.compression,
                    chunks_path,
                )
            })
            .collect();

        for (path, hash) in try_join_all(futures).await? {
            hashes.insert(path, hash);
        }
    }

    println!("Generating manifest...");
    let mut manifest = "".to_string();

    match args.compression {
        Compression::Zstd => manifest += "Compressed: zstd\n",
        Compression::None => (),
    }

    manifest += "---\n";

    for file in files {
        let hash = hashes
            .get(&file)
            .expect("tried adding file to manifest that has no hash");
        let metadata = fs::metadata(&file).await?;
        // Unix permission mode
        let mode = metadata.mode();
        // Size in KILOBYTES
        let size = metadata.size() / 1024;
        let path = file
            .strip_prefix(&args.input_path)
            .expect("tried adding file to manifest that is outside of input_path");

        manifest += &format!("{mode};{size};{hash};{path:?}\n");
    }

    fs::write(args.output_path.join("manifest"), manifest).await?;

    Ok(())
}

async fn compress_file(
    file_path: PathBuf,
    hash: HashType,
    compression: Compression,
    chunks_path: &Path,
) -> Result<(PathBuf, String), Box<dyn std::error::Error>> {
    let mut source_file = match File::open(&file_path).await {
        Ok(file) => file,
        Err(e) => {
            eprintln!("couldn't open source file: {}", file_path.display());
            panic!("{e}")
        }
    };
    let mut xxh_hasher = xxhash_rust::xxh3::Xxh3Default::new();
    let mut blake3_hasher = blake3::Hasher::new();

    let mut buf = [0; 8192];
    loop {
        let n = source_file.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        let chunk = &buf[0..n];
        match hash {
            HashType::Blake3 => blake3_hasher.write_all(chunk)?,
            HashType::Xxh3_128 => xxh_hasher.write_all(chunk)?,
        }
    }

    let hash = match hash {
        HashType::Xxh3_128 => hex::encode(xxh_hasher.finish().to_le_bytes()),
        HashType::Blake3 => blake3_hasher.finalize().to_hex().to_string(),
    };

    let chunk_path = &chunks_path.join(&hash);

    if !chunk_path.exists() {
        let mut source_file = File::open(&file_path).await.unwrap();
        let temp_file_path = temp_file::TempFile::new()?;
        let temp_file = File::create(&temp_file_path).await?;

        if compression == Compression::Zstd {
            let mut zstd = ZstdEncoder::new(temp_file);

            let mut buf = [0; 8192];
            loop {
                let n = source_file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }

                zstd.write_all(&buf).await?;
            }

            let mut zstd_path = chunk_path.clone();
            zstd_path.set_extension("zstd");
            fs::copy(temp_file_path, zstd_path).await?;
        }

        if fs::hard_link(&file_path, chunk_path).await.is_err() {
            fs::copy(&file_path, chunk_path).await?;
        };

        println!("Created chunk from path {file_path:?}");
    };

    Ok((file_path, hash))
}
