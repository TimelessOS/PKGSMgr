use async_compression::tokio::write::ZstdEncoder;
use clap::Parser;
use nix::fcntl::{AT_FDCWD, RenameFlags, renameat2};
use std::boxed::Box;
use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

use pkgsmgr::types::*;
use pkgsmgr::utils::Hasher;

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

    for file_path in &files {
        let hash = hash_file(file_path, args.hash).await?;

        compress(file_path, args.compression, chunks_path, &hash).await?;

        if fs::hard_link(&file_path, chunks_path.join(&hash))
            .await
            .is_err()
        {
            fs::copy(&file_path, chunks_path.join(&hash)).await?;
        };

        hashes.insert(file_path, hash);
    }

    println!("Generating manifest...");
    let mut manifest = "".to_string();

    match args.compression {
        Compression::Zstd => manifest += "Compression: zstd\n",
        Compression::None => (),
    }
    match args.hash {
        HashType::Blake3 => manifest += "Hasher: blake3\n",
        HashType::Xxh3_128 => manifest += "Hasher: xxh3_128\n",
    }

    manifest += "---\n";

    for file in &files {
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
            .expect("tried adding file to manifest that is outside of input_path")
            .to_str()
            .unwrap();

        manifest += &format!("{mode};{size};{hash};{path}\n");
    }

    // Atomically replace on-disk manifest
    let hash = &blake3::hash(manifest.as_bytes()).to_hex().to_string();
    let tmp_link_path = args.output_path.join("manifest.tmp");
    let main_link_path = args.output_path.join("manifest");
    let manifest_path = args.output_path.join(hash);

    fs::write(manifest_path, manifest).await?;
    fs::write(&tmp_link_path, hash).await?;

    if !&main_link_path.exists() {
        fs::write(&main_link_path, "").await?;
    }

    renameat2(
        AT_FDCWD,
        &tmp_link_path,
        AT_FDCWD,
        &main_link_path,
        RenameFlags::RENAME_EXCHANGE,
    )?;

    fs::remove_file(&tmp_link_path).await?;

    Ok(())
}

async fn hash_file(
    file_path: &Path,
    hash_method: HashType,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut source_file = match File::open(&file_path).await {
        Ok(file) => file,
        Err(e) => {
            eprintln!("couldn't open source file: {}", file_path.display());
            panic!("{e}")
        }
    };

    let mut hasher = Hasher::new(hash_method);

    let mut buf = [0; 8192];
    loop {
        let n = source_file.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        let chunk = &buf[0..n];
        hasher.write(chunk);
    }

    let hash = hasher.digest();

    Ok(hash)
}

async fn compress(
    file_path: &Path,
    compression: Compression,
    chunks_path: &Path,
    hash: &str,
) -> Result<(), std::io::Error> {
    let compressed_chunk_filename = match compression {
        Compression::Zstd => format!("{hash}.zstd"),
        Compression::None => panic!("Tried to compress on a non-compressable request."),
    };
    let compressed_chunk_path = &chunks_path.join(compressed_chunk_filename);

    if !compressed_chunk_path.exists() {
        let mut source_file = File::open(&file_path).await.unwrap();
        let temp_file_path = temp_file::TempFile::new()?;
        let mut temp_file = File::create(&temp_file_path).await?;

        let mut compressor: Box<dyn AsyncWrite + Sync + Unpin> = match compression {
            Compression::Zstd => Box::new(ZstdEncoder::new(&mut temp_file)),
            Compression::None => panic!("Tried to copmress on a non-compressable request."),
        };

        let mut buf = [0; 8192];
        loop {
            let n = source_file.read(&mut buf).await?;
            if n == 0 {
                break;
            }

            compressor.write_all(&buf[0..n]).await?;
        }

        // Finish compressing
        compressor.flush().await?;
        compressor.shutdown().await?;

        // Move compressed from memory and onto disk
        fs::copy(temp_file_path, compressed_chunk_path).await?;

        println!("Compressed chunk from path {file_path:?}");
    };

    Ok(())
}
