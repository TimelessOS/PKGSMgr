use clap::Parser;
use nix::fcntl::{AT_FDCWD, RenameFlags, renameat2};
use std::fs;
use std::path::PathBuf;
use std::sync::LazyLock;

use pkgsmgr::chunks::{chunk_filename, clean_old_chunks, install_chunk};
use pkgsmgr::manifest::{build_tree, parse_manifest, try_update_manifest_hash, update_manifest};
use pkgsmgr::types::{Compression, HashType};
use pkgsmgr::utils::get;

static MAJOR_VERSION: LazyLock<usize> =
    LazyLock::new(|| env!("CARGO_PKG_VERSION_MAJOR").parse::<usize>().unwrap());
static MINOR_VERSION: LazyLock<usize> =
    LazyLock::new(|| env!("CARGO_PKG_VERSION_MINOR").parse::<usize>().unwrap());

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    repo_url: String,
    #[arg(long)]
    root_path: Option<PathBuf>,
    #[arg(long)]
    /// Useful for installers, where the installation media may contain relevant chunks already
    additional_cache_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let root_path = &args.root_path.unwrap_or_else(|| PathBuf::from("/"));
    let internal_path = &root_path.join(".pkgsmgr");
    let chunks_path = &internal_path.join("chunkstore");
    fs::create_dir_all(chunks_path)?;
    let staging_path = &internal_path.join("staging");
    let manifests_path = &internal_path.join("manifests");
    fs::create_dir_all(manifests_path)?;

    let manifest_hash = get(&format!("{}/manifest", &args.repo_url))
        .await?
        .error_for_status()?
        .text()
        .await?;

    if !try_update_manifest_hash(manifests_path, &manifest_hash)? {
        println!("[INFO] Skipping, no update found.");
        std::process::exit(0);
    }
    println!("[INFO] Update found, downloading manifest...");

    let manifest_raw = get(&format!("{}/{}", &args.repo_url, manifest_hash))
        .await?
        .text()
        .await
        .expect("server responded with 200, yet not valid utf8 text.");

    let (headers, chunklist) = parse_manifest(&manifest_raw);

    let mut compression = Compression::None;
    let mut hasher = HashType::Blake3;

    for (key, value) in headers {
        match key {
            "MinVersion" => {
                let parts: Vec<usize> = value.split('.').map(|str| str.parse().unwrap()).collect();

                // Major version check
                if parts[0] < *MAJOR_VERSION {
                    panic!("MinVersion declares major incompatibility. Outdated update client.")
                }

                // Minor version check
                // Also checks major version is the same.
                if let Some(min_version) = parts.get(1)
                    && *min_version > *MINOR_VERSION
                    && *MAJOR_VERSION == parts[0]
                {
                    panic!("MinVersion declares minor incompatibility. Outdated update client.")
                }
            }
            "Compression" => match value.to_lowercase().as_str() {
                "zstd" => {
                    compression = Compression::Zstd;
                }
                _ => {
                    eprintln!("Unknown compression requested: {}", value);
                }
            },
            "Hasher" => match value.to_lowercase().as_str() {
                "blake3" => {
                    hasher = HashType::Blake3;
                }
                "xxh3_128" => hasher = HashType::Xxh3_128,
                _ => {
                    eprintln!("Unknown compression requested: {}", value);
                }
            },
            _ => {
                eprintln!("[WARNING] Unknown header: {key}");
            }
        }
    }

    // Install all chunks in chunklist before doing anything else.
    for chunk in &chunklist {
        let chunk_path = chunks_path.join(chunk_filename(chunk));

        if !chunk_path.exists() {
            install_chunk(chunk, &args.repo_url, chunks_path, &compression, hasher)
                .await
                .expect("could not download chunk");
        }
    }

    // Quit early if nothing has changed
    if !update_manifest(&manifest_raw, manifests_path)
        .expect("could not update local manifest cache")
    {
        return Ok(());
    }

    build_tree(staging_path, chunks_path, &chunklist).expect("could not build staging");

    println!("[INFO] Swapping tree...");

    let usr_path = root_path.join("usr");
    if !usr_path.exists() {
        fs::create_dir_all(&usr_path)?;
    }

    renameat2(
        AT_FDCWD,
        staging_path,
        AT_FDCWD,
        &usr_path,
        RenameFlags::RENAME_EXCHANGE,
    )?;

    println!("[INFO] Cleaning up old chunks...");

    let freed_bytes =
        clean_old_chunks(manifests_path, chunks_path).expect("could not free old chunks");
    println!("Freed {}kb", freed_bytes / 1024);

    Ok(())
}
