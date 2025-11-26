use clap::Parser;
use nix::fcntl::{AT_FDCWD, RenameFlags, renameat2};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

pub mod chunks;
pub mod manifest;

use chunks::{Chunk, chunk_filename, clean_old_chunks, install_chunk};
use manifest::{parse_manifest, update_manifest};

static MAJOR_VERSION: LazyLock<usize> =
    LazyLock::new(|| env!("CARGO_PKG_VERSION_MAJOR").parse::<usize>().unwrap());
static MINOR_VERSION: LazyLock<usize> =
    LazyLock::new(|| env!("CARGO_PKG_VERSION_MINOR").parse::<usize>().unwrap());

pub enum Compression {
    None,
    Zstd,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    repo_url: String,
    #[arg(long)]
    root_path: Option<PathBuf>,
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

    let manifest_raw = get(&format!("{}/manifest", &args.repo_url))
        .await?
        .text()
        .await
        .expect("server responded with 200, yet not valid utf8 text.");

    // Quit early if nothing has changed
    if !update_manifest(&manifest_raw, manifests_path)
        .expect("could not update local manifest cache")
    {
        return Ok(());
    }

    let (headers, chunklist) = parse_manifest(&manifest_raw);

    let mut compression = Compression::None;

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
            _ => {
                eprintln!("[WARNING] Unknown header: {key}");
            }
        }
    }

    // Install all chunks in chunklist before doing anything else.
    for chunk in &chunklist {
        if !chunks_path.join(chunk_filename(chunk)).exists() {
            install_chunk(chunk, &args.repo_url, chunks_path, &compression)
                .await
                .expect("could not download chunk");
        }
    }

    build_tree(staging_path, chunks_path, &chunklist).expect("could not build staging");

    let cwd = AT_FDCWD;

    renameat2(
        cwd,
        staging_path,
        cwd,
        &root_path.join("usr"),
        RenameFlags::RENAME_EXCHANGE,
    )?;

    let freed_bytes =
        clean_old_chunks(manifests_path, chunks_path).expect("could not free old chunks");
    println!("Freed {}kb", freed_bytes / 1024);

    Ok(())
}

async fn get(url: &str) -> Result<reqwest::Response, reqwest::Error> {
    let req = reqwest::get(url).await?;
    let req = req.error_for_status()?;

    Ok(req)
}

fn build_tree(
    staging_path: &Path,
    chunkstore_path: &Path,
    chunks: &[Chunk],
) -> Result<(), std::io::Error> {
    if staging_path.exists() {
        fs::remove_dir_all(staging_path)?;
    }
    fs::create_dir_all(staging_path)?;

    for chunk in chunks {
        let path = staging_path.join(&chunk.path);
        let parent_path = path.parent().unwrap_or_else(|| Path::new("/"));
        if !parent_path.exists() {
            fs::create_dir_all(parent_path)?;
        }

        fs::hard_link(chunkstore_path.join(chunk_filename(chunk)), path)?;
    }

    Ok(())
}
