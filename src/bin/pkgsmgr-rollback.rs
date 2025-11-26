use clap::Parser;
use nix::fcntl::{AT_FDCWD, RenameFlags, renameat2};
use std::fs;
use std::path::PathBuf;

use pkgsmgr::manifest::{build_tree, parse_manifest, update_manifest};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
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

    let old_manifest_path = &manifests_path.join("old");

    if !old_manifest_path.exists() {
        eprintln!("No previous versions exist to rollback to.");
        std::process::exit(1)
    }

    // Rollback to previous manifest
    let old_manifest = fs::read_to_string(old_manifest_path)?;
    update_manifest(&old_manifest, manifests_path)?;

    let (_, chunklist) = parse_manifest(&old_manifest);

    build_tree(staging_path, chunks_path, &chunklist).expect("could not build staging");

    renameat2(
        AT_FDCWD,
        staging_path,
        AT_FDCWD,
        &root_path.join("usr"),
        RenameFlags::RENAME_EXCHANGE,
    )?;

    println!("Rolled back successfully.");

    Ok(())
}
