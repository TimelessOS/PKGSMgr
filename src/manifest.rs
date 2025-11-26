use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

use crate::chunks::{Chunk, chunk_filename};

pub fn parse_manifest(raw_manifest: &str) -> (HashMap<&str, &str>, Vec<Chunk>) {
    let (raw_headers, raw_chunklist) = raw_manifest
        .split_once("---")
        .expect("No divider. Invalid repo.");

    let headers = parse_headers(raw_headers);
    let chunklist = parse_chunklist(raw_chunklist);

    (headers, chunklist)
}

fn parse_headers(raw_headers: &str) -> HashMap<&str, &str> {
    let mut headers = HashMap::new();

    for line in raw_headers.lines() {
        if let Some((key, value)) = line.split_once(":") {
            headers.insert(key, value.trim());
        }
    }

    headers
}

fn parse_chunklist(raw_chunklist: &str) -> Vec<Chunk> {
    let mut chunklist = Vec::new();

    for line in raw_chunklist.lines() {
        let parts: Vec<&str> = line.split(";").collect();
        if parts.len() < 3 {
            continue;
        }

        let chunk = Chunk {
            permissions: parts[0]
                .parse()
                .expect("permissions/first field in chunk invalid, expected u32"),
            size: parts[1]
                .parse()
                .expect("size/second field in chunk invalid, expected u32"),
            hash: parts[2].into(),
            path: parts[3..].join(";"),
        };

        chunklist.push(chunk);
    }

    chunklist
}

// Returns whether the manifest has changed
pub fn update_manifest(new_manifest: &str, manifests_path: &Path) -> Result<bool, io::Error> {
    let current_path = &manifests_path.join("current");
    let old_path = &manifests_path.join("old");

    if !current_path.exists() {
        fs::write(current_path, new_manifest)?;
        return Ok(true);
    }

    let current = fs::read_to_string(current_path)?;

    // Skip updating as the manifests are the same
    if current == new_manifest {
        return Ok(false);
    }

    fs::rename(current_path, old_path)?;
    fs::write(current_path, new_manifest)?;

    Ok(true)
}

pub fn build_tree(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunklist_parsing() {
        let raw_chunklist =
            "420;16000;example_hash;this/is/a;path\n420;127510;anotherhash;path/path/path/path";

        let chunklist = parse_chunklist(raw_chunklist);

        assert_eq!(chunklist.len(), 2);
        assert_eq!(
            chunklist[0],
            Chunk {
                permissions: 420,
                size: 16000,
                hash: "example_hash".into(),
                path: "this/is/a;path".into()
            }
        )
    }

    #[test]
    fn test_header_parsing() {
        let raw_headers = "Header: Key\nAnotherHeader: Slightly secret key \n ";

        let headers = parse_headers(raw_headers);

        assert_eq!(headers.len(), 2);
        assert_eq!(headers.get("Header").unwrap(), &"Key")
    }
}
