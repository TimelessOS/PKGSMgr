pub mod chunks;
pub mod manifest;
pub mod utils;

// TODO: This needs to go somewhere else.
pub enum Compression {
    None,
    Zstd,
}
