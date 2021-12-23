use std::path::PathBuf;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use structopt::StructOpt;

pub mod simple_uring;
pub mod with_register_files;
pub mod without_uring;

pub const RING_SIZE: usize = 16;
pub const MAX_READ_SIZE: usize = 4096 * 16;

#[derive(StructOpt)]
pub struct Opt {
    #[structopt()]
    /// The files to be checksummed.
    pub files: Vec<PathBuf>,

    /// Use the io_uring feature of pre-registering files to be read before the read is requested.
    #[structopt(long)]
    pub preregister_files: bool,

    /// Use the io_uring feature of reading into fixed position buffers.
    #[structopt(long)]
    pub use_fixed_buffers: bool,

    /// Compute checksums without the io_uring feature.
    #[structopt(long, conflicts_with_all = &["preregister_files", "use_fixed_buffers"])]
    pub no_uring: bool,
}
