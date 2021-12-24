use std::{
    fs::{File, OpenOptions},
    os::unix::prelude::OpenOptionsExt,
    path::{Path, PathBuf},
};

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
    pub pre_register_files: bool,

    /// Use the io_uring feature of reading into fixed position buffers.
    #[structopt(long)]
    pub use_fixed_buffers: bool,

    /// Compute checksums without the io_uring feature.
    #[structopt(long, conflicts_with_all = &["preregister_files", "use_fixed_buffers", "o_direct"])]
    pub no_uring: bool,

    /// Open files with the O_DIRECT flag for performance.
    #[structopt(long)]
    pub o_direct: bool,
}

/// Open a file for reading.
pub fn open(path: &Path, o_direct: bool) -> std::io::Result<File> {
    if o_direct {
        OpenOptions::new()
            .read(true)
            // see man 2 open, search for O_DIRECT
            .custom_flags(libc::O_DIRECT)
            .open(path)
    } else {
        File::open(path)
    }
}
