use std::{
    fs::{File, OpenOptions},
    os::unix::prelude::OpenOptionsExt,
    path::{Path, PathBuf},
};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use structopt::StructOpt;

pub mod simple_uring;
pub mod with_fixed_buffers;
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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::Write,
        path::PathBuf,
        sync::mpsc::{channel, Sender},
    };

    use anyhow::Result;
    #[allow(unused_imports)]
    use log::{debug, error, info, trace, warn};
    use md5::{Digest, Md5};

    use crate::{
        simple_uring, with_fixed_buffers, with_register_files, without_uring, MAX_READ_SIZE,
    };

    fn setup() {
        // Try init because multiple tests may invoke this:
        let _ = env_logger::try_init();
    }

    fn file_setup() -> Result<HashMap<PathBuf, [u8; 16]>> {
        match std::fs::create_dir("test") {
            Ok(_) => Ok(()),
            Err(err) if err.raw_os_error() == Some(17) => Ok(()), // directory exists; okay
            Err(err) => Err(err),
        }?;

        // Repeat characters in a pattern that's easy to read/debug:
        let iter = &mut std::iter::repeat_with(|| {
            ('0' as u8..'f' as u8)
                .map(|character| std::iter::repeat(character).take(10))
                .flatten()
        })
        .flatten();

        let mut hasher = Md5::new();
        let mut checksums = HashMap::new();

        for size in [
            25,
            4096,
            MAX_READ_SIZE - 1,
            MAX_READ_SIZE,
            MAX_READ_SIZE + 1,
            MAX_READ_SIZE * 3,
        ] {
            let fname = PathBuf::from(format!("test/file-{}", size));
            let mut file = std::fs::File::create(&fname)?;
            let data = iter.take(size).collect::<Vec<_>>();
            file.write(&data)?;
            hasher.update(&data);
            let checksum: [u8; 16] = hasher.finalize_reset().try_into()?;
            assert!(checksums.insert(fname, checksum).is_none());
        }

        Ok(checksums)
    }

    fn assert_checksums<F>(get_checksums: F, o_direct: bool) -> Result<()>
    where
        F: Fn(Vec<PathBuf>, Sender<(PathBuf, Result<Md5>)>, bool) -> Result<()> + Sync + 'static,
    {
        let checksums = file_setup()?;

        let (tx, rx) = channel();
        crossbeam::scope(|s| -> Result<()> {
            let handle = s.spawn(|_| -> Result<()> {
                get_checksums(checksums.keys().cloned().collect(), tx, o_direct)?;
                Ok(())
            });

            for (path, result) in rx {
                let checksum: [u8; 16] = result?.finalize().try_into()?;
                assert_eq!(checksums.get(&path).unwrap(), &checksum);
            }
            handle.join().unwrap()?;
            Ok(())
        })
        .unwrap()?;

        Ok(())
    }

    #[test]
    fn test_without_uring() -> Result<()> {
        setup();
        assert_checksums(without_uring::get_checksums, false)?;
        Ok(())
    }

    #[test]
    fn test_without_uring_o_direct() -> Result<()> {
        setup();
        assert_checksums(without_uring::get_checksums, true)?;
        Ok(())
    }

    #[test]
    fn test_simple_uring() -> Result<()> {
        setup();
        assert_checksums(simple_uring::get_checksums, false)?;
        Ok(())
    }

    #[test]
    fn test_simple_uring_o_direct() -> Result<()> {
        setup();
        assert_checksums(simple_uring::get_checksums, true)?;
        Ok(())
    }

    #[test]
    fn test_preregistered_files() -> Result<()> {
        setup();
        assert_checksums(with_register_files::get_checksums, false)?;
        Ok(())
    }

    #[test]
    fn test_preregistered_files_o_direct() -> Result<()> {
        setup();
        assert_checksums(with_register_files::get_checksums, true)?;
        Ok(())
    }

    #[test]
    fn test_fixed_buffers() -> Result<()> {
        setup();
        assert_checksums(with_fixed_buffers::get_checksums, false)?;
        Ok(())
    }

    #[test]
    fn test_fixed_buffers_o_direct() -> Result<()> {
        setup();
        assert_checksums(with_fixed_buffers::get_checksums, true)?;
        Ok(())
    }
}
