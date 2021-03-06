use std::{
    fs::{File, OpenOptions},
    ops::{Deref, DerefMut},
    os::unix::prelude::OpenOptionsExt,
    path::{Path, PathBuf},
    slice,
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
pub const ALIGNMENT: usize = 4096;

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
    #[structopt(long, conflicts_with_all = &["pre-register-files", "use-fixed-buffers", "o-direct"])]
    pub no_uring: bool,

    /// Open files with the O_DIRECT flag for performance.
    #[structopt(long)]
    pub o_direct: bool,
}

#[repr(C, align(4096))]
#[derive(std::fmt::Debug)]
/// Aligned buffer. Put this in a box to avoid overfilling the stack.
pub struct AlignedBuffer {
    buf: [u8; MAX_READ_SIZE],
    // A lot of space is wasted by storing such a small value with 4096 byte alignment:
    len: usize,
}

impl AlignedBuffer {
    pub fn new() -> AlignedBuffer {
        AlignedBuffer {
            buf: [0u8; MAX_READ_SIZE],
            len: MAX_READ_SIZE,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// Panics if the size is over the capacity (the default size).
    pub fn resize(&mut self, len: usize) {
        assert!(
            len <= MAX_READ_SIZE,
            "Cannot resize buffer to {} bytes--larger than the full allocated region: {}",
            len,
            MAX_READ_SIZE
        );
        self.len = len;
    }
}

impl Default for AlignedBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<[u8]> for AlignedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buf[0..self.len]
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // unsafe: copied from Vec for performance:
        unsafe { slice::from_raw_parts(self.buf.as_ptr(), self.len) }
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // unsafe: copied from Vec for performance:
        unsafe { slice::from_raw_parts_mut(self.buf.as_mut_ptr(), self.len) }
    }
}

/// Open a file for reading. Note that O_DIRECT seems not to work on some systems like
/// WSL2.
pub fn open(path: impl AsRef<Path>, o_direct: bool) -> std::io::Result<File> {
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
        fs::OpenOptions,
        io::{ErrorKind, Read, Write},
        mem::align_of,
        path::PathBuf,
        sync::{
            mpsc::{channel, Sender},
            Mutex,
        },
    };

    use anyhow::Result;
    #[allow(unused_imports)]
    use log::{debug, error, info, trace, warn};
    use md5::{Digest, Md5};
    use structopt::lazy_static::lazy_static;
    use structopt::StructOpt;

    use crate::{
        open, simple_uring, with_fixed_buffers, with_register_files, without_uring, AlignedBuffer,
        Opt, ALIGNMENT, MAX_READ_SIZE,
    };

    fn setup() {
        // Try init because multiple tests may invoke this:
        let _ = env_logger::try_init();
    }

    fn file_setup() -> Result<HashMap<PathBuf, [u8; 16]>> {
        lazy_static! {
            static ref CHECKSUMS: Mutex<HashMap<PathBuf, [u8; 16]>> = Default::default();
        };

        let mut mutex_guard = CHECKSUMS.lock().unwrap();

        setup();

        let checksums: &mut HashMap<_, _> = &mut mutex_guard;
        if checksums.len() > 0 {
            // Don't create/modify the files twice
            return Ok(checksums.clone());
        }

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

        for size in [
            25,
            4096,
            MAX_READ_SIZE - 1,
            MAX_READ_SIZE,
            MAX_READ_SIZE + 1,
            MAX_READ_SIZE * 3,
        ] {
            let fname = PathBuf::from(format!("test/file-{}", size));

            let data = iter.take(size).collect::<Vec<_>>();
            hasher.update(&data);
            let checksum: [u8; 16] = hasher.finalize_reset().try_into()?;
            assert!(checksums.insert(fname.clone(), checksum).is_none());

            let file = OpenOptions::new()
                .write(true)
                .read(false)
                // fail on existing:
                .create_new(true)
                .open(&fname);

            if matches!(&file, Err(err) if err.kind() == ErrorKind::AlreadyExists) {
                // Skip files that already exist. They should already have the same contents,
                // and overwriting them is a security risk since some of these tests need to
                // run as root:
                continue;
            }

            file?.write(&data)?;
        }

        Ok(checksums.clone())
    }

    fn assert_checksums<F>(get_checksums: F, o_direct: bool) -> Result<()>
    where
        F: Fn(Vec<PathBuf>, Sender<(PathBuf, Result<Md5>)>, bool) -> Result<()> + Sync + 'static,
    {
        let checksums = file_setup()?;

        let (tx, rx) = channel();
        crossbeam_utils::thread::scope(|s| -> Result<()> {
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

    #[test]
    /// This will fail on WSL2 and networked files.
    fn test_simplest_o_direct() -> Result<()> {
        setup();
        let _ = file_setup()?;
        let len = 25;
        let expected_contents = "0000000000111111111122222";

        let mut file = open("test/file-25", true)?;
        let mut buf: Box<AlignedBuffer> = Default::default();
        buf.resize(len);
        file.read_exact(&mut buf)?;
        let data = String::from_utf8_lossy(&buf);
        trace!("Read file: {}", data);
        assert_eq!(data, expected_contents);

        Ok(())
    }

    #[test]
    fn test_arguments() {
        setup();

        assert!(
            matches!(
                Opt::from_iter_safe(&["", "--o-direct", "--no-uring"]),
                Err(_)
            ),
            "--o-direct and --no-uring should be an illegal combination."
        );
    }

    #[test]
    fn test_alignment() {
        setup();
        let mut buf = Box::new(AlignedBuffer::new());
        let ptr = buf.as_mut_ptr();
        assert_eq!(ptr as usize % ALIGNMENT, 0);

        let mut buf = Box::pin(AlignedBuffer::new());
        let ptr = buf.as_mut_ptr();
        assert_eq!(ptr as usize % ALIGNMENT, 0);

        assert_eq!(
            align_of::<AlignedBuffer>(),
            ALIGNMENT,
            "Aligned buffer size is {}, should be {}.",
            align_of::<AlignedBuffer>(),
            ALIGNMENT
        );
    }
}
