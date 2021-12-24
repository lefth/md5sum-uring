/// This module uses calculates checksums without io_uring.
use std::{fs::File, path::PathBuf, sync::mpsc::Sender};

use anyhow::Result;
use md5::{Digest, Md5};
use memmap2::MmapOptions;

pub fn get_checksums(files: Vec<PathBuf>, tx: Sender<(PathBuf, Result<Md5>)>) -> Result<()> {
    for path in files {
        let result = (|| {
            let file = File::open(&path)?;
            let mut md5 = Md5::new();
            let mmap = unsafe { MmapOptions::new().map(&file)? };
            md5.update(&mmap);
            Ok(md5)
        })();
        tx.send((path, result))?;
    }
    Ok(())
}
