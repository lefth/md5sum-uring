/// This module uses calculates checksums without io_uring.
use std::{fs::File, io::Read, path::PathBuf, sync::mpsc::Sender};

use anyhow::Result;
use md5::{Digest, Md5};

pub fn get_checksums(files: Vec<PathBuf>, tx: Sender<(PathBuf, Result<Md5>)>) -> Result<()> {
    for path in files {
        let result = (|| {
            let mut file = File::open(&path)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let mut md5 = Md5::new();
            md5.update(&buf);
            Ok(md5)
        })();
        tx.send((path, result))?;
    }
    Ok(())
}
