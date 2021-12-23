use std::{sync::mpsc::channel, thread};

use anyhow::Result;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use md5::Digest;
use structopt::StructOpt;

use md5sum_uring::*;

mod simple_uring;
mod with_fixed_buffers;
mod with_register_files;
mod without_uring;

fn main() -> Result<()> {
    env_logger::init();

    let options = Opt::from_args();

    let (tx, rx) = channel();

    let handle = thread::spawn(move || {
        if options.no_uring {
            without_uring::get_checksums(options.files, tx)
        } else if options.use_fixed_buffers {
            if !options.preregister_files {
                warn!("Fixed buffers without preregistered files is not implemented. Using preregistered files.");
            }
            with_fixed_buffers::get_checksums(options.files, tx)
        } else if options.preregister_files {
            with_register_files::get_checksums(options.files, tx)
        } else {
            simple_uring::get_checksums(options.files, tx)
        }
    });

    for (path, result) in rx {
        let path = path.to_string_lossy();
        match result {
            Ok(checksum) => {
                println!("{:x}  {}", checksum.finalize(), path);
            }
            Err(err) => {
                eprintln!("{}: {}", path, err);
            }
        }
    }

    handle.join().unwrap()?;
    Ok(())
}
