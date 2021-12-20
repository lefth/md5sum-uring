use std::{fs::File, io::Read, path::PathBuf};

use anyhow::Result;
use md5::{Digest, Md5};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opt {
    #[structopt()]
    pub files: Vec<PathBuf>,
}

fn get_checksums(files: Vec<PathBuf>) -> Result<Vec<(PathBuf, Result<Md5>)>> {
    Ok(files
        .into_iter()
        .map(|path| {
            let result = (|| {
                let mut file = File::open(&path)?;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)?;
                let mut md5 = Md5::new();
                md5.update(&buf);
                Ok(md5)
            })();
            (path, result)
        })
        .collect())
}

fn main() -> Result<()> {
    let options = Opt::from_args();
    for (path, result) in get_checksums(options.files)? {
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
    Ok(())
}
