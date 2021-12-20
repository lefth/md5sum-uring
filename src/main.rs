use std::{
    cmp::min,
    fs::File,
    os::unix::io::AsRawFd,
    path::{Path, PathBuf},
    sync::mpsc::{channel, Sender},
    thread,
};

use anyhow::Result;
use io_uring::{opcode, types, IoUring};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use md5::{Digest, Md5};
use structopt::StructOpt;

const RING_SIZE: usize = 16;
const NO_BUFFER: Option<Buffer> = None;
const MAX_READ_SIZE: usize = 1024 * 16;

#[derive(StructOpt)]
struct Opt {
    #[structopt()]
    pub files: Vec<PathBuf>,
}

/// This struct holds the state and buffers of a file that's being read, particularly
/// when one read finishes but more reads are required to finish the file.
struct Buffer {
    pub path: PathBuf,
    pub fd: File,
    file_len: u64,
    pub buf: Vec<u8>,
    /// How many bytes have been read
    pub position: usize,
    /// The md5 state is updated as more bytes are read
    ctx: Md5,
}

impl Buffer {
    pub fn new(path: &Path) -> Result<Buffer> {
        let fd = File::open(path)?;
        let file_len = fd.metadata()?.len();
        let mut ret = Buffer {
            path: path.to_owned(),
            fd,
            file_len,
            buf: Vec::new(),
            position: 0,
            ctx: Md5::new(),
        };
        ret.set_buffer_size();
        Ok(ret)
    }

    /// Reset the buffer size, useful whenever the read position changes.
    pub fn set_buffer_size(&mut self) {
        let needed_bytes = min(self.file_len as usize - self.position, MAX_READ_SIZE);
        trace!(
            "Set the buffer size to {} because we read {} of a {} file.",
            needed_bytes,
            self.position,
            self.file_len
        );
        self.buf.resize(needed_bytes, 0);
    }
}

/// Get all checksums and send the results through a channel.
fn get_checksums(files: Vec<PathBuf>, tx: Sender<(PathBuf, Result<Md5>)>) -> Result<()> {
    // Set up shared state that's applicable to all individual reads or for choosing what to read:
    let mut shared_buffers: [Option<Buffer>; RING_SIZE] = [NO_BUFFER; RING_SIZE];
    let mut ring = IoUring::new(RING_SIZE as u32)?;
    let mut files = files.into_iter().peekable();

    loop {
        let mut new_work_queued = false;

        // Only proceed if there's both a free index and a file:
        // TODO: manually queue free indices
        while let Some(free_idx) = shared_buffers
            .iter()
            .enumerate()
            .filter_map(|(idx, elem)| if elem.is_none() { Some(idx) } else { None })
            .next()
        {
            debug_assert!(
                !ring.submission().is_full(),
                "Submission queue must have a free spot if there's a free shared buffer",
            );

            if let Some(ref path) = files.next() {
                // Queue a read with this file:
                let partial_buffer = match Buffer::new(path) {
                    Ok(partial_buffer) => partial_buffer,
                    Err(err) => {
                        tx.send((path.to_owned(), Err(err))).unwrap();
                        continue;
                    }
                };

                // Put the buffer into the array so it will have a constant location until it's removed
                // after being populated:
                shared_buffers[free_idx].replace(partial_buffer);
                // TODO: write this more nicely, testing that there is no move.
                let buffer_ref = shared_buffers[free_idx].as_mut().unwrap();
                submit_for_read(&mut ring, buffer_ref, free_idx);

                new_work_queued = true;
            } else {
                break;
            }
        }

        if new_work_queued || files.peek().is_some() {
            if files.peek().is_some() {
                debug_assert!(
                    shared_buffers.iter().all(|elem| elem.is_some()),
                    "We should have filled all the slots"
                )
            }

            // Wait for a result since the jobs list is full or we just added something
            trace!("Waiting for / handling a result");
            submit_wait_and_handle_result(&mut ring, &mut shared_buffers, &tx)?;
        } else {
            // There's no more work that can be added right now, but we still need to handle any
            // active buffers
            while shared_buffers.iter().any(|elem| elem.is_some()) {
                trace!("Did not submit work, waiting for old work");
                submit_wait_and_handle_result(&mut ring, &mut shared_buffers, &tx)?;
            }
            break;
        }
    }

    Ok(())
}

fn submit_wait_and_handle_result(
    ring: &mut IoUring,
    shared_buffers: &mut [Option<Buffer>; RING_SIZE],
    tx: &Sender<(PathBuf, Result<md5::Md5, anyhow::Error>)>,
) -> Result<()> {
    ring.submit_and_wait(1)?;
    let completed_idx = ring
        .completion()
        .next()
        .expect("completion queue is empty")
        .user_data() as usize;

    // Next, consume and handle bytes in the buffer:
    let mut buffer = shared_buffers[completed_idx]
        .as_mut()
        .expect("should exist because we chose its index");

    buffer.position += buffer.buf.len();

    trace!(
        "Incorporating {} bytes into checksum. Finished?: {} ({:?})",
        buffer.buf.len(),
        buffer.position as u64 + buffer.buf.len() as u64 == buffer.file_len,
        &buffer.path,
    );
    buffer.ctx.update(&buffer.buf);
    buffer.set_buffer_size();
    if buffer.buf.len() == 0 {
        // It's finished, so free the slot (and get an owned object):
        let buffer = shared_buffers[completed_idx].take().unwrap();
        tx.send((buffer.path, Ok(buffer.ctx))).unwrap();
    } else {
        trace!("Checksum not finished, resubmitting for read");
        submit_for_read(
            ring,
            shared_buffers[completed_idx].as_mut().unwrap(),
            completed_idx,
        );
    }

    Ok(())
}

/// Put a job in the read queue and submit it to the kernel. The buffer struct tracks
/// how much has been read already and how much more is needed.
fn submit_for_read(ring: &mut IoUring, buffer_ref: &mut Buffer, idx: usize) {
    // get data uring needs to queue a read:
    let raw_fd = buffer_ref.fd.as_raw_fd();
    let buf = &mut buffer_ref.buf;
    let read_e = opcode::Read::new(types::Fd(raw_fd), buf.as_mut_ptr(), buf.len() as _)
        .offset(buffer_ref.position as i64)
        .build()
        .user_data(idx as u64);

    unsafe {
        ring.submission()
            .push(&read_e)
            .expect("submission queue is full");
    }
}

fn main() -> Result<()> {
    env_logger::init();

    let options = Opt::from_args();
    let (tx, rx) = channel();

    let handle = thread::spawn(|| get_checksums(options.files, tx));

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
