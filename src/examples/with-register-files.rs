use std::{
    cmp::min,
    fs::File,
    os::unix::io::AsRawFd,
    path::{Path, PathBuf},
    sync::mpsc::{channel, Sender},
    thread,
};

use anyhow::Result;
use io_uring::{opcode, types, IoUring, Probe};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use md5::{Digest, Md5};
use structopt::StructOpt;

const RING_SIZE: usize = 16;
const NO_BUFFER: Option<Buffer> = None;
const MAX_READ_SIZE: usize = 4096 * 16;

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
    pub file_idx: u32,
}

impl Buffer {
    pub fn new(path: &Path, file_idx: u32) -> Result<Buffer> {
        let fd = File::open(path)?;
        let file_len = fd.metadata()?.len();
        let mut ret = Buffer {
            path: path.to_owned(),
            fd,
            file_len,
            buf: Vec::new(),
            position: 0,
            ctx: Md5::new(),
            file_idx,
        };
        ret.set_buffer_size();
        Ok(ret)
    }

    /// Reset the buffer size, useful whenever the read position changes.
    pub fn set_buffer_size(&mut self) {
        let needed_bytes = min(self.file_len as usize - self.position, MAX_READ_SIZE);
        trace!(
            "Set the buffer size to {} because we read {} of a {} byte file.",
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
    let mut ring = IoUring::new(RING_SIZE as u32)?;
    let mut probe = Probe::new();
    ring.submitter().register_probe(&mut probe)?;
    if !probe.is_supported(opcode::Read::CODE) {
        panic!("Reading files is not supported. Try a newer kernel.");
    }
    // XXX: opcode::sys::IORING_REGISTER_FILES is private:
    if !probe.is_supported(2) {
        panic!("Registering files is not supported. Try a newer kernel.");
    }

    let mut file_idx = 0;
    let mut shared_buffers: [Option<Buffer>; RING_SIZE] = [NO_BUFFER; RING_SIZE];
    let mut free_index_list: Vec<_> = (0..RING_SIZE).into_iter().collect();
    let mut raw_fds = Vec::new();
    let mut files = files
        .into_iter()
        .filter_map(|path| match Buffer::new(&path, file_idx) {
            Ok(buffer) => {
                file_idx += 1;
                raw_fds.push(buffer.fd.as_raw_fd());
                Some(buffer)
            }
            Err(err) => {
                tx.send((path.to_owned(), Err(err))).unwrap();
                None
            }
        })
        // Reverse so we can pop the first files off the end
        .rev()
        .collect::<Vec<_>>();
    ring.submitter().register_files(&raw_fds)?;

    loop {
        let mut new_work_queued = false;

        // Only proceed if there's both a free index and a file:
        while let Some(free_idx) = free_index_list.pop() {
            debug_assert!(
                !ring.submission().is_full(),
                "Submission queue must have a free spot if there's a free shared buffer",
            );

            if let Some(buffer) = files.pop() {
                // Put the buffer into the array so it will have a constant location until it's removed
                // after being populated:
                shared_buffers[free_idx].replace(buffer);
                debug_assert_eq!(
                    free_index_list.len(),
                    shared_buffers.iter().filter(|elem| elem.is_none()).count(),
                    "The free index list is out of sync with the work buffers (1)"
                );
                let buffer_ref = shared_buffers[free_idx].as_mut().unwrap();
                new_work_queued = true;
                submit_for_read(&mut ring, buffer_ref, free_idx);
            } else {
                // We didn't use this buffer index
                free_index_list.push(free_idx);
                break;
            }
        }

        if new_work_queued || files.len() > 0 {
            if files.len() > 0 {
                debug_assert_eq!(
                    free_index_list.len(),
                    0,
                    "We should have filled all the slots"
                );
            }

            // Wait for a result since the jobs list is full or we just added something
            trace!("Waiting for / handling a result");
            submit_wait_and_handle_result(
                &mut ring,
                &mut shared_buffers,
                &tx,
                &mut free_index_list,
            )?;
        } else {
            // There's no more work that can be added right now, but we still need to handle any
            // active buffers
            while free_index_list.len() < RING_SIZE {
                trace!(
                    "Did not submit work, waiting for old work. {}/{} free indices",
                    free_index_list.len(),
                    RING_SIZE
                );
                submit_wait_and_handle_result(
                    &mut ring,
                    &mut shared_buffers,
                    &tx,
                    &mut free_index_list,
                )?;
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
    free_index_list: &mut Vec<usize>,
) -> Result<()> {
    debug_assert_eq!(
        free_index_list.len(),
        shared_buffers.iter().filter(|elem| elem.is_none()).count(),
        "The free index list is out of sync with the work buffers (2)"
    );

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
        free_index_list.push(completed_idx);
        debug_assert_eq!(
            free_index_list.len(),
            shared_buffers.iter().filter(|elem| elem.is_none()).count(),
            "The free index list is out of sync with the work buffers (3)"
        );
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
    let buf = &mut buffer_ref.buf;
    let read_e = opcode::Read::new(
        types::Fixed(buffer_ref.file_idx),
        buf.as_mut_ptr(),
        buf.len() as _,
    )
    .offset(buffer_ref.position as i64)
    .build()
    .user_data(idx as u64);

    unsafe {
        ring.submission()
            .push(&read_e)
            .expect("submission queue is full");
    }
}

