// This module pre-registers files and buffers with io_uring before the reads start.
use std::{
    cmp::min,
    fs::File,
    hash::BuildHasherDefault,
    os::unix::io::AsRawFd,
    path::{Path, PathBuf},
    pin::Pin,
    sync::mpsc::Sender,
};

use anyhow::{bail, Result};
use io_uring::{opcode, types, IoUring, Probe};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use md5::{Digest, Md5};
use nohash_hasher::NoHashHasher;

use crate::*;

type HashMap<K, V> = std::collections::HashMap<K, V, BuildHasherDefault<NoHashHasher<K>>>;

/// This struct holds the state of a file that's being read, particularly
/// when one read finishes but more reads are required to finish the file.
/// This struct is called "Buffer" in other modules, but in this case the buffer
/// needs to be stored separately.
struct ReadState {
    pub path: PathBuf,
    pub fd: File,
    file_len: u64,
    /// How many bytes have been read
    pub position: u64,
    /// The md5 state is updated as more bytes are read
    ctx: Md5,
    pub file_idx: u32,
    pub buf: Option<Pin<Box<AlignedBuffer>>>,
    pub buf_idx: Option<u16>,
}

impl ReadState {
    pub fn new(path: &Path, file_idx: u32, o_direct: bool) -> Result<ReadState> {
        let fd = open(path, o_direct)?;
        let file_len = fd.metadata()?.len();
        Ok(ReadState {
            path: path.to_owned(),
            fd,
            file_len,
            position: 0,
            ctx: Md5::new(),
            file_idx,
            buf: None,
            buf_idx: None,
        })
    }

    /// Get ready to read file data into a buffer. This takes ownership of the buffer
    /// and free index.
    fn initialize(&mut self, mut buf: Pin<Box<AlignedBuffer>>, buf_idx: u16) {
        self.buf_idx.replace(buf_idx);
        Self::set_buffer_size(&mut buf, self.file_len, self.position);
        self.buf.replace(buf);
    }

    /// Reset the buffer size, useful whenever the read position changes.
    /// Returns whether the file has been fully read.
    pub fn set_buffer_size(buf: &mut AlignedBuffer, file_len: u64, position: u64) -> bool {
        let needed_bytes = min(file_len - position, MAX_READ_SIZE as u64);
        trace!(
            "Set the buffer size to {} because we read {} of a {} byte file.",
            needed_bytes,
            position,
            file_len
        );
        buf.resize(needed_bytes as usize);

        needed_bytes == 0
    }

    /// Returns whether the file has been fully read.
    pub(crate) fn update(&mut self) -> bool {
        let mut buf = self.buf.as_mut().unwrap();
        self.ctx.update(&buf[..]);
        self.position += buf.len() as u64;
        let finished = Self::set_buffer_size(&mut buf, self.file_len, self.position);

        finished
    }
}

/// Get all checksums and send the results through a channel.
pub fn get_checksums(
    files: Vec<PathBuf>,
    tx: Sender<(PathBuf, Result<Md5>)>,
    o_direct: bool,
) -> Result<()> {
    // Set up shared state that's applicable to all individual reads or for choosing what to read:
    let mut ring = IoUring::new(RING_SIZE as u32)?;
    let mut probe = Probe::new();
    ring.submitter().register_probe(&mut probe)?;
    if !probe.is_supported(opcode::Read::CODE) {
        bail!("Reading files is not supported. Try a newer kernel.");
    }
    // opcode::sys::IORING_REGISTER_FILES is private, so just use its number "2"
    if !probe.is_supported(2) {
        bail!("Registering files is not supported. Try a newer kernel.");
    }
    if !probe.is_supported(opcode::ReadFixed::CODE) {
        bail!("Reading into fixed buffers is not supported. Try a newer kernel.");
    }

    let mut file_idx = 0;
    let mut read_states: HashMap<usize, ReadState> = Default::default();
    let mut shared_buffers: HashMap<usize, Pin<Box<AlignedBuffer>>> = Default::default();
    let mut iovecs: Vec<libc::iovec> = Vec::new();
    for i in 0..RING_SIZE {
        let mut buffer: Pin<Box<AlignedBuffer>> = Box::pin(Default::default());
        let buffer_ptr = buffer.as_mut().as_mut_ptr();
        iovecs.push(libc::iovec {
            iov_base: buffer_ptr as *mut _,
            iov_len: buffer.len(),
        });
        shared_buffers.insert(i, buffer);
    }

    let mut free_index_list: Vec<_> = (0..RING_SIZE).into_iter().collect();
    let mut raw_fds = Vec::new();
    let mut files = files
        .into_iter()
        .filter_map(|path| match ReadState::new(&path, file_idx, o_direct) {
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

    if raw_fds.len() > 0 {
        ring.submitter().register_files(&raw_fds)?;

        if let Err(err) = ring.submitter().register_buffers(&iovecs) {
            bail!(
                "Failed to register fixed buffers (are you running without root?): {}",
                err
            );
        }
    }

    loop {
        let mut new_work_queued = false;

        // Only proceed if there's both a free index and a file:
        while let Some(free_idx) = free_index_list.pop() {
            debug_assert!(
                !ring.submission().is_full(),
                "Submission queue must have a free spot if there's a free read state slot",
            );

            if let Some(mut state) = files.pop() {
                state.initialize(shared_buffers.remove(&free_idx).unwrap(), free_idx as u16);
                read_states.insert(free_idx, state);
                debug_assert_eq!(
                    free_index_list.len(),
                    RING_SIZE - read_states.len(),
                    "The free index list is out of sync with the work read states (1)"
                );
                let read_state_ref = read_states.get_mut(&free_idx).unwrap();
                new_work_queued = true;
                submit_for_read(&mut ring, read_state_ref, free_idx);
            } else {
                // We didn't use this index
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
                &mut read_states,
                &tx,
                &mut free_index_list,
                &mut shared_buffers,
            )?;
        } else {
            // There's no more work that can be added right now, but we still need to handle any
            // active read states
            while free_index_list.len() < RING_SIZE {
                trace!(
                    "Did not submit work, waiting for old work. {}/{} free indices",
                    free_index_list.len(),
                    RING_SIZE
                );
                submit_wait_and_handle_result(
                    &mut ring,
                    &mut read_states,
                    &tx,
                    &mut free_index_list,
                    &mut shared_buffers,
                )?;
            }
            break;
        }
    }

    Ok(())
}

fn submit_wait_and_handle_result(
    ring: &mut IoUring,
    read_states: &mut HashMap<usize, ReadState>,
    tx: &Sender<(PathBuf, Result<md5::Md5, anyhow::Error>)>,
    free_index_list: &mut Vec<usize>,
    shared_buffers: &mut HashMap<usize, Pin<Box<AlignedBuffer>>>,
) -> Result<()> {
    debug_assert_eq!(
        free_index_list.len(),
        RING_SIZE - read_states.len(),
        "The free index list is out of sync with the read states (2)"
    );

    ring.submit_and_wait(1)?;
    let completed_idx = ring
        .completion()
        .next()
        .expect("completion queue is empty")
        .user_data() as usize;

    // Next, consume and handle bytes in the buffer:
    let read_state = read_states
        .get_mut(&completed_idx)
        .expect("should exist because we chose its index");

    let finished = read_state.update();
    trace!(
        "Incorporated bytes into checksum. Finished?: {} ({:?})",
        finished,
        &read_state.path,
    );
    if finished {
        // It's finished, so free the slot (and get an owned object):
        let mut read_state = read_states.remove(&completed_idx).unwrap();
        free_index_list.push(completed_idx);
        debug_assert_eq!(
            free_index_list.len(),
            RING_SIZE - read_states.len(),
            "The free index list is out of sync with the read states (3)"
        );
        // Also return the fixed buffer:
        shared_buffers.insert(completed_idx, read_state.buf.take().unwrap());

        tx.send((read_state.path, Ok(read_state.ctx))).unwrap();
    } else {
        trace!("Checksum not finished, resubmitting for read");
        submit_for_read(
            ring,
            read_states.get_mut(&completed_idx).unwrap(),
            completed_idx,
        );
    }

    Ok(())
}

/// Put a job in the read queue and submit it to the kernel. The read state struct tracks
/// how much has been read already and how much more is needed.
fn submit_for_read(ring: &mut IoUring, read_state_ref: &mut ReadState, idx: usize) {
    // get data uring needs to queue a read:
    let buf = read_state_ref.buf.as_mut().unwrap();
    let read_e = opcode::ReadFixed::new(
        types::Fixed(read_state_ref.file_idx),
        buf.as_mut_ptr(),
        buf.len() as _,
        read_state_ref.buf_idx.unwrap(),
    )
    .offset(read_state_ref.position as i64)
    .build()
    .user_data(idx as u64);

    unsafe {
        ring.submission()
            .push(&read_e)
            .expect("submission queue is full");
    }
}
