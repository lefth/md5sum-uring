/// This module pre-registers files and buffers with io_uring before the reads start.
use std::{
    cell::{RefCell, RefMut},
    cmp::min,
    fs::File,
    os::unix::io::AsRawFd,
    path::{Path, PathBuf},
    sync::{mpsc::Sender, Arc},
};

use anyhow::{bail, Result};
use io_uring::{opcode, types, IoUring, Probe};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use md5::{Digest, Md5};

use crate::*;

const READSTATE_NONE: Option<ReadState> = None;

/// This struct holds the state of a file that's being read, particularly
/// when one read finishes but more reads are required to finish the file.
/// This struct is called "Buffer" in other modules, but in this case the buffer
/// needs to be stored separately.
struct ReadState {
    pub path: PathBuf,
    pub fd: File,
    file_len: u64,
    /// How many bytes have been read
    pub position: usize,
    /// The md5 state is updated as more bytes are read
    ctx: Md5,
    pub file_idx: u32,
    pub buf: Option<Arc<RefCell<Vec<u8>>>>,
    pub buf_idx: Option<u16>,
}

impl ReadState {
    pub fn new(path: &Path, file_idx: u32) -> Result<ReadState> {
        let fd = File::open(path)?;
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

    /// Get ready to read file data into a buffer.
    fn initialize(&mut self, buf: Arc<RefCell<Vec<u8>>>, buf_idx: u16) {
        self.buf_idx.replace(buf_idx);
        {
            let mut buf = buf.borrow_mut();
            self.set_buffer_size(&mut buf);
        }
        self.buf.replace(buf);
    }

    /// Reset the buffer size, useful whenever the read position changes.
    /// Returns whether the file has been fully read.
    pub fn set_buffer_size(&mut self, buf: &mut RefMut<Vec<u8>>) -> bool {
        let needed_bytes = min(self.file_len as usize - self.position, MAX_READ_SIZE);
        trace!(
            "Set the buffer size to {} because we read {} of a {} byte file.",
            needed_bytes,
            self.position,
            self.file_len
        );
        buf.resize(needed_bytes, 0);

        needed_bytes == 0
    }

    /// Returns whether the file has been fully read.
    pub(crate) fn update(&mut self) -> bool {
        let buf = self.buf.take().unwrap();
        let finished = {
            let mut buf = buf.borrow_mut();
            self.ctx.update(&buf[..]);
            self.position += buf.len();
            self.set_buffer_size(&mut buf)
        };
        self.buf.replace(buf);

        finished
    }
}

/// Get all checksums and send the results through a channel.
pub fn get_checksums(files: Vec<PathBuf>, tx: Sender<(PathBuf, Result<Md5>)>) -> Result<()> {
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
    let mut read_states: [Option<ReadState>; RING_SIZE] = [READSTATE_NONE; RING_SIZE];
    let shared_buffers: [Arc<RefCell<Vec<u8>>>; RING_SIZE] = (0..RING_SIZE)
        .map(|_| Arc::new(RefCell::new(vec![0u8; MAX_READ_SIZE])))
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    let mut free_index_list: Vec<_> = (0..RING_SIZE).into_iter().collect();
    let mut raw_fds = Vec::new();
    let mut files = files
        .into_iter()
        .filter_map(|path| match ReadState::new(&path, file_idx) {
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

        let buffers = shared_buffers
            .iter()
            .map(|buffer| {
                let mut buffer = buffer.borrow_mut();
                let buffer_ptr = buffer.as_mut_ptr() as *mut _;
                libc::iovec {
                    iov_base: buffer_ptr,
                    iov_len: buffer.len(),
                }
            })
            .collect::<Vec<_>>();

        if let Err(err) = ring.submitter().register_buffers(&buffers) {
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
                state.initialize(shared_buffers[free_idx].clone(), free_idx as u16);
                read_states[free_idx].replace(state);
                debug_assert_eq!(
                    free_index_list.len(),
                    read_states.iter().filter(|elem| elem.is_none()).count(),
                    "The free index list is out of sync with the work read states (1)"
                );
                let read_state_ref = read_states[free_idx].as_mut().unwrap();
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
            submit_wait_and_handle_result(&mut ring, &mut read_states, &tx, &mut free_index_list)?;
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
                )?;
            }
            break;
        }
    }

    Ok(())
}

fn submit_wait_and_handle_result(
    ring: &mut IoUring,
    read_states: &mut [Option<ReadState>; RING_SIZE],
    tx: &Sender<(PathBuf, Result<md5::Md5, anyhow::Error>)>,
    free_index_list: &mut Vec<usize>,
) -> Result<()> {
    debug_assert_eq!(
        free_index_list.len(),
        read_states.iter().filter(|elem| elem.is_none()).count(),
        "The free index list is out of sync with the read states (2)"
    );

    ring.submit_and_wait(1)?;
    let completed_idx = ring
        .completion()
        .next()
        .expect("completion queue is empty")
        .user_data() as usize;

    // Next, consume and handle bytes in the buffer:
    let read_state = read_states[completed_idx]
        .as_mut()
        .expect("should exist because we chose its index");

    let finished = read_state.update();
    trace!(
        "Incorporated bytes into checksum. Finished?: {} ({:?})",
        finished,
        &read_state.path,
    );
    if finished {
        // It's finished, so free the slot (and get an owned object):
        let read_state = read_states[completed_idx].take().unwrap();
        free_index_list.push(completed_idx);
        debug_assert_eq!(
            free_index_list.len(),
            read_states.iter().filter(|elem| elem.is_none()).count(),
            "The free index list is out of sync with the read states (3)"
        );
        tx.send((read_state.path, Ok(read_state.ctx))).unwrap();
    } else {
        trace!("Checksum not finished, resubmitting for read");
        submit_for_read(
            ring,
            read_states[completed_idx].as_mut().unwrap(),
            completed_idx,
        );
    }

    Ok(())
}

/// Put a job in the read queue and submit it to the kernel. The read state struct tracks
/// how much has been read already and how much more is needed.
fn submit_for_read(ring: &mut IoUring, read_state_ref: &mut ReadState, idx: usize) {
    // get data uring needs to queue a read:
    let mut buf = read_state_ref.buf.as_ref().unwrap().borrow_mut();
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
