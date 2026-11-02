//! C ABI v1 for synchronous streaming. Every handle is single-consumer: calls
//! must not overlap, including close. Callbacks and their context must remain
//! valid until close and must never unwind. Nonempty buffers must be valid for
//! their declared length; output pointers must be writable and nonaliasing.
//! A read record is borrowed until the next reader call or close. Close discards
//! unfinished writers; callers must explicitly finish to flush the last block.

use shardpack_format::{
    BlockCodec, EncryptionKey, ReaderLimits, Record, ShardReader, ShardWriter, WriterOptions,
};
use std::{
    cell::RefCell,
    ffi::c_void,
    io::{Read, Write},
    panic::{AssertUnwindSafe, catch_unwind},
};

#[derive(Debug, snafu::Snafu)]
enum Error {
    #[snafu(display("{reason}"))]
    Invalid { reason: &'static str },
    #[snafu(display("{source}"))]
    Format { source: shardpack_format::Error },
}
impl From<shardpack_format::Error> for Error {
    fn from(source: shardpack_format::Error) -> Self {
        Self::Format { source }
    }
}
type Result<T> = std::result::Result<T, Error>;
thread_local! { static LAST_ERROR: RefCell<Vec<u8>> = RefCell::new(vec![0]); }

fn boundary(action: impl FnOnce() -> Result<i32>) -> i32 {
    let message = match catch_unwind(AssertUnwindSafe(action)) {
        Ok(Ok(status)) => return status,
        Ok(Err(error)) => error.to_string(),
        Err(_) => "native panic; close this handle".to_owned(),
    };
    LAST_ERROR.with(|slot| {
        let mut message = message.into_bytes();
        for byte in &mut message {
            if *byte == 0 {
                *byte = b'?';
            }
        }
        message.push(0);
        *slot.borrow_mut() = message;
    });
    -1
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct StreamCallbacks {
    pub context: *mut c_void,
    pub read: Option<unsafe extern "C" fn(*mut c_void, *mut u8, usize) -> isize>,
    pub write: Option<unsafe extern "C" fn(*mut c_void, *const u8, usize) -> isize>,
    pub flush: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
}
struct Stream(StreamCallbacks);
impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let callback = self
            .0
            .read
            .ok_or_else(|| std::io::Error::other("missing read callback"))?;
        // SAFETY: callback and context lifetime is the caller's ABI contract.
        let size = unsafe { callback(self.0.context, buf.as_mut_ptr(), buf.len()) };
        match usize::try_from(size) {
            Ok(size) if size <= buf.len() => Ok(size),
            _ => Err(std::io::Error::other("managed stream read failed")),
        }
    }
}
impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let callback = self
            .0
            .write
            .ok_or_else(|| std::io::Error::other("missing write callback"))?;
        // SAFETY: buffer stays alive throughout this synchronous callback.
        let size = unsafe { callback(self.0.context, buf.as_ptr(), buf.len()) };
        match usize::try_from(size) {
            Ok(size) if size <= buf.len() => Ok(size),
            _ => Err(std::io::Error::other("managed stream write failed")),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let callback = self
            .0
            .flush
            .ok_or_else(|| std::io::Error::other("missing flush callback"))?;
        // SAFETY: callback and context remain alive until the writer is closed.
        if unsafe { callback(self.0.context) } == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other("managed stream flush failed"))
        }
    }
}
pub struct ReaderHandle {
    reader: ShardReader<Stream>,
    current: Option<Record>,
    failed: bool,
}
pub struct WriterHandle {
    writer: Option<ShardWriter<Stream>>,
    failed: bool,
}

// SAFETY: callers provide either null (no encryption) or exactly 32 readable bytes.
unsafe fn key(pointer: *const u8) -> Option<EncryptionKey> {
    if pointer.is_null() {
        None
    } else {
        Some(EncryptionKey::new(
            unsafe { std::slice::from_raw_parts(pointer, 32) }
                .try_into()
                .unwrap(),
        ))
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn spk_abi_version() -> u32 {
    1
}

/// Thread-local UTF-8 message; valid until the next failed call on this thread.
#[unsafe(no_mangle)]
pub extern "C" fn spk_last_error() -> *const u8 {
    LAST_ERROR.with(|slot| slot.borrow().as_ptr())
}

/// # Safety
/// See module ABI contract. `key_bytes` is null or 32 bytes. `output` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spk_reader_open(
    callbacks: StreamCallbacks,
    key_bytes: *const u8,
    max_records: u32,
    max_stored: u64,
    max_raw: u64,
    output: *mut *mut ReaderHandle,
) -> i32 {
    boundary(|| {
        if output.is_null() {
            return Err(Error::Invalid {
                reason: "null reader output",
            });
        }
        unsafe {
            *output = std::ptr::null_mut();
        }
        if callbacks.read.is_none() {
            return Err(Error::Invalid {
                reason: "missing read callback",
            });
        }
        let reader = ShardReader::with_limits_and_key(
            Stream(callbacks),
            ReaderLimits {
                max_records_per_block: max_records,
                max_stored_body_bytes: max_stored,
                max_raw_payload_bytes: max_raw,
            },
            unsafe { key(key_bytes) },
        )?;
        unsafe {
            *output = Box::into_raw(Box::new(ReaderHandle {
                reader,
                current: None,
                failed: false,
            }));
        }
        Ok(0)
    })
}

/// Returns 0 for a record, 1 for EOF, -1 for error. Borrowed payload expires on next call.
/// # Safety
/// Handle must be live and exclusively held; all four output buffers must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spk_reader_next(
    handle: *mut ReaderHandle,
    sample_id: *mut u8,
    flags: *mut u32,
    payload: *mut *const u8,
    length: *mut usize,
) -> i32 {
    boundary(|| {
        if handle.is_null()
            || sample_id.is_null()
            || flags.is_null()
            || payload.is_null()
            || length.is_null()
        {
            return Err(Error::Invalid {
                reason: "null reader argument",
            });
        }
        let reader = unsafe { &mut *handle };
        if reader.failed {
            return Err(Error::Invalid {
                reason: "reader failed; close it",
            });
        }
        reader.failed = true;
        reader.current = None;
        unsafe {
            *payload = std::ptr::null();
            *length = 0;
        }
        reader.current = reader.reader.next_record()?;
        reader.failed = false;
        let Some(record) = &reader.current else {
            return Ok(1);
        };
        unsafe {
            std::ptr::copy_nonoverlapping(record.sample_id.as_ptr(), sample_id, 16);
            *flags = record.flags;
            *payload = record.payload.as_ptr();
            *length = record.payload.len();
        }
        Ok(0)
    })
}

/// # Safety
/// Handle is null or live, exclusively held, and closed exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spk_reader_close(handle: *mut ReaderHandle) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle) });
    }
}

/// # Safety
/// See module ABI contract. `key_bytes` is null or 32 bytes. `output` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spk_writer_open(
    callbacks: StreamCallbacks,
    key_bytes: *const u8,
    block_bytes: usize,
    codec: u8,
    level: i32,
    output: *mut *mut WriterHandle,
) -> i32 {
    boundary(|| {
        if output.is_null() {
            return Err(Error::Invalid {
                reason: "null writer output",
            });
        }
        unsafe {
            *output = std::ptr::null_mut();
        }
        if block_bytes == 0 || callbacks.write.is_none() || callbacks.flush.is_none() {
            return Err(Error::Invalid {
                reason: "invalid writer options or callbacks",
            });
        }
        let options = WriterOptions {
            target_block_bytes: block_bytes,
            codec: BlockCodec::try_from(codec)?,
            zstd_level: level,
        };
        let writer = match unsafe { key(key_bytes) } {
            Some(key) => ShardWriter::new_encrypted(Stream(callbacks), options, key)?,
            None => ShardWriter::new(Stream(callbacks), options)?,
        };
        unsafe {
            *output = Box::into_raw(Box::new(WriterHandle {
                writer: Some(writer),
                failed: false,
            }));
        }
        Ok(0)
    })
}

/// # Safety
/// Handle is live and exclusively held. ID points to 16 bytes; payload is readable
/// for `length` bytes (may be null only when length is zero).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spk_writer_add(
    handle: *mut WriterHandle,
    sample_id: *const u8,
    flags: u32,
    payload: *const u8,
    length: usize,
) -> i32 {
    boundary(|| {
        if handle.is_null()
            || sample_id.is_null()
            || (length != 0 && payload.is_null())
            || length > isize::MAX as usize
        {
            return Err(Error::Invalid {
                reason: "invalid writer buffer",
            });
        }
        let state = unsafe { &mut *handle };
        if state.failed {
            return Err(Error::Invalid {
                reason: "writer failed; close it",
            });
        }
        let writer = state.writer.as_mut().ok_or(Error::Invalid {
            reason: "writer already finished",
        })?;
        // Poison before calling into I/O so both errors and caught panics forbid retries.
        state.failed = true;
        let id = unsafe { std::slice::from_raw_parts(sample_id, 16) }
            .try_into()
            .unwrap();
        let body = if length == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(payload, length) }
        };
        writer.add_record(id, flags, body)?;
        state.failed = false;
        Ok(0)
    })
}

/// Summary is 116 bytes: hash[32], byte_size LE64, records LE64, blocks LE32,
/// fingerprint accumulator A[32], accumulator B[32].
/// # Safety
/// Handle is live and exclusively held. Summary points to 116 writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spk_writer_finish(handle: *mut WriterHandle, summary: *mut u8) -> i32 {
    boundary(|| {
        if handle.is_null() || summary.is_null() {
            return Err(Error::Invalid {
                reason: "null finish argument",
            });
        }
        let state = unsafe { &mut *handle };
        if state.failed {
            return Err(Error::Invalid {
                reason: "writer failed; close it",
            });
        }
        let writer = state.writer.take().ok_or(Error::Invalid {
            reason: "writer already finished",
        })?;
        let (_, result) = writer.finish()?;
        let bytes = unsafe { std::slice::from_raw_parts_mut(summary, 116) };
        bytes[..32].copy_from_slice(&result.shard_id);
        bytes[32..40].copy_from_slice(&result.byte_size.to_le_bytes());
        bytes[40..48].copy_from_slice(&result.record_count.to_le_bytes());
        bytes[48..52].copy_from_slice(&result.block_count.to_le_bytes());
        bytes[52..84].copy_from_slice(&result.content_fingerprint.accumulator_a);
        bytes[84..116].copy_from_slice(&result.content_fingerprint.accumulator_b);
        Ok(0)
    })
}

/// # Safety
/// Handle is null or live, exclusively held, and closed exactly once. Does not finish.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spk_writer_close(handle: *mut WriterHandle) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn last_error() -> String {
        // SAFETY: the ABI returns a live, thread-local NUL-terminated buffer.
        unsafe { std::ffi::CStr::from_ptr(spk_last_error().cast()) }
            .to_str()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn catches_panics_and_keeps_errors_thread_local() {
        assert_eq!(boundary(|| panic!("injected panic")), -1);
        assert_eq!(last_error(), "native panic; close this handle");
        let other = std::thread::spawn(|| {
            assert_eq!(last_error(), "");
            boundary(|| {
                Err(Error::Invalid {
                    reason: "other error",
                })
            });
            last_error()
        });
        assert_eq!(other.join().unwrap(), "other error");
        assert_eq!(last_error(), "native panic; close this handle");
    }

    #[test]
    fn rejects_missing_callbacks_and_clears_output_handles() {
        let callbacks = StreamCallbacks {
            context: std::ptr::null_mut(),
            read: None,
            write: None,
            flush: None,
        };
        let mut reader = std::ptr::dangling_mut::<ReaderHandle>();
        let mut writer = std::ptr::dangling_mut::<WriterHandle>();
        // SAFETY: output pointers are valid, and absent callbacks reject before
        // any stream access. Sentinel handle values are never dereferenced.
        unsafe {
            assert_eq!(
                spk_reader_open(callbacks, std::ptr::null(), 1, 1024, 1024, &mut reader),
                -1
            );
            assert!(reader.is_null());
            assert_eq!(
                spk_writer_open(callbacks, std::ptr::null(), 1024, 0, 1, &mut writer),
                -1
            );
            assert!(writer.is_null());
            spk_reader_close(reader);
            spk_writer_close(writer);
        }
    }

    #[test]
    fn rejects_callback_sizes_larger_than_supplied_buffers() {
        unsafe extern "C" fn invalid(_: *mut c_void, _: *mut u8, size: usize) -> isize {
            (size + 1) as isize
        }
        let mut stream = Stream(StreamCallbacks {
            context: std::ptr::null_mut(),
            read: Some(invalid),
            write: None,
            flush: None,
        });
        assert!(stream.read(&mut [0; 8]).is_err());
    }
}
