//! Unix platform support.

use std::fs::File;
use std::io::Result;
use std::os::unix::fs::FileExt;

/// Fills a buffer from the given file at the given offset.
///
/// This command fills the entire buffer unless at EOF.
pub fn fill_buffer(file: &File, buffer: &mut [u8], offset: u64) -> Result<usize> {
    let mut len = 0;
    while len < buffer.len() {
        match file.read_at(&mut buffer[len..], offset + u64::try_from(len).unwrap())? {
            0 => break,
            n => len += n,
        }
    }

    Ok(len)
}

/// Helper for writing to a file at a given offset.
///
/// This is simply a wrapper around the `write_at` unix extension method.
pub fn write_at(file: &File, buffer: &[u8], offset: u64) -> Result<usize> {
    file.write_at(buffer, offset)
}
