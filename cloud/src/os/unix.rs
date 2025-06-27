//! Unix platform support.

use std::fs::File;
use std::os::unix::fs::FileExt;

/// Fills a buffer from the given file at the given offset.
///
/// This command fills the entire buffer unless at EOF.
pub fn fill_buffer(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
    let mut len = 0;
    while len < buffer.len() {
        match file.read_at(&mut buffer[len..], offset + u64::try_from(len).unwrap())? {
            0 => break,
            n => len += n,
        }
    }

    Ok(len)
}
