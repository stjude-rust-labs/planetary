//! OS-specific implementation.

#[cfg(unix)]
mod unix;

#[cfg(unix)]
pub use unix::*;
