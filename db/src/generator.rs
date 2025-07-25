//! Utility for random alphanumeric string generation.

use std::fmt;

use rand::Rng as _;
use rand::rngs::ThreadRng;

/// An alphanumeric string generator.
///
/// Every display of the generator will create a new random alphanumeric string.
#[derive(Clone, Copy)]
pub struct Alphanumeric {
    /// The length of the alphanumeric string.
    length: usize,
}

impl Alphanumeric {
    /// Creates a new alphanumeric string generator with the given string
    /// length.
    pub fn new(length: usize) -> Self {
        Self { length }
    }
}

impl Default for Alphanumeric {
    fn default() -> Self {
        Self { length: 12 }
    }
}

impl fmt::Display for Alphanumeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for c in ThreadRng::default()
            .sample_iter(&rand::distr::Alphanumeric)
            .take(self.length)
            .map(|c| c.to_ascii_lowercase())
        {
            write!(f, "{c}", c = c as char)?;
        }

        Ok(())
    }
}
