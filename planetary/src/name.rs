//! Name generation services.

use rand::Rng as _;
use rand::rngs::ThreadRng;

/// A name generator.
pub trait Generator {
    /// Generates a new name.
    fn generate(&self) -> String;
}

/// An alphanumeric name generator.
pub struct Alphanumeric {
    /// The length of the randomized portion of the name.
    length: usize,
}

impl Default for Alphanumeric {
    fn default() -> Self {
        Self { length: 12 }
    }
}

impl Generator for Alphanumeric {
    fn generate(&self) -> String {
        let mut rng = ThreadRng::default();

        let random: String = (&mut rng)
            .sample_iter(&rand::distr::Alphanumeric)
            .take(self.length)
            .map(char::from)
            .map(|c| c.to_ascii_lowercase())
            .collect();

        format!("job-{}", random)
    }
}
