//! Tasks.

use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;

use tes::v1::types::Task as TesTask;
use tes::v1::types::task::State as TesState;
use twox_hash::RandomXxh3HashBuilder64;

/// The default capacity of maps and sets.
pub const DEFAULT_CAPACITY: usize = 0xFFFF;

/// The inner type of maps and sets.
type Inner<K, V> = HashMap<K, V, RandomXxh3HashBuilder64>;

/// An underlying map type.
struct Map(Inner<String, TesTask>);

impl Deref for Map {
    type Target = Inner<String, TesTask>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Map {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for Map {
    fn default() -> Self {
        Self(HashMap::with_capacity_and_hasher(
            DEFAULT_CAPACITY,
            RandomXxh3HashBuilder64::default(),
        ))
    }
}

/// A map of task names to TES tasks.
pub type TaskMap = Inner<String, TesTask>;

/// A map of task names to TES states.
pub type StateMap = Inner<String, TesState>;

/// A map of task names to job outputs.
// TODO(clay): for now, this just holds unit structs.
pub type ResultsMap = Inner<String, ()>;
