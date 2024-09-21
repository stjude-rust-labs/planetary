//! A Kubernetes-based task executor for the Task Execution Service (TES)
//! specification.

pub mod engine;
pub mod name;
pub mod server;
pub mod task;

pub use engine::Engine;
pub use server::Server;
pub use task::TaskMap;
