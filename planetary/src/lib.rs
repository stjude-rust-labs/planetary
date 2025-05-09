//! A Kubernetes-based task executor for the Task Execution Service (TES)
//! specification.

pub mod server;
pub mod services;

pub use server::Server;

/// Formats a log message by including a time stamp.
#[macro_export]
macro_rules! format_log_message {
    ($($arg:tt)*) => { format!("[{ts}] {args}", ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ"), args = format_args!($($arg)*)) }
}
