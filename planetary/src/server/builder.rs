//! Builders for a server.

use std::sync::Arc;

use kube::Client;
use tes::v1::types::responses::ServiceInfo;

use crate::Engine;
use crate::Server;
use crate::name::Alphanumeric;
use crate::server::State;

/// The default host to bind to.
pub const DEFAULT_HOST: &str = "0.0.0.0";

/// The default port to bind to.
pub const DEFAULT_PORT: usize = 6492;

/// An error related to a [`Builder`].
#[derive(Debug)]
pub enum Error {
    /// A required value was missing for a builder field.
    Missing(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Missing(field) => write!(
                f,
                "missing required value for '{field}' in a server builder"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// A [`Result`](std::result::Result) with an [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// A builder for a [`Server`].
#[derive(Default)]
pub struct Builder {
    /// The host to bind to.
    host: Option<String>,

    /// The port to bind to.
    port: Option<usize>,

    /// The Kubernetes client.
    client: Option<Client>,

    /// The service information.
    service_info: Option<ServiceInfo>,
}

impl Builder {
    /// Sets the host for the service.
    ///
    /// # Notes
    ///
    /// This silently overrides any previously set host for the service.
    pub fn host(mut self, value: impl Into<String>) -> Self {
        self.host = Some(value.into());
        self
    }

    /// Sets the port for the service.
    ///
    /// # Notes
    ///
    /// This silently overrides any previously set port for the service.
    pub fn port(mut self, value: impl Into<usize>) -> Self {
        self.port = Some(value.into());
        self
    }

    /// Sets the client for the service.
    ///
    /// # Notes
    ///
    /// This silently overrides any previously set client for the service.
    pub fn client(mut self, value: impl Into<Client>) -> Self {
        self.client = Some(value.into());
        self
    }

    /// Sets the information for the service.
    ///
    /// # Notes
    ///
    /// This silently overrides any previously set information for the service.
    pub fn service_info(mut self, value: impl Into<ServiceInfo>) -> Self {
        self.service_info = Some(value.into());
        self
    }

    /// Consumes `self` and attempts to builde a [`Server`].
    pub fn try_build(self) -> Result<Server> {
        let host = self.host.unwrap_or(String::from(DEFAULT_HOST));
        let port = self.port.unwrap_or(DEFAULT_PORT);
        let client = self.client.ok_or(Error::Missing("kubernetes client"))?;
        let info = self
            .service_info
            .ok_or(Error::Missing("service information"))?;

        Ok(Server {
            host,
            port,
            state: State {
                info: Arc::new(info),
                names: Arc::new(Alphanumeric::default()),
                engine: Arc::new(Engine::new(client)),
            },
        })
    }
}
