//! Support for the Planetary functional tests.
//!
//! The functional tests require `docker`, `kind`, `kubectl`, and `helm` to be
//! installed.
//!
//! The shared [`TestEnvironment`] builds the Planetary container images,
//! creates a `kind` cluster named `planetary-tests`, and installs the
//! Planetary chart into the cluster; it is initialized once for the entire
//! test process.
//!
//! The following environment variables alter the behavior of the tests:
//!
//! * `PLANETARY_TESTS_SKIP_BUILD` — when set to `1`, the container images are
//!   not rebuilt; the images from a previous run are used instead.
//! * `PLANETARY_TESTS_FRESH` — when set to `1`, any existing `planetary-tests`
//!   cluster is deleted and recreated.
//!
//! The cluster is intentionally left running when the tests complete so that
//! subsequent runs are fast; delete it with `kind delete cluster --name
//! planetary-tests`.

use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use anyhow::bail;
use serde_json::Value;

/// The name of the `kind` cluster used by the tests.
const CLUSTER_NAME: &str = "planetary-tests";

/// The name of the `kubectl` context for the test cluster.
const KUBE_CONTEXT: &str = "kind-planetary-tests";

/// The name of the Docker container running the cluster's only node.
const NODE_CONTAINER: &str = "planetary-tests-control-plane";

/// The namespace the chart is installed into.
const NAMESPACE: &str = "planetary";

/// The name of the Helm release.
const RELEASE: &str = "planetary";

/// The password used for the PostgreSQL database in the test cluster.
const POSTGRES_PASSWORD: &str = "planetary-tests-password";

/// The tag used for the locally built container images.
const IMAGE_TAG: &str = "test";

/// The Dockerfile targets built by the tests.
///
/// Each target `<name>` is tagged as `stjude-rust-labs/<name>:test`, matching
/// the image names in `tests/values.yaml`.
const IMAGE_TARGETS: &[&str] = &[
    "planetary-api",
    "planetary-orchestrator",
    "planetary-monitor",
    "planetary-transporter",
    "planetary-migration",
];

/// The path on the cluster node backing the orchestrator's shared storage.
const ORCHESTRATOR_STORAGE_PATH: &str = "/mnt/planetary/orchestrator";

/// The path on the cluster node backing the per-user "local" storage.
const LOCAL_STORAGE_PATH: &str = "/mnt/planetary/local";

/// The user and group ids that task pods run as; the host path directories
/// must be owned by this user for task pods to be able to write to them.
const TASK_USER: &str = "1000:1000";

/// The interval between task state polls.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// The maximum time to wait for a task to reach an expected state.
const POLL_TIMEOUT: Duration = Duration::from_secs(300);

/// The task states that are terminal.
const TERMINAL_STATES: &[&str] = &[
    "COMPLETE",
    "EXECUTOR_ERROR",
    "SYSTEM_ERROR",
    "CANCELED",
    "PREEMPTED",
];

/// Runs a command to completion with its output streamed to the test's
/// stderr.
///
/// Returns an error if the command cannot be spawned or exits with a non-zero
/// status.
fn run(command: &mut Command) -> Result<()> {
    eprintln!(
        "running `{program} {args}`",
        program = command.get_program().display(),
        args = command
            .get_args()
            .map(|a| a.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
    );

    let status = command
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| {
            format!(
                "failed to spawn `{program}`",
                program = command.get_program().display()
            )
        })?;

    if !status.success() {
        bail!(
            "`{program}` failed with {status}",
            program = command.get_program().display()
        );
    }

    Ok(())
}

/// Runs a command to completion with its output captured, returning the
/// command's stdout.
///
/// Returns an error containing the command's stderr if the command cannot be
/// spawned or exits with a non-zero status.
fn run_with_output(command: &mut Command) -> Result<String> {
    let output = command.output().with_context(|| {
        format!(
            "failed to spawn `{program}`",
            program = command.get_program().display()
        )
    })?;

    if !output.status.success() {
        bail!(
            "`{program}` failed with {status}:\n{stderr}",
            program = command.get_program().display(),
            status = output.status,
            stderr = String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8(output.stdout).with_context(|| {
        format!(
            "`{program}` output is not UTF-8",
            program = command.get_program().display()
        )
    })
}

/// Returns whether the given environment variable is set to `1`.
fn env_enabled(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|v| v == "1")
}

/// Generates a username unique to this test process.
///
/// As the test cluster's database may persist between runs, tests use unique
/// usernames to isolate themselves from the tasks of previous runs.
pub fn unique_username(prefix: &str) -> String {
    /// Differentiates usernames generated in the same instant.
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after the epoch")
        .as_nanos();

    format!(
        "{prefix}-{nanos:x}-{counter}",
        counter = COUNTER.fetch_add(1, Ordering::SeqCst)
    )
}

/// Removes and returns the value at the given JSON pointer.
///
/// This is used to extract dynamic fields (e.g. timestamps and logs) from a
/// response before comparing the remainder of the response against an
/// expected value.
///
/// Note that JSON pointer escape sequences (`~0` and `~1`) are not supported.
///
/// # Panics
///
/// Panics if there is no value at the given pointer.
pub fn take(value: &mut Value, pointer: &str) -> Value {
    /// Removes the value at `pointer` from `value`, returning `None` if there
    /// is no such value.
    fn imp(value: &mut Value, pointer: &str) -> Option<Value> {
        let (parent, token) = pointer.rsplit_once('/')?;
        match value.pointer_mut(parent)? {
            Value::Object(map) => map.remove(token),
            Value::Array(values) => {
                let index: usize = token.parse().ok()?;
                (index < values.len()).then(|| values.remove(index))
            }
            _ => None,
        }
    }

    imp(value, pointer).unwrap_or_else(|| panic!("no value at pointer `{pointer}` in {value:#}"))
}

/// The shared environment for the functional tests.
///
/// Use [`TestEnvironment::get`] to access the environment; the first access
/// performs the (slow) setup of the test cluster.
pub struct TestEnvironment {
    /// The path to the root of the repository.
    root: PathBuf,
}

impl TestEnvironment {
    /// Gets the shared test environment.
    ///
    /// The environment is initialized on first access; initialization builds
    /// the container images, creates the `kind` cluster, and installs the
    /// Planetary chart.
    ///
    /// # Panics
    ///
    /// Panics if initialization of the environment fails.
    pub fn get() -> &'static Self {
        /// The shared test environment; the result of a failed initialization
        /// is cached so that it is not repeated for every test.
        static ENV: OnceLock<Result<TestEnvironment, Error>> = OnceLock::new();

        match ENV.get_or_init(TestEnvironment::new) {
            Ok(env) => env,
            Err(e) => panic!("failed to initialize the test environment: {e:#}"),
        }
    }

    /// Creates a new test environment.
    fn new() -> Result<Self> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crate should have a parent directory")
            .to_path_buf();

        let env = Self { root };
        env.check_prerequisites()?;

        if env_enabled("PLANETARY_TESTS_SKIP_BUILD") {
            eprintln!("skipping image builds: `PLANETARY_TESTS_SKIP_BUILD` is set");
        } else {
            env.build_images()?;
        }

        let reused = env.create_cluster()?;
        env.create_storage_directories()?;
        env.load_images()?;
        env.install_chart()?;

        if reused {
            env.restart_services()?;
        }

        Ok(env)
    }

    /// Checks that the tools required by the tests are available.
    fn check_prerequisites(&self) -> Result<()> {
        for (program, args) in [
            ("docker", &["info"] as &[&str]),
            ("kind", &["version"]),
            ("kubectl", &["version", "--client"]),
            ("helm", &["version"]),
        ] {
            run_with_output(Command::new(program).args(args)).with_context(|| {
                format!("`{program}` is required to run the functional tests (is it installed?)")
            })?;
        }

        Ok(())
    }

    /// Builds the Planetary container images.
    fn build_images(&self) -> Result<()> {
        for target in IMAGE_TARGETS {
            run(Command::new("docker")
                .args([
                    "build",
                    "--target",
                    target,
                    "-t",
                    &format!("stjude-rust-labs/{target}:{IMAGE_TAG}"),
                ])
                .arg(&self.root))
            .with_context(|| format!("failed to build the `{target}` image"))?;
        }

        Ok(())
    }

    /// Creates the `kind` cluster if it does not already exist.
    ///
    /// If the `PLANETARY_TESTS_FRESH` environment variable is set to `1`, any
    /// existing cluster is deleted first.
    ///
    /// Returns `true` if an existing cluster was reused.
    fn create_cluster(&self) -> Result<bool> {
        let clusters = run_with_output(Command::new("kind").args(["get", "clusters"]))
            .context("failed to list the kind clusters")?;
        let mut exists = clusters.lines().any(|line| line == CLUSTER_NAME);

        if exists && env_enabled("PLANETARY_TESTS_FRESH") {
            eprintln!("deleting existing cluster: `PLANETARY_TESTS_FRESH` is set");
            run(Command::new("kind").args(["delete", "cluster", "--name", CLUSTER_NAME]))
                .context("failed to delete the kind cluster")?;
            exists = false;
        }

        if !exists {
            run(Command::new("kind")
                .args(["create", "cluster", "--config"])
                .arg(self.root.join("tests/kind-config.yml"))
                .args(["--wait", "5m"]))
            .context("failed to create the kind cluster")?;
        }

        Ok(exists)
    }

    /// Creates the directories on the cluster node that back the chart's
    /// host path volumes.
    ///
    /// The directories are owned by the task user, as Kubernetes does not
    /// apply a pod's `fsGroup` to host path volumes.
    fn create_storage_directories(&self) -> Result<()> {
        run_with_output(Command::new("docker").args([
            "exec",
            NODE_CONTAINER,
            "sh",
            "-c",
            &format!(
                "mkdir -p {ORCHESTRATOR_STORAGE_PATH} {LOCAL_STORAGE_PATH} && chown {TASK_USER} \
                 {ORCHESTRATOR_STORAGE_PATH} {LOCAL_STORAGE_PATH}"
            ),
        ]))
        .context("failed to create the storage directories on the cluster node")?;

        Ok(())
    }

    /// Loads the Planetary container images into the cluster.
    fn load_images(&self) -> Result<()> {
        run(Command::new("kind")
            .args(["load", "docker-image", "--name", CLUSTER_NAME])
            .args(
                IMAGE_TARGETS
                    .iter()
                    .map(|target| format!("stjude-rust-labs/{target}:{IMAGE_TAG}")),
            ))
        .context("failed to load the images into the kind cluster")
    }

    /// Installs (or upgrades) the Planetary chart in the cluster.
    fn install_chart(&self) -> Result<()> {
        run(Command::new("helm")
            .args(["upgrade", "--install", RELEASE])
            .arg(self.root.join("chart"))
            .args(["--kube-context", KUBE_CONTEXT])
            .args(["-n", NAMESPACE, "--create-namespace"])
            .arg("-f")
            .arg(self.root.join("tests/values.yaml"))
            .args(["--set", &format!("postgresql.password={POSTGRES_PASSWORD}")])
            .args(["--wait", "--wait-for-jobs", "--timeout", "15m"]))
        .context("failed to install the Planetary chart")
    }

    /// Restarts the Planetary services so that they run the freshly loaded
    /// images.
    ///
    /// This is required when the cluster is reused: loading an image into the
    /// cluster does not restart the pods that were running a previous image
    /// with the same tag.
    ///
    /// The pods are deleted (rather than performing a rolling restart) as the
    /// services require pod anti-affinity: on the single-node test cluster, a
    /// rolling restart would deadlock waiting for a surged pod that can never
    /// be scheduled.
    fn restart_services(&self) -> Result<()> {
        /// The deployments to restart.
        const DEPLOYMENTS: &[&str] = &[
            "deployment/planetary-api",
            "deployment/planetary-orchestrator",
            "deployment/planetary-monitor",
        ];

        run(Command::new("kubectl").args([
            "--context",
            KUBE_CONTEXT,
            "-n",
            NAMESPACE,
            "delete",
            "pods",
            "-l",
            "app.kubernetes.io/component in (api, orchestrator, monitor)",
        ]))
        .context("failed to restart the Planetary services")?;

        for deployment in DEPLOYMENTS {
            run(Command::new("kubectl").args([
                "--context",
                KUBE_CONTEXT,
                "-n",
                NAMESPACE,
                "rollout",
                "status",
                deployment,
                "--timeout",
                "5m",
            ]))
            .with_context(|| format!("failed to wait for the restart of `{deployment}`"))?;
        }

        Ok(())
    }

    /// Creates a client for the TES API that authenticates with the
    /// `X-Forwarded-User` header.
    ///
    /// If a username is provided, the user's local storage directory is
    /// created on the cluster node; every task pod mounts the local storage
    /// host path, so the directory must exist before any task is created for
    /// the user.
    ///
    /// # Panics
    ///
    /// Panics if the client cannot be created.
    pub fn client(&self, username: Option<&str>) -> Client {
        self.new_client(username, |username| {
            Auth::ForwardedUser(username.to_string())
        })
    }

    /// Creates a client for the TES API that authenticates with a basic
    /// `Authorization` header.
    ///
    /// # Panics
    ///
    /// Panics if the client cannot be created.
    pub fn basic_auth_client(&self, username: &str) -> Client {
        self.new_client(Some(username), |username| Auth::Basic(username.to_string()))
    }

    /// Creates a client with the authentication produced by the given
    /// function.
    fn new_client(&self, username: Option<&str>, auth: impl Fn(&str) -> Auth) -> Client {
        let auth = match username {
            Some(username) => {
                self.create_local_storage_directory(username)
                    .unwrap_or_else(|e| panic!("{e:#}"));
                auth(username)
            }
            None => Auth::None,
        };

        let forward = PortForward::new().unwrap_or_else(|e| panic!("{e:#}"));

        Client {
            url: format!("http://127.0.0.1:{port}", port = forward.port),
            _forward: forward,
            http: reqwest::blocking::Client::new(),
            auth,
        }
    }

    /// Creates the local storage directory for the given user on the cluster
    /// node.
    fn create_local_storage_directory(&self, username: &str) -> Result<()> {
        let dir = format!("{LOCAL_STORAGE_PATH}/{username}");
        run_with_output(Command::new("docker").args([
            "exec",
            NODE_CONTAINER,
            "sh",
            "-c",
            &format!("mkdir -p {dir} && chown {TASK_USER} {dir}"),
        ]))
        .with_context(|| {
            format!("failed to create the local storage directory for `{username}`")
        })?;

        Ok(())
    }

    /// Writes a file to the given user's local storage.
    ///
    /// The path is relative to the root of the user's local storage (i.e. a
    /// task input URL of `file:///foo/bar.txt` corresponds to a path of
    /// `foo/bar.txt`).
    ///
    /// # Panics
    ///
    /// Panics if the file cannot be written.
    pub fn write_local_file(&self, username: &str, path: &str, content: &str) {
        /// Writes the file via the node container.
        fn imp(username: &str, path: &str, content: &str) -> Result<()> {
            let path = format!("{LOCAL_STORAGE_PATH}/{username}/{path}");
            let dir = path
                .rsplit_once('/')
                .expect("path should contain a separator")
                .0
                .to_string();

            let mut child = Command::new("docker")
                .args([
                    "exec",
                    "-i",
                    NODE_CONTAINER,
                    "sh",
                    "-c",
                    &format!("mkdir -p {dir} && cat > {path} && chown -R {TASK_USER} {dir} {path}"),
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .context("failed to spawn `docker`")?;

            child
                .stdin
                .take()
                .expect("stdin should be piped")
                .write_all(content.as_bytes())
                .context("failed to write the file content")?;

            let output = child
                .wait_with_output()
                .context("failed to wait for `docker`")?;
            if !output.status.success() {
                bail!(
                    "`docker` failed with {status}:\n{stderr}",
                    status = output.status,
                    stderr = String::from_utf8_lossy(&output.stderr)
                );
            }

            Ok(())
        }

        imp(username, path, content).unwrap_or_else(|e| {
            panic!("failed to write local file `{path}` for user `{username}`: {e:#}")
        })
    }

    /// Reads a file from the given user's local storage.
    ///
    /// The path is relative to the root of the user's local storage (i.e. a
    /// task output URL of `file:///foo/bar.txt` corresponds to a path of
    /// `foo/bar.txt`).
    ///
    /// # Panics
    ///
    /// Panics if the file cannot be read.
    pub fn read_local_file(&self, username: &str, path: &str) -> String {
        run_with_output(Command::new("docker").args([
            "exec",
            NODE_CONTAINER,
            "cat",
            &format!("{LOCAL_STORAGE_PATH}/{username}/{path}"),
        ]))
        .unwrap_or_else(|e| {
            panic!("failed to read local file `{path}` for user `{username}`: {e:#}")
        })
    }
}

/// The authentication used by a [`Client`].
enum Auth {
    /// No authentication is sent with requests.
    None,
    /// The username is sent in the `X-Forwarded-User` header.
    ForwardedUser(String),
    /// The username is sent in a basic `Authorization` header (with no
    /// password).
    Basic(String),
}

/// A guard for a `kubectl port-forward` to the Planetary API service.
///
/// The port forward is terminated when the guard is dropped.
struct PortForward {
    /// The `kubectl port-forward` child process.
    child: Child,
    /// The local port that forwards to the API service.
    port: u16,
}

impl PortForward {
    /// Starts a new port forward to the Planetary API service.
    fn new() -> Result<Self> {
        /// The prefix of the line that `kubectl port-forward` prints once the
        /// forward is established.
        const FORWARDING_PREFIX: &str = "Forwarding from 127.0.0.1:";

        let mut child = Command::new("kubectl")
            .args([
                "--context",
                KUBE_CONTEXT,
                "-n",
                NAMESPACE,
                "port-forward",
                "service/planetary-api",
                ":8080",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .context("failed to spawn `kubectl port-forward`")?;

        let mut reader = BufReader::new(child.stdout.take().expect("stdout should be piped"));

        // Read lines until the local port is printed
        let port = loop {
            let mut line = String::new();
            if reader
                .read_line(&mut line)
                .context("failed to read the output of `kubectl port-forward`")?
                == 0
            {
                let _ = child.kill();
                let _ = child.wait();
                bail!("`kubectl port-forward` exited unexpectedly");
            }

            // The line has the form `Forwarding from 127.0.0.1:<port> -> 8080`
            if let Some(rest) = line.strip_prefix(FORWARDING_PREFIX) {
                break rest
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .parse()
                    .context("failed to parse the local port of `kubectl port-forward`")?;
            }
        };

        // Drain the remainder of the output so that the child process never
        // blocks writing to a full pipe
        thread::spawn(move || {
            let _ = std::io::copy(&mut reader, &mut std::io::sink());
        });

        Ok(Self { child, port })
    }
}

impl Drop for PortForward {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A blocking client for the TES API of the test environment.
///
/// Each client maintains its own port forward to the API service; the forward
/// is terminated when the client is dropped.
pub struct Client {
    /// The port forward to the API service.
    _forward: PortForward,
    /// The base URL of the API service.
    url: String,
    /// The underlying HTTP client.
    http: reqwest::blocking::Client,
    /// The authentication to send with requests.
    auth: Auth,
}

impl Client {
    /// Sends a `GET` request to the given path.
    ///
    /// Returns the response's status code and JSON body.
    ///
    /// # Panics
    ///
    /// Panics if the request cannot be sent or if the response is not JSON.
    pub fn get(&self, path: &str) -> (u16, Value) {
        self.send(self.http.get(format!("{url}{path}", url = self.url)))
    }

    /// Sends a `POST` request with the given JSON body to the given path.
    ///
    /// Returns the response's status code and JSON body.
    ///
    /// # Panics
    ///
    /// Panics if the request cannot be sent or if the response is not JSON.
    pub fn post(&self, path: &str, body: &Value) -> (u16, Value) {
        self.send(
            self.http
                .post(format!("{url}{path}", url = self.url))
                .json(body),
        )
    }

    /// Creates a task from the given request.
    ///
    /// Asserts that the task is successfully created and returns the new
    /// task's identifier.
    ///
    /// # Panics
    ///
    /// Panics if the task is not created or if the response is malformed.
    pub fn create_task(&self, task: &Value) -> String {
        let (status, mut body) = self.post("/v1/tasks", task);
        assert_eq!(status, 200, "unexpected response: {body:#}");

        let id = take(&mut body, "/id");
        assert_eq!(body, serde_json::json!({}), "unexpected response fields");

        let id = id.as_str().expect("task id should be a string");
        assert_eq!(id.len(), 20, "unexpected task id length for id `{id}`");
        assert!(
            id.chars().all(|c| c.is_ascii_alphanumeric()),
            "task id `{id}` is not alphanumeric"
        );

        id.to_string()
    }

    /// Cancels the task with the given identifier.
    ///
    /// Returns the response's status code and JSON body.
    ///
    /// # Panics
    ///
    /// Panics if the request cannot be sent or if the response is not JSON.
    pub fn cancel_task(&self, id: &str) -> (u16, Value) {
        self.post(
            &format!("/v1/tasks/{id}:cancel"),
            &Value::Object(Default::default()),
        )
    }

    /// Gets the task with the given identifier using the given view.
    ///
    /// Returns the response's status code and JSON body.
    ///
    /// # Panics
    ///
    /// Panics if the request cannot be sent or if the response is not JSON.
    pub fn get_task(&self, id: &str, view: &str) -> (u16, Value) {
        self.get(&format!("/v1/tasks/{id}?view={view}"))
    }

    /// Waits for the task with the given identifier to reach the expected
    /// state.
    ///
    /// # Panics
    ///
    /// Panics if the task reaches a different terminal state or if the task
    /// does not reach the expected state before a timeout; the panic message
    /// includes the task's `FULL` view to aid in debugging.
    pub fn wait_for_task_state(&self, id: &str, expected: &str) {
        let start = Instant::now();

        loop {
            let (status, body) = self.get_task(id, "MINIMAL");
            assert_eq!(status, 200, "unexpected response: {body:#}");

            let state = body["state"].as_str().expect("state should be a string");
            if state == expected {
                return;
            }

            if TERMINAL_STATES.contains(&state) {
                panic!(
                    "task `{id}` reached terminal state `{state}` instead of \
                     `{expected}`:\n{full:#}",
                    full = self.get_task(id, "FULL").1
                );
            }

            if start.elapsed() > POLL_TIMEOUT {
                panic!(
                    "timed out waiting for task `{id}` to reach state `{expected}` (currently \
                     `{state}`):\n{full:#}",
                    full = self.get_task(id, "FULL").1
                );
            }

            thread::sleep(POLL_INTERVAL);
        }
    }

    /// Sends the given request with the client's authentication.
    ///
    /// Returns the response's status code and JSON body.
    fn send(&self, request: reqwest::blocking::RequestBuilder) -> (u16, Value) {
        let request = match &self.auth {
            Auth::None => request,
            Auth::ForwardedUser(username) => request.header("X-Forwarded-User", username),
            Auth::Basic(username) => request.basic_auth(username, None::<&str>),
        };

        let response = request.send().expect("failed to send the request");
        let status = response.status().as_u16();
        let body = response
            .json()
            .unwrap_or_else(|e| panic!("response body is not JSON: {e}"));

        (status, body)
    }
}
